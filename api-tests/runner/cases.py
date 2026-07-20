from __future__ import annotations

import copy
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .catalog import (
    ExampleResolutionError,
    Operation,
    operation_example,
)


class CaseError(ValueError):
    pass


@dataclass
class TestCase:
    operation: Operation
    case_id: str
    profile: str
    source: str
    request: dict[str, Any]
    expected_statuses: tuple[int, ...]
    checks: set[str]
    quality: dict[str, Any]
    semantic: list[dict[str, Any]]
    validate_request: bool = True
    request_validation_reason: str = ""
    allow_mutation: bool = False
    blocked: bool = False
    blocked_reason: str = ""


def load_manifest(path: Path) -> dict[str, Any]:
    value = yaml.safe_load(path.read_text())
    if not isinstance(value, dict):
        raise CaseError(f"Expected a mapping in {path}")
    return value


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(base)
    for key, value in override.items():
        if isinstance(result.get(key), dict) and isinstance(value, dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = copy.deepcopy(value)
    return result


class FixtureStore:
    def __init__(self, data: dict[str, Any], network: str) -> None:
        self.data = data
        self.network = network

    def get(self, name: str) -> Any:
        for section in self.data.values():
            if not isinstance(section, dict):
                continue
            value = section.get(name)
            if isinstance(value, dict) and self.network in value:
                return copy.deepcopy(value[self.network])
        raise CaseError(f"No fixture named {name!r} for network {self.network!r}")

    def resolve(self, value: Any) -> Any:
        if isinstance(value, str) and value.startswith("fixture:"):
            return self.get(value.split(":", 1)[1])
        if isinstance(value, dict):
            return {key: self.resolve(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self.resolve(item) for item in value]
        return value


class CaseResolver:
    def __init__(
        self,
        manifest: dict[str, Any],
        operations: list[Operation],
        document: dict[str, Any],
        fixtures: FixtureStore,
    ) -> None:
        self.manifest = manifest
        self.operations = {operation.operation_id: operation for operation in operations}
        self.document = document
        self.fixtures = fixtures
        self.defaults = manifest.get("defaults", {})
        self.operation_configs = manifest.get("operations", {})
        if not isinstance(self.operation_configs, dict):
            raise CaseError("Manifest operations must be a mapping")
        unknown = sorted(set(self.operation_configs) - set(self.operations))
        if unknown:
            raise CaseError(f"Manifest has unknown operations: {', '.join(unknown)}")
        baseline = self.defaults.get("baseline")
        if not isinstance(baseline, dict):
            raise CaseError("Manifest defaults.baseline must be a mapping")
        self.default_baseline = baseline
        self.global_bindings = self.fixtures.resolve(manifest.get("parameter_bindings", {}))

    def cases_for_profile(self, profile: str) -> list[TestCase]:
        if profile not in {"baseline", "deep"}:
            raise CaseError(f"Unsupported profile: {profile}")
        cases: list[TestCase] = []
        for operation in self.operations.values():
            config = self.operation_configs.get(operation.operation_id, {})
            if not isinstance(config, dict):
                raise CaseError(f"Configuration for {operation.operation_id} must be a mapping")
            baseline_config = deep_merge(self.default_baseline, config.get("baseline", {}))
            cases.append(self._make_case(operation, "baseline", profile, baseline_config))
            if profile == "deep":
                deep_configs = []
                if operation.operation_id not in {"submittx", "ogmios"}:
                    default_deep = self.defaults.get("deep_defaults", [])
                    if isinstance(default_deep, dict):
                        default_deep = [default_deep]
                    if not isinstance(default_deep, list):
                        raise CaseError("Manifest defaults.deep_defaults must be a list")
                    deep_configs.extend(default_deep)
                configured_deep = config.get("deep", [])
                if isinstance(configured_deep, dict):
                    configured_deep = [configured_deep]
                if not isinstance(configured_deep, list):
                    raise CaseError(f"Deep cases for {operation.operation_id} must be a list")
                deep_configs.extend(configured_deep)
                if not isinstance(deep_configs, list):
                    raise CaseError(f"Deep cases for {operation.operation_id} must be a list")
                for index, deep_config in enumerate(deep_configs):
                    if not isinstance(deep_config, dict):
                        raise CaseError(f"Deep case {operation.operation_id}[{index}] must be a mapping")
                    case_id = str(deep_config.get("id", f"deep-{index + 1}"))
                    cases.append(
                        self._make_case(
                            operation,
                            case_id,
                            profile,
                            deep_merge(self.default_baseline, deep_config),
                        )
                    )
        return cases

    def _make_case(
        self,
        operation: Operation,
        case_id: str,
        profile: str,
        config: dict[str, Any],
    ) -> TestCase:
        if config.get("blocked"):
            return TestCase(
                operation=operation,
                case_id=case_id,
                profile=profile,
                source=str(config.get("source", "blocked")),
                request={},
                expected_statuses=(),
                checks=set(),
                quality={"mode": "not_applicable"},
                semantic=[],
                blocked=True,
                blocked_reason=str(config.get("reason", "No reason supplied")),
            )
        source = str(config.get("source", self.defaults.get("request_source", "openapi")))
        named_example = config.get("example") if source == "named_openapi_example" else None
        bindings = deep_merge(self.global_bindings, self.fixtures.resolve(config.get("bindings", {})))
        try:
            if source in {"openapi", "fixture", "named_openapi_example", "derived"}:
                request = operation_example(
                    operation,
                    self.document,
                    parameter_bindings=bindings,
                    named_example=named_example,
                )
            elif source == "explicit":
                request = {"query": {}, "headers": {}, "path": {}}
            else:
                raise CaseError(f"Unknown case source {source} for {operation.operation_id}")
        except ExampleResolutionError as exc:
            raise CaseError(f"{operation.operation_id} {case_id}: {exc}") from exc
        request = deep_merge(request, self.fixtures.resolve(config.get("request", {})))
        expected = config.get("expected_status", operation.success_statuses)
        if isinstance(expected, int):
            expected_statuses = (expected,)
        elif isinstance(expected, list) and all(isinstance(value, int) for value in expected):
            expected_statuses = tuple(expected)
        elif isinstance(expected, tuple):
            expected_statuses = expected
        else:
            raise CaseError(f"Invalid expected_status for {operation.operation_id} {case_id}")
        checks = config.get("checks", self.defaults.get("checks", []))
        if not isinstance(checks, list):
            raise CaseError(f"Checks for {operation.operation_id} {case_id} must be a list")
        quality = config.get("quality", self.defaults.get("quality", {"mode": "non_empty"}))
        if not isinstance(quality, dict):
            raise CaseError(f"Quality for {operation.operation_id} {case_id} must be a mapping")
        semantic = config.get("semantic", self.defaults.get("semantic", []))
        if not isinstance(semantic, list):
            raise CaseError(f"Semantic assertions for {operation.operation_id} {case_id} must be a list")
        validate_request = bool(config.get("validate_request", True))
        validation_reason = str(config.get("request_validation_reason", ""))
        if not validate_request and not validation_reason:
            raise CaseError(
                f"Disabled request validation for {operation.operation_id} {case_id} requires a reason"
            )
        return TestCase(
            operation=operation,
            case_id=case_id,
            profile=profile,
            source=source,
            request=request,
            expected_statuses=expected_statuses,
            checks=set(str(check) for check in checks),
            quality=quality,
            semantic=semantic,
            validate_request=validate_request,
            request_validation_reason=validation_reason,
            allow_mutation=bool(config.get("allow_mutation", False)),
        )


def load_fixture_store(path: Path, network: str) -> FixtureStore:
    value = yaml.safe_load(path.read_text())
    if not isinstance(value, dict):
        raise CaseError(f"Expected a mapping in {path}")
    return FixtureStore(value, network)
