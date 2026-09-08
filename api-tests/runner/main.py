from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

from .cases import CaseError, CaseResolver, TestCase, load_fixture_store, load_manifest
from .catalog import (
    CatalogError,
    MISSING,
    Operation,
    _example_value,
    build_catalog,
    load_yaml,
    response_media_types,
    response_schema,
)
from .contract import ContractValidator, validate_document
from .ledger import Ledger
from .quality import CheckResult, evaluate_response
from .report import write_reports
from .transport import HttpTransport, TransportError


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run deterministic Koios Preview API tests")
    parser.add_argument("--spec", type=Path, required=True)
    parser.add_argument("--cases", type=Path, required=True)
    parser.add_argument("--fixtures", type=Path, required=True)
    parser.add_argument("--base-url")
    parser.add_argument("--profile", choices=("baseline", "deep"), default="baseline")
    parser.add_argument("--reference-commit", required=True)
    parser.add_argument("--report-dir", type=Path, default=Path("api-test-report"))
    parser.add_argument("--token-env", default="KOIOS_API_TOKEN")
    parser.add_argument("--plan-only", action="store_true")
    return parser.parse_args(argv)


def validate_generated_specs(spec_path: Path) -> None:
    generator = (spec_path.parent.parent / "createspecs.py").resolve()
    if not generator.exists():
        return
    process = subprocess.run(
        [sys.executable, str(generator), "--check"],
        cwd=generator.parent,
        capture_output=True,
        text=True,
        check=False,
    )
    if process.returncode:
        message = (process.stderr or process.stdout).strip()
        raise CatalogError(f"Generated specification check failed: {message}")


def static_findings(
    spec_path: Path,
    document: dict[str, Any],
    operations: list[Operation],
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    if "##_" in spec_path.read_text():
        raise CatalogError("The generated spec contains unreplaced fixture placeholders")
    for operation in operations:
        if not operation.success_statuses:
            raise CatalogError(f"{operation.label} has no declared 2xx response")
        for status in operation.success_statuses:
            if response_schema(operation, document, status) is None:
                raise CatalogError(f"{operation.label} response {status} has no schema")
        for parameter in operation.parameters:
            if _example_value(parameter, document) is MISSING:
                findings.append(
                    {
                        "category": "missing_parameter_example",
                        "operation_id": operation.operation_id,
                        "message": f"{operation.label} parameter {parameter.get('name')} has no OpenAPI example",
                    }
                )
    return findings


def prepare_context(args: argparse.Namespace) -> tuple[
    dict[str, Any],
    dict[str, Any],
    list[Operation],
    list[TestCase],
    ContractValidator,
    HttpTransport,
    list[dict[str, Any]],
]:
    validate_generated_specs(args.spec)
    validate_document(args.spec)
    document = load_yaml(args.spec)
    operations = build_catalog(document)
    manifest = load_manifest(args.cases)
    target = manifest.get("target", {})
    network = str(target.get("network", "preview"))
    base_url = args.base_url or target.get("base_url")
    if not base_url:
        raise CaseError("A base URL is required")
    fixtures = load_fixture_store(args.fixtures, network)
    resolver = CaseResolver(manifest, operations, document, fixtures)
    cases = resolver.cases_for_profile(args.profile)
    defaults = manifest.get("defaults", {})
    transport = HttpTransport(
        base_url=str(base_url),
        token=os.getenv(args.token_env),
        timeout=float(defaults.get("timeout_seconds", 60)),
        retries=int(defaults.get("retries", 2)),
        rate_limit=str(defaults.get("rate_limit", "30/m")),
    )
    contract = ContractValidator(args.spec)
    findings = static_findings(args.spec, document, operations)
    expected_baselines = {operation.operation_id for operation in operations}
    planned_baselines = {case.operation.operation_id for case in cases if case.case_id == "baseline"}
    if expected_baselines != planned_baselines:
        missing = sorted(expected_baselines - planned_baselines)
        extra = sorted(planned_baselines - expected_baselines)
        raise CaseError(f"Baseline coverage mismatch; missing={missing}, extra={extra}")
    for case in cases:
        if case.blocked:
            continue
        call = transport.prepare(case)
        if not case.validate_request:
            continue
        errors = contract.request_errors(call.request)
        if errors:
            raise CaseError(
                f"OpenAPI request validation failed for {case.operation.operation_id} {case.case_id}: "
                + "; ".join(errors)
            )
    return document, manifest, operations, cases, contract, transport, findings


def is_unsafe_mutation(case: TestCase) -> bool:
    if case.allow_mutation:
        return False
    if case.operation.operation_id == "submittx":
        return any(status < 400 for status in case.expected_statuses)
    if case.operation.operation_id == "ogmios":
        body = case.request.get("body")
        if isinstance(body, dict):
            return body.get("method") in {"submitTransaction", "evaluateTransaction"}
    return False


def run_case(
    case: TestCase,
    document: dict[str, Any],
    contract: ContractValidator,
    transport: HttpTransport,
    ledger: Ledger,
    token: str | None,
) -> None:
    if case.blocked:
        ledger.fail(case, "blocked", case.blocked_reason)
        return
    if is_unsafe_mutation(case):
        ledger.fail(case, "blocked", "Mutating request requires allow_mutation")
        return
    try:
        call = transport.prepare(case)
    except TransportError as exc:
        ledger.fail(case, "input_failure", str(exc))
        return
    request_errors = contract.request_errors(call.request) if case.validate_request else []
    if request_errors:
        ledger.fail(
            case,
            "input_failure",
            "; ".join(request_errors),
            {"request_errors": request_errors},
        )
        return
    ledger.transition(case, "input_resolved")
    ledger.transition(case, "request_sent")
    try:
        response = transport.send(call)
    except TransportError as exc:
        ledger.fail(case, "transport_failure", str(exc))
        return
    ledger.transition(case, "response_received")
    actual_status = int(response.status_code)
    schema = response_schema(case.operation, document, actual_status)
    media_types = response_media_types(case.operation, document, actual_status)
    results = evaluate_response(
        operation=case.operation,
        response=response,
        expected_statuses=case.expected_statuses,
        expected_media_types=media_types,
        response_schema=schema,
        quality=case.quality,
        semantic=case.semantic,
        checks=case.checks,
    )
    if "response_schema" in case.checks:
        response_errors = contract.response_errors(call.request, response)
        results.append(
            CheckResult(
                "response_schema",
                not response_errors,
                "Response conforms to OpenAPI" if not response_errors else "; ".join(response_errors),
                {"errors": response_errors},
            )
        )
    snippet = response.text[:2000]
    if token:
        snippet = snippet.replace(token, "[REDACTED]")
    result = {
        "request": {
            "url": call.request.url,
            "method": call.request.method,
            "source": case.source,
            "validation": {
                "enabled": case.validate_request,
                "reason": case.request_validation_reason,
            },
        },
        "response": {
            "status": actual_status,
            "content_type": response.headers.get("Content-Type", ""),
            "attempts": call.attempts,
            "body": snippet,
        },
        "checks": [item.as_dict() for item in results],
    }
    failures = [item for item in results if not item.passed]
    if not failures:
        ledger.complete(case, result)
        return
    names = {item.name for item in failures}
    if "response_status" in names:
        category = "status_failure"
    elif "response_schema" in names or "content_type" in names:
        category = "schema_failure"
    elif "native_error" in names:
        category = "native_error_failure"
    elif "semantic_invariants" in names:
        category = "semantic_failure"
    else:
        category = "quality_failure"
    ledger.fail(case, category, "; ".join(item.message for item in failures), result)


def write_plan(
    path: Path,
    operations: list[Operation],
    cases: list[TestCase],
    findings: list[dict[str, Any]],
    args: argparse.Namespace,
) -> None:
    path.mkdir(parents=True, exist_ok=True)
    payload = {
        "reference_commit": args.reference_commit,
        "profile": args.profile,
        "operations": len(operations),
        "cases": [
            {
                "operation_id": case.operation.operation_id,
                "method": case.operation.method.upper(),
                "path": case.operation.path,
                "case_id": case.case_id,
                "source": case.source,
                "expected_statuses": list(case.expected_statuses),
                "blocked": case.blocked,
                "request_validation": {
                    "enabled": case.validate_request,
                    "reason": case.request_validation_reason,
                },
            }
            for case in cases
        ],
        "static_findings": findings,
    }
    (path / "plan.json").write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def execute(args: argparse.Namespace) -> int:
    (
        document,
        manifest,
        operations,
        cases,
        contract,
        transport,
        findings,
    ) = prepare_context(args)
    if args.plan_only:
        write_plan(args.report_dir, operations, cases, findings, args)
        print(f"Planned {len(cases)} cases for {len(operations)} operations")
        return 0
    ledger = Ledger(cases)
    token = os.getenv(args.token_env)
    target = manifest.get("target", {})
    base_url = args.base_url or str(target.get("base_url"))
    target_name = str(target.get("network", "preview"))
    # Checkpoint reports after every case so that a transport-induced hang or
    # job cancellation still produces a partial report for upload.
    for case in cases:
        try:
            run_case(case, document, contract, transport, ledger, token)
        finally:
            row = ledger.row(case)
            print(f"{row.state:24} {row.key}")
            try:
                write_reports(
                    report_dir=args.report_dir,
                    ledger=ledger,
                    target=target_name,
                    base_url=str(base_url),
                    reference_commit=args.reference_commit,
                    profile=args.profile,
                    static_findings=findings,
                )
            except OSError as exc:
                print(f"Failed to checkpoint report: {exc}", file=sys.stderr)
    print(json.dumps({"states": ledger.summary(), "report_dir": str(args.report_dir)}, sort_keys=True))
    return 0 if ledger.finalize() else 1


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        return execute(args)
    except (CatalogError, CaseError, OSError, ValueError) as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
