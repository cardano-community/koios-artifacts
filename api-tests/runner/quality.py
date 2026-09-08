from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

from .catalog import Operation


@dataclass
class CheckResult:
    name: str
    passed: bool
    message: str
    details: dict[str, Any] | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "passed": self.passed,
            "message": self.message,
            "details": self.details or {},
        }


def json_body(response: Any) -> tuple[Any, str | None]:
    data = getattr(response, "content", b"")
    if not data:
        return None, "empty response body"
    try:
        return response.json(), None
    except (ValueError, json.JSONDecodeError) as exc:
        return None, str(exc)


def pointer_value(value: Any, pointer: str) -> Any:
    if pointer in ("", "/"):
        return value
    current = value
    for part in pointer.lstrip("/").split("/"):
        part = part.replace("~1", "/").replace("~0", "~")
        if isinstance(current, list):
            current = current[int(part)]
        elif isinstance(current, dict):
            current = current[part]
        else:
            raise KeyError(pointer)
    return current


def meaningful(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, (str, bytes)):
        return bool(value)
    if isinstance(value, list):
        return any(meaningful(item) for item in value)
    if isinstance(value, dict):
        return any(meaningful(v) for v in value.values())
    return True


def schema_allows_null(schema: Any) -> bool:
    if not isinstance(schema, dict):
        return False
    if schema.get("nullable") is True:
        return True
    schema_type = schema.get("type")
    if isinstance(schema_type, list) and "null" in schema_type:
        return True
    for key in ("anyOf", "oneOf", "allOf"):
        for option in schema.get(key, []):
            if schema_allows_null(option):
                return True
    return False


def native_error(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    keys = {str(key).lower() for key in value}
    return "message" in keys and ("code" in keys or "details" in keys or "hint" in keys)


def jsonrpc_error(value: Any) -> bool:
    """Detect a JSON-RPC 2.0 error envelope (top-level `error` member)."""
    if not isinstance(value, dict):
        return False
    if "error" not in value:
        return False
    err = value["error"]
    if isinstance(err, str):
        return bool(err)
    if isinstance(err, dict):
        keys = {str(k).lower() for k in err}
        return "message" in keys and "code" in keys
    return False


def native_error_text(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    patterns = (
        r"sqlstate",
        r"pgrst\d+",
        r"relation .* does not exist",
        r"column .* does not exist",
        r"syntax error",
        r"postgrest",
    )
    return any(re.search(pattern, value, re.IGNORECASE) for pattern in patterns)


def status_check(response: Any, expected: tuple[int, ...]) -> CheckResult:
    status = int(response.status_code)
    if status in expected:
        return CheckResult("response_status", True, f"HTTP {status}")
    return CheckResult(
        "response_status",
        False,
        f"Expected one of {list(expected)}, received HTTP {status}",
        {"expected": list(expected), "actual": status},
    )


def content_type_check(response: Any, expected_media_types: tuple[str, ...]) -> CheckResult:
    actual = response.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
    expected = {item.split(";", 1)[0].strip().lower() for item in expected_media_types}
    if not expected or actual in expected or (actual.endswith("+json") and "application/json" in expected):
        return CheckResult("content_type", True, actual or "no content type")
    return CheckResult(
        "content_type",
        False,
        f"Expected content type matching {sorted(expected)}, received {actual or '<missing>'}",
        {"expected": sorted(expected), "actual": actual},
    )


def native_error_check(response: Any, expected: tuple[int, ...], body: Any) -> CheckResult:
    status = int(response.status_code)
    detected = (
        native_error(body)
        or jsonrpc_error(body)
        or native_error_text(getattr(response, "text", ""))
    )
    if 200 <= status < 300 and detected:
        return CheckResult("native_error", False, "2xx response contains a native error envelope")
    if status >= 500:
        return CheckResult("native_error", False, f"HTTP {status} indicates a server/native failure")
    if status >= 400 and status not in expected:
        return CheckResult("native_error", True, "Error response classified outside expected status check")
    if status >= 400 and detected:
        return CheckResult("native_error", True, "Expected error response contains a native error envelope")
    return CheckResult("native_error", True, "No native error detected")


def null_check(response: Any, schema: Any, body: Any, parse_error: str | None) -> CheckResult:
    if parse_error is not None:
        return CheckResult("null_body", False, f"Response body could not be parsed: {parse_error}")
    if body is None and not schema_allows_null(schema):
        return CheckResult("null_body", False, "Response body is JSON null but the schema does not allow null")
    return CheckResult("null_body", True, "Response body is not an unexpected null")


def quality_check(policy: dict[str, Any], body: Any) -> CheckResult:
    mode = policy.get("mode", "non_empty")
    if mode == "not_applicable":
        return CheckResult("response_quality", True, "Quality policy is not applicable")
    if mode == "allow_empty":
        return CheckResult("response_quality", True, "Empty result explicitly allowed", {"reason": policy.get("reason", "")})
    if mode not in {"non_empty", "non_empty_any"}:
        return CheckResult("response_quality", False, f"Unknown quality mode: {mode}")
    required = policy.get("required_paths", [])
    if mode == "non_empty" and not meaningful(body):
        return CheckResult("response_quality", False, "Response body is empty or not meaningful")
    if mode == "non_empty_any":
        if isinstance(body, list):
            candidates = body
        elif isinstance(body, dict):
            candidates = list(body.values())
        else:
            candidates = [body]
        if not any(meaningful(value) for value in candidates):
            return CheckResult("response_quality", False, "Response has no meaningful values")
    missing = []
    for pointer in required:
        try:
            value = pointer_value(body, pointer)
        except (KeyError, IndexError, TypeError, ValueError):
            missing.append(pointer)
            continue
        if not meaningful(value):
            missing.append(pointer)
    if missing:
        return CheckResult("response_quality", False, "Required response values are missing", {"missing": missing})
    return CheckResult("response_quality", True, "Response contains meaningful data")


def semantic_check(policy: list[dict[str, Any]], body: Any) -> CheckResult:
    failures: list[str] = []
    for assertion in policy:
        kind = assertion.get("kind")
        if kind == "max_items":
            if isinstance(body, list) and len(body) > int(assertion["value"]):
                failures.append(f"max_items>{assertion['value']}")
        elif kind == "required_paths":
            for pointer in assertion.get("paths", []):
                try:
                    if not meaningful(pointer_value(body, pointer)):
                        failures.append(f"missing:{pointer}")
                except (KeyError, IndexError, TypeError, ValueError):
                    failures.append(f"missing:{pointer}")
        else:
            failures.append(f"unknown:{kind}")
    if failures:
        return CheckResult("semantic_invariants", False, "Semantic assertions failed", {"failures": failures})
    return CheckResult("semantic_invariants", True, "Semantic assertions passed")


def evaluate_response(
    operation: Operation,
    response: Any,
    expected_statuses: tuple[int, ...],
    expected_media_types: tuple[str, ...],
    response_schema: Any,
    quality: dict[str, Any],
    semantic: list[dict[str, Any]],
    checks: set[str],
) -> list[CheckResult]:
    body, parse_error = json_body(response)
    results = [status_check(response, expected_statuses)]
    if "response_schema" in checks:
        results.append(content_type_check(response, expected_media_types))
    results.append(native_error_check(response, expected_statuses, body))
    results.append(null_check(response, response_schema, body, parse_error))
    if "response_quality" in checks and results[0].passed:
        results.append(quality_check(quality, body))
    if "semantic_invariants" in checks and results[0].passed:
        results.append(semantic_check(semantic, body))
    return results
