from pathlib import Path

from runner.cases import CaseResolver, load_fixture_store, load_manifest
from runner.catalog import build_catalog, load_yaml
from runner.ledger import Ledger
from runner.quality import evaluate_response
from runner.transport import HttpTransport
from runner.contract import ContractValidator

ROOT = Path(__file__).parents[2]
SPEC = ROOT / "specs/results/koiosapi-preview.yaml"
MANIFEST = ROOT / "api-tests/cases/preview.yaml"
FIXTURES = ROOT / "specs/examples/fixtures.yaml"


def context():
    document = load_yaml(SPEC)
    operations = build_catalog(document)
    manifest = load_manifest(MANIFEST)
    fixtures = load_fixture_store(FIXTURES, "preview")
    resolver = CaseResolver(manifest, operations, document, fixtures)
    return document, operations, resolver


def test_baseline_covers_every_operation():
    _, operations, resolver = context()
    cases = resolver.cases_for_profile("baseline")
    assert len(operations) == 102
    assert len(cases) == len(operations)
    assert {case.operation.operation_id for case in cases} == {
        operation.operation_id for operation in operations
    }


def test_deep_keeps_baseline_and_adds_explicit_cases():
    _, operations, resolver = context()
    cases = resolver.cases_for_profile("deep")
    assert len(cases) == 204
    assert sum(case.case_id == "baseline" for case in cases) == len(operations)
    assert sum(case.case_id == "limit-one" for case in cases) == 100
    assert all(
        "semantic_invariants" in case.checks
        for case in cases
        if case.case_id == "limit-one"
    )
    assert [case.case_id for case in cases if case.operation.operation_id == "submittx"] == ["baseline"]


def test_request_examples_validate_before_network():
    document, _, resolver = context()
    cases = resolver.cases_for_profile("baseline")
    transport = HttpTransport(
        "https://preview.koios.rest/api/v1",
        token=None,
        timeout=1,
        retries=0,
        rate_limit="1000/m",
    )
    contract = ContractValidator(SPEC)
    for case in cases:
        call = transport.prepare(case)
        if case.validate_request:
            assert contract.request_errors(call.request) == [], case.operation.operation_id


def test_submittx_is_explicitly_binary_negative_case():
    _, _, resolver = context()
    case = next(case for case in resolver.cases_for_profile("baseline") if case.operation.operation_id == "submittx")
    assert case.request["body_hex"] == "00"
    assert case.expected_statuses == (400,)
    assert case.validate_request is False
    assert case.request_validation_reason


def test_quality_rejects_empty_array():
    class Response:
        status_code = 200
        headers = {"Content-Type": "application/json"}
        content = b"[]"
        text = "[]"

        def json(self):
            return []

    results = evaluate_response(
        operation=next(iter(context()[1])),
        response=Response(),
        expected_statuses=(200,),
        expected_media_types=("application/json",),
        response_schema={"type": "array", "items": {"type": "object"}},
        quality={"mode": "non_empty"},
        semantic=[],
        checks={"response_schema", "response_quality"},
    )
    assert any(result.name == "response_quality" and not result.passed for result in results)


def test_quality_rejects_successful_native_error():
    class Response:
        status_code = 200
        headers = {"Content-Type": "application/json"}
        content = b'{"code":"PGRST000","message":"database failure"}'
        text = content.decode()

        def json(self):
            return {"code": "PGRST000", "message": "database failure"}

    results = evaluate_response(
        operation=next(iter(context()[1])),
        response=Response(),
        expected_statuses=(200,),
        expected_media_types=("application/json",),
        response_schema={"type": "object"},
        quality={"mode": "non_empty"},
        semantic=[],
        checks={"response_quality"},
    )
    assert any(result.name == "native_error" and not result.passed for result in results)


def test_ledger_does_not_accept_unfinished_cases():
    _, _, resolver = context()
    cases = resolver.cases_for_profile("baseline")[:1]
    ledger = Ledger(cases)
    assert ledger.finalize() is False
    ledger.fail(cases[0], "quality_failure", "empty")
    assert ledger.finalize() is False
    ledger.complete(cases[0], {"checks": []})
    assert ledger.finalize() is True


def test_limit_one_semantic_check_rejects_multiple_rows():
    class Response:
        status_code = 200
        headers = {"Content-Type": "application/json"}
        content = b"[{\"value\": 1}, {\"value\": 2}]"
        text = content.decode()

        def json(self):
            return [{"value": 1}, {"value": 2}]

    results = evaluate_response(
        operation=next(iter(context()[1])),
        response=Response(),
        expected_statuses=(200,),
        expected_media_types=("application/json",),
        response_schema={"type": "array", "items": {"type": "object"}},
        quality={"mode": "not_applicable"},
        semantic=[{"kind": "max_items", "value": 1}],
        checks={"semantic_invariants"},
    )
    assert any(result.name == "semantic_invariants" and not result.passed for result in results)
