# Post-deployment Preview API Testing Design

Status: implemented baseline/deep runner; fuzz remains an optional separate profile

## Scope correction

This is a post-deployment integration test, not a pull-request test.

The test job must not run from `pull_request`, must not use a pull-request checkout as the test reference, and must not claim that an API change is covered before its SQL has been deployed to the distributed Preview network. The tested specification, fixtures, runner, and reference commit must come from `main` after the deployment pipeline reports that the commit is deployed.

The target is:

```text
https://preview.koios.rest/api/v1
```

The existing endpoint-specific tests are excluded from this design. The current repository is used only to inspect the OpenAPI surface and deployment constraint; the new runner should have its own execution model and coverage accounting.

## Tool research and decision

### Candidates evaluated

| Tool | Strengths | Limitation for this purpose | Decision |
| --- | --- | --- | --- |
| Schemathesis | OpenAPI 3.1 support, schema examples, generated positive and negative data, response checks, rate limiting, reports, and optional stateful testing. | Its execution model is intentionally case-generation-oriented. Examples, coverage, fuzzing, filters, deprecated-operation settings, hooks, generated values, and missing-test-data behavior can produce a run that is useful but not an authoritative operation ledger. It does not know whether a database-backed example is meaningful, whether a `200 null` is a native failure, or whether an empty result is acceptable for a particular operation. | Do not use as the required baseline. Use later for bounded exploratory fuzzing. |
| Dredd | Explicitly compiles API-description transactions, runs each transaction, supports hooks, and reports per-transaction results. | Current documentation describes OpenAPI 3 support as experimental. The upstream repository is archived, and the model is not a good fit for this OpenAPI 3.1 document, special JSON-RPC behavior, fixture-derived database values, or custom result-quality rules. | Reject. |
| Postman/Newman | Imports OpenAPI 3.1 and is convenient for manually maintained collections and scripts. | The imported collection becomes a second artifact whose operation coverage and request examples can drift from the committed specification. It is not the best source for fail-closed 102-operation accounting or typed OpenAPI response validation. | Useful for manual exploration, not the CI authority. |
| `openapi-core` | Python client/server support for OpenAPI 3.0, 3.1, and 3.2; validates and unmarshals requests and responses; supports response selection by status and content type. | It is a validation library, not a complete live endpoint test runner. Request construction, fixture resolution, quality policies, and reporting must be supplied. | Use as the contract-validation component. |
| `openapi-spec-validator` | Validates OpenAPI 2.0, 3.0, 3.1, and 3.2 documents and has CLI and Python APIs. | It validates the document, not the live API or data quality. | Use for the static specification phase. |
| Deterministic custom runner | Can enumerate every path/method, require an input case, fail on unresolved or skipped operations, detect native errors and null responses, and apply endpoint-specific quality rules. | Requires a small amount of repository-owned code and a case manifest. | Use as the required integration-test runner. |

Research references:

- [Schemathesis documentation](https://schemathesis.readthedocs.io/en/stable/)
- [Schemathesis example testing](https://schemathesis.readthedocs.io/en/stable/explanations/examples/)
- [Schemathesis CLI phases and filtering](https://schemathesis.readthedocs.io/en/stable/reference/cli/)
- [Schemathesis extension and hook behavior](https://schemathesis.readthedocs.io/en/stable/guides/extending/)
- [Dredd documentation](https://dredd.org/en/latest/)
- [Dredd repository status](https://github.com/apiaryio/dredd)
- [openapi-core documentation](https://openapi-core.readthedocs.io/en/latest/)
- [openapi-spec-validator documentation](https://openapi-spec-validator.readthedocs.io/en/latest/)
- [Postman OpenAPI import](https://learning.postman.com/v11/docs/design-apis/api-builder/importing-an-api.md)

### Recommendation

Build a standalone Python command-line runner with these responsibilities:

- `openapi-spec-validator` validates the committed document before traffic is sent.
- `openapi-core` validates each actual request and response against the selected operation.
- A normal HTTP client performs the request.
- Repository-owned code resolves examples, applies fixtures, evaluates response quality, detects native error envelopes, tracks the operation ledger, and produces reports.
- Schemathesis is an optional later profile. Its results must be attached to the deterministic baseline, never used to replace it.

This is not a rejection of Schemathesis as a bug-finding tool. It is a separation of responsibilities: deterministic coverage and meaningful data are the release gate; generated cases are additional discovery.

## Current surface used for sizing

The generated Preview specification currently contains 102 operations: 62 GET and 40 POST operations. It also contains deprecated operations, `/ogmios` with named JSON-RPC examples, and mutating `/submittx` behavior.

These numbers define the first coverage denominator only. The runner must calculate the denominator from the selected specification at runtime and print the exact operation list. It must not copy an endpoint list into Python.

## Proposed structure

```text
api-tests/
  cases/
    preview.yaml
  runner/
    catalog.py
    cases.py
    transport.py
    contract.py
    quality.py
    ledger.py
    report.py
    main.py
  requirements.txt

.github/workflows/
  preview-api-tests.yml
```

The directory can be named differently. The important properties are that it is independent of the existing tests, is executable as a standalone command, and has no implicit test discovery that can silently omit an operation.

The runner should be usable locally with the same command and inputs as CI, except that the reference checkout and environment variables are supplied by the caller.

## Execution contract

The runner receives:

- `--spec`: the OpenAPI document from the deployed reference commit.
- `--cases`: the Preview case manifest from that same commit.
- `--fixtures`: network-specific values from that same commit.
- `--base-url`: the deployed API base URL.
- `--profile`: `baseline`, `deep`, `fuzz`, or `mutating`.
- `--reference-commit`: the main-branch commit being validated.
- An optional bearer token from the CI environment.

A run records the reference commit and target URL in every report. A run without a reference commit is invalid.

## Case input model

The case manifest should provide intent, not duplicate all OpenAPI data. A baseline case should normally reference the examples in the specification and add only the assertions that OpenAPI cannot express.

```yaml
version: 1

target:
  network: preview
  base_url: https://preview.koios.rest/api/v1
  spec: specs/results/koiosapi-preview.yaml
  fixtures: specs/examples/fixtures.yaml

defaults:
  request_source: openapi
  timeout_seconds: 60
  retries: 2
  rate_limit: 30/m

operations:
  tip:
    baseline:
      source: openapi
      expected_status: 200
      checks:
        - response_schema
        - response_quality
      quality:
        mode: non_empty
        required_paths:
          - /0/hash
          - /0/block_height

  epoch_info:
    baseline:
      source: fixture
      request:
        query:
          _epoch_no: preview_epoch
      expected_status: 200
      checks:
        - response_schema
        - response_quality
      quality:
        mode: non_empty
        required_paths:
          - /0/epoch_no

  ogmios:
    baseline:
      source: named_openapi_example
      example: tip
      expected_status: 200
      checks:
        - response_schema
        - response_quality
      quality:
        mode: non_empty

  submittx:
    baseline:
      source: explicit
      profile: negative
      request:
        media_type: application/cbor
        body_file: cases/invalid-transaction.cbor
      expected_status: 400
      checks:
        - response_status
        - native_error_classification
      quality:
        mode: not_applicable
```

The manifest must support these request sources:

- `openapi`: resolve parameter and request-body examples from the operation and referenced components.
- `named_openapi_example`: select one named media-type example, required for `/ogmios`.
- `fixture`: bind a named value from `fixtures.yaml` for `preview`.
- `derived`: extract a value from a previous response with a JSON Pointer.
- `explicit`: use an intentionally specified request or binary body.

Resolution precedence should be explicit and reported:

1. Case-level explicit request.
2. Named OpenAPI example.
3. Operation or component OpenAPI example.
4. Network fixture binding.
5. No fallback.

If a required value cannot be resolved, the case fails during planning. The operation is not skipped and a generated random value is not substituted.

## Fail-closed operation ledger

The ledger is the most important difference from a generic schema-test invocation.

Before making any HTTP request, the runner builds one planned row for every path and method in the selected OpenAPI document. Each row includes:

- Path.
- HTTP method.
- Operation ID.
- Deprecated flag.
- Mutating flag.
- Request media type.
- Required inputs.
- Expected success statuses.
- Baseline case ID.
- Deep-profile case IDs.

Each row moves through an explicit state machine:

```text
planned -> input_resolved -> request_sent -> response_received -> checks_complete
```

Failure states are recorded rather than converted into skips:

```text
input_failure
transport_failure
status_failure
schema_failure
quality_failure
semantic_failure
blocked
```

At the end of a required baseline run:

- Every planned operation must have reached `checks_complete` or a failure state.
- `planned`, `input_resolved`, `not_started`, `filtered`, `skipped`, and `unknown` are fatal ledger states.
- `blocked` is fatal unless the selected profile explicitly permits that operation class and records the reason.
- The runner continues after individual failures so the final report contains all endpoints.
- The exit code is non-zero if any operation failed or any operation was not attempted.

The report must show `planned`, `executed`, `failed`, and `blocked` counts and list every operation in each category. A green run with a reduced denominator is impossible.

## Static contract phase

This phase runs against the reference commit before live requests.

It should:

- Verify that generated specifications match their fragments with `createspecs.py --check`.
- Validate the selected OpenAPI 3.1 document with `openapi-spec-validator`.
- Resolve every local reference.
- Verify unique operation IDs.
- Verify that every operation has a declared success response.
- Verify that every success response has a schema, including inline schemas.
- Validate request examples against their request schemas.
- Validate parameter examples against their parameter schemas.
- Report missing examples separately from missing cases.
- Reject unreplaced fixture placeholders.
- Validate the case manifest against the operation catalog.
- Require exactly one baseline case for every ordinary operation.
- Require an explicit policy for deprecated, JSON-RPC, and mutating operations.

Static validation is not the live API test and does not establish that Preview is healthy. It is a prerequisite to the live phase.

The current `_after_block_height` example gap should appear as a report finding. A case may provide a fixture binding while the specification is repaired, but the runner must identify that the request did not originate from a complete OpenAPI example.

## Live request and response validation

For every baseline case, the runner should:

1. Build the request and validate it against the operation with `openapi-core`.
2. Send it to the supplied Preview base URL.
3. Retry only bounded transport failures and rate limits.
4. Require a response status declared by the case or the operation.
5. Require a compatible response content type.
6. Parse the response according to the declared media type.
7. Validate the response against the schema for the actual returned status.
8. Run native-error, null, quality, and semantic checks.
9. Store a redacted result in the ledger.

No global `200` assumption is allowed. `/submittx` has a documented success status of 202, and negative cases intentionally expect an error status.

## Native PostgREST and null detection

Schema validation alone is not enough. A permissive schema can allow a server failure to appear valid, and a response may be structurally valid while containing no useful data.

The response pipeline should apply these checks in addition to OpenAPI validation:

### HTTP failure classification

- Any 5xx is a server failure.
- A 4xx is a valid result only for a case that expects an error.
- A status outside the case's allowed statuses is a status failure.
- Redirects, unexpected HTML, and malformed JSON are failures for JSON operations.

### Native error envelope detection

When an error status occurs, classify common PostgREST error envelopes separately from transport errors. Capture status, `code`, `message`, `details`, and `hint` fields when present, with truncation and secret redaction.

When a 2xx response contains a recognized native error envelope, fail it even if the declared response schema is permissive. The error-envelope detector must be configurable per operation so legitimate domain objects with similarly named fields can opt out explicitly.

The detector should also classify database/native error text such as SQLSTATE-like codes, missing relations, missing columns, or PostgREST error messages. This is classification for diagnostics; the status and schema checks still determine pass or fail.

### Null and empty response detection

- A JSON `null` response fails when the selected schema does not permit null.
- A top-level empty array fails under `quality.mode: non_empty`.
- An empty object fails under `non_empty` unless required paths prove it is meaningful.
- An empty result is allowed only under `allow_empty`, with a manifest reason and continued schema validation.
- A response that contains only an error-like envelope fails `non_empty` even if it is not literally empty.
- A response-quality failure is separate from a schema failure so fixture maintenance can be distinguished from contract breakage.

This directly catches successful HTTP responses that are null, empty, or native errors instead of silently accepting them.

## Quality policies

OpenAPI describes shape, not whether an example exercises useful data. Quality is therefore input-driven.

Supported policies:

- `non_empty`: arrays contain at least one item, objects have meaningful content, and required JSON Pointers exist and are non-null.
- `non_empty_any`: at least one configured JSON Pointer has a meaningful value.
- `allow_empty`: empty data is a documented and reviewed result for this fixture.
- `not_applicable`: only for protocol responses where non-empty collection semantics do not apply.

The default for a data-returning operation is `non_empty`. The manifest must make exceptions visible. There is no global `response.text != "[]"` hook.

## Deep profile

The baseline proves endpoint reachability, request construction, documented status, response schema, and example quality. The deep profile adds bounded cases selected by the manifest:

- Optional query parameter present and absent.
- Boolean and extended flags in both supported states.
- `limit` and `offset` pagination cases.
- Required parameter and required-body-field removal.
- Invalid type, malformed hash, malformed address, and invalid enum cases.
- Empty-filter cases that are expected to return a valid empty result.
- Response semantic invariants.
- Cross-endpoint relationships using values extracted from successful responses.

Deep cases are still deterministic. They do not select random rows from a response and do not use an empty result as a successful setup value.

Initial relationship candidates include:

- A tip or block value into a block query.
- A pool from pool listing into pool information and account-related queries.
- An epoch from epoch information into epoch-filtered queries.
- An asset into asset information, holder, history, and transaction queries.
- A transaction hash into status, CBOR, metadata, and UTxO queries.
- A DRep into DRep information and history queries.

A failed extraction is a dependent-case failure, not a skip. Relationship assertions should check documented identity and type relationships, not unstable full-response equality.

## Schemathesis role

Schemathesis should be considered only after the deterministic baseline is working.

The optional `fuzz` profile may use its OpenAPI 3.1 support for:

- Generated positive and negative values.
- Boundary values and malformed data.
- Stateful exploration where useful.
- Minimal reproducible cases and replay artifacts.

The fuzz profile must:

- Run after the operation ledger is complete.
- Use an explicit rate limit, worker count, time budget, and seed policy.
- Report operation filters and generated-case counts.
- Fail if its configured filters remove an operation that the baseline requires.
- Never replace the baseline example request for an operation.
- Remain non-mutating by default.

This uses Schemathesis for the area where it is strongest while preventing its generated-case behavior from becoming the only proof that an endpoint was tested.

## Special operations

### `/ogmios`

The current operation has named examples for multiple JSON-RPC methods but documents a tip-shaped success response. The baseline should fully validate the declared tip example. Other named examples should be protocol smoke cases until each method has its own response schema and status contract.

Resource-sensitive or mutating JSON-RPC methods require an explicit profile and environment approval.

### `/submittx`

Ordinary Preview integration runs must not submit a valid transaction. The static phase validates the CBOR request schema. The deep negative profile may send an intentionally invalid or truncated payload and expect the declared error status. A valid submission belongs to a separate manually approved profile.

### Deprecated operations

Deprecated operations remain in the catalog and baseline denominator while they remain in the specification. They receive a deprecated label in reports. Removing them from coverage requires removing them from the OpenAPI document and regenerating the committed result.

## Post-deployment GitHub workflow

The live workflow must have no `pull_request` trigger.

The preferred integration is a deployment event emitted after Preview deployment completes. The deployment system should provide:

```json
{
  "event_type": "preview-deployed",
  "client_payload": {
    "reference_commit": "<main-branch-sha>",
    "environment": "preview",
    "deployment_id": "<deployment-id>"
  }
}
```

If the deployment system is another workflow in the same repository, `workflow_run` may be used instead. If it is external or in another repository, `repository_dispatch` is the clearer contract. Manual dispatch should accept the same `reference_commit` for replaying a deployment test.

The workflow must:

1. Receive a reference commit only from the deployment event or manual input.
2. Verify that the reference commit is an ancestor of `main`.
3. Check out exactly that commit, never the PR merge ref.
4. Install pinned runner dependencies.
5. Run the static phase from that commit.
6. Wait for the Preview deployment to be ready, with a bounded retry budget.
7. Run the baseline ledger against `https://preview.koios.rest/api/v1`.
8. Run deep cases when the deployment event requests the deep profile.
9. Upload redacted JUnit, JSON, ledger, and failure reports.
10. Mark the deployment check failed if any operation is skipped, unresolved, blocked without policy, or otherwise failed.

A plain push-to-main trigger is not sufficient if deployment is asynchronous. It can start before the distributed network serves the reference commit. The deployment event or an equivalent deployment-readiness signal must be the trigger.

A scheduled job may replay the latest known deployed reference, but it must receive or resolve deployment metadata. It must not assume that the current `main` SHA is already live on Preview.

## Profiles and required status

| Profile | Trigger | Purpose |
| --- | --- | --- |
| `baseline` | After every successful Preview deployment | One deterministic case per operation, status, content, schema, native-error, null, and quality checks. |
| `deep` | Deployment event or manual dispatch | Parameter matrices, negative cases, semantic invariants, and relationships. |
| `fuzz` | Manual or scheduled after baseline | Bounded Schemathesis discovery with explicit limits and replay artifacts. |
| `mutating` | Manual approval only | Valid transaction or resource-sensitive protocol behavior. |

The required deployment status is `baseline`. `deep` can initially be advisory while fixtures and rate budgets are established. `fuzz` is a separate diagnostic signal. No profile may silently lower the operation denominator.

## Reports

Produce:

- A human-readable summary.
- Machine-readable JSON.
- JUnit-compatible results.
- The full operation ledger.
- A separate example inventory.

Every operation result should include:

- Reference commit.
- Target environment and URL.
- Path, method, and operation ID.
- Case ID and source.
- Request media type.
- Expected and actual status.
- Content type.
- Response latency.
- Schema result and JSON Pointer error location.
- Native-error classification.
- Quality result.
- Final ledger state.

Response bodies must be truncated and redacted. Authorization headers, tokens, environment secrets, and raw credentials must never be recorded.

Report these categories separately:

- Specification failure.
- Missing example.
- Fixture or input-resolution failure.
- Transport failure.
- Rate-limit failure.
- HTTP status failure.
- Content-type or parse failure.
- OpenAPI request failure.
- OpenAPI response-schema failure.
- Native PostgREST error.
- Null or empty quality failure.
- Semantic failure.
- Explicitly allowed empty result.
- Unexecuted or blocked operation.

## Implementation order

1. Build the standalone operation catalog and ledger against the committed Preview spec without making HTTP requests.
2. Add the case-manifest validator and require one baseline policy per catalog operation.
3. Add request resolution from OpenAPI examples and network fixtures with no random fallback.
4. Add `openapi-spec-validator` and `openapi-core` validation using synthetic request and response tests.
5. Add native-error, null, empty, and quality policies.
6. Run the first post-deployment baseline and inspect all 102 operation results.
7. Repair missing or stale fixtures and specification examples based on the report.
8. Add deep deterministic cases and relationships.
9. Add the deployment event workflow.
10. Add Schemathesis as an isolated bounded fuzz profile only after baseline coverage is trustworthy.

## First-run acceptance criteria

The first post-deployment run is acceptable only if it demonstrates:

- The test reference is a commit on `main` that Preview reports as deployed.
- The operation denominator is derived from the selected specification.
- Every operation has a planned baseline case.
- No operation is silently skipped, filtered, unresolved, or hidden by early exit.
- Every attempted response has status, content-type, schema, native-error, null, and quality results.
- Native PostgREST errors, `null`, empty arrays, and empty objects appear as explicit findings where applicable.
- Missing or stale examples are reported separately from API defects.
- Mutating operations are blocked by policy rather than accidentally executed.
- The result includes a complete ledger and reproducible request metadata without secrets.

This first run is intentionally diagnostic. It should establish trustworthy endpoint accounting and reveal fixture/spec problems before generated fuzzing is allowed to influence release confidence.
