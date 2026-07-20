# Preview API test runner

Install the pinned dependencies and validate the generated test plan without sending requests:

```bash
python3 -m pip install --requirement api-tests/requirements.txt
PYTHONPATH=api-tests python3 api-tests/run.py \
  --spec specs/results/koiosapi-preview.yaml \
  --cases api-tests/cases/preview.yaml \
  --fixtures specs/examples/fixtures.yaml \
  --base-url https://preview.koios.rest/api/v1 \
  --profile baseline \
  --reference-commit <main-commit> \
  --report-dir api-test-report \
  --plan-only
```

Run the live baseline by removing `--plan-only`. Set `KOIOS_API_TOKEN` when higher API limits are required.

The GitHub workflow is intentionally triggered only by `repository_dispatch` with a `preview-deployed` event or manual dispatch. It does not run on pull requests or arbitrary pushes.

The deep profile adds a `limit=1` case for every non-mutating operation and the explicit cases in `api-tests/cases/preview.yaml`.
