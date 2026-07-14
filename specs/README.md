# Koios OpenAPI Spec Generator

This folder contains the source templates and build script for network-specific OpenAPI specs in `results/`.

## Directory layout

```
specs/
├── createspecs.py          # Build script
├── networks.yaml           # Network registry (mainnet, preview, preprod, guild)
├── template.yaml           # OpenAPI skeleton (servers, components shell, tags)
├── examples/
│   └── fixtures.yaml       # Per-network example values for parameters and request bodies
├── fragments/
│   ├── info.yaml           # API info block
│   ├── parameters.yaml     # Shared query parameters
│   ├── request-bodies.yaml # Shared request body definitions
│   ├── macros.yaml         # Reusable response snippets
│   ├── paths/              # Endpoint definitions, split by domain
│   └── schemas/            # Response schemas, split by domain
└── results/                # Generated specs (committed, used by tests and docs)
```

Path and schema fragments align with the grest RPC folders in `files/grest/rpc/` (network, epoch, block, account, etc.).

## Build commands

Generate all network specs:

```bash
python3 createspecs.py
```

Generate a single network:

```bash
python3 createspecs.py --network guild
```

Verify that `results/` matches what the generator would produce:

```bash
python3 createspecs.py --check
```

## Adding or changing an endpoint

1. **Path** — Edit or add the endpoint in the matching file under `fragments/paths/` (e.g. `fragments/paths/account.yaml` for stake-account endpoints).
2. **Schema** — Add or update the response schema in `fragments/schemas/`.
3. **Parameters / request body** — If new shared components are needed, update `fragments/parameters.yaml` or `fragments/request-bodies.yaml`.
4. **Examples** — Add network-specific example values to `examples/fixtures.yaml` under `params` or `requestBodies`.
5. **Regenerate** — Run `python3 createspecs.py` and commit the updated `results/` files.

## Placeholders

| Syntax | Purpose |
|--------|---------|
| `#!info!#` | Injects `fragments/info.yaml` |
| `#!paths!#` | Merges all `fragments/paths/*.yaml` |
| `#!parameters!#` | Injects `fragments/parameters.yaml` |
| `#!requestBodies!#` | Injects `fragments/request-bodies.yaml` |
| `#!schemas!#` | Merges all `fragments/schemas/*.yaml` |
| `#!macro:koios_errors!#` | Expands standard 400/401/404 response refs |
| `##_name_param##` | Replaced with per-network param example from `fixtures.yaml` |
| `##_name_rb##` | Replaced with per-network request-body example from `fixtures.yaml` |

## Response macro

Most endpoints use the shared error-response block via:

```yaml
responses:
  "200":
    ...
  #!macro:koios_errors!#
```

Defined in `fragments/macros.yaml`. Endpoints with non-standard responses (e.g. `/ogmios`) omit the macro.

## Networks

Configured in `networks.yaml`. To add a network:

1. Add an entry with a short `key` and `output` path.
2. Add example values for that network in `examples/fixtures.yaml`.
3. Run `python3 createspecs.py`.
