Automation including ETL and forecasting for demand and capacity planning  
## Local development

The GitHub Actions workflows install `requirements.txt` with pip on Python
3.10.14. Locally, [uv](https://docs.astral.sh/uv/) reproduces that exactly —
`.python-version` pins the interpreter, so `uv venv` fetches 3.10.14 if you
don't have it.

```bash
uv venv
uv pip install -r requirements-dev.txt
uv run pytest
```

`requirements.txt` stays the source of truth for dependencies (CI reads it
directly); `requirements-dev.txt` adds only the test tooling.

### Testing the ShipBob pipelines

The 74 tests under `tests/` need no credentials and no network — they run
against recorded ShipBob 2026-07 payloads.

To exercise the live API without touching S3 or Athena, use `--dry-run`. Only
`SHIPBOB_API_SECRET` is required; CSVs are written under `./dryrun` using the
production S3 key layout so they can be diffed against a real partition.

```bash
SHIPBOB_API_SECRET=... uv run python src/shipbob_inventory_details/main.py --dry-run
```

```bash
SHIPBOB_API_SECRET=... uv run python src/shipbob_order_details/main.py --start_date 2026-07-15 --end_date 2026-07-16 --dry-run
```

`test.py` is a preflight that checks the token authenticates against the pinned
API version and prints the channel scopes it carries.
