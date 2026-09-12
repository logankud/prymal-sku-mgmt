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

### Testing the Shopify pipeline

The Shopify job takes the same `--dry-run`. It pulls from the live API, writes
CSVs under `./dryrun` using the production S3 key layout, and then checks the
columns of what it wrote against `src/shopify_order_details/ddl.sql`, failing
if they disagree. It touches no AWS service.

```bash
SHOPIFY_API_KEY=... SHOPIFY_API_PW=... uv run python src/shopify_order_details/main.py --start_date 2026-08-15 --end_date 2026-08-15 --dry-run
```

A dry run proves the file is shaped correctly. It cannot prove Athena reads it
back correctly, because the production CSV is headerless and Athena maps field
position to column position. For that, run the **Shopify Order Details**
workflow manually with `start_date` and `end_date` set to a single recent day,
then query that partition. The S3 key is deterministic per day, so re-running a
date rewrites its file rather than duplicating it.

The same workflow has a `dry_run` checkbox, which runs the command above on the
runner and uploads the CSVs as a build artifact. That is the way to inspect
output without Shopify credentials on your own machine.

`test.py` is a preflight that checks the token authenticates against the pinned
API version and prints the channel scopes it carries.
