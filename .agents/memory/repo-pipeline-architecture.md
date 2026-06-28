---
name: Repo pipeline architecture
description: 9 GitHub Actions scheduled workflows, their datasets, S3/Athena targets, and backfill patterns
---

# Prymal Data Pipeline — Repo Architecture

## Stack
- Source APIs: ShipBob, Shopify, Katana
- Storage: AWS S3 (partitioned CSVs) → AWS Glue/Athena (`prymal` database)
- Agent DB: `prymal_agent` Glue database (curated tables for reporting/AI)
- Alerting: AWS SNS
- Orchestration: GitHub Actions (scheduled, daily)
- Code root: `src/` with one subdirectory per workflow

## 9 Scheduled Workflows

| Workflow | Script | Dataset | Backfill arg |
|---|---|---|---|
| Shopify Order Details | `src/shopify_order_details/main.py` | Shopify order headers + line items | `--start_date / --end_date` |
| Shopify Active Variant SKU Details | `src/shopify_active_variant_sku_details/main.py` | Active Shopify SKU snapshot | `--partition_date` (added Jun 2026) |
| Shipbob Order Details | `src/shipbob_order_details/main.py` | ShipBob order line items | `--start_date / --end_date` |
| Shipbob Inventory Details | `src/shipbob_inventory_details/main.py` | ShipBob inventory snapshot | `--partition_date` (added Jun 2026) |
| Shipbob Inventory Run Rate | `src/shipbob_inventory_run_rate/main.py` | EWMA run rate + days on hand per SKU | `--start_date / --end_date` |
| Katana Raw Material Status | `src/katana_raw_material_status/main.py` | RM replenishment status vs open MOs | `--partition_date` (added Jun 2026) |
| Katana Raw Material Run Rate | `src/raw_material_run_rate/main.py` | RM daily consumption rate from BOM x run rate | `--partition_date` (added Jun 2026) |
| Prymal Agent Report Automation | `src/prymal_agent/main.py` | Curated tables in prymal_agent DB | `--partition_date` (built-in) |
| Sku Mgmt Alerts | `src/alerts/main.py` | SNS alerts only — no data written | N/A |

## S3 Partition Paths
- `shipbob/order_details/order_date=YYYY-MM-DD/`
- `shipbob/inventory_details/partition_date=YYYY-MM-DD/`
- `shipbob/inventory_run_rate/partition_date=YYYY-MM-DD/`
- `shopify/orders/year=Y/month=M/day=D/`
- `shopify/line_items/year=Y/month=M/day=D/`
- `shopify/active_variant_sku_details/partition_date=YYYY-MM-DD/`
- `katana/raw_material_status/partition_date=YYYY-MM-DD/`
- `katana/raw_material_run_rate/partition_date=YYYY-MM-DD/`
- `staging/prymal_agent/<table>/partition_date=YYYY-MM-DD/` (prymal_agent staging)

## Backfill Dependency Order
1. Shopify Order Details (independent)
2. Shipbob Order Details (independent)
3. Shopify Active Variant SKU Details (snapshot — writes current data to target partition)
4. Shipbob Inventory Details (snapshot — writes current data to target partition)
5. Shipbob Inventory Run Rate (requires 1–4; falls back to MAX(partition_date) for inventory)
6. Katana Raw Material Status (requires Katana source tables; uses MAX from katana_inventory + katana_open_manufacturing_orders)
7. Katana Raw Material Run Rate (requires 5 + Katana sources)
8. Prymal Agent Report Automation (requires 4 + 5)
9. Sku Mgmt Alerts — no backfill needed

## Key Design Notes
- All jobs that previously hardcoded `today` as partition date had `--partition_date` added in Jun 2026 to enable backfill
- Snapshot-only jobs (Inventory Details, Active SKUs) cannot recover historical API state; `--partition_date` only controls output partition label
- Shipbob Inventory Run Rate gracefully falls back to `MAX(partition_date) <= target_date` if exact inventory snapshot is missing
- All jobs call `MSCK REPAIR TABLE` after writing to keep Athena partitions current
- Prymal Agent runner uses a 6-step staging pattern: DDL → clear staging → drop staging table → CREATE AS SELECT → drop final partition → add final partition

**Why:** Recorded after full audit of all 9 workflow scripts (Jun 2026). Future changes to backfill behavior should update the arg names in this table.
