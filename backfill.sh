#!/bin/bash
set -e

LOGDIR="backfill_logs"
mkdir -p $LOGDIR

MISSING_DATES=(
  "2026-06-17"
  "2026-06-18"
  "2026-06-19"
  "2026-06-20"
  "2026-06-21"
  "2026-06-22"
  "2026-06-23"
  "2026-06-24"
  "2026-06-25"
  "2026-06-26"
  "2026-06-27"
)

START_DATE="2026-06-17"
END_DATE="2026-06-27"

echo "========================================"
echo "BACKFILL START: $(date)"
echo "Dates: $START_DATE to $END_DATE"
echo "========================================"


# ============================================================
# LAYER 1a: Date-range jobs (fetch real historical data from APIs)
# ============================================================

echo ""
echo "--- LAYER 1a: Shopify Order Details ($START_DATE to $END_DATE) ---"
python src/shopify_order_details/main.py \
  --start_date $START_DATE \
  --end_date $END_DATE \
  2>&1 | tee $LOGDIR/shopify_order_details.log
echo "--- Shopify Order Details: DONE ---"

echo ""
echo "--- LAYER 1a: Shipbob Order Details ($START_DATE to $END_DATE) ---"
python src/shipbob_order_details/main.py \
  --start_date $START_DATE \
  --end_date $END_DATE \
  2>&1 | tee $LOGDIR/shipbob_order_details.log
echo "--- Shipbob Order Details: DONE ---"


# ============================================================
# LAYER 1b: Snapshot jobs (stamps current API data into each missing partition)
# NOTE: These write today's snapshot data into historical partitions.
#       This fills partition gaps so downstream joins resolve correctly.
# ============================================================

echo ""
echo "--- LAYER 1b: Shopify Active Variant SKU Details (snapshot per date) ---"
for DATE in "${MISSING_DATES[@]}"; do
  echo "  Running for partition_date=$DATE ..."
  python src/shopify_active_variant_sku_details/main.py \
    --partition_date $DATE \
    2>&1 | tee -a $LOGDIR/shopify_active_variant_sku_details.log
  echo "  Done: $DATE"
done
echo "--- Shopify Active Variant SKU Details: DONE ---"

echo ""
echo "--- LAYER 1b: Shipbob Inventory Details (snapshot per date) ---"
for DATE in "${MISSING_DATES[@]}"; do
  echo "  Running for partition_date=$DATE ..."
  python src/shipbob_inventory_details/main.py \
    --partition_date $DATE \
    2>&1 | tee -a $LOGDIR/shipbob_inventory_details.log
  echo "  Done: $DATE"
done
echo "--- Shipbob Inventory Details: DONE ---"


# ============================================================
# LAYER 2: Shipbob Inventory Run Rate (depends on layers 1a + 1b)
# ============================================================

echo ""
echo "--- LAYER 2: Shipbob Inventory Run Rate ($START_DATE to $END_DATE) ---"
python src/shipbob_inventory_run_rate/main.py \
  --start_date $START_DATE \
  --end_date $END_DATE \
  2>&1 | tee $LOGDIR/shipbob_inventory_run_rate.log
echo "--- Shipbob Inventory Run Rate: DONE ---"


# ============================================================
# LAYER 3: Katana jobs (depend on layer 2)
# ============================================================

echo ""
echo "--- LAYER 3: Katana Raw Material Status (per date) ---"
for DATE in "${MISSING_DATES[@]}"; do
  echo "  Running for partition_date=$DATE ..."
  python src/katana_raw_material_status/main.py \
    --partition_date $DATE \
    2>&1 | tee -a $LOGDIR/katana_raw_material_status.log
  echo "  Done: $DATE"
done
echo "--- Katana Raw Material Status: DONE ---"

echo ""
echo "--- LAYER 3: Katana Raw Material Run Rate (per date) ---"
for DATE in "${MISSING_DATES[@]}"; do
  echo "  Running for partition_date=$DATE ..."
  python src/raw_material_run_rate/main.py \
    --partition_date $DATE \
    2>&1 | tee -a $LOGDIR/katana_raw_material_run_rate.log
  echo "  Done: $DATE"
done
echo "--- Katana Raw Material Run Rate: DONE ---"


# ============================================================
# LAYER 4: Prymal Agent Report Automation (depends on layer 2)
# ============================================================

echo ""
echo "--- LAYER 4: Prymal Agent — shipbob_current_inventory (per date) ---"
for DATE in "${MISSING_DATES[@]}"; do
  echo "  Running for partition_date=$DATE ..."
  python src/prymal_agent/main.py \
    --job_dir src/prymal_agent/shipbob_current_inventory \
    --partition_date $DATE \
    2>&1 | tee -a $LOGDIR/prymal_agent_shipbob_current_inventory.log
  echo "  Done: $DATE"
done
echo "--- Prymal Agent shipbob_current_inventory: DONE ---"

echo ""
echo "--- LAYER 4: Prymal Agent — shipbob_inventory_run_rate (per date) ---"
for DATE in "${MISSING_DATES[@]}"; do
  echo "  Running for partition_date=$DATE ..."
  python src/prymal_agent/main.py \
    --job_dir src/prymal_agent/shipbob_inventory_run_rate \
    --partition_date $DATE \
    2>&1 | tee -a $LOGDIR/prymal_agent_shipbob_inventory_run_rate.log
  echo "  Done: $DATE"
done
echo "--- Prymal Agent shipbob_inventory_run_rate: DONE ---"


echo ""
echo "========================================"
echo "BACKFILL COMPLETE: $(date)"
echo "========================================"
