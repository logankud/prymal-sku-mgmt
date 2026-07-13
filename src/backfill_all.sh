#!/bin/bash
# =============================================================================
# backfill_all.sh
# One-time (or on-demand) full backfill across all ingestion jobs.
# Runs jobs in dependency order; skips dates already present in Athena.
#
# Usage:
#   src/backfill_all.sh                  # default: 30-day lookback
#   src/backfill_all.sh 45               # custom lookback in days
# =============================================================================

set -euo pipefail

LOOKBACK=${1:-30}
echo "========================================================"
echo " Prymal full backfill — lookback: ${LOOKBACK} days"
echo " $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "========================================================"

# ── helper: log a section header ─────────────────────────────────────────────
section() { echo; echo "-------- $*"; }

# ── helper: backfill a --partition_date job ───────────────────────────────────
backfill_partition() {
    local table=$1
    local script=$2
    section "Gap check: $table (lookback=${LOOKBACK}d)"
    MISSING=$(python3 src/gap_detector.py --table "$table" --date_col partition_date --lookback_days "$LOOKBACK" 2>/dev/null || true)
    if [ -n "$MISSING" ]; then
        echo "  Missing dates: $MISSING"
        for dt in $MISSING; do
            echo "  → backfilling $dt"
            python3 "$script" --partition_date "$dt"
        done
    else
        echo "  No gaps found."
    fi
}

# ── helper: backfill a --start_date/--end_date inclusive job ─────────────────
backfill_inclusive() {
    local table=$1
    local date_col=$2
    local script=$3
    section "Gap check: $table (lookback=${LOOKBACK}d)"
    MISSING=$(python3 src/gap_detector.py --table "$table" --date_col "$date_col" --lookback_days "$LOOKBACK" 2>/dev/null || true)
    if [ -n "$MISSING" ]; then
        echo "  Missing dates: $MISSING"
        for dt in $MISSING; do
            echo "  → backfilling $dt"
            python3 "$script" --start_date "$dt" --end_date "$dt"
        done
    else
        echo "  No gaps found."
    fi
}

# ── helper: backfill shopify_orders (exclusive end_date, y/m/d partitioned) ──
backfill_shopify_orders() {
    section "Gap check: shopify_orders (lookback=${LOOKBACK}d)"
    MISSING=$(python3 src/gap_detector.py --table shopify_orders --lookback_days "$LOOKBACK" 2>/dev/null || true)
    if [ -n "$MISSING" ]; then
        echo "  Missing dates: $MISSING"
        for dt in $MISSING; do
            echo "  → backfilling $dt"
            python3 src/shopify_order_details/main.py --start_date "$dt" --end_date "$dt"
        done
    else
        echo "  No gaps found."
    fi
}

# ── helper: backfill shipbob_inventory_run_rate (exclusive end_date loop) ────
backfill_run_rate() {
    section "Gap check: shipbob_inventory_run_rate (lookback=${LOOKBACK}d)"
    MISSING=$(python3 src/gap_detector.py --table shipbob_inventory_run_rate --date_col partition_date --lookback_days "$LOOKBACK" 2>/dev/null || true)
    if [ -n "$MISSING" ]; then
        echo "  Missing dates: $MISSING"
        for dt in $MISSING; do
            NEXT=$(date -d "$dt + 1 day" +%Y-%m-%d)
            echo "  → backfilling $dt (end_date=$NEXT)"
            python3 src/shipbob_inventory_run_rate/main.py --start_date "$dt" --end_date "$NEXT"
        done
    else
        echo "  No gaps found."
    fi
}

# ── helper: backfill prymal_agent sub-jobs ───────────────────────────────────
backfill_agent() {
    section "Gap check: prymal_agent / shipbob_current_inventory (lookback=${LOOKBACK}d)"
    AGENT_DB=${GLUE_DATABASE_NAME_AGENT:-}
    if [ -z "$AGENT_DB" ]; then
        echo "  WARNING: GLUE_DATABASE_NAME_AGENT not set — skipping prymal_agent backfill"
        return
    fi
    MISSING=$(python3 src/gap_detector.py --table shipbob_current_inventory --date_col partition_date --database "$AGENT_DB" --lookback_days "$LOOKBACK" 2>/dev/null || true)
    if [ -n "$MISSING" ]; then
        echo "  Missing dates: $MISSING"
        for dt in $MISSING; do
            echo "  → backfilling $dt for all prymal_agent sub-jobs"
            python3 src/prymal_agent/main.py --job_dir src/prymal_agent/shipbob_retention_by_cohort_month --partition_date "$dt"
            python3 src/prymal_agent/main.py --job_dir src/prymal_agent/shipbob_daily_order_cnt_by_channel  --partition_date "$dt"
            python3 src/prymal_agent/main.py --job_dir src/prymal_agent/shipbob_orders_obfuscated           --partition_date "$dt"
            python3 src/prymal_agent/main.py --job_dir src/prymal_agent/shipbob_current_inventory           --partition_date "$dt"
        done
    else
        echo "  No gaps found."
    fi
}

# =============================================================================
# Run jobs in dependency order
# =============================================================================

# Tier 1 — raw API pulls (no dependencies between each other)
backfill_partition   shipbob_inventory_details          src/shipbob_inventory_details/main.py
backfill_inclusive   shipbob_order_details   order_date src/shipbob_order_details/main.py
backfill_partition   shopify_active_variant_sku_details src/shopify_active_variant_sku_details/main.py
backfill_shopify_orders
backfill_partition   katana_raw_material_status         src/katana_raw_material_status/main.py

# Tier 2 — depends on Tier 1 outputs
backfill_run_rate                                                   # needs shipbob_order_details + inventory_details
backfill_partition   katana_raw_material_run_rate  src/raw_material_run_rate/main.py   # needs katana_raw_material_status

# Tier 3 — aggregations over Tier 1+2 outputs
backfill_agent                                                      # needs shipbob_inventory_run_rate + order_details

echo
echo "========================================================"
echo " Backfill complete — $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "========================================================"
