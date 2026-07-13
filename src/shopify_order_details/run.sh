#!/bin/bash

echo "-------- Running run.sh"
echo "-------- Input parameters: $@"
echo "-------- Parameter count $#"

if [[ $# -eq 0 ]]; then
    # Auto-backfill: detect and fill any missing dates (lookback 7 days)
    # shopify_orders table uses year/month/day partition columns — gap_detector handles this automatically
    MISSING_DATES=$(python3 src/gap_detector.py --table shopify_orders --lookback_days 7 2>/dev/null)
    if [ -n "$MISSING_DATES" ]; then
        echo "-------- Auto-backfill: missing dates detected: $MISSING_DATES"
        for dt in $MISSING_DATES; do
            echo "-------- Backfilling $dt"
            python3 src/shopify_order_details/main.py --start_date $dt --end_date $dt
        done
    else
        echo "-------- Auto-backfill: no missing dates in the last 7 days"
    fi

    # Run daily job (yesterday's data)
    python3 src/shopify_order_details/main.py

elif [[ $# -eq 2 ]]; then
    start_date=$1
    end_date=$2
    echo "-------- Running manual backfill: $start_date to $end_date"
    python3 src/shopify_order_details/main.py --start_date $start_date --end_date $end_date

else
    echo "Usage: run.sh [start_date end_date]"
    exit 1
fi
