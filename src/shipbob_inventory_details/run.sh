#!/bin/bash

echo "-------- Running run.sh"

# Auto-backfill: detect and fill any missing dates (lookback 7 days)
MISSING_DATES=$(python3 src/gap_detector.py --table shipbob_inventory_details --date_col partition_date --lookback_days 7 2>/dev/null)
if [ -n "$MISSING_DATES" ]; then
    echo "-------- Auto-backfill: missing dates detected: $MISSING_DATES"
    for dt in $MISSING_DATES; do
        echo "-------- Backfilling $dt"
        python src/shipbob_inventory_details/main.py --partition_date $dt
    done
else
    echo "-------- Auto-backfill: no missing dates in the last 7 days"
fi

# Run daily job (today's snapshot)
python src/shipbob_inventory_details/main.py
