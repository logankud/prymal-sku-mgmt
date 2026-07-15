#!/bin/bash

echo "-------- Running run.sh"
echo "-------- Input parameters: $@"
echo "-------- Parameter count $#"

if [[ $# -eq 0 ]]; then
    # Auto-backfill: detect and fill any missing dates (lookback 7 days)
    MISSING_DATES=$(python3 src/gap_detector.py --table shipbob_order_details --date_col order_date --lookback_days 7 2>/dev/null)
    if [ -n "$MISSING_DATES" ]; then
        echo "-------- Auto-backfill: missing dates detected: $MISSING_DATES"
        for dt in $MISSING_DATES; do
            # ShipBob API treats StartDate/EndDate as midnight boundaries, so
            # end_date must be dt+1 to cover the full day (same as daily job default)
            NEXT=$(date -d "$dt + 1 day" +%Y-%m-%d)
            echo "-------- Backfilling $dt (end_date=$NEXT)"
            python3 src/shipbob_order_details/main.py --start_date $dt --end_date $NEXT
        done
    else
        echo "-------- Auto-backfill: no missing dates in the last 7 days"
    fi

    # Run daily job (yesterday's data)
    python3 src/shipbob_order_details/main.py

elif [[ $# -eq 2 ]]; then
    start_date=$1
    end_date=$2
    echo "-------- Running manual backfill: $start_date to $end_date"
    python3 src/shipbob_order_details/main.py --start_date $start_date --end_date $end_date

else
    echo "Usage: run.sh [start_date end_date]"
    exit 1
fi
