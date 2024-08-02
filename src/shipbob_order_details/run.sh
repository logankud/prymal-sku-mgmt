#!/bin/bash

echo "-------- Running run.sh"

echo "-------- Input parameters: $@"
echo "-------- Parameter count $#"

# Check if start and end dates are provided
if [[ $# -eq 0 ]]; then      # if count of input params = 0 
    # No arguments, run job for yesterday
    python3 src/shipbob_order_details/main.py
elif [[ $# -eq 2 ]]; then      # if count of input params = 4 (--start_date X and --end_date Y) 
    # Start and end dates provided, run backfill
    start_date=$1
    end_date=$2
    echo python3 src/shipbob_order_details/main.py $start_date $end_date
    python3 src/shipbob_order_details/main.py --start_date $start_date --end_date $end_date
else
    echo "Usage: run.sh $@"

    exit 1
fi
