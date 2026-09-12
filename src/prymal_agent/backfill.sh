#!/bin/bash
# Gap-check one partitioned prymal_agent table and re-run its job for every
# missing date in the lookback window.
#
# Usage:
#   src/prymal_agent/backfill.sh <job_dir> <table> <partition_column> [lookback_days]
#
# Only tables whose select_query.sql reads the source partition for
# ${RUN_DATE} should be backfilled this way; a full-recompute report such as
# retention would just relabel today's answer with an old date.
set -euo pipefail

job_dir=$1
table=$2
partition_column=$3
lookback=${4:-7}

if [ -z "${GLUE_DATABASE_NAME_AGENT:-}" ]; then
    echo "-------- WARNING: GLUE_DATABASE_NAME_AGENT is not set - skipping backfill of $table"
    exit 0
fi

# gap_detector exits non-zero if the table does not exist yet (first run);
# treat that as nothing to backfill and let the daily run create it.
missing=$(python3 src/gap_detector.py \
    --table "$table" \
    --date_col "$partition_column" \
    --database "$GLUE_DATABASE_NAME_AGENT" \
    --lookback_days "$lookback" 2>/dev/null || true)

if [ -z "$missing" ]; then
    echo "-------- $table: no missing dates in the last $lookback days"
    exit 0
fi

echo "-------- $table: missing dates: $(echo $missing | tr '\n' ' ')"
for dt in $missing; do
    echo "-------- Backfilling $table for $dt"
    python3 src/prymal_agent/main.py --job_dir "$job_dir" --partition_date "$dt"
done
