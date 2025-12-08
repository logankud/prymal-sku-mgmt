
#!/bin/bash

echo "-------- Prymal Agent Table Runner --------"

# Check if job_dir is provided
if [ -z "$1" ]; then
    echo "Usage: ./run.sh <job_dir> [partition_date]"
    echo "Example: ./run.sh src/prymal_agent/new_sample"
    echo "Example: ./run.sh src/prymal_agent/new_sample 2025-01-15"
    exit 1
fi

job_dir=$1

# Run with job_dir and optional partition_date
if [ -z "$2" ]; then
    # No partition date provided
    python3 src/prymal_agent/main.py --job_dir "$job_dir"
else
    # Partition date provided
    partition_date=$2
    python3 src/prymal_agent/main.py --job_dir "$job_dir" --partition_date "$partition_date"
fi
