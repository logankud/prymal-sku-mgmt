
#!/bin/bash

echo "-------- Prymal Agent Table Runner --------"

# 

# Check if table name is provided
if [ -z "$1" ]; then
    echo "Usage: ./run.sh [table_name] [partition_date]"
    echo "       ./run.sh --list    (to see available tables)"
    echo ""
    python src/prymal_agent/runner.py --list
    exit 1
fi

# Run with table name and optional partition date
if [ -z "$2" ]; then
    python src/prymal_agent/runner.py --table "$1"
else
    python src/prymal_agent/runner.py --table "$1" --partition_date "$2"
fi
