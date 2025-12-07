
#!/bin/bash

echo "-------- Prymal Agent Table Runner --------"

# Default to list action if no arguments provided
if [ $# -eq 0 ]; then
    echo "Usage: ./run.sh [action] [table_name] [partition_date]"
    echo "Actions: create, drop, load, list"
    echo ""
    echo "Listing available tables:"
    python src/prymal_agent/runner.py --action list
    exit 0
fi

# Execute with provided arguments
python src/prymal_agent/runner.py "$@"
