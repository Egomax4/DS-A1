#!/bin/bash

# Check if correct number of arguments are provided
if [ "$#" -ne 2 ]; then
    echo "Error: Missing arguments."
    echo "Usage: ./run_pagerank.sh <input_file> <output_file>"
    exit 1
fi

INPUT_FILE=$1
OUTPUT_FILE=$2

echo "Starting Page Rank Job..."
echo "Input: $INPUT_FILE | Output: $OUTPUT_FILE"

# Run the python script
python3 main.py "$INPUT_FILE" "$OUTPUT_FILE"

if [ $? -eq 0 ]; then
    echo "Job Completed Successfully."
else
    echo "Job Failed."
    exit 1
fi