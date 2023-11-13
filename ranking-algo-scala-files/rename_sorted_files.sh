#!/bin/bash

cd concatenated_files

# Initialize a counter for the new file names
counter=1

# List files starting with 'concatenated_', sort them by size, and process them in sorted order
for file in $(ls -S concatenated_*.scala)
do
    # Extract the original file numbers
    original_numbers=$(echo $file | grep -oP 'concatenated_\K.*(?=.scala)')

    # Rename the file with the new counter while keeping the original file numbers
    mv "$file" "renamed_${counter}_original_${original_numbers}.scala"
    echo "Renamed $file to renamed_${counter}_original_${original_numbers}.scala"

    # Increment the counter
    ((counter++))
done
