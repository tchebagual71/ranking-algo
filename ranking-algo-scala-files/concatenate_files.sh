#!/bin/bash

# Create the concatenated_files directory if it doesn't exist
mkdir -p concatenated_files

count=0
file_number=1

for file in *.scala; do
    if (( count % 10 == 0 )); then
        if (( count > 0 )); then
            file_number=$((file_number + 1))
        fi
        current_file="concatenated_files/concat_$file_number.scala"
    fi
    cat "$file" >> "$current_file"
    count=$((count + 1))
done
