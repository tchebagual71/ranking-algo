#!/bin/bash

# Navigate to the directory containing the files
cd concatenated_files

# Loop through the files in steps of 2
for i in {1..26..2}
do
    # Calculate the next file number
    j=$((i + 1))

    # Check if the next file exists
    if [ -f "concat_$j.scala" ]; then
        # Concatenate the pair of files and output to a new file
        cat "concat_$i.scala" "concat_$j.scala" > "concatenated_$i-$j.scala"
        echo "Concatenated files concat_$i.scala and concat_$j.scala into concatenated_$i-$j.scala"
    else
        echo "File concat_$j.scala does not exist. Skipping concatenation with concat_$i.scala."
    fi
done
