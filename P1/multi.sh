#!/usr/bin/bash

# Check if wi.txt exists
if [ ! -f "wi.txt" ]; then
    echo "wi.txt not found. Running download.sh..."
    /usr/local/bin/download.sh 
fi

# Count lines containing "Multifamily" (case-insensitive)
count=$(grep -i "Multifamily" wi.txt | wc -l)

# Print the count
echo $count
