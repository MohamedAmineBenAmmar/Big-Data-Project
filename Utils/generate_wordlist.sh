#!/bin/bash

wordlist="large_wordlist.txt"   # Path to the wordlist
target_size=$((1024 * 1024 * 1024)) # 1GB in bytes

# Create an initial file with some content if it doesn't exist
if [ ! -f "$wordlist" ]; then
    echo "Creating initial wordlist file"
    echo "word" > "$wordlist"
fi

current_size=$(stat -c%s "$wordlist")

# Double the file size in each iteration
while [ $current_size -lt $target_size ]; do
    cat "$wordlist" "$wordlist" > temp && mv temp "$wordlist"
    current_size=$(stat -c%s "$wordlist")
done

echo "Generated file $wordlist with size $(du -h $wordlist | cut -f1)"

