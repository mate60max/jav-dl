#!/bin/bash

dir=.
if [ $1 ]; then
    dir=$1
    echo "dir is $dir"
fi

find $dir -type f >> files.txt
cat files.txt | wc -l