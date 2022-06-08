#!/bin/bash

if [ $# -eq 0 ]; then
    echo "collect.sh SYMBOLS OUTPUT_PATH"
    echo "example: collect.sh btcusdt,ethusdt /mnt/data"
    exit 1
fi

python collect/main.py $1 $2
