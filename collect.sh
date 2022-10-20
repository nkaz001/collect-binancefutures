#!/bin/bash

if [ $# -eq 0 ]; then
    echo "collect.sh EXCHANGE SYMBOLS OUTPUT_PATH"
    echo "example: collect.sh binancefutures btcusdt,ethusdt /mnt/data"
    exit 1
fi

python3 collect/main.py $1 $2 $3