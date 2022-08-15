#!/bin/bash

if [ $# -eq 0 ]; then
    echo "convert.sh SRC_FILENAME DST_PATH"
    echo "example: convert.sh /mnt/data/btcusdt_20220812.dat /mnt/data"
    exit 1
fi

python3 convert/convert.py $1 $2