#!/bin/bash

if [ $# -eq 0 ]; then
    echo "convert.sh OPTION SRC_FILENAME DST_PATH OPTIONAL:SNAPSHOT"
    echo "example: convert.sh full /mnt/data/btcusdt_20220812.dat /mnt/data"
    exit 1
fi

python3 convert/convert.py "$@"