# Binance Futures feed collector

## Requirements
Python3: run by `python3` command.  
aiohttp: `pip3 install aiohttp`

## Run
`collect.sh [symbols separated by comma.] [output path]`  
example: `collect.sh btcusdt,ethusdt /mnt/data`

**AWS tokyo region is recommended to minimize latency.**


# Converter: feed data to Pandas Dataframe pickle file
## Requirements
Python3: run by `python3` command.  
pandas: `pip3 install pandas`

## Run
`convert.sh [input file] [output path]`  
example: `convert.sh /mnt/data/btcusdt_20220811.dat /mnt/data`
