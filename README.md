# Original project is https://github.com/nkaz001/hftbacktest/tree/master/collector

#This is a spinoff in order to have a simple subscriber in python

# Binance feed collector
## Requirements
Python3: run by `python3` command.  
aiohttp: `pip3 install aiohttp`

## Run
`collect.sh [exchange] [symbols separated by comma.] [output path]`  
example: `collect.sh binancefutures btcusdt,ethusdt /mnt/data`

## Exchanges  
* **binance**: binance spot  
* **binancefutures**: binance usd(s)-m futures    
* **binancefuturescoin**: binance coin-m futures
example: `collect.sh binancefuturescoin btcusd_perp /mnt/data`   
 

**AWS tokyo region is recommended to minimize latency.**


# Converter: feed data to Pandas Dataframe pickle file
## Requirements
Python3: run by `python3` command.  
pandas: `pip3 install pandas`

## Run
`convert.sh [-s INITIAL_MARKET_DEPTH_SNAPSHOT] [-f] [-c] src_file dst_path

option:  
with -f: including mark price, funding, book ticker streams  
without -f: only market depth and trade streams  
with -c: correct exchange timestamp to be monotonically increased  
  
example:  
`convert.sh /mnt/data/btcusdt_20220811.dat /mnt/data`  
or with the initial market depth snapshot  
`convert.sh /mnt/data/btcusdt_20220811.dat /mnt/data -s /mnt/data/btcusdt_20220810.snapshot.pkl`
  
`/mnt/data/btcusdt_20220810.snapshot.pkl` is End-Of-Day market depth snapshot of 20220810 so it's initial market depth snapshot of 20220811.  
