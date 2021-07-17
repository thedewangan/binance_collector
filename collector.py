import ccxt
from datetime import datetime
import math
from pprint import pprint
import asyncio
import ccxt.async_support as ccxt
import pytz
import threading, time
import mysql.connector
from asyncio.tasks import sleep
from processor import data_processor
import json
import getpass
import sys
import logging
import setproctitle


config_file = "config.test.json" if(len(sys.argv)>2 and str(sys.argv[2]) == "test") else "config.json"

with open(config_file) as json_data_file:
    config = json.load(json_data_file)

logging.basicConfig(filename=config['collector_log_file'],  level=logging.INFO)
# to change password getting mechanism
db_pass = str(sys.argv[1])
setproctitle.setproctitle("collector.py")

exchange = ccxt.binance()
exchange.enableRateLimit = True
exchange.rateLimit = config['exchange_ratelimit']
# what should be good timeout value ?
exchange.timeout = config['exchange_timeout']

dbcon= config['mysql']
conn = mysql.connector.connect(user=dbcon['user'], password=db_pass, host=dbcon['host'], database=dbcon['database'])
cursor = conn.cursor()

INTERVAL_IN_SEC = config['collector_interval_seconds']

# returns latest minute since epoch in millis
def get_cur_min():
    s = math.floor(datetime.now(pytz.timezone('utc')).timestamp())
    s = s - s%60
    ms = s*1000
    return ms

#-------------------------------------------------------------------------------

async def minute_collect_market(symbol, start):
    try:
        ans = await exchange.fetchOHLCV(symbol, timeframe='1m', since=start, limit=1)
        if(len(ans) and len(ans[0])==6):
            t = ans[0][0]
            timestamp = datetime.utcfromtimestamp(float(t)/1000)
            # print(timestamp)
            query = "INSERT INTO one_min VALUES (%s, %s, %s, %s, %s, %s, %s)"
            data = (timestamp, symbol, ans[0][1], ans[0][2], ans[0][3], ans[0][4], ans[0][5])
            try:
                cursor.execute(query, data)
                conn.commit()
            except:
                conn.rollback()
            # print(symbol, end = " ")
            # pprint(ans)
            # print()
    except:
        return

#-------------------------------------------------------------------------------

async def minute_collect_all(start):
    tasks = []
    market_limit = config['market_limit']
    for symbol in exchange.symbols[:market_limit]:
        tasks.append(minute_collect_market(symbol, start))
    await asyncio.gather(*tasks)

#-------------------------------------------------------------------------------

async def collector(params):
    min_in_millis = get_cur_min()
    if(params['running'] == False):
            await exchange.load_markets()
            params['running'] = True
            min_in_millis = get_cur_min()
            params['service_start_time'] = min_in_millis
            f = open("binance_collector_info","w+")
            f.write(str(min_in_millis))                 # minute of first request / service start time
            f.write("\n"+str(len(exchange.symbols)))    # number of markets present in exchange
            # actual number of markets was greater than reuqest limit per second !?
            f.close()
        
    start = min_in_millis - 60000
    uptime_in_min = (min_in_millis - params['service_start_time'])/60000

    start_str = ''.join(datetime.utcfromtimestamp(min_in_millis/1000).strftime("%m/%d/%Y, %H:%M:%S"))
    logging.info("Starting collection: " + start_str)

    await minute_collect_all(start)
    minutes = min_in_millis/60000
    # async support for databases ?
    if(minutes % 5 == 0 and uptime_in_min >= 5):
        logging.info("Calling 5 min processor")
        data_processor(5, minutes, conn)
    if(minutes % 15 == 0 and uptime_in_min >= 15):
        logging.info("Calling 15 min processor")
        data_processor(15, minutes, conn)
    if(minutes % 30 == 0 and uptime_in_min >= 30):
        logging.info("Calling 30 min processor")
        data_processor(30, minutes, conn)
    if(minutes % 60 == 0 and uptime_in_min >= 60):
        logging.info("Calling 60 min processor")
        data_processor(60, minutes, conn)
    
#-------------------------------------------------------------------------------

def starter(params):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(collector(params))

    
#-------------------------------------------------------------------------------

ticker = threading.Event()
params = {'running': False}  

WAIT_TIME = 0
while not ticker.wait(WAIT_TIME):
    WAIT_TIME = INTERVAL_IN_SEC
    starter(params)