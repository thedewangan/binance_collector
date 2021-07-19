import ccxt
from datetime import datetime
from pprint import pprint
import asyncio
import ccxt.async_support as ccxt
from processor import data_processor, set_processor_log_config
from util import *
import json
import sys
import logging
import getpass

config_file = "config.test.json" if(len(sys.argv)>1 and str(sys.argv[1]) == "test") else "config.json"

with open(config_file) as json_data_file:
    config = json.load(json_data_file)

logging.basicConfig(filename=config['collector_log_file'],  level=logging.INFO)
set_processor_log_config(config['collector_log_file'])

exchange = ccxt.binance()
exchange.enableRateLimit = True
exchange.rateLimit = config['exchange_ratelimit']
exchange.timeout = config['exchange_timeout']

dbcon = config['mysql']
db_pass = getpass.getpass("Enter mysql password: ")
INTERVAL_IN_SEC = config['collector_interval_seconds']

#-------------------------------------------------------------------------------

async def minute_collect_market(symbol, start, conn):
    try:
        cursor = conn.cursor()
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
            except Exception as e:
                conn.rollback()
                logging.error("Collector\t" + symbol + "\t{}".format(type(e).__name__))
                logging.error("Collector\t" + symbol + "\t{}".format(e))
        # print(symbol, end = " ")
        # pprint(ans)
        # print()
    except Exception as e:
        logging.error("Collector\t" + symbol + "\t{}".format(type(e).__name__))
        logging.error("Collector\t" + symbol + "\t{}".format(e))

#-------------------------------------------------------------------------------

async def minute_collect_all(start, conn):
    tasks = []
    market_limit = config['market_limit']
    for symbol in exchange.symbols[:market_limit]:
        tasks.append(minute_collect_market(symbol, start, conn))
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
            f.close()
            # actual number of markets was greater than request limit per second !?
            logging.info("Markets loaded")
        
    start = min_in_millis - 60000
    uptime_in_min = (min_in_millis - params['service_start_time'])/60000
    minutes = min_in_millis/60000
    time_str = ''.join(datetime.utcfromtimestamp(min_in_millis/1000).strftime("%Y-%m-%d %H:%M:%S"))
    logging.info("Starting collection: " + time_str)

    try:
        conn = create_connection(dbcon, db_pass)
    except Exception as e:
        logging.error("Collector" + "\t{}".format(type(e).__name__))
        logging.error("Collector" + "\t{}".format(e))
        return

    # async support for databases ?
    await minute_collect_all(start, conn)

    if(minutes % 5 == 0 and uptime_in_min >= 5):
        data_processor(5, minutes, conn)
    if(minutes % 15 == 0 and uptime_in_min >= 15):
        data_processor(15, minutes, conn)
    if(minutes % 30 == 0 and uptime_in_min >= 30):
        data_processor(30, minutes, conn)
    if(minutes % 60 == 0 and uptime_in_min >= 60):
        data_processor(60, minutes, conn)
    if(minutes % 5 == 0 and uptime_in_min >= 5):
        logging.info("All processing finished " + get_cur_min_str())
    conn.close()
#-------------------------------------------------------------------------------

async def time_manager(params):
    while True:
            await asyncio.gather(asyncio.sleep(INTERVAL_IN_SEC), collector(params))
            # note that if collector takes more time than interval, actual wait would be more
            # so ensure that collector does work before interval
            # could not find something like setinterval
            # other options were maintaining end to start interval rather than start to start
#-------------------------------------------------------------------------------

params = {'running': False}  
loop = asyncio.get_event_loop()
loop.run_until_complete(time_manager(params))

