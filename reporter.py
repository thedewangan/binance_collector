import smtplib, ssl
import math
from datetime import datetime
from time import strftime
import pytz
import mysql.connector
import json
import sys
import logging
import asyncio
from util import *
import setproctitle

config_file = "config.test.json" if(len(sys.argv)>3 and str(sys.argv[3]) == "test") else "config.json"

with open(config_file) as json_data_file:
    config = json.load(json_data_file)

logging.basicConfig(filename=config['reporter_log_file'],  level=logging.INFO)
INTERVAL_IN_SEC = config['reporter_interval_seconds']

# start time in millis
f = open("binance_collector_info", "r")
start_time = int(f.readline()) 
market_total_count = int(f.readline())   
f.close()

market_limit = min(config['market_limit'], market_total_count)
port = config['port']

# to change password getting mechanism
db_pass = str(sys.argv[1])
mail_pass = str(sys.argv[2])
setproctitle.setproctitle("reporter.py")

smtp_server = config['smtp_server']
sender = config['sender_email']
receivers = config['receiver_emails']

#-------------------------------------------------------------------------------

def get_table_name(period):
    return {
        5: "five_min",
        15: "fifteen_min",
        30: "thirty_min",
        60: "one_hour"
    }.get(period)

#-------------------------------------------------------------------------------

def get_count(period, params):

    end_time = params['end_time']
    start_time = end_time - INTERVAL_IN_SEC*1000
    end_time -= 60*1000
    # subtracting 1 min from end since SQL between is inclusive and we want exlusive

    end = datetime.utcfromtimestamp(end_time/1000)
    start = datetime.utcfromtimestamp(start_time/1000)
    
    start_str = ''.join(start.strftime("%Y-%m-%d %H:%M:%S"))
    end_str = ''.join(end.strftime("%Y-%m-%d %H:%M:%S"))

    # print("Period: ", period, "\tStart: ", start_str, "\tEnd: ", end_str)
    logging.info("Period: " + str(period) + "\tStart: " + start_str + "\tEnd: " + end_str)

    table_name = get_table_name(period)
    query = "SELECT COUNT(time) FROM " + table_name + " WHERE time BETWEEN %s AND %s "
    data = (start, end)
    try:
        cursor = params['conn'].cursor()
        cursor.execute(query, data)
        result = cursor.fetchone()
    except Exception as e:
        logging.error("Reporter {}".format(type(e).__name__))
        logging.error("Reporter {}".format(e))
        return 0
    return result[0]

#-------------------------------------------------------------------------------

async def send_report():

    min_in_millis = get_cur_min() 
    min_in_millis = min_in_millis - (min_in_millis % (config['reporting_time_divisor']*1000))

    # uptime in millis
    uptime = min_in_millis - start_time
    
    # expected count per market
    expected = {}
    periods = [5, 15, 30, 60]
    for p in periods:
        expected[p] = INTERVAL_IN_SEC/(p*60)  
    
    dbcon= config['mysql']
    conn = mysql.connector.connect(user=dbcon['user'], password=db_pass, host=dbcon['host'], database=dbcon['database'])
    params = {
        'conn': conn,
        'end_time': min_in_millis
    }
    p1 = round(get_count(5, params)/(expected[5]*market_limit)*100, 2)
    p2 = round(get_count(15, params)/(expected[15]*market_limit)*100, 2)
    p3 = round(get_count(30, params)/(expected[30]*market_limit)*100, 2)
    p4 = round(get_count(60, params)/(expected[60]*market_limit)*100, 2)
    conn.close()

    data = {
        'Number of markets present in exchange': market_total_count,
        'Number of markets after limiting':  market_limit,
        'Number of data points expected in last one interval for 5 min': expected[5]*market_limit,
        'Number of data points expected in last one interval for 15 min': expected[15]*market_limit,
        'Number of data points expected in last one interval for 30 min': expected[30]*market_limit,
        'Number of data points expected in last one interval for 1 hour': expected[60]*market_limit,
        'Percentage of data points available for 5 min': p1,
        'Percentage of data points available for 15 min': p2,
        'Percentage of data points available for 30 min': p3,
        'Percentage of data points available for 1 hour': p4
    }

    report_time = datetime.utcfromtimestamp(min_in_millis/1000).strftime("%Y-%m-%d %H:%M:%S"),
    report_time = ''.join(report_time)
    subject = "Binance Report UTC: " + report_time
    body = json.dumps(data, indent=2)
    message = 'Subject: {}\n\n{}'.format(subject, body)

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
        for receiver in receivers:
            # print("Time: ", report_time, "\tSending mail to: ", receiver)
            logging.info("Report till: " + report_time + "\tSending mail to: " + receiver)
            try:
                server.login(sender, mail_pass)
                server.sendmail(sender, receiver, message)
            except Exception as e:
                logging.error("Reporter {}".format(type(e).__name__))
                logging.error("Reporter {}".format(e))

#-------------------------------------------------------------------------------

async def time_manager():
    logging.info("Starting reporter " + get_cur_min_str())

    rem = get_cur_min() % (config['reporting_time_divisor']*1000)
    wait_time = INTERVAL_IN_SEC - rem/1000 + config['reporting_delay'] + config['initial_delay']
    # set intial delay eg 60 min to ensure atleast one 60 min processor was called before reporting
    # set reporting delay to >= 1 min to ensure processing was finished before reporting

    await asyncio.sleep(wait_time)
    while True:
        logging.info("Awaking reporter " + get_cur_min_str())
        await asyncio.gather(asyncio.sleep(INTERVAL_IN_SEC), send_report())

#-------------------------------------------------------------------------------

loop = asyncio.get_event_loop()
loop.run_until_complete(time_manager())


