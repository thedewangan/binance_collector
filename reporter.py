import smtplib, ssl
import getpass
import threading
import math
from datetime import datetime
from time import strftime
import pytz
import mysql.connector
import json
import sys
import setproctitle


with open("config.json") as json_data_file:
    config = json.load(json_data_file)


INTERVAL_IN_SEC = config['reporter_interval_seconds']

f = open("binance_collector_info", "r")

# start time in millis
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

# returns latest minute since epoch in millis
def get_cur_min():
    s = math.floor(datetime.now(pytz.timezone('utc')).timestamp())
    s = s - s%60
    ms = s*1000
    return ms

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
    print("Period: ", period, "\tStart: ", start, "\tEnd: ", end)

    cursor = params['conn'].cursor()
    table_name = get_table_name(period)
    query = "SELECT COUNT(time) FROM " + table_name + " WHERE time BETWEEN %s AND %s "
    data = (start, end)
    try:

        cursor.execute(query, data)
        result = cursor.fetchone()
    except:
        return -1
    return result[0]

#-------------------------------------------------------------------------------

def send_report(conn):

    min_in_millis = get_cur_min() 
    min_in_millis = min_in_millis - (min_in_millis % (config['reporting_time_divisor']*1000))

    # uptime in millis
    uptime = min_in_millis - start_time
    
    # expected count per market
    expected = {}
    periods = [5, 15, 30, 60]
    for p in periods:
        expected[p] = INTERVAL_IN_SEC/(p*60)  

    params = {
        'conn': conn,
        'end_time': min_in_millis
    }
    data = {
        'Number of markets present in exchange': market_total_count,
        'Number of markets after limiting':  market_limit,
        'Number of data points expected in last one interval for 5 min': expected[5]*market_limit,
        'Number of data points expected in last one interval for 15 min': expected[15]*market_limit,
        'Number of data points expected in last one interval for 30 min': expected[30]*market_limit,
        'Number of data points expected in last one interval for 1 hour': expected[60]*market_limit,
        'Percentage of data points available for 5 min': get_count(5, params)/(expected[5]*market_limit)*100,
        'Percentage of data points available for 15 min': get_count(15, params)/(expected[15]*market_limit)*100,
        'Percentage of data points available for 30 min': get_count(30, params)/(expected[30]*market_limit)*100,
        'Percentage of data points available for 1 hour': get_count(60, params)/(expected[60]*market_limit)*100,
    }

    body = json.dumps(data, indent=2)

    report_time = datetime.utcfromtimestamp(min_in_millis/1000).strftime("%m/%d/%Y, %H:%M:%S"),
    report_time = ''.join(report_time)
    subject = "Binance Report UTC: " + report_time
    message = 'Subject: {}\n\n{}'.format(subject, body)

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
        try:
            for receiver in receivers:
                print("Time: ", report_time, "\tSending mail to: ", receiver)
                server.login(sender, mail_pass)
                server.sendmail(sender, receiver, message)
        except:
            return

#-------------------------------------------------------------------------------
ticker = threading.Event()

rem = get_cur_min() % (config['reporting_time_divisor']*1000)
# setting an additional delay as some processing might be happening at current time
WAIT_TIME= INTERVAL_IN_SEC - rem/1000 + config['reporting_delay']

while not ticker.wait(WAIT_TIME):
    WAIT_TIME = INTERVAL_IN_SEC
    dbcon= config['mysql']
    conn = mysql.connector.connect(user=dbcon['user'], password=db_pass, host=dbcon['host'], database=dbcon['database'])
    send_report(conn)
    conn.close()