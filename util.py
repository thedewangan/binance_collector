import mysql.connector, time
from datetime import datetime
import pytz
import math

#-------------------------------------------------------------------------------
# returns latest minute since epoch in millis
def get_cur_min():
    s = math.floor(datetime.now(pytz.timezone('utc')).timestamp())
    s = s - s%60
    ms = s*1000
    return ms

#-------------------------------------------------------------------------------

def get_cur_min_str():
    cur_time = datetime.utcfromtimestamp(get_cur_min()/1000).strftime("%Y-%m-%d %H:%M:%S"),
    return ''.join(cur_time)

#-------------------------------------------------------------------------------

def create_connection(dbcon, db_pass):
    retry_count = 0
    while retry_count < 5:
        try:
            conn = mysql.connector.connect(user=dbcon['user'], password=db_pass, host=dbcon['host'], database=dbcon['database'])
            return conn
        except Exception as e:
            retry_count += 1
            if(retry_count == 5):
                raise e
            time.sleep(1)