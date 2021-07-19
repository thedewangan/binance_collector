import mysql.connector
import json
import getpass
import sys

config_file = "config.test.json" if(len(sys.argv)>1 and str(sys.argv[1]) == "test") else "config.json"

with open(config_file) as json_data_file:
    config = json.load(json_data_file)

dbcon= config['mysql']
db_pass = getpass.getpass("Enter mysql password: ")

conn = mysql.connector.connect(user=dbcon['user'],  password=db_pass, host=dbcon['host'], database=dbcon['database'])
cursor = conn.cursor()

cursor.execute("DROP TABLE IF EXISTS one_min")
cursor.execute("DROP TABLE IF EXISTS five_min")
cursor.execute("DROP TABLE IF EXISTS fifteen_min")
cursor.execute("DROP TABLE IF EXISTS thirty_min")
cursor.execute("DROP TABLE IF EXISTS one_hour")

# ONE_MIN
sql ='''CREATE TABLE one_min(
   time TIMESTAMP NOT NULL,
   market VARCHAR(255),
   open DOUBLE,
   high DOUBLE,
   low DOUBLE,
   close DOUBLE,
   volume DOUBLE
)'''
cursor.execute(sql)

# FIVE_MIN
sql ='''CREATE TABLE five_min(
   time TIMESTAMP NOT NULL,
   market VARCHAR(255),
   open DOUBLE,
   high DOUBLE,
   low DOUBLE,
   close DOUBLE,
   volume DOUBLE,
   PRIMARY KEY (time, market)
)'''
cursor.execute(sql)

# FIFTEEN_MIN
sql ='''CREATE TABLE fifteen_min(
   time TIMESTAMP NOT NULL,
   market VARCHAR(255),
   open DOUBLE,
   high DOUBLE,
   low DOUBLE,
   close DOUBLE,
   volume DOUBLE,
   PRIMARY KEY (time, market)
)'''
cursor.execute(sql)

# THIRTY_MIN
sql ='''CREATE TABLE thirty_min(
   time TIMESTAMP NOT NULL,
   market VARCHAR(255),
   open DOUBLE,
   high DOUBLE,
   low DOUBLE,
   close DOUBLE,
   volume DOUBLE,
   PRIMARY KEY (time, market)
)'''
cursor.execute(sql)

# ONE_HOUR
sql ='''CREATE TABLE one_hour(
   time TIMESTAMP NOT NULL,
   market VARCHAR(255),
   open DOUBLE,
   high DOUBLE,
   low DOUBLE,
   close DOUBLE,
   volume DOUBLE,
   PRIMARY KEY (time, market)
)'''
cursor.execute(sql)

conn.close()
