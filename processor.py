from datetime import datetime

def process(query_result, md):
    #inp is query result while md is a dict
    for row in query_result:
        market = row[1]
        if(md.__contains__(market) == False):
            md[market] = list(row)
        else:
            md[market][3] = max(md[market][3], row[3])  # high  
            md[market][4] = min(md[market][4], row[4])  # low
            md[market][5] = row[5]                      # close
            md[market][6] += row[6]                     # volume

#-------------------------------------------------------------------------------

def get_table_name(period):
    return {
        5: ("one_min", "five_min"),
        15: ("five_min", "fifteen_min"),
        30: ("fifteen_min", "thirty_min"),
        60: ("thirty_min", "one_hour")
    }.get(period)

#-------------------------------------------------------------------------------

def data_processor(period, cur_min, conn):
    print("INSIDE DATA PROCESSOR %s ", period)
    (in_table, out_table) = get_table_name(period)
    cursor = conn.cursor()

    start = datetime.utcfromtimestamp((cur_min-period)*60)
    end = datetime.utcfromtimestamp((cur_min-1)*60)

    query = "SELECT * FROM " + in_table + " WHERE time BETWEEN %s AND %s order by time"
    data = (start, end)
    try:
        cursor.execute(query, data)
    except:
        return
    result = cursor.fetchall()
    d = {}
    process(result, d)
    query = "INSERT INTO " + out_table + " VALUES (%s, %s, %s, %s, %s, %s, %s)"
    for market, row in d.items():
        try:
            cursor.execute(query, tuple(row))
            conn.commit()
        except:
            conn.rollback()


