"""
BIAC LOT 5 COMPUTED
====================================
Read Data from biac_availability* (-equipment:obw AND lot:5) to compute biac_month_availability. Computed data monthly, weekly and globaly


Listens to:
-------------------------------------

* /topic/BIAC_AVAILABILITY_LOT5_IMPORTED

Collections:
-------------------------------------

* **biac_month_availability** 

VERSION HISTORY
===============

* 09 Dec 2019 1.0.8 **VME** Bug fixing previous month
* 09 Dec 2019 1.0.9 **VME** Replacing pte by es_helper
"""   


import json
import time
import math
import uuid
import base64
import tzlocal
import threading
import os,logging
import numpy as np
import pandas as pd

from copy import deepcopy
from functools import wraps
from datetime import datetime
from datetime import timedelta
from elastic_helper import es_helper 
from amqstompclient import amqstompclient
from pandas.io.json import json_normalize
from logging.handlers import TimedRotatingFileHandler
from logstash_async.handler import AsynchronousLogstashHandler
from elasticsearch import Elasticsearch as ES, RequestsHttpConnection as RC


VERSION="1.0.9"
MODULE="BIAC_LOT5_COMPUTED"
QUEUE=["/topic/BIAC_AVAILABILITY_LOT5_IMPORTED"]


INDEX_PATTERN = "biac_month_availability"


########### QUERIES BODY #######################



#####################################################

def log_message(message):
    global conn

    message_to_send={
        "message":message,
        "@timestamp":datetime.now().timestamp() * 1000,
        "module":MODULE,
        "version":VERSION
    }
    logger.info("LOG_MESSAGE")
    logger.info(message_to_send)
    conn.send_message("/queue/NYX_LOG",json.dumps(message_to_send))

################################################################################

def process_thresh(row):
    parameters_thresh = [98, 75, 50]
    coef = [0, 1, 3, 6]
    coef_boarding_bridge = 2

    # parameters of the avail formula :
    # if boarding_bridge : Calcul 1 = ((2 * Somme equpts - (2 * Somme equpts >75% et <98%) \
    #                                                   - 2 * 3 * (Somme equpts > 50% et < 75%) \
    #                                                  - 2 * 6 * (Somme equpts < 50% ))
    # else               : Calcul 3 = ((Somme equpts - (Somme equpts >75% et <98%) \
    #                                               - 3 * (Somme equpts > 50% et < 75%) \
    #                                              - 6 * (Somme equpts < 50% ))

    count = 0

    for i in parameters_thresh:
        if row['availability'] > i:
            if row['category'] == 'BoardingBridge':
                return int(coef[count]*coef_boarding_bridge)
            else:
                return int(coef[count])

        count += 1

    if row['category'] == 'BoardingBridge':
        return int(coef[count]*coef_boarding_bridge)
    else:
        return int(coef[count])


def get_str_max_week(num, start=1):
    ret = ''

    for i in range(start, num+1):
        if ret == '':
            ret = 'W'+str(i)
        else:
            ret += '+'+'W'+str(i)

    return ret


def get_str_max_week_fr(num, start=1):
    ret = ''

    for i in range(start, num+1):
        if ret == '':
            ret = 'S'+str(i)
        else:
            ret += '+'+'S'+str(i)

    return ret

def get_str_range_week(row):
    weekOfMonth = row['max_week']
    weekOfYear = row['max_week_year']

    return 'W' + str(weekOfYear-weekOfMonth+1) + ' - W' + str(weekOfYear)

def get_str_range_week_fr(row):
    weekOfMonth = row['max_week']
    weekOfYear = row['max_week_year']

    return 'S' + str(weekOfYear-weekOfMonth+1) + ' - S' + str(weekOfYear)

def get_str_weeks(row):
    weekOfMonth = row['max_week']
    weekOfYear = row['max_week_year']

    return get_str_max_week(weekOfYear, weekOfYear-weekOfMonth+1)

def get_str_weeks_fr(row):
    weekOfMonth = row['max_week']
    weekOfYear = row['max_week_year']

    return get_str_max_week_fr(weekOfYear, weekOfYear-weekOfMonth+1)

def week_to_ts(week, month):
    weekToCalc = month[:4] + '-W'+str(int(week))+'-1'
    d = datetime.strptime(weekToCalc, '%G-W%V-%u')
    return d

def getDisplayWeek(week, maxWeek):
    if int(week) == maxWeek:
        return 1
    else:
        return 0

def getPreviousMonth(lastmonth):
    year = int(lastmonth[:4])
    month = int(lastmonth[-2:])
    
    if month == 1:
        month = 12
        year = year-1
    else:
        month -=1
        
    return str(year)+'-'+str(month).zfill(2)

def es_search_with_scroll(es, index, doc_type, query, size, scroll):
    print('es_search_with_scroll')
    res = es.search(index=index, doc_type=doc_type,
                    size=size, scroll=scroll, body=query)

    sid = res['_scroll_id']
    scroll_size = len(res['hits']['hits'])
    df = pd.DataFrame()
    if scroll_size > 0:

        df = json_normalize(res['hits']['hits'])
        df.set_index('_id', inplace=True)

        while (scroll_size > 0):
            print("Scrolling...")
            res = es.scroll(scroll_id=sid, scroll='2m')
            # Update the scroll ID
            sid = res['_scroll_id']
            # Get the number of results that we returned in the last scroll
            scroll_size = len(res['hits']['hits'])
            print("scroll size: " + str(scroll_size))

            if scroll_size > 0:
                df2 = json_normalize(res['hits']['hits'])
                df2.set_index('_id', inplace=True)

                df = pd.concat([df, df2])

        newcolumns = []
        for column in df.columns:
            newcolumns.append(column.replace("_source.", ""))
        df.columns = newcolumns
    return df

def getKPI(name, equipments, df):
    df_kpi = df[df.equipment.isin(equipments)]
    df_kpi = df_kpi.groupby(['month']) \
            .agg({'@timestamp': 'max', 'category': 'size', 'value': 'mean', 'weekOfMonth': 'max', 'weekOfYear': 'max'}) \
            .rename(columns={'value': 'availability', 'weekOfMonth': 'max_week', 'weekOfYear': 'max_week_year'}).reset_index()
    df_kpi['equipment'] = name
    df_kpi['category'] = 'tri'
    df_kpi['month'] = pd.to_datetime(df_kpi['@timestamp'], unit='ms')
    df_kpi['month'] = df_kpi['month'].apply(
            lambda x: x.strftime("%Y-%m"))
    
    df_grouped = df_kpi.copy()
    df_grouped['week'] = df_grouped['max_week_year']
    df_grouped['year_week'] = df_grouped['week']
    df_grouped['year_week'] = df_grouped['year_week'].astype(int)
    df_grouped['year_week'] = df_grouped['year_week'].apply(lambda x: str(x))
    df_grouped['year_month'] = df_grouped['month']
    df_grouped.index = df_grouped['equipment'] + \
        df_grouped['month'].str.replace('-', '')
        
    #+'_W' + df_grouped['year_week'].str.replace('-', '')

    df_grouped['year'] = df_grouped['month'].apply(lambda x: x[:4])

    #df_grouped['@timestamp'] = df_grouped.apply(lambda row: week_to_ts(row['week'], row['month']), axis=1)
    #df_grouped['@timestamp'] = df_grouped['@timestamp'].apply(
    #   lambda x: int(x.timestamp()*1000))
    df_grouped['str_max_week'] = df_grouped['max_week'].apply(
        get_str_max_week)
    df_grouped['str_max_week_abs'] = df_grouped.apply(
        lambda row: get_str_weeks(row), axis=1)

    df_grouped['str_max_week_abs_fr'] = df_grouped.apply(
        lambda row: get_str_weeks_fr(row), axis=1)

    df_grouped['str_range_week'] = df_grouped.apply(
        lambda row: get_str_range_week(row), axis=1)

    df_grouped['str_range_week_fr'] = df_grouped.apply(
        lambda row: get_str_range_week_fr(row), axis=1)


    df_grouped['type'] = 'equipment'
    df_grouped.loc[df_grouped['equipment'] == 'global', 'type'] = 'global'
    df_grouped[df_grouped['equipment'] != 'global']
    df_grouped['interval'] = 'week'
    maxWeek = int(df_grouped['week'].max())
    #df_grouped['display'] = df_grouped['week'].apply(lambda x: getDisplayWeek(x, maxWeek))
    lastmonth = df_grouped['year_month'].max()
    prevmonth = getPreviousMonth(lastmonth)
    df_grouped['display'] = df_grouped['year_month'].apply(lambda x : 1 if x == lastmonth else 0)
    df_grouped['previousmonth'] = df_grouped['year_month'].apply(lambda x : 1 if x == prevmonth else 0)
    
    df_grouped['year'] = df_grouped['month'].apply(lambda x: x[:4])
    df_grouped['_index'] = df_grouped['year'].apply(lambda x: 'biac_month_availability-'+str(x))
    
    df_grouped['lot'] = 5

    df_grouped['_id'] = df_grouped.index
    
    return df_grouped
    


################################################################################
def messageReceived(destination,message,headers):
    global es
    records=0
    starttime = time.time()
    logger.info("==> "*10)
    logger.info("Message Received %s" % destination)
    logger.info(headers)

    time.sleep(3)

    now = datetime.now()

    local_timezone = tzlocal.get_localzone()
    obj = json.loads(message)

    start_ts = obj['start_ts']
    #end_ts = obj['end_ts']
    end_ts = int(datetime.timestamp(now)+7200)*1000

    query = {"query": {"bool": {"must": [{"query_string": {"query": "-equipment:obw AND lot:5", "analyze_wildcard": True}}, {
            "range": {
                "@timestamp": {
                    "gte": start_ts,
                    "lte": end_ts,
                    "format": "epoch_millis"
                }
            }
        }]}}}


    logger.info(query)


    df_ret = es_search_with_scroll(
            es, "biac_availability*", "doc", query, 10000, '2m')

    df_from_es = df_ret.copy()
    df_from_es['month'] = pd.to_datetime(df_from_es['@timestamp'], unit='ms')
    df_from_es['month'] = df_from_es['month'].apply(
            lambda x: x.strftime("%Y-%m"))

    dfs = []
    dfkpi1 = getKPI('kpi1', ['gth_sorter_a','gth_sorter_b'], df_from_es)
    dfs.append(dfkpi1)

    dfkpi2 = getKPI('kpi2', ['gth_oog'], df_from_es)
    dfs.append(dfkpi2)
    dfkpi3a = getKPI('kpi3a', ['gth_reclaim'], df_from_es)
    dfs.append(dfkpi3a)
    dfkpi3b = getKPI('kpi3b', ['gth_transfer_inbound', 'gtha inbound infeed area'], df_from_es)
    dfs.append(dfkpi3b)
    kpi4 = getKPI('kpi4', ['gth_belt_island'], df_from_es)
    dfs.append(kpi4)
    kpi5 = getKPI('kpi5', ['gth_checkin_rows'], df_from_es)
    dfs.append(kpi5)

    for df in dfs:
        es_helper.dataframe_to_elastic(es, df)

    logger.info("<== "*10)





logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(module)s - %(funcName)s: %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger()

lshandler=None

if os.environ["USE_LOGSTASH"]=="true":
    logger.info ("Adding logstash appender")
    lshandler=AsynchronousLogstashHandler("logstash", 5001, database_path='logstash_test.db')
    lshandler.setLevel(logging.ERROR)
    logger.addHandler(lshandler)

handler = TimedRotatingFileHandler("logs/"+MODULE+".log",
                                when="d",
                                interval=1,
                                backupCount=30)

logFormatter = logging.Formatter('%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s')
handler.setFormatter( logFormatter )
logger.addHandler(handler)

logger.info("==============================")
logger.info("Starting: %s" % MODULE)
logger.info("Module:   %s" %(VERSION))
logger.info("==============================")


#>> AMQC
server={"ip":os.environ["AMQC_URL"],"port":os.environ["AMQC_PORT"]
                ,"login":os.environ["AMQC_LOGIN"],"password":os.environ["AMQC_PASSWORD"]
                ,"heartbeats":(120000,120000),"earlyack":True}
logger.info(server)
conn=amqstompclient.AMQClient(server
    , {"name":MODULE,"version":VERSION,"lifesign":"/topic/NYX_MODULE_INFO"},QUEUE,callback=messageReceived)
#conn,listener= amqHelper.init_amq_connection(activemq_address, activemq_port, activemq_user,activemq_password, "RestAPI",VERSION,messageReceived)
connectionparameters={"conn":conn}

#>> ELK
es=None
logger.info (os.environ["ELK_SSL"])

if os.environ["ELK_SSL"]=="true":
    host_params = {'host':os.environ["ELK_URL"], 'port':int(os.environ["ELK_PORT"]), 'use_ssl':True}
    es = ES([host_params], connection_class=RC, http_auth=(os.environ["ELK_LOGIN"], os.environ["ELK_PASSWORD"]),  use_ssl=True ,verify_certs=False)
else:
    host_params="http://"+os.environ["ELK_URL"]+":"+os.environ["ELK_PORT"]
    es = ES(hosts=[host_params])


if __name__ == '__main__':
    logger.info("AMQC_URL          :"+os.environ["AMQC_URL"])
    while True:
        time.sleep(5)
        try:
            conn.send_life_sign()
        except Exception as e:
            logger.error("Unable to send life sign.")
            logger.error(e)
    #app.run(threaded=True,host= '0.0.0.0')
