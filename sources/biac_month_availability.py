"""
BIAC MONTH AVAILABILITY
====================================
Read Data from biac_availability* (query=-equipment:obw AND lot:6) to compute biac_month_availability. Computed data monthly, weekly and globaly


Listens to:
-------------------------------------

* /topic/BIAC_AVAILABILITY_IMPORTED

Collections:
-------------------------------------

* **biac_month_availability** 

VERSION HISTORY
===============

* 01 Aug 2019 1.0.14 **VME** Bug fixing when month switching
"""   

import json
import time
import uuid
import base64
import threading
import os,logging
import pandas as pd

from logging.handlers import TimedRotatingFileHandler
from amqstompclient import amqstompclient
from datetime import datetime
from datetime import timedelta
from functools import wraps
from elasticsearch import Elasticsearch as ES, RequestsHttpConnection as RC
from logstash_async.handler import AsynchronousLogstashHandler
from lib import pandastoelastic as pte
import numpy as np

import math
from copy import deepcopy
from pandas.io.json import json_normalize


VERSION="1.0.14"
MODULE="BIAC_MONTH_AVAILABILITY"
QUEUE=["/topic/BIAC_AVAILABILITY_IMPORTED"]


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

def update_availability_last_week(last_month, last_week, es):
    print("update avaiability last week - month: " +
          str(last_month)+" week: "+str(last_week))

    script_0 = {"source": "ctx._source.lastWeek=0", "lang": "painless"}
    script_1 = {"source": "ctx._source.lastWeek=1", "lang": "painless"}

    query_0 = {"bool": {"must_not": [{"bool": {"must": [{"term": {"weekOfMonth": {"value": int(last_week)}}},
                                                        {"term": {"year_month": {"value": str(last_month)}}}]}}],
                        "must": [[{"term": {"lastWeek": {"value": 1}}}]]}}

    query_1 = {"bool": {"must": [{"term": {"weekOfMonth": {"value": int(last_week)}}},
                                 {"term": {"year_month": {"value": str(last_month)}}}]}}

    update_query_0 = {}
    update_query_1 = {}

    update_query_0["script"] = script_0
    update_query_1["script"] = script_1

    update_query_0["query"] = query_0
    update_query_1["query"] = query_1

    #print(update_query_0)
    #print(update_query_1)

    es.update_by_query(body=update_query_0, doc_type="doc",
                       index="biac_availability*")

    es.update_by_query(body=update_query_1, doc_type="doc",
                       index="biac_availability*")

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
    if int(month[5:]) > d.month:
        d = d.replace(month=int(month[5:]), day=1) 
    return d

def getDisplayWeek(week, maxWeek, thisMonth, month):
    if thisMonth != month:
        return 0
    elif int(week) == maxWeek:
        return 1
    else:
        return 0

def getPreviousMonth(lastmonth):
    year = int(lastmonth[:4])
    month = int(lastmonth[-2:])
    
    if month == 12:
        month = 1
        year = year-1
    else:
        month -=1
        
    return str(year)+'-'+str(month).zfill(2)

def getRange(availability):
    if availability >= 98:
        return '98to100'
    elif availability >= 75:
        return '75to98'
    elif availability >= 50:
        return '50to75'
    else:
        return '0to50'


def daily_avail_to_week(df_from_es):
    df_from_es['week'] = df_from_es['weekOfYear']
    df_grouped = df_from_es.groupby(['week', 'month', 'equipment', 'category']) \
        .agg({'equipment': 'size', 'value': 'mean', 'weekOfMonth': 'max', 'weekOfYear': 'max'}) \
        .rename(columns={'equipment': 'count', 'value': 'availability', 'weekOfMonth': 'max_week', 'weekOfYear': 'max_week_year'}).reset_index()
    df_grouped['thresh'] = df_grouped.apply(
        lambda row: process_thresh(row), axis=1)
    df_grouped2 = df_grouped.groupby(['week', 'month', 'category']) \
    .agg({'equipment': 'size', 'thresh': 'sum', 'max_week': 'max', 'max_week_year': 'max'}) \
    .rename(columns={}).reset_index()

    df_grouped2['equipment_dbl'] = df_grouped2['equipment']*2
    df_bb = df_grouped2[df_grouped2['category'] == 'BoardingBridge']
    df_other = df_grouped2[df_grouped2['category'] != 'BoardingBridge']

    df_bb['avail'] = df_bb['equipment_dbl'] - df_bb['thresh']
    df_bb['equipment'] = df_bb['equipment_dbl']
    df_other['avail'] = df_other['equipment'] - df_other['thresh']

    df = df_bb.append(df_other)

    df_grouped3 = df.groupby(['week', 'month']) \
        .agg({'equipment': 'sum', 'avail': 'sum', 'max_week': 'max', 'max_week_year': 'max'}) \
        .rename(columns={'avail': 'availability'}).reset_index()

    df_grouped3['availability'] = round(
        (df_grouped3['availability']/df_grouped3['equipment'])*100, 2)
    df_grouped3['equipment'] = 'global'
    df_grouped3

    df_grouped3b = df_grouped[['week', 'month', 'equipment',
                            'category', 'availability', 'max_week', 'max_week_year']]
    df_grouped3b['availability'] = df_grouped3b['availability'].apply(
        lambda x: round(x, 2))

    df_grouped3 = df_grouped3b.append(df_grouped3)
    df_grouped3['category'].fillna('', inplace=True)
    df_grouped3['year_week'] = df_grouped3['week']
    df_grouped3['year_week'] = df_grouped3['year_week'].astype(int)
    df_grouped3['year_week'] = df_grouped3['year_week'].apply(lambda x: str(x))
    df_grouped3.index = df_grouped3['equipment'] + \
        df_grouped3['month'].str.replace('-', '')+'_W' + df_grouped3['year_week'].str.replace('-', '')

    df_grouped3['year'] = df_grouped3['month'].apply(lambda x: x[:4])

    df_grouped3['@timestamp'] = df_grouped3.apply(lambda row: week_to_ts(row['week'], row['month']), axis=1)
    df_grouped3['@timestamp'] = df_grouped3['@timestamp'].apply(
        lambda x: int(x.timestamp()*1000))
    df_grouped3['str_max_week'] = df_grouped3['max_week'].apply(
        get_str_max_week)
    df_grouped3['str_max_week_abs'] = df_grouped3.apply(
        lambda row: get_str_weeks(row), axis=1)

    df_grouped3['str_max_week_abs_fr'] = df_grouped3.apply(
        lambda row: get_str_weeks_fr(row), axis=1)

    df_grouped3['str_range_week'] = df_grouped3.apply(
        lambda row: get_str_range_week(row), axis=1)

    df_grouped3['str_range_week_fr'] = df_grouped3.apply(
        lambda row: get_str_range_week_fr(row), axis=1)


    df_grouped3['type'] = 'equipment'
    df_grouped3.loc[df_grouped3['equipment'] == 'global', 'type'] = 'global'
    df_grouped3[df_grouped3['equipment'] != 'global']
    df_grouped3['interval'] = 'week'
    maxWeek = int(df_grouped3['week'].max())
    now = datetime.now()
    thisMonth = str(now.year)+'-'+str(now.month).zfill(2)

    thisMonth = df_grouped3['month'].max()
    logger.info('this month'*100)
    logger.info(thisMonth)
    #df_grouped3['display'] = df_grouped3['week'].apply(lambda x: getDisplayWeek(x, maxWeek))
    df_grouped3['display'] = df_grouped3.apply(lambda row: getDisplayWeek(row['week'], maxWeek, row['month'], thisMonth), axis=1)

    return df_grouped3




def daily_avail_to_month(df_from_es):
    df_from_es['month'] = pd.to_datetime(df_from_es['@timestamp'], unit='ms')
    df_from_es['month'] = df_from_es['month'].apply(
        lambda x: x.strftime("%Y-%m"))

    df_grouped = df_from_es.groupby(['month', 'equipment', 'category']) \
        .agg({'equipment': 'size', 'value': 'mean', 'weekOfMonth': 'max', 'weekOfYear': 'max'}) \
        .rename(columns={'equipment': 'count', 'value': 'availability', 'weekOfMonth': 'max_week', 'weekOfYear': 'max_week_year'}).reset_index()

    df_grouped['thresh'] = df_grouped.apply(
        lambda row: process_thresh(row), axis=1)

    df_grouped2 = df_grouped.groupby(['month', 'category']) \
        .agg({'equipment': 'size', 'thresh': 'sum', 'max_week': 'max', 'max_week_year': 'max'}) \
        .rename(columns={}).reset_index()

    df_grouped2['equipment_dbl'] = df_grouped2['equipment']*2

    df_bb = df_grouped2[df_grouped2['category'] == 'BoardingBridge']
    df_other = df_grouped2[df_grouped2['category'] != 'BoardingBridge']

    df_bb['avail'] = df_bb['equipment_dbl'] - df_bb['thresh']
    df_bb['equipment'] = df_bb['equipment_dbl']
    df_other['avail'] = df_other['equipment'] - df_other['thresh']

    df = df_bb.append(df_other)

    df_grouped3 = df.groupby(['month']) \
        .agg({'equipment': 'sum', 'avail': 'sum', 'max_week': 'max', 'max_week_year': 'max'}) \
        .rename(columns={'avail': 'availability'}).reset_index()
    df_grouped3

    df_grouped3 = df.groupby(['month']) \
        .agg({'equipment': 'sum', 'avail': 'sum', 'max_week': 'max', 'max_week_year': 'max'}) \
        .rename(columns={'avail': 'availability'}).reset_index()

    df_grouped3['availability'] = round(
        (df_grouped3['availability']/df_grouped3['equipment'])*100, 2)
    df_grouped3['equipment'] = 'global'
    df_grouped3

    df_grouped3b = df_grouped[['month', 'equipment',
                               'category', 'availability', 'max_week', 'max_week_year']]
    df_grouped3b['availability'] = df_grouped3b['availability'].apply(
        lambda x: round(x, 2))
    df_grouped3b

    df_grouped3 = df_grouped3b.append(df_grouped3)
    df_grouped3['category'].fillna('', inplace=True)
    df_grouped3['year_month'] = df_grouped3['month']
    df_grouped3.index = df_grouped3['equipment'] + \
        '_' + df_grouped3['year_month'].str.replace('-', '')

    df_m_y = df_grouped3['month'].str.extract(r'([0-9]{4})-([0-9]{2})')

    del df_grouped3['month']
    df_m_y.columns = ['year', 'month']
    df_grouped3 = df_grouped3.merge(df_m_y, left_index=True, right_index=True)

    df_grouped3['@timestamp'] = pd.to_datetime(
        df_grouped3['year_month'], format='%Y-%m', errors='ignore')
    df_grouped3['@timestamp'] = df_grouped3['@timestamp'].apply(
        lambda x: int(x.timestamp()*1000))
    df_grouped3['str_max_week'] = df_grouped3['max_week'].apply(
        get_str_max_week)
    df_grouped3['str_max_week_abs'] = df_grouped3.apply(
        lambda row: get_str_weeks(row), axis=1)

    df_grouped3['str_max_week_abs_fr'] = df_grouped3.apply(
        lambda row: get_str_weeks_fr(row), axis=1)

    df_grouped3['str_range_week'] = df_grouped3.apply(
        lambda row: get_str_range_week(row), axis=1)

    df_grouped3['str_range_week_fr'] = df_grouped3.apply(
        lambda row: get_str_range_week_fr(row), axis=1)

    df_grouped3['type'] = 'equipment'
    df_grouped3.loc[df_grouped3['equipment'] == 'global', 'type'] = 'global'
    df_grouped3[df_grouped3['equipment'] != 'global']
    df_grouped3['interval'] = 'month'


    df_grouped3['kpi1'] = 0.0

    newdf = df_grouped3[df_grouped3['equipment'] != 'global']
    newdf['range'] = newdf['availability'].apply(lambda x: getRange(x))
    newdf2 = newdf.groupby(['range', 'category', 'year_month']) \
        .agg({'equipment': 'count', 'year': 'max', 'month': 'max', 'str_max_week': 'max','str_max_week_abs': 'max','str_max_week_abs_fr': 'max', 'str_range_week': 'max', 'str_range_week_fr': 'max'}) \
        .reset_index()
    months = newdf2.year_month.unique()

    for month in months:
        monthdf = newdf2[newdf2.year_month == month]
        totaleq = 0
        totBB = 0
        totOther = 0
        totBB98 = 0
        totOther98 = 0
        totBB75 = 0
        totOther75 = 0
        totBB50 = 0
        totOther50 = 0
        totBB0 = 0
        totOther0 = 0
        
        totBB = monthdf[monthdf.category == 'BoardingBridge'].sum()['equipment']
        totOther = monthdf[monthdf.category != 'BoardingBridge'].sum()['equipment']
        totaleq = totBB*2 + totOther
        df75 = monthdf[monthdf.range == '75to98']
        totBB75 = df75[df75.category == 'BoardingBridge'].sum()['equipment']
        totOther75 = df75[df75.category != 'BoardingBridge'].sum()['equipment']
        df50 = monthdf[monthdf.range == '50to75']
        totBB50 = df50[df50.category == 'BoardingBridge'].sum()['equipment']
        totOther50 = df50[df50.category != 'BoardingBridge'].sum()['equipment']
        df0 = monthdf[monthdf.range == '0to50']
        totBB0 = df0[df0.category == 'BoardingBridge'].sum()['equipment']
        totOther0 = df0[df0.category != 'BoardingBridge'].sum()['equipment']
        kpi = ((totaleq) - (totBB75*2+totOther75) - (totBB50*2+totOther50)*3 - (totBB0*2+totOther0)*6 ) / totaleq
        df_grouped3.at['global_' + month.replace('-', ''), 'kpi1'] = kpi*100

    lastmonth = df_grouped3['year_month'].max()
    prevmonth = getPreviousMonth(lastmonth)
    df_grouped3['display'] = df_grouped3['year_month'].apply(lambda x : 1 if x == lastmonth else 0)
    df_grouped3['previousmonth'] = df_grouped3['year_month'].apply(lambda x : 1 if x == prevmonth else 0)


    return df_grouped3




################################################################################
def messageReceived(destination,message,headers):
    global es
    records=0
    starttime = time.time()
    logger.info("==> "*10)
    logger.info("Message Received %s" % destination)
    logger.info(headers)

    now = datetime.now()
    time.sleep(3)

    obj = json.loads(message)

    start_ts = obj['start_ts']
    end_ts = obj['end_ts']

    query = {"query": {"bool": {"must": [{"query_string": {"query": "-equipment:obw AND lot:6", "analyze_wildcard": True}}, {
            "range": {
                "@timestamp": {
                    "gte": start_ts,
                    "lte": end_ts,
                    "format": "epoch_millis"
                }
            }
        }]}}}

    print(query)

    df_ret = es_search_with_scroll(
        es, "biac_availability*", "doc", query, 10000, '2m')

    to_bulk = daily_avail_to_month(df_ret)

    to_bulk2 =daily_avail_to_week(df_ret)


    print(to_bulk2)

    message_body = ''
    reserrors = []
    imported_records =0

    for index, row in to_bulk.iterrows():
        _index = INDEX_PATTERN + "-" + row['year']
        _id = index

        action = {}
        action["index"] = {"_index": _index, "_type": "doc", "_id": _id}
        #action["index"] = {"_index": _index, "_type": "doc"}
        message_body += json.dumps(action)+"\r\n"
        message_body += row.to_json()+"\r\n"

        if len(message_body) > 512000:
            bulkres = es.bulk(message_body)
            message_body = ""

            if(not(bulkres["errors"])):
                    logger.info("BULK done without errors.")
            else:
                for item in bulkres["items"]:
                    if "error" in item["index"]:
                        imported_records -= 1
                        logger.info(item["index"]["error"])
                        reserrors.append(
                            {"error": item["index"]["error"], "id": item["index"]["_id"]})
    
    for index, row in to_bulk2.iterrows():
        _index = INDEX_PATTERN + "-" + row['year']
        _id = index

        action = {}
        action["index"] = {"_index": _index, "_type": "doc", "_id": _id}
        #action["index"] = {"_index": _index, "_type": "doc"}
        message_body += json.dumps(action)+"\r\n"
        message_body += row.to_json()+"\r\n"

        if len(message_body) > 512000:
            bulkres = es.bulk(message_body)
            message_body = ""

            if(not(bulkres["errors"])):
                    logger.info("BULK done without errors.")
            else:
                for item in bulkres["items"]:
                    if "error" in item["index"]:
                        imported_records -= 1
                        logger.info(item["index"]["error"])
                        reserrors.append(
                            {"error": item["index"]["error"], "id": item["index"]["_id"]})



    if message_body:

        #print(bulkres)
        bulkres = es.bulk(message_body)

        if(not(bulkres["errors"])):
                logger.info("BULK done without errors.")
        else:
            for item in bulkres["items"]:
                if "error" in item["index"]:
                    imported_records -= 1
                    logger.info(item["index"]["error"])
                    reserrors.append(
                        {"error": item["index"]["error"], "id": item["index"]["_id"]})


    last_month = to_bulk[to_bulk['equipment'] == 'global']['year_month'].max()
    last_week = to_bulk[to_bulk['year_month'] == last_month]['max_week'].max()

    update_availability_last_week(last_month, last_week, es)




    logger.info("<== "*10)

if __name__ == '__main__':
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


    logger.info("AMQC_URL          :"+os.environ["AMQC_URL"])
    while True:
        time.sleep(5)
        try:
            conn.send_life_sign()
        except Exception as e:
            logger.error("Unable to send life sign.")
            logger.error(e)
    #app.run(threaded=True,host= '0.0.0.0')
