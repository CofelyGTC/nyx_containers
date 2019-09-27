"""
OPTIBOX COMPUTED
====================================

Collections:
-------------------------------------


VERSION HISTORY
===============

* 26 Sep 2019 0.0.1 **PDE** First version


"""  
import re
import sys
import json
import time
import uuid
import pytz
import base64
import tzlocal
import platform
import requests
import traceback
import threading
import os,logging
import numpy as np
import pandas as pd
from elastic_helper import es_helper 
from dateutil.tz import tzlocal
from tzlocal import get_localzone

from functools import wraps
import datetime as dt
from datetime import datetime
from datetime import timedelta
#from lib import pandastoelastic as pte
from amqstompclient import amqstompclient
from logging.handlers import TimedRotatingFileHandler
from logstash_async.handler import AsynchronousLogstashHandler
from elasticsearch import Elasticsearch as ES, RequestsHttpConnection as RC

import collections
import dateutil.parser


containertimezone=pytz.timezone(get_localzone().zone)

MODULE  = "GTC_OPTIBOX_COMPUTED"
VERSION = "0.0.1"
QUEUE   = ["GTC_OPTIBOX_COMPUTED_RANGE"]

class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, dt.datetime):
            return o.isoformat()

        elif isinstance(o, dt.time):
            return o.isoformat()

        return json.JSONEncoder.default(self, o)

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

def retrieve_raw_data(day):
    start_dt = datetime(day.year, day.month, day.day)
    end_dt   = datetime(start_dt.year, start_dt.month, start_dt.day, 23, 59, 59)

    df_raw=es_helper.elastic_to_dataframe(es, index='opt_optibox*', 
                                           query='index1 >= 0', 
                                           start=start_dt, 
                                           end=end_dt,
                                           scrollsize=10000,
                                           size=1000000)

    containertimezone=pytz.timezone(get_localzone().zone)
    logger.info('Data retrieved')
    logger.info(df_raw)
    df_raw['@timestamp'] = pd.to_datetime(df_raw['@timestamp'], \
                                               unit='ms', utc=True).dt.tz_convert(containertimezone)
    df_raw=df_raw.sort_values('@timestamp') 
    
    return df_raw


def getDate(ts):
    #ts = int(ts)/1000
    #dt = datetime.fromtimestamp(ts)
    dt = ts
    dateStr = str(dt.year) + '-' + "{:02d}".format(dt.month) + '-' + "{:02d}".format(dt.day)
    return dateStr

def getMonth(dateStr):
    month = dateStr[:-3]
    return month

def getYear(dateStr):
    year = dateStr[:4]
    return year

def getIndex(dateStr):
    _index = 'opt_sites_computed-'+dateStr[:-3]
    return _index

def getCustomMin(minrow):
    minrow = minrow[minrow!=0]
    if np.isnan(minrow.min()):
        return 0
    else:
        return minrow.min()

def removeStr(x):
    response = x
    if isinstance(x, str):
        response = int(x)
        
    return response

        
def doTheWork(start):
    #now = datetime.now()

    start = datetime(start.year, start.month, start.day)

    df = retrieve_raw_data(start)


    try:
        df['index1'] = df['index1'].apply(lambda x: removeStr(x))
        df['index2'] = df['index2'].apply(lambda x: removeStr(x))
        df['index1_min'] = df['index1']
        df['index1_min_sec'] = df['index1']
        df['index1_max'] = df['index1']
        df['index1_avg'] = df['index1']
        df['index2_min'] = df['index2']
        df['index2_min_sec'] = df['index2']
        df['index2_max'] = df['index2']
        df['index2_avg'] = df['index2']
        df_grouped = df.groupby(['id', 'messSubType', 'messType', 'client', 'area'])\
        .agg({'@timestamp': 'min', 'index1_min': 'min', 'index1_max': 'max', 'index1_avg': 'mean', 'index1_min_sec':getCustomMin,\
        'index2_min': 'min', 'index2_max': 'max', 'index2_avg': 'mean', 'index2_min_sec':getCustomMin}).reset_index()

        df_grouped['pinindex1'] = df_grouped['messSubType'].apply(lambda x: int(int(x)/16))
        df_grouped['pinindex2'] = df_grouped['messSubType'].apply(lambda x: int(int(x)%16))
        df1 = df_grouped[['id', 'messSubType', 'messType', 'client', 'area', '@timestamp','index1_min', 'index1_max', 'index1_avg', 'index1_min_sec', 'pinindex1']]
        df1 = df1.rename(columns= {'index1_min': 'value_min', 'index1_max': 'value_max', 'index1_avg': 'value_avg', 'index1_min_sec': 'value_min_sec', 'pinindex1': 'pin'})
        df2 = df_grouped[['id', 'messSubType', 'messType', 'client', 'area', '@timestamp','index2_min', 'index2_max', 'index2_avg', 'index2_min_sec', 'pinindex2']]
        df2 = df2.rename(columns= {'index2_min': 'value_min', 'index2_max': 'value_max', 'index2_avg': 'value_avg', 'index2_min_sec': 'value_min_sec', 'pinindex2': 'pin'})

        df_grouped = pd.concat( [df1, df2],axis=0,ignore_index=True)

        df_grouped['value_day'] = df_grouped['value_max'] - df_grouped['value_min']
        df_grouped['conso_day'] = df_grouped['value_avg'] * 24
        df_grouped['availability']= df_grouped['value_avg'] * 1440
        df_grouped['availability_perc'] = df_grouped['value_avg'] * 100
        df_grouped['value_day_sec'] = df_grouped['value_max'] - df_grouped['value_min_sec']
        df_grouped['date'] = df_grouped['@timestamp'].apply(lambda x: getDate(x))
        df_grouped['month'] = df_grouped['date'].apply(lambda x: getMonth(x))
        df_grouped['year'] = df_grouped['date'].apply(lambda x: getYear(x))
        df_grouped['pinStr'] = df_grouped['pin'].apply(lambda x: 'Cpt'+str(x))
        df_grouped['client_area_name'] = df_grouped['client'] + '-' + df_grouped['area'] + '-' + df_grouped['pinStr']
        df_grouped['_id'] = df_grouped['client_area_name'] +'-'+ df_grouped['date']
        df_grouped['_index'] = df_grouped['date'].apply(lambda x : getIndex(x))


        res = es_helper.dataframe_to_elastic(es, df_grouped)

        print("data inserted for day " + str(start))
            
        print("finished")
    except Exception as er:
        logger.error('Unable to compute data for ' + str(start))
        logger.error(er)


def messageReceived(destination,message,headers):
    global es
    logger.info("==> "*10)
    logger.info("Message Received %s" % destination)
    logger.info(headers)

    msg = json.loads(message)
    start = datetime.fromtimestamp(int(msg['start']))
    stop = datetime.fromtimestamp(int(msg['stop']))

    while start <= stop:
        doTheWork(start)
        start = start + timedelta(1)





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
                    ,"heartbeats":(1200000,1200000),"earlyack":True}
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

    SECONDSBETWEENCHECKS=3600

    nextload=datetime.now()

    while True:
        time.sleep(5)
        try:            
            variables={"platform":"_/_".join(platform.uname()),"icon":"list-alt"}
            
            conn.send_life_sign(variables=variables)

            if (datetime.now() > nextload):
                try:
                    start = datetime.now()
                    start = start.replace(hour=0,minute=0,second=0, microsecond=0)
                    nextload=datetime.now()+timedelta(seconds=SECONDSBETWEENCHECKS)
                    #doTheWork(start-timedelta(1))
                    doTheWork(start)
                except Exception as e2:
                    logger.error("Unable to load sites data.")
                    logger.error(e2,exc_info=True)
            
        except Exception as e:
            logger.error("Unable to send life sign.")
            logger.error(e)
    #app.run(threaded=True,host= '0.0.0.0')