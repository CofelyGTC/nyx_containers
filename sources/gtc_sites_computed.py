"""
BIAC IMPORT KIZEO
====================================

Collections:
-------------------------------------


VERSION HISTORY
===============

* 04 Sep 2019 0.0.2 **PDE** First version

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
from elastic_helper import es_helper as esh

from functools import wraps
from datetime import datetime
from datetime import timedelta
#from lib import pandastoelastic as pte
from amqstompclient import amqstompclient
from logging.handlers import TimedRotatingFileHandler
from logstash_async.handler import AsynchronousLogstashHandler
from elasticsearch import Elasticsearch as ES, RequestsHttpConnection as RC

MODULE  = "GTC_SITES_COMPUTED"
VERSION = "0.0.2"
QUEUE   = ["GTC_SITES_COMPUTED"]

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

def getDate(ts):
    ts = int(ts)/1000
    dt = datetime.fromtimestamp(ts)
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


def doTheWork():
    now = datetime.now()
    start = datetime(now.year, now.month, now.day, 0,0,0)
    end = datetime(now.year, now.month, now.day, 23,59,59)


    while start < now:
        df = esh.elastic_to_dataframe(es, 'opt_sites_data*', query='*',start=start, end=end, timestampfield='@timestamp', size=1000000)
        df['value_min'] = df['value']
        df['value_min_sec'] = df['value']
        df['value_max'] = df['value']
        df['value_avg'] = df['value']
        df_grouped = df.groupby(['client_area_name', 'area_name', 'client', 'area']).agg({'@timestamp': 'min', 'value_min': 'min', 'value_max': 'max', 'value_avg': 'mean', 'value_min_sec':getCustomMin}).reset_index()
        df_grouped['value_day'] = df_grouped['value_max'] - df_grouped['value_min']
        df_grouped['conso_day'] = df_grouped['value_avg'] * 24
        df_grouped['availability']= df_grouped['value_avg'] * 1440
        df_grouped['availability_perc'] = df_grouped['value_avg'] * 100
        df_grouped['value_day_sec'] = df_grouped['value_max'] - df_grouped['value_min_sec']
        df_grouped['date'] = df_grouped['@timestamp'].apply(lambda x: getDate(x))
        df_grouped['month'] = df_grouped['date'].apply(lambda x: getMonth(x))
        df_grouped['year'] = df_grouped['date'].apply(lambda x: getYear(x))
        df_grouped['_id'] = df_grouped['client_area_name'] +'-'+ df_grouped['date']
        df_grouped['_index'] = df_grouped['date'].apply(lambda x : getIndex(x))
        

        esh.dataframe_to_elastic(es, df_grouped)
        print("data inserted for day " + str(start))
        start = start + timedelta(1)
        end = end + timedelta(1)
        
    print("finished")



def messageReceived(destination,message,headers):
    global es
    logger.info("==> "*10)
    logger.info("Message Received %s" % destination)
    logger.info(headers)

    xlsbytes = base64.b64decode(message)
    f = open('./tmp/excel.xlsx', 'wb')
    f.write(xlsbytes)
    f.close()

    doTheWork()





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
                    ,"login":os.environ["AMQC_LOGIN"],"password":os.environ["AMQC_PASSWORD"]}
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
                    nextload=datetime.now()+timedelta(seconds=SECONDSBETWEENCHECKS)
                    doTheWork()
                except Exception as e2:
                    logger.error("Unable to load sites data.")
                    logger.error(e2,exc_info=True)
            
        except Exception as e:
            logger.error("Unable to send life sign.")
            logger.error(e)
    #app.run(threaded=True,host= '0.0.0.0')