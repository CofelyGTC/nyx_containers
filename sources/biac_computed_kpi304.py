import json
import time
import uuid
import base64
import threading
import os,logging
import pandas as pd

from logging.handlers import TimedRotatingFileHandler
import amqstomp as amqstompclient
from datetime import datetime
from datetime import timedelta
from functools import wraps
from elasticsearch import Elasticsearch as ES
from logstash_async.handler import AsynchronousLogstashHandler
from elasticsearch import Elasticsearch as ES
#from lib import pandastoelastic as pte
#from lib import elastictopandas as etp
from elastic_helper import es_helper 
import numpy as np


VERSION="1.1.1"
MODULE="BIAC_COMPUTE_KPI304"
QUEUE=["/topic/BIAC_KPI304_IMPORTED"]


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
def getTimestamp(timeD):
    logger.info(timeD)
    ts = 0
    try:
        dtt = timeD.timetuple()
        ts = int(time.mktime(dtt))
    except Exception as er:
        logger.error(er)
        logger.error(str(timeD))
    return ts

################################################################################

def get_id(datestop, week):
    res = 'kpi304'+str(datestop)+week
    return res


################################################################################
def messageReceived(destination,message,headers):
    global es
    records=0
    starttime = time.time()
    logger.info("==> "*10)
    logger.info("Message Received %s" % destination)
    logger.info(headers)

    now = datetime.now()
    start = now-timedelta(days=365)
    end = now+timedelta(days=365)

    df = es_helper.elastic_to_dataframe(es, "biac_kpi304-*", query='*', start=start, end=end)
    df2 = df[['@timestamp', 'date', 'display',
       'elecobj', 'elecscore', 'electotal',  'fireobj',
       'firescore', 'firetotal', 'hvacobj',
       'hvacscore', 'hvactotal',  'lot1obj',
       'lot1score',  'lot1total', 'lot2obj', 'lot2score',
       'lot2total', 'lot3obj', 'lot3score', 
       'lot3total',  'lot4obj', 'lot4score', 
       'lot4total',  'saniobj', 'saniscore',
       'sanitotal', 'week']].copy()
    dfs = df2.groupby('week')
    newcols = ['_timestamp', 'startweek', 'stopweek', 'week', 'totaljour', 'totalelec', 'strelec', 'totalfire', 'strfire','totalhvac', 'strhvac','totalsani', 'strsani','totallot1', 'strlot1','totallot2', 'strlot2','totallot3', 'strlot3','totallot4', 'strlot4', 'display']
    dffinal = pd.DataFrame(columns=newcols)

    week=''
    cpt=0
    for dfweek in dfs:
        df3 = dfweek[1]
        week = dfweek[0]
        totaljour = len(df3)
        totaljourelecok = df3['elecscore'].sum()
        strelec = str(totaljourelecok)+'/'+str(totaljour)
        totaljoursaniok = df3['saniscore'].sum()
        strsani = str(totaljoursaniok)+'/'+str(totaljour)
        totaljourfireok = df3['firescore'].sum()
        strfire = str(totaljourfireok)+'/'+str(totaljour)
        totaljourhvacok = df3['hvacscore'].sum()
        strhvac = str(totaljourhvacok)+'/'+str(totaljour)
        totaljourlot1ok = df3['lot1score'].sum()
        strlot1 = str(totaljourlot1ok)+'/'+str(totaljour)
        totaljourlot2ok = df3['lot2score'].sum()
        strlot2 = str(totaljourlot2ok)+'/'+str(totaljour)
        totaljourlot3ok = df3['lot3score'].sum()
        strlot3 = str(totaljourlot3ok)+'/'+str(totaljour)
        totaljourlot4ok = df3['lot4score'].sum()
        strlot4 = str(totaljourlot4ok)+'/'+str(totaljour)
        startweek = df3['date'].min()
        stopweek = df3['date'].max()
        _timestamp = df3['@timestamp'].min()
        display = df3['display'].max()
        
        line = [ _timestamp, startweek, stopweek, week, totaljour, totaljourelecok, strelec,totaljourfireok, strfire,totaljourhvacok, strhvac,totaljoursaniok, strsani,totaljourlot1ok, strlot1,totaljourlot2ok, strlot2,totaljourlot3ok, strlot3,totaljourlot4ok, strlot4, display]
        dffinal.loc[cpt]=line
        cpt+=1

    dffinal['_index'] = 'biac_kpi304_computed'
    dffinal['_id'] = dffinal.apply(lambda row: get_id(row['_timestamp'], row['week']), axis=1)
    res = es_helper.pandas_to_elastic(es, dffinal)



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
