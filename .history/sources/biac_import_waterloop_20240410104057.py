import json
import time
import uuid
import base64
import threading
import os,logging
import pandas as pd
import re
import platform
import calendar
import tzlocal


from logging.handlers import TimedRotatingFileHandler
from amqstompclient import amqstompclient
from datetime import datetime
from datetime import timedelta
from functools import wraps
from elasticsearch import Elasticsearch as ES, RequestsHttpConnection as RC
from logstash_async.handler import AsynchronousLogstashHandler
from lib import pandastoelastic as pte
import numpy as np
from math import ceil


VERSION="1.0.4"
MODULE="BIAC_IMPORT_WATERLOOP"
QUEUE=["/queue/WATERLOOP_IMPORT"]
INDEX_PATTERN = "biac_waterloop"


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

#######################################################################################
# get_month_day_range
#######################################################################################
def get_month_day_range(date):
    date=date.replace(hour = 0)
    date=date.replace(minute = 0)
    date=date.replace(second = 0)
    date=date.replace(microsecond = 0)
    first_day = date.replace(day = 1)
    last_day = date.replace(day = calendar.monthrange(date.year, date.month)[1])
    last_day=last_day+timedelta(1)
    last_day = last_day - timedelta(seconds=1)
    
    local_timezone = tzlocal.get_localzone()
    first_day=first_day.astimezone(local_timezone)
    last_day=last_day.astimezone(local_timezone)
    
    return first_day, last_day

def week_of_month(dt):
    """ Returns the week of the month for the specified date.
    """

    first_day = dt.replace(day=1)

    dom = dt.day
    adjusted_dom = dom + first_day.weekday()

    return int(ceil(adjusted_dom/7.0))


def week_of_year(ts):
    #print('Timestamp:' + str(ts))
    ts = int(ts) / 1000
    dt = datetime.utcfromtimestamp(ts)
    weekOfYear = dt.isocalendar()[1]
    return weekOfYear
def roundDate(x):
    x = x[:-5]
    x = x + '00:00'
    return x

def getKPI(row):
    season = row['season']
    spwinter = row['wintersetpoint']
    spsummer = row['summersetpoint']
    t121 = row['PCA_HEEXH001EPRDT']
    t118 = row['PCA_HEEXCH001ESEDT']
    t113 = row['PCA_COPRDEPT']
    KPI = 0
    
    if season == 1:
        if t121 <= (1.15 * spwinter) and t118 >= (spwinter - 10):
            KPI = 1
        #elif t118 >= (spwinter - 10):
        #    KPI = 1
        else:
            KPI=0
    else:
        if t113 <= (spsummer + 2):
            KPI = 1
        else:
            KPI = 0
    
    return KPI


def getScore(row):
    season = row['season']
    spwinter = row['wintersetpoint']
    spsummer = row['summersetpoint']
    t121 = row['PCA_HEEXH001EPRDT']
    t118 = row['PCA_HEEXCH001ESEDT']
    t113 = row['PCA_COPRDEPT']
    score = 0
    
    if season == 1:
        if t121 <= (1.15 * spwinter):
            score = 2
        elif t118 >= (spwinter - 10):
            score = 2
        else:
            score=0
    else:
        if t113 <= (spsummer + 2):
            score = 1
        else:
            score = 0
    
    return score
    
def getActualSP(row):
    sp = 0
    if row['season'] == 1:
        sp = row['wintersetpoint']
    else:
        sp = row['summersetpoint']
    
    return sp


def getTimestamp(date_time_str):
    timeD = datetime.strptime(date_time_str, '%d/%m/%Y %H:%M:%S')
    #print(timeD)
    dtt = int(time.mktime(timeD.timetuple()))
    #ts = int(time.mktime(dtt))
    return dtt*1000

def getIndex(x):
    timeD = datetime.strptime(x, '%d/%m/%Y %H:%M:%S')
    index = 'biac_waterloop-'+str(timeD.year)
    return index

def getDisplayedTemp(row):
    #print(row)
    displayedTemp = 0
    season = row['season']
    
    if season == 1:
        displayedTemp = row['PCA_HEEXCH001ESEDT']
    else:
        displayedTemp = row['PCA_COPRDEPT']
    
    return displayedTemp

def getDisplayedSP(row):
    displayedSP = 0
    season = row['season']
    
    if season == 1:
        displayedSP = row['wintersetpoint']
    else:
        displayedSP = row['summersetpoint']
    
    return displayedSP

def getDisplayedLimit(row):
    displayedLimit = 0
    season = row['season']
    
    if season == 1:
        displayedLimit = row['wintersetpoint'] - 10
    else:
        displayedLimit = row['summersetpoint'] + 2
    
    return displayedLimit



################################################################################
def messageReceived(destination,message,headers):
    global es
    records=0
    starttime = time.time()
    imported_records=0
    reserrors = dict()
    logger.info("==> "*10)
    logger.info("Message Received %s" % destination)
    logger.info(headers)
    decodedmessage = base64.b64decode(message)

    if "file" in headers:
        headers['CamelFileNameOnly'] = headers['file']
        log_message("Import of file [%s] started." % headers["file"])

    if ".txt" in headers["file"]:
    #####################
        # TO MODIFY ########
        lot = 6
        filename = headers['CamelFileNameOnly']
        category = 'BoardingBridge'
        #####################

        f = open('dataFile.xlsm', 'w+b')
        f.write(decodedmessage)
        f.close()

        file = 'dataFile.xlsm'

        df = pd.read_csv(file, delimiter='\t')

        df["lot"] = lot
        df = df.rename(index=str, columns={"PCA_PRODmodus.PointValue": "season", "PCA_HEEXCcspdt.PointValue": "wintersetpoint", "PCA_COPRcspdt.PointValue": "summersetpoint", "PCA_COPRDEPT.PointValue": "PCA_COPRDEPT", "PCA_HEEXCH001ESEDT.PointValue": "PCA_HEEXCH001ESEDT", "PCA_HEEXH001EPRDT.PointValue": "PCA_HEEXH001EPRDT"})
        df['score'] = df.apply(lambda row: getScore(row), axis=1)
        df['dateround'] = df['Date'].apply(lambda x: roundDate(x))
        df_grouped = df.groupby(['dateround']) \
            .agg({'season': 'max','lot': 'max', 'wintersetpoint': 'mean', 'summersetpoint': 'mean', 'PCA_COPRDEPT': 'mean', 'PCA_HEEXCH001ESEDT': 'mean', 'PCA_HEEXH001EPRDT': 'mean', 'score': 'min'}) \
            .rename(columns={'dateround': 'date'})
        df_grouped = df_grouped.reset_index()
        df_grouped['KPI'] = df_grouped.apply(lambda row: getKPI(row), axis=1)
        df_grouped['kpipercent'] = df_grouped['KPI']*100
        df_grouped['actualsp'] = df_grouped.apply(lambda row: getActualSP(row), axis=1)
        df_grouped['@timestamp'] = df_grouped['dateround'].apply(lambda x: getTimestamp(x))
        df_grouped['_id'] = df_grouped['@timestamp'].apply(lambda x: 'kpipca_' + str(x))
        df_grouped['_index'] = df_grouped['dateround'].apply(lambda x: getIndex(x))
        df_grouped['displayedTemp'] = df_grouped.apply(lambda row: getDisplayedTemp(row), axis=1)
        df_grouped['displayedSP'] = df_grouped.apply(lambda row: getDisplayedSP(row), axis=1)
        df_grouped['displayedLimit'] = df_grouped.apply(lambda row: getDisplayedLimit(row), axis=1)

        pte.pandas_to_elastic(es, df_grouped)
        
        first_alarm_ts = int(df_grouped['@timestamp'].min())
        last_alarm_ts = int(df_grouped['@timestamp'].max())
        obj = {
                'start_ts': int(first_alarm_ts),
                'end_ts': int(last_alarm_ts)
            }

        if len(reserrors)>0:
            log_message("Import of file [%s] failed. Duration: %d. %d records were not imported." % (headers["file"],(endtime-starttime),len(reserrors)))        

        endtime = time.time()    
        try:
            log_message("Import of file [%s] finished. Duration: %d Records: %d." % (headers["file"],(endtime-starttime),df.shape[0]))         
        except:
            log_message("Import of file [%s] finished. Duration: %d." % (headers["file"],(endtime-starttime)))

        logger.info('sending message to /topic/BIAC_WATERLOOP_IMPORTED')
        logger.info(obj)

        conn.send_message('/topic/BIAC_WATERLOOP_IMPORTED', json.dumps(obj))


        logger.info(">>>>Set Active Fields")
        logger.info("Reset active records ")
        logger.info("=====================")
        time.sleep(3)

        updatequery={
            "script": {
                "inline": "ctx._source.active=0",
                "lang": "painless"
            },
            'query': {'bool': {'must': [{'query_string': {'query': '*'}}]}}
        }
        logger.info("*="*30)
        logger.info(json.dumps(updatequery))        

        try:
            resupdate=es.update_by_query(body=updatequery,index="biac_waterloop*")
            logger.info(resupdate)
        except Exception as e3:            
            logger.error(e3)   
            logger.error("Unable to update records waterloop.") 

        now=datetime.now()
        if now.day>1:
            upstart, upend = get_month_day_range(now)
        else:
            upstart, upend = get_month_day_range(now-timedelta(days=1))
        
        logger.info("RANGE:%s <-> %s" %(upstart, upend))

        logger.info("Set active records ")
        logger.info("=====================")
        time.sleep(3)

        updatequery={
            "script": {
                "inline": "ctx._source.active=1",
                "lang": "painless"
            },
            'query': {
                    "range" : {
                        "@timestamp" : {
                            "gte" : upstart.isoformat(),
                            "lte" : upend.isoformat(),                            
                        }
                    }
                }
        }

        logger.info(updatequery)

        try:
            resupdate=es.update_by_query(body=updatequery,index="biac_waterloop*")
            logger.info(resupdate)
        except Exception as e3:            
            logger.error(e3)   
            logger.error("Unable to update records waterloop.") 
            
            


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
            variables={"platform":"_/_".join(platform.uname()),"icon":"clipboard-check"}
            
            conn.send_life_sign(variables=variables)
        except Exception as e:
            logger.error("Unable to send life sign.")
            logger.error(e)
    #app.run(threaded=True,host= '0.0.0.0')
