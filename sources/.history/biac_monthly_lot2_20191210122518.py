"""
BIAC MONTHLY LOT2
====================================

Sends:
-------------------------------------

* /topic/BIAC_LOT2_MONTHLTY_AVAILABILITY_IMPORTED

Listens to:
-------------------------------------

* /queue/BIAC_FILE_2_Lot2AvailabilityMonthly

Collections:
-------------------------------------

* **biac_monthly_lot2** (Raw Data)

VERSION HISTORY
===============

* 01 Aug 2019 1.0.5 **VME** Bug fixing : nan values + code cleaning
"""  


import json
import time
import uuid
import base64
import threading
import os,logging
import pandas as pd
import re


from logging.handlers import TimedRotatingFileHandler
from amqstompclient import amqstompclient
from datetime import datetime
from datetime import timedelta
from datetime import date
from functools import wraps
from elasticsearch import Elasticsearch as ES, RequestsHttpConnection as RC
from logstash_async.handler import AsynchronousLogstashHandler
from lib import pandastoelastic as pte
import numpy as np
from math import ceil


VERSION="1.0.5"
MODULE="BIAC_IMPORT_MONTHLY_LOT2"
QUEUE=["/queue/BIAC_FILE_2_Lot2AvailabilityMonthly","/queue/BIAC_FILE_1_Lot1AvailabilityMonthly", "/queue/BIAC_FILE_3_Lot3AvailabilityMonthly"]
INDEX_PATTERN = "biac_monthly_lot2"


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


def week_of_month(dt):
    """ Returns the week of the month for the specified date.
    """

    first_day = dt.replace(day=1)

    dom = dt.day
    adjusted_dom = dom + first_day.weekday()

    return int(ceil(adjusted_dom/7.0))


def week_of_year(ts):
    ts = int(ts) / 1000
    dt = datetime.utcfromtimestamp(ts)
    weekOfYear = dt.isocalendar()[1]
    return weekOfYear


def getTimestamp(timeD):
    dtt = timeD.timetuple()
    ts = int(time.mktime(dtt))
    return ts


################################################################################
def getDisplayStart(now):
    start = ''
    if 1 < now.day < 8:
        month = 12
        year = now.year - 1
        if now.month != 1:
            month = now.month -1
            year = now.year
        start = datetime(year, month, 1, 0, 0, 0, 0)
    else:
        start = datetime(now.year, now.month, 1, 0, 0, 0, 0)

    return int(start.timestamp())

################################################################################

def getDisplayStop(now):
    datestop = now - timedelta(days=6)
    datestop = datestop.replace(hour=0)
    datestop = datestop.replace(minute=0)
    datestop = datestop.replace(second=0)
    datestop = datestop.replace(microsecond=0)
    return int(datestop.timestamp())



################################################################################
def messageReceived(destination,message,headers):
    global es
    starttime = time.time()
    imported_records=0
    reserrors = []
    logger.info("==> "*10)
    logger.info("Message Received %s" % destination)
    logger.info(headers)
    decodedmessage = base64.b64decode(message)

    if "file" in headers:
        headers['CamelFileNameOnly'] = headers['file']
        log_message("Import of file [%s] started." % headers["file"])

    now = datetime.now()

#####################
    # TO MODIFY ########
    lot = 2
    category = 'lot2_monthly'
    filename = headers['CamelFileNameOnly']
    #category = headers['category']
    #####################

    if 'Lot3' in filename:
        lot = 3
        category = 'lot3_monthly'
        INDEX_PATTERN = "biac_monthly_lot3"

    if 'Lot1' in filename:
        lot = 1
        category = 'lot1_monthly'
        INDEX_PATTERN = "biac_monthly_lot1"        

    f = open('dataFile.xlsm', 'w+b')
    f.write(decodedmessage)
    f.close()

    file = 'dataFile.xlsm'
    dfrepdef = pd.read_excel(file, sheetname='REPDEF')
    dfdata = pd.read_excel(file, sheetname='REPORT', skiprows=7)
    filter_col = [col for col in dfdata if col.startswith('KPI') or col.startswith('GTA') or col.startswith('GTF') or col.startswith('GTK')]
    columns = ['Unnamed: 2', 'Unnamed: 3', 'EQ']
    columns2 = columns + filter_col
    dfdata2 = dfdata[columns2]
    dfdata2 = dfdata2.iloc[2:]
    dfdata2 = dfdata2.dropna(how='all')
    dfdata2 = dfdata2.rename(index=str, columns={"Unnamed: 2": "startDate", "Unnamed: 3": "stopDate"})
    id_report = dfrepdef['id_report'][0]
    interval  = dfrepdef['interval'][0]
    equipment = columns2[-1]
    es_index= INDEX_PATTERN
    
    

    bulkbody = ''
    for index, row in dfdata2.iterrows():
        for col in row.index:
            if col.startswith('KPI') or col.startswith('GTA') or col.startswith('GTF') or col.startswith('GTK'):
                #print(col+ ':'+str(row[col]))
                start_date = getTimestamp(row.startDate)
                stop_date = getTimestamp(row.stopDate)

                equipment = col
                ts= start_date * 1000
                display = 0
                if row.stopDate.month == now.month - 1:
                    display = 1

                es_id = str(id_report) + '_' +str(col) + '_' + str(ts)

                action = {}
                action["index"] = {"_index": es_index,
                    "_type": "doc", "_id": es_id}

                try:
                    newrec = {
                        "@timestamp": ts,
                        "reportID": int(id_report),
                        "category": category,
                        "startDate": start_date*1000,
                        "stopDate": stop_date*1000,
                        "interval": interval,
                        "equipment": equipment,
                        "lot": lot,
                        "filename": filename,
                        "display": display,
                        "kpi": filename[3:6],
                        "numInterval": int(row['EQ']),
                        "value": int(row[equipment]),
                        "floatvalue": row[equipment]
                    }
                    bulkbody += json.dumps(action)+"\r\n"
                    bulkbody += json.dumps(newrec) + "\r\n"
                except:
                    logger.warning('unable to insert equipment: '+str(equipment)+' week: '+str(start_date)+' value: '+str(row[equipment])+ ' (type: '+str(type(row[equipment])))


    # logger.info(bulkbody)
    logger.info("BULK READY:" + str(len(bulkbody)))
    bulkres = es.bulk(bulkbody, request_timeout=30)
    logger.info("BULK DONE")
    bulkbody = ''
    if(not(bulkres["errors"])):
        logger.info("BULK done without errors.")
    else:
        for item in bulkres["items"]:
            if "error" in item["index"]:
                imported_records -= 1
                logger.info(item["index"]["error"])
                reserrors.append(
                    {"error": item["index"]["error"], "id": item["index"]["_id"]})





    first_alarm_ts = int(getTimestamp(dfdata2['startDate'].min()))
    last_alarm_ts = int(getTimestamp(dfdata2['stopDate'].max()))
    obj = {
            'start_ts': int(first_alarm_ts),
            'end_ts': int(last_alarm_ts)
        }
    
    endtime = time.time()    
    if len(reserrors)>0:
        log_message("Import of file [%s] failed. Duration: %d. %d records were not imported." % (headers["file"],(endtime-starttime),len(reserrors)))        

    try:
        log_message("Import of file [%s] finished. Duration: %d Records: %d." % (headers["file"],(endtime-starttime),df.shape[0]))         
    except:
        log_message("Import of file [%s] finished. Duration: %d." % (headers["file"],(endtime-starttime)))

    logger.info('sending message to /topic/BIAC_LOT2_MONTHLTY_AVAILABILITY_IMPORTED')
    logger.info(obj)
    
    conn.send_message('/topic/BIAC_LOT2_MONTHLTY_AVAILABILITY_IMPORTED', json.dumps(obj))

    


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