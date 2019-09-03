"""
BIAC LOT2 AVAILABILITIES
====================================

Sends:
-------------------------------------

* BIAC_LOT2_AVAILABILITY_IMPORTED

Listens to:
-------------------------------------

* /queue/BIAC_FILE_2_Lot2Availability

Collections:
-------------------------------------

* **biac_availability** (Raw Data)

VERSION HISTORY
===============

* 01 Aug 2019 1.0.9 **VME** Bug fixing : nan values + display 1 based on last row instead of now (better if we want to replay a file). Code cleaning
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


VERSION="1.0.9"
MODULE="BIAC_IMPORT_LOT2_AVAILABILITIES"
QUEUE=["/queue/BIAC_FILE_2_Lot2Availability"]
INDEX_PATTERN = "biac_availability"


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

    filename = 'NA'

    if "CamelSplitAttachmentId" in headers:
        headers["file"] = headers["CamelSplitAttachmentId"]

    if "file" in headers:
        logger.info("File:%s" %headers["file"])
        log_message("Import of file [%s] started." % headers["file"])
        filename = headers['file']


#####################
    # TO MODIFY ########
    lot = headers['lot']
    category = headers['category']
    #####################

    f = open('dataFile.xlsm', 'w+b')
    f.write(decodedmessage)
    f.close()

    file = 'dataFile.xlsm'
    ef = pd.ExcelFile(file)
    dfs = []
    for sheet in ef.sheet_names:
        df = pd.read_excel(file, sheetname=sheet)
        dfs.append(df)

    dfdef = dfs[0]
    dfdata=dfs[2]
    dfdata=dfdata[6:]
    dfdata.columns=dfdata.iloc[0]
    newcolumns=[]
    for col in dfdata.columns:
        if str(col)!="nan" and str(col)!="NaT"  and str(col)!="EQT"  and str(col)!="KPI" and "AVERAGE" not in str(col):
            newcolumns.append(col)

    dfdata=dfdata[newcolumns]        
    dfdata=dfdata[1:]
    objectives=dfdata.iloc[0]
    dfdata = dfdata[1:]
    filtered_df = dfdata[dfdata['EQ'].notnull()]
    dfdata = filtered_df
    objectives.fillna(value=98, inplace=True)

    dfdata = dfdata[1:]

    idrepport = dfdef.get_value(0,'id_report')
    interval = dfdef.get_value(0,'interval')
    startDate = dfdef.get_value(0,'g_start_date')
    stopDate = dfdef.get_value(0,'end_date')
    report_type = dfdef.get_value(0,'report_type')
    name  = dfdef.get_value(0,'name').lower()
    name=name.replace('%','')

    first_day_year = startDate.to_pydatetime().replace(
        month=1, day=1, hour=0, minute=0, second=0)

    if interval == 'week':
        dfdata['dt'] = dfdata['EQ'].apply(
            lambda x: startDate + ((x-1)*timedelta(days=7)))
        print(10*'###')
        print(dfdata['dt'])
    elif interval == 'day':
        dfdata['dt'] = dfdata['EQ'].apply(
            lambda x: first_day_year + ((x)*timedelta(days=1)))


    if interval == 'week':
        dfdata2 = dfdata.copy()
        dfdata2 = dfdata2.tail(1)
        dfdata2['dt']= dfdata2['dt'].apply(lambda x: x + timedelta(days=6))
        dfdata = pd.concat([dfdata, dfdata2])

    dfdata.drop_duplicates(subset='dt', inplace=True, keep='first')

    dfdata.set_index('dt', inplace=True)

    dfdata = dfdata.resample('1d').pad()

    dfdata.reset_index(inplace=True)

    dfdata['week_of_month'] = dfdata.dt.apply(week_of_month)
    dfdata['_index'] = dfdata.dt.map(
        lambda x: INDEX_PATTERN+'-'+x.strftime('%Y.%m'))
    dfdata['year_month'] = dfdata.dt.map(lambda x: x.strftime('%Y-%m'))
    dfdata['@timestamp'] = dfdata.dt.values.astype(np.int64) // 10 ** 6
    dfdata.set_index('dt', inplace=True)
    dfdata.columns = map(str.lower, dfdata.columns)



    #now = datetime.now()
    now = dfdata.index[-1].to_pydatetime()+timedelta(days=1)
    displayStart = getDisplayStart(now)



    regex1 = r"^gt[af]_"
    regex2 = r"^kpi_"

    bulkbody = ''
    i= 1

    for index, row in dfdata.iterrows():
        eq = row['eq']

        week_of_the_month = row['week_of_month']
        es_index = row['_index']
        ts = row['@timestamp']


        for i in row.index:
            if re.match(regex1, i) or re.match(regex2, i):
                objective = objectives.get_value(1, i)

                weekOfYear = week_of_year(ts)
                display = 0

                nowts = int(now.timestamp())

                if displayStart < ts/1000 < nowts:
                    display = 1
                
                dtts = date.fromtimestamp(ts/1000)
                dtstart = date.fromtimestamp(displayStart)

                if dtts.isocalendar()[1] == dtstart.isocalendar()[1]:
                    display = 1
                    logger.info("Timestamp:" + str(ts))
                    logger.info("dateStart" + str(dtstart))
                    logger.info("Week of date :" + dtts.strftime("%U"))
                    logger.info("Week of start :" + dtstart.strftime("%U"))

                es_id = str(idrepport) + '_' + i + '_' + str(ts)

                action = {}
                action["index"] = {"_index": es_index,
                    "_type": "doc", "_id": es_id}

                try:
                    newrec = {
                        "@timestamp": ts,
                        "reportID": int(idrepport),
                        "category": category,
                        "reportType": int(report_type),
                        "startDate": int(getTimestamp(startDate)*1000),
                        "stopDate": int(getTimestamp(stopDate)*1000),
                        "interval": interval,
                        "equipment": i,
                        "display": display,
                        "cleanEquipement": i[4:],
                        "lot": lot,
                        "filename": filename,
                        "objective": objective,
                        "numInterval": int(row['eq']),
                        "value": int(row[i]),
                        "floatvalue": row[i],
                        "year_month": row['year_month'],
                        "realWeek": eq,
                        "weekOfMonth": week_of_the_month,
                        "weekOfYear": weekOfYear,
                        "lastWeek": 0
                    }

                    bulkbody += json.dumps(action)+"\r\n"
                    bulkbody += json.dumps(newrec) + "\r\n"
                except:
                    logger.warning('unable to insert equipment: '+str(i)+' week: '+str(weekOfYear)+' value: '+str(row[i])+ ' (type: '+str(type(row[i])))

                if len(bulkbody) > 512000:
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


    if len(bulkbody) > 0:
        logger.info("BULK READY FINAL:" + str(len(bulkbody)))
        bulkres = es.bulk(bulkbody)
        logger.info("BULK DONE FINAL")
        if(not(bulkres["errors"])):
            logger.info("BULK done without errors.")
        else:
            for item in bulkres["items"]:
                if "error" in item["index"]:
                    imported_records -= 1
                    logger.info(item["index"]["error"])
                    reserrors.append(
                        {"error": item["index"]["error"], "id": item["index"]["_id"]})


    first_alarm_ts = int(dfdata['@timestamp'].min())
    last_alarm_ts = int(dfdata['@timestamp'].max())
    obj = {
        'start_ts': int(first_alarm_ts),
        'end_ts': int(last_alarm_ts)
    }

    logger.info('sending message to /topic/BIAC_AVAILABILITY_IMPORTED')
    logger.info(obj)
    
    conn.send_message('/topic/BIAC_LOT2_AVAILABILITY_IMPORTED', json.dumps(obj))

    endtime = time.time()    
    try:
        log_message("Import of file [%s] finished. Duration: %d Records: %d." % (headers["file"],(endtime-starttime),df.shape[0]))         
    except:
        log_message("Import of file [%s] finished. Duration: %d." % (headers["file"],(endtime-starttime)))    
    


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
