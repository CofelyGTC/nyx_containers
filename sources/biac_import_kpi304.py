import json
import time
import uuid
import base64
import platform
import threading
import os,logging
import pandas as pd

from logging.handlers import TimedRotatingFileHandler
import amqstomp as amqstompclient
from datetime import datetime
from datetime import date
from datetime import timedelta
from functools import wraps
from elasticsearch import Elasticsearch as ES
from logstash_async.handler import AsynchronousLogstashHandler
#from lib import pandastoelastic as pte
from elastic_helper import es_helper 
import numpy as np


VERSION="1.1.1"
MODULE="BIAC_KPI304_IMPORTER"
QUEUE=["BIAC_EXCELS_KPI304"]

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
    #logger.info(timeD)
    ts = 0
    if timeD == 0:
        return 0
    else:
        try:
            dtt = timeD.timetuple()
            ts = int(time.mktime(dtt))
        except Exception as er:
            logger.error(er)
            logger.error(str(timeD))
        return ts

################################################################################
def getDisplayStart(now):
    start = ''
    if 1 <= now.day < 8:
        month = 12
        year = now.year - 1
        if now.month != 1:
            month = now.month -1
            year = now.year
        start = datetime(year, month, 1, 0, 0, 0, 0)
    else:
        start = datetime(now.year, now.month, 1, 0, 0, 0, 0)

    return start

################################################################################

def get_week(date):
    week = datetime.date(date).isocalendar()[1]
    return 'W'+str(week)

################################################################################

def getScores(total, obj):
    if obj == total:
        return 1
    else:
        return 0

################################################################################

def getDisplay(date, now):
    displaystart = getDisplayStart(now)
    datedisplay = datetime.today() - timedelta(days=datetime.today().isoweekday() % 7)
    if displaystart <= date < datedisplay:
        return 1
    
    return 0


################################################################################
def messageReceived(destination,message,headers):
    global es
    records=0
    starttime = time.time()
    logger.info("==> "*10)
    logger.info("Message Received %s" % destination)
    logger.info(headers)

    if "CamelSplitAttachmentId" in headers:
        headers["file"] = headers["CamelSplitAttachmentId"]

    if "file" in headers:
        logger.info("File:%s" %headers["file"])
        log_message("Import of file [%s] started." % headers["file"])
    #elif "CamelSplitAttachmentId" in headers:
    #    logger.info("File:%s" %headers["CamelSplitAttachmentId"])
    #    log_message("Import of file [%s] started." % headers["CamelSplitAttachmentId"])

    now = datetime.now()
    displayStart = getDisplayStart(now)

    xlsbytes = base64.b64decode(message)
    f = open('./tmp/excel.xlsx', 'wb')
    f.write(xlsbytes)
    f.close()
    file = './tmp/excel.xlsx'
    #xlsdf=pd.read_excel('./tmp/excel.xlsx', index_col=None)    
    #xlsname=headers["file"]
    importyear = 0
    bulkbody = ''
    newcols = ['date', 'lot1tech', 'lot1hoofd', 'lot1total', 'lot1score', 'sanitech','sanihoofd', 'hvactech', 'hvachoofd','electech','elechoofd', 'firetech', 'firehoofd', 'lot2total', 'lot2score', 'lot3tech', 'lot3hoofd', 'lot3total', 'lot3score', 'lot4tech', 'lot4hoofd', 'lot4total', 'lot4score']

    df = None
    try:        
        logger.info("trying to open file")
        ef = pd.ExcelFile('./tmp/excel.xlsx')
        for sheet in ef.sheet_names:
            dfobj = pd.read_excel(file, sheetname=sheet, skiprows=18, usecols='C:Y')
            dfobj = dfobj.iloc[:1]
            lot1obj = 0
            lot2obj = 0
            lot3obj = 0
            lot4obj = 0
            saniobj = 0
            hvacobj = 0
            elecobj = 0
            fireobj = 0

            for index, row in dfobj.iterrows():
                lot1obj = row[3]
                lot2obj = row[13]
                lot3obj = row[17]
                lot4obj = row[21]
                saniobj = row[5] + row[6]
                hvacobj = row[7] + row[8]
                elecobj = row[9] + row[10]
                fireobj = row[11] + row[12]
            df = pd.read_excel(file, sheetname=sheet, skiprows=19, usecols='C:Y')
            df.columns = newcols
            df['sanitotal'] = df['sanitech'] + df['sanihoofd']
            df['hvactotal'] = df['hvactech'] + df['hvachoofd']
            df['electotal'] = df['electech'] + df['elechoofd']
            df['firetotal'] = df['firetech'] + df['firehoofd']

            df['lot1obj']=df['lot1total']
            df['lot2obj']=df['lot2total']
            df['lot3obj']=df['lot3total']
            df['lot4obj']=df['lot4total']
            df['saniobj']=df['sanitotal']
            df['hvacobj']=df['hvactotal']
            df['elecobj']=df['electotal']
            df['fireobj']=df['firetotal']

            for index, row in df.iterrows():
                if not(pd.isna(row.lot1total)):
                    df.set_value(index, 'lot1obj', lot1obj)
                    df.set_value(index, 'lot2obj', lot2obj)
                    df.set_value(index, 'lot3obj', lot3obj)
                    df.set_value(index, 'lot4obj', lot4obj)
                    df.set_value(index, 'saniobj', saniobj)
                    df.set_value(index, 'hvacobj', hvacobj)
                    df.set_value(index, 'elecobj', elecobj)
                    df.set_value(index, 'fireobj', fireobj)


            df.fillna(0, inplace=True)

            df['week'] = df['date'].apply(lambda x: get_week(x))
            df['_timestamp'] = df['date'].apply(lambda x: getTimestamp(x)*1000)
            df['_index'] = df['date'].apply(lambda x: 'biac_kpi304-'+ x.strftime('%Y'))
            df['_id'] = df['_timestamp'].apply(lambda x: 'kpi304_'+ str(x))
            df['display'] = df['date'].apply(lambda x: getDisplay(x, now))
            
            df['lot1score'] = df.apply(lambda row: getScores(row['lot1total'], row['lot1obj']), axis=1)
            df['lot2score'] = df.apply(lambda row: getScores(row['lot2total'], row['lot2obj']), axis=1)
            df['lot3score'] = df.apply(lambda row: getScores(row['lot3total'], row['lot3obj']), axis=1)
            df['lot4score'] = df.apply(lambda row: getScores(row['lot4total'], row['lot4obj']), axis=1)
            df['saniscore'] = df.apply(lambda row: getScores(row['lot1total'], row['lot1obj']), axis=1)
            df['hvacscore'] = df.apply(lambda row: getScores(row['lot1total'], row['lot1obj']), axis=1)
            df['elecscore'] = df.apply(lambda row: getScores(row['lot1total'], row['lot1obj']), axis=1)
            df['firescore'] = df.apply(lambda row: getScores(row['lot1total'], row['lot1obj']), axis=1)
            
            es_helper.pandas_to_elastic(es, df)



        importyear = now.year
        startdate =  datetime.strptime('01/01/'+str(importyear), "%d/%M/%Y")
        stopdate =  datetime.strptime('31/12/'+str(importyear), "%d/%M/%Y")
        first_alarm_ts = getTimestamp(startdate)
        last_alarm_ts = getTimestamp(stopdate)
        obj = {
                'start_ts': int(first_alarm_ts),
                'end_ts': int(last_alarm_ts)
            }
        logger.info(obj)
        conn.send_message('/topic/BIAC_KPI304_IMPORTED', json.dumps(obj))
    except Exception as e:
        endtime = time.time()
        logger.error(e)
        log_message("Import of file [%s] failed. Duration: %d Exception: %s." % (headers["file"],(endtime-starttime),str(e)))        


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
    host_params=os.environ["ELK_URL"]
    es = ES([host_params], http_auth=(os.environ["ELK_LOGIN"], os.environ["ELK_PASSWORD"]), verify_certs=False)
else:
    host_params="http://"+os.environ["ELK_URL"]+":"+os.environ["ELK_PORT"]
    es = ES(hosts=[host_params])


if __name__ == '__main__':    
    logger.info("AMQC_URL          :"+os.environ["AMQC_URL"])
    while True:
        time.sleep(5)
        try:            
            variables={"platform":"_/_".join(platform.uname()),"icon":"thumbs-up"}
            conn.send_life_sign(variables=variables)
        except Exception as e:
            logger.error("Unable to send life sign.")
            logger.error(e)
    #app.run(threaded=True,host= '0.0.0.0')
