import json
import time
import uuid
import base64
import threading
import os,logging
import pandas as pd
import platform
from io import StringIO

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
import tzlocal

VERSION="1.0.2"
MODULE="GTC_IMPORT_TELEPHONY"
QUEUE=["TELEPHONY_IMPORT"]


INDEX_PATTERN = "gtc_import_telephony"


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



def messageReceived(destination,message,headers):
    global es, basefile
    records=0
    starttime = time.time()
    logger.info("==> "*10)
    logger.info("Message Received %s" % destination)
    logger.info(headers)
    now = datetime.now()

    local_timezone = tzlocal.get_localzone()

    if "CamelSplitAttachmentId" in headers:
        headers["file"] = headers["CamelSplitAttachmentId"]

    if "file" in headers:
        logger.info("File:%s" %headers["file"])
        log_message("Import of file [%s] started." % headers["file"])
    #elif "CamelSplitAttachmentId" in headers:
    #    logger.info("File:%s" %headers["CamelSplitAttachmentId"])
    #    log_message("Import of file [%s] started." % headers["CamelSplitAttachmentId"])

    mess = base64.b64decode(message)
    mesin=mess.decode("utf-8", "ignore")
    full=basefile+"\r\n"+mesin
    #f = open('./tmp/excel.xlsx', 'wb')
    #f.write(xlsbytes)
    #f.close()

    df = None
    #try:
    #    df = pd.read_csv('./tmp/excel.xlsx',delim_whitespace=True, header=None,converters={"Date":str,"Hour":str,"Called":str,"Caller":str,"Desk":str})
    #except Exception as er:
    #    logger.error('Unable to read csv')
    #    logger.error(er)
    

    #te = df.copy()

    mesin=mess.decode("utf-8", "ignore")

    te=pd.read_fwf(StringIO(full)
               , names=["Date","Hour","Duration","A4","Code","A6","A7","Called","Caller","A10","Desk","A12","A13","A14","A15","A16","A17"]
               ,delim_whitespace=True, header=None,converters={"Date":str,"Hour":str,"Called":str,"Caller":str,"Desk":str})


    te=te[100:]
    te=te[89:]



#        te = te[te.Called.notnull()]

    te['InternalCalled1']=te['Called'].str.replace(' ','')
    te['InternalCalled']=(te['InternalCalled1'].str.len()<6)
    te['InternalCaller1']=te['Caller'].str.replace(' ','')
    te['InternalCaller']=(te['InternalCaller1'].str.len()<6)
    te['SolidusCalled']=(te['InternalCalled1'].str.match("93901"))

    te['Desk2']=pd.to_numeric(te['Desk'], errors='coerce',downcast='integer')
    te['DeskCaller']=pd.to_numeric(te['Caller'], errors='coerce',downcast='integer')
    #te['Desk2']=te['Desk'].astype(int)

    del te['InternalCalled1']
    del te['InternalCaller1']

    #te['Calltype']=(te['InternalCalled1'].str.match("93904"))

    calltype=[]



    for index,row in te.iterrows():

#            print ("#####"*20)
#            print(row)

        if(row["Caller"].find("91931")>=0):
            calltype.append("Test")
        elif (row["Desk2"]>76800) and (row["Desk2"]<76810) and (row["DeskCaller"]>76900) and (row["DeskCaller"]<76910):
            calltype.append("Transfer")
        elif (row["DeskCaller"]>76800) and (row["DeskCaller"]<76810) and (row["Desk2"]>76900) and (row["Desk2"]<76910):
            calltype.append("Transfer")
        elif(row["Desk2"]>76900) and (row["Desk2"]<76910):
            calltype.append("InDispa")
        elif(row["Desk2"]>76800) and (row["Desk2"]<76810):
            calltype.append("InDesk")
    #    else:
    #        calltype.append("Solidus")

        elif(row["SolidusCalled"]):
            if (not(row["InternalCaller"])):
                calltype.append("In")
            else:
                calltype.append("InOther")
        else:
            if (row["InternalCaller"]):

                if(row["DeskCaller"]>76900) and (row["DeskCaller"]<76910):
                    calltype.append("OutDispa")
                elif(row["DeskCaller"]>76800) and (row["DeskCaller"]<76810):
                    calltype.append("OutDesk")
                else:
                    calltype.append("Out")

            else:
                calltype.append("OutOther")

    te['CallType']=calltype
    te['DurationSecond']=te['Duration']%100
    te['DurationMinute']=te['Duration']/100
    te['DurationMinute2']=te['DurationMinute'].astype(int)

    te['Duration']=te['DurationMinute2']*60+te['DurationSecond']


    logger.info(te)
    messagebody=""

    action={}

    te2=te

    del te2["A4"]
    del te2["A6"]
    del te2["A10"]
    #del te2["A11"]
    del te2["A12"]
    del te2["A13"]
    del te2["A14"]
    del te2["A15"]
    del te2["A16"]
    del te2["A17"]
    del te2["A7"]

    te2["timestamp"]=te2["Date"]+te2["Hour"]

    te2["Date"]=pd.to_datetime(te2["timestamp"], format="%d%m%y%H%M%S")
    del te2["timestamp"]
    del te2["Hour"]


    te2

    for index,row in te2.iterrows():
        obj={}
        obj["@timestamp"]=int(row["Date"].timestamp())*1000
        obj["Duration"]=row["Duration"]
        obj["Code"]=row["Code"].replace(' ','')
        obj["Called"]=row["Called"].replace(' ','')
        obj["Caller"]=row["Caller"].replace(' ','')
        try:
            obj["Desk"]=int(row["Desk"])
        except:
            obj["Desk"]=row["Desk"]

        try:
            obj["DeskCaller"]=int(row["DeskCaller"])
        except:
            obj["DeskCaller"]=row["DeskCaller"]

        obj["InternalCaller"]=row["InternalCaller"]
        obj["InternalCalled"]=row["InternalCalled"]
        obj["SolidusCalled"]=row["SolidusCalled"]
        obj["CallType"]=row["CallType"]

        action["index"]={"_index":"telephony","_type":"doc","_id":str(obj["@timestamp"])+'_'+obj["Called"]+'_'+obj["Caller"]}

        messagebody+=json.dumps(action)+"\r\n";
        messagebody+=json.dumps(obj)+"\r\n"

        if(len(messagebody)>500000):
            logger.info ("BULK")
            try:
                es.bulk(messagebody)
            except:
                logger.error("==================>Retry")
                print("==================>Retry")
                es.bulk(messagebody)
            messagebody=""

    try:
        es.bulk(messagebody)
    except:
        logger.error("==================>Retry")
        print("==================>Retry")
        es.bulk(messagebody)
    #print (messagebody)
    logger.info ("FINISHED")

    endtime = time.time()    
    try:
        log_message("Import of file [%s] finished. Duration: %d Records: %d." % (headers["file"],(endtime-starttime),df.shape[0]))         
    except:
        log_message("Import of file [%s] finished. Duration: %d." % (headers["file"],(endtime-starttime)))












###################################################################################################################################################

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
                ,"heartbeats":(180000,180000),"earlyack":True}

lastvaluecache={}
dayvaluecache={}
conn=amqstompclient.AMQClient(server
    , {"name":MODULE,"version":VERSION,"lifesign":"/topic/NYX_MODULE_INFO"},QUEUE,callback=messageReceived)
#conn,listener= amqHelper.init_amq_connection(activemq_address, activemq_port, activemq_user,activemq_password, "RestAPI",VERSION,messageReceived)
connectionparameters={"conn":conn}

file = open("./telephony/TelephonycleanSub2.bkp", "r")
basefile= file.read()

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
            variables={"platform":"_/_".join(platform.uname()),"icon":"wrench"}
            conn.send_life_sign(variables=variables)
        except Exception as e:
            logger.error("Unable to send life sign.")
            logger.error(e)
    #app.run(threaded=True,host= '0.0.0.0')
