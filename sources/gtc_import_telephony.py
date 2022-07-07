"""
BIAC GTC IMPORT TELEPHONY
====================================

This process decodes MITTEL log file and store recordxs into ealstic search./

Listens to:
-------------------------------------

* /queue/TELEPHONY_IMPORT


Collections:
-------------------------------------

* **telephony** (Raw Data)
* **nyx_config_telephony** (Raw Data)



VERSION HISTORY
===============

* 19 Dec 2019 1.0.8 **AMA** OutOther renamed. Added to doc
* 07 Jan 2020 1.1.0 **AMA** Format customized to match the raw log format
* 08 Jan 2020 1.2.0 **AMA** Set file header if not present
* 03 Mar 2020 1.3.0 **AMA** Desk filled with NA if required
"""  
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
from elastic_helper import es_helper 
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

VERSION="1.3.0"
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
    logger.info(message)
    now = datetime.now()

    local_timezone = tzlocal.get_localzone()

    if "CamelSplitAttachmentId" in headers:
        headers["file"] = headers["CamelSplitAttachmentId"]

    if "file" in headers:
        logger.info("File:%s" %headers["file"])
        log_message("Import of file [%s] started." % headers["file"])
    else:
        headers["file"]="From_Rest_API"
    
    
    dfconfig = es_helper.elastic_to_dataframe(es,index="nyx_config_telephony")
    dfconfig=dfconfig.set_index(["DnisNr"])
    dfconfight=dfconfig.to_dict('index')

    mess = base64.b64decode(message)
    df = None

    mesin=mess.decode("utf-8", "ignore")

#    te=pd.read_fwf(StringIO(full)
##               , names=["Date","Hour","Duration","A4","Code","A6","A7","Called","Caller","A10","Desk","A12","A13","A14","A15","A16","A17"]
#               ,delim_whitespace=True, header=None,converters={"Date":str,"Hour":str,"Called":str,"Caller":str,"Desk":str})

    # colspecs=[[0, 6],
    #     [6, 13],
    #     [13, 19],
    #     [20, 24],#A4
    #     [25, 27],#COde
    #     [27, 30],#A6
    #     [30, 37],#A7
    #     [37, 57],#CALLED
    #     [58, 88],#CALLER
    #     [89, 96],#Rings
    #     [97, 107],#DESK
    #     [108, 123],#A12
    #     [124, 133],#A13
    #     [134, 138],#A14
    #     [137, 143],#A15
    #     [144, 155],
    #     [156, 159]]

    colspecs=[[0, 6],
        [6, 13],
        [13, 19],
        [20, 24],#A4
        [25, 27],#COde
        [27, 30],#A6
        [30, 37],#A7
        [37, 60],#CALLED
        [61, 88],#CALLER
        [89, 96],#Rings
        [97, 107],#DESK
        [108, 123],#A12
        [124, 135],#A13
        [137, 140],#A14
        [139, 145],#A15
        [146, 156],
        [157, 161]]

    logger.info("Remove LIFE SIGNS")
    mesin=mesin.split("\n")
    mesin=[_.strip() for _ in mesin if "Cofely" not in _]
    mesin="\n".join(mesin)

    logger.info("Panda read...")
    te=pd.read_fwf(StringIO(mesin),header=None,colspecs=colspecs
                ,converters={"A13":str,"A4":str,"A15":str,"A16":str,"Date":str,"Hour":str,"Called":str,"Caller":str,"Desk":str}
                , names=["Date","Hour","Duration","A4","Code","A6","A7","Called","Caller","Rings","Desk","A12","A13","A14","A15","A16","A17"])

    logger.info("Done")
    #te=te[89:]  # IMPORTANT get rid of the base template file


    te["Caller"]=te["Caller"].fillna("")
    te["Code"]=te["Code"].fillna("")
    te["Desk"]=te["Desk"].fillna(0)
    te['InternalCalled1']=te['Called'].str.replace(' ','')
    te['InternalCalled']=(te['InternalCalled1'].str.len()<6)
    te['InternalCaller1']=te['Caller'].str.replace(' ','')
    te['InternalCaller']=(te['InternalCaller1'].str.len()<6)
    te['SolidusCalled']=(te['InternalCalled1'].str.match("93901"))

    te['Desk2']=pd.to_numeric(te['Desk'], errors='coerce',downcast='integer')
    te['DeskCaller']=pd.to_numeric(te['Caller'], errors='coerce',downcast='integer')

    del te['InternalCalled1']
    del te['InternalCaller1']

    calltype=[]



    for index, row in te.iterrows():
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
                calltype.append("Other")

    te['CallType']=calltype
    te['DurationSecond']=te['Duration']%100
    te['DurationMinute']=te['Duration']/100
    te['DurationMinute2']=te['DurationMinute'].astype(int)

    te['Duration']=te['DurationMinute2']*60+te['DurationSecond']


    #logger.info(te)
    messagebody=""

    action={}

    te2=te

    del te2["A4"]
    del te2["A6"]
    #del te2["A10"]
    #del te2["A11"]
    del te2["A12"]
    del te2["A13"]
    del te2["A14"]
    #del te2["A15"]
    #del te2["A16"]
    #del te2["A17"]
    del te2["A7"]
    te2["A15"].fillna(0,inplace=True)
    te2["A16"].fillna(0,inplace=True)
    te2["A17"].fillna(0,inplace=True)

    te2["timestamp"]=te2["Date"]+te2["Hour"]

    te2["Date2"]=pd.to_datetime(te2["timestamp"], format="%d%m%y%H%M%S")
    te2["Date"]=pd.to_datetime(te2["timestamp"], format="%d%m%y%H%M%S")
    te2["Date"]=te2['Date'].dt.tz_localize(tz='Europe/Paris',ambiguous=True)
    del te2["timestamp"]
    del te2["Hour"]


    te2

    for index,row in te2.iterrows():
        obj={}
        #obj["@timestamp"]=int(row["Date"].timestamp())*1000
        obj["@timestamp"]=row['Date'].isoformat() 
        obj["Duration"]=row["Duration"]
        obj["Code"]=row["Code"].replace(' ','')
        obj["Called"]=str(row["Called"]).replace(' ','')

        try:
            if int(obj["Called"]) in dfconfight:
                obj["Client"]=dfconfight[int(obj["Called"])]["Name"] 
        except:
            pass       

        obj["Caller"]=row["Caller"].replace(' ','')
        try:
            obj["Desk"]=int(row["Desk"])
        except:
            obj["Desk"]=row["Desk"]

        try:
            obj["DeskCaller"]=int(row["DeskCaller"])
        except:
            obj["DeskCaller"]=row["DeskCaller"]
        if str(obj["DeskCaller"])=='nan':
            obj["DeskCaller"]=""



        obj["InternalCaller"]=row["InternalCaller"]
        obj["InternalCalled"]=row["InternalCalled"]
        obj["SolidusCalled"]=row["SolidusCalled"]
        obj["CallType"]=row["CallType"]
        obj["Rings"]=row["Rings"]

        obj["A15"]=row["A15"]
        obj["A16"]=row["A16"]
        obj["A17"]=row["A17"]

        if "nan" not in obj["Called"]+'_'+obj["Caller"]:
            action["index"]={"_index":"telephony","_type":"doc","_id":str(int(row["Date2"].timestamp())*1000)+'_'+obj["Called"]+'_'+obj["Caller"]}        
            messagebody+=json.dumps(action)+"\r\n";
            messagebody+=json.dumps(obj)+"\r\n"

        

        # if "NaN" in messagebody:
        #     print("BAD")
        #     pass

        if(len(messagebody)>50000):
            logger.info ("BULK")
            try:
                resbulk=es.bulk(messagebody)
                #print(resbulk["errors"])
                if resbulk["errors"]:
                    logger.error("BULK ERROR")
                    for item in resbulk["items"]:
                    
                        for key in item:
                            if "error" in item[key]:                                
                                logger.error(item)
                    logger.info(messagebody)
                        

            except:
                logger.error("Unable to bulk",exc_info=True)
                logger.info(resbulk)
                
            messagebody=""

    try:
        if len(messagebody)>0:
            resbulk=es.bulk(messagebody)
    except:
        logger.error("Unable to bulk",exc_info=True)
        logger.error(resbulk)
    #print (messagebody)
    logger.info ("FINISHED")

    endtime = time.time()    
    try:
        log_message("Import of file [%s] finished. Duration: %d Records: %d." % (headers["file"],(endtime-starttime),df.shape[0]))         
    except:
        log_message("Import of file [%s] finished. Duration: %d." % (headers["file"],(endtime-starttime)))












###################################################################################################################################################




#>> ELK
es=None


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
                    ,"heartbeats":(180000,180000),"earlyack":True}

    lastvaluecache={}
    dayvaluecache={}
    conn=amqstompclient.AMQClient(server
        , {"name":MODULE,"version":VERSION,"lifesign":"/topic/NYX_MODULE_INFO"},QUEUE,callback=messageReceived)
    connectionparameters={"conn":conn}
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
            variables={"platform":"_/_".join(platform.uname()),"icon":"wrench"}
            conn.send_life_sign(variables=variables)
        except Exception as e:
            logger.error("Unable to send life sign.")
            logger.error(e)
    #app.run(threaded=True,host= '0.0.0.0')
