import sys
import traceback

import json
import time
import uuid
import json
import base64
import platform
import threading
import os,logging
import pandas as pd
from logging.handlers import TimedRotatingFileHandler
from amqstompclient import amqstompclient
import datetime
from datetime import datetime
from datetime import timedelta
from functools import wraps
from elasticsearch import Elasticsearch as ES, RequestsHttpConnection as RC
from logstash_async.handler import AsynchronousLogstashHandler
from lib import pandastoelastic as pte
import numpy as np
import collections


import tzlocal # $ pip install tzlocal


VERSION="1.0.5"
MODULE="GTC_IMPORT_ONERP"
QUEUE=["DISPATCHING_EVENTS"]

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


def cleanTitle(t):
    if "span>" in t:
        sp=t.split("span>")
        return sp[len(sp)-1]
    return t

################################################################################
def messageReceived(destination,message,headers):
    global es
    records=0
    starttime = time.time()
    logger.info("==> "*10)
    logger.info("Message Received %s" % destination)
    logger.info(headers)

    inmes=json.loads(message)    

    client = "ONERP"
    #area=inmes['area'];

    logger.info("client:"+client)
    #logger.info("area:"+area);

    indexname="ONERP_EVENTDISPA-%s" %(datetime.now().strftime("%Y-%m"))
    indexname=indexname.lower()

    logger.info("index:"+indexname)
    bulkbody=""

    action={}
    action["index"]={"_index":indexname,"_type":"doc"}
    jsonaction=json.dumps(action)
    logger.info("action:"+jsonaction)

    action2={}
    action2["index"]={"_index":"ONERP_ALERTDISPA".lower(),"_type":"doc","_id":"id"+str(inmes["evtID"])}
    jsonaction2=json.dumps(action2)
    logger.info("action:"+jsonaction2)
    
    inmes["dispatchingTS"]=inmes["utcDispatching"].replace(" ","T")+"Z"
    inmes["sourceTS"]=inmes["utcSource"].replace(" ","T")+"Z"

    if "alrMsg" in inmes:
        inmes["alrMsg"]=cleanTitle(inmes["alrMsg"])

    jsonobj=json.dumps(inmes)
#    bulkbody+=jsonaction+"\n"
#    bulkbody+=jsonobj+"\n"

    bulkbody+=jsonaction2+"\n"
    bulkbody+=jsonobj+"\n"

    logger.info("Final %s=" %(bulkbody))
    logger.info("Bulk RTU ready.")
    if(bulkbody != ""):
        res = es.bulk(body=bulkbody)
        logger.info(res)
    else:
        logger.error("No BODY !!!")
    logger.info("Bulk RTU gone.")    

    endtime = time.time()    
    try:
        log_message("Import of Onerp message finished. Duration: %d." % ((endtime-starttime)))         
    except:
        log_message("Import of Onerp message finished but in except. Duration: %d." % ((endtime-starttime)))    
    
        

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
                ,"login":os.environ["AMQC_LOGIN"],"password":os.environ["AMQC_PASSWORD"]
                ,"heartbeats":(180000,180000),"earlyack":True}


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
            variables={"platform":"_/_".join(platform.uname()),"icon":"wrench"}
            conn.send_life_sign(variables=variables)
        except Exception as e:
            logger.error("Unable to send life sign.")
            logger.error(e)
    #app.run(threaded=True,host= '0.0.0.0')
