import sys
import traceback

import copy
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
from datetime import timezone
from io import StringIO
from dateutil import tz
import dateutil.parser


import tzlocal # $ pip install tzlocal


VERSION="1.0.2"
MODULE="GTC_IMPORT_OPTIBOX"
QUEUE=["GTC_IMPORT_SIGFOX"]



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
    global es
    records=0
    starttime = time.time()
    logger.info("==> "*10)
    logger.info("Message Received %s" % destination)
    logger.info(headers)
    
    inmes=json.loads(message)
    idDev = inmes['id']
    subType = int(inmes['messSubType'])
    subType1 = int(subType / 16)
    subType2 = subType % 16
    inmes['messType'] = int(inmes['messType'], 16)
    inmes['sequence'] = int(inmes['sequence'], 16)
    index1 = int(inmes['index1'])
    index2 = int(inmes['index2'])
    pin1 = subType1
    pin2 = subType2
    inmes['@timestamp'] = int(inmes['time'])*1000

    #relays_array[6]
    relays = bin(int(inmes['relays'], 16))[2:].zfill(8)
    relaysList = list(relays)

    logger.info("Relays state : "+relays)

    inmes['relay1'] = relaysList[0]
    inmes['relay2'] = relaysList[1]
    inmes['relay3'] = relaysList[2]
    inmes['relay4'] = relaysList[3]
    inmes['relay5'] = relaysList[4]
    inmes['relay6'] = relaysList[5]

    params_opti = {}
    confs = es.search(index="optibox_parameters", size=10000)
    config=confs['hits']['hits']
    confid = ''
    for param in config:
        params = param['_source']
        #print(params)
        if idDev in params['sigfoxid']:
            params_opti = params
            confid = param['_id']

    logger.info("ID Device : " + idDev)
    logger.info(params_opti)
    logger.info(confid)

    if params_opti == {}:
        params_opti = {
        "lastseen": inmes['@timestamp'],
        "sigfoxid": idDev,
        "client": "to be defined",
        "area": "to be defined",
        "hours": 0,
        "starts":0
        }
        logger.info('New Optibox')
    else:
        params_opti['lastseen'] = inmes['@timestamp']

    client = params_opti['client']
    area  = params_opti['area']
    hours = params_opti['hours']
    starts = params_opti['starts']

    logger.info("client:"+client)
    logger.info("area:"+area)
    logger.info("Start : " + str(starts) + " Hours: " + str(hours))
    inmes['area'] = area
    inmes['client'] = client

    inmes1 = dict()
    inmes2 = dict()

    source = 'optibox'

    inmes1['area'] = area
    inmes1['client'] = client
    inmes1['area_name'] = area
    inmes1['client_area_name'] = client
    inmes1['code'] = pin1
    inmes1['name'] = idDev
    inmes1['source'] = source
    inmes1['value'] = index1
    inmes1['value_15'] = index1
    inmes1['@timestamp'] = inmes['@timestamp']

    inmes2 = copy.deepcopy(inmes1)

    inmes2['code'] = pin2
    inmes2['value'] = index2
    inmes2['value_15'] = index2

    indexname="OPT_OPTIBOX_%s" %(datetime.now().strftime("%Y-%m"))
    indexname=indexname.lower()

    logger.info("index:"+indexname)
    bulkbody=""
    bulkbody1=""
    bulkbody2=""
    action={}
    action["index"]={"_index":indexname, "_type": "doc"}
    action1={}
    action1["index"]={"_index":indexname, "_type": "doc"}
    #action["_type"]
    jsonaction=json.dumps(action)
    logger.info("action:"+jsonaction)
    jsonaction1=json.dumps(action1)
    logger.info("action:"+jsonaction1)

    jsonobj=json.dumps(inmes)
    jsonobj1=json.dumps(inmes1)
    jsonobj2=json.dumps(inmes2)
    bulkbody+=jsonaction+"\n"
    bulkbody+=jsonobj+"\n"

    bulkbody1+=jsonaction1+"\n"
    bulkbody1+=jsonobj1+"\n"

    bulkbody2+=jsonaction1+"\n"
    bulkbody2+=jsonobj2+"\n"

    logger.info("Final %s=" %(bulkbody))
    logger.info("Bulk RTU ready.")
    if(bulkbody != ""):
        es.bulk(body=bulkbody)
        es.bulk(body=bulkbody1)
        es.bulk(body=bulkbody2)
    else:
        logger.error("No BODY !!!")
    logger.info("Bulk RTU gone.")

    if confid == '':
        action2 = {}
        action2["index"]={"_index":"optibox_parameters", "_type": "doc"}
        bulkbody3=""
        bulkbody3+=json.dumps(action2)+"\n"
        bulkbody3+=json.dumps(params_opti)+"\n"
        es.bulk(body=bulkbody3)
        #print(bulkbody3)
        logger.info("NEW OPTIBOX"*20)
        time.sleep(2)
    else:
        updatebulk = json.dumps({"doc": params_opti})
        logger.info(confid)
        logger.info(updatebulk)
        es.update(index='optibox_parameters',doc_type='doc',id=confid, body=updatebulk)
        logger.info("UPDATE OPTIBOX"*20)


    endtime = time.time()    
    try:
        log_message("Import of reccord finished. Duration: %d Records: %d." % ((endtime-starttime),df.shape[0]))         
    except:
        log_message("Import of reccord finished. Duration: %d." % ((endtime-starttime)))    
    
        

    logger.info("<== "*10)


def utc_to_local(utc_dt):
    return utc_dt.replace(tzinfo=timezone.utc).astimezone(tz=tz.tzlocal())

    

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
