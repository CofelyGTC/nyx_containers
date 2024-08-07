"""
NYX MONITOR DOCKER
====================================
Stores the current status of the docker containers in the elastic search
docker_status collection.

Listens to commands to /start/sop/restart containers.


Listens to:
-------------------------------------

* /topic/DOCKER_COMMAND

Payload
-------

.. code-block:: json
   :linenos:

   {
       "command":"restart",
        "name":"nyx_lambda_1"}

VERSION HISTORY
===============

* 11 May 2020 1.0.5 **AMA** First version
"""

import re
import json
import time
import uuid
import base64
import docker
import threading
import os,logging
import platform
import pandas as pd
from functools import wraps
from datetime import datetime,timedelta
from elastic_helper import es_helper
import amqstomp as amqstompclient
from logging.handlers import TimedRotatingFileHandler
from logstash_async.handler import AsynchronousLogstashHandler
from elasticsearch import Elasticsearch as ES
from elasticsearch.helpers import bulk
import dotenv
dotenv.load_dotenv()

VERSION="1.1.4"
MODULE="MonitorDocker"
QUEUE=["/topic/DOCKER_COMMAND"]


################################################################################
def getELKVersion(es):
    return int(es.info().get('version').get('number').split('.')[0])

################################################################################
def messageReceived(destination,message,headers):
    global es,FORCE_COMPUTATION
    logger.info("==> "*10)
    logger.info("Message Received %s" % destination)
    logger.info(message)
    jsonmes=json.loads(message)
    name=jsonmes["name"]
    command=jsonmes["command"]

    for container in client.containers.list(all=True):    
        if container.name==name:
            logger.info("FOUND")
            if command=="restart":
                container.restart()
            if command=="stop":
                container.stop()
            if command=="start":
                container.start()

        
    logger.info("<== "*10)



def load_data():
    logger.info("Load Data............................")
    global elkversion
    if elkversion <= 7: bulkbody=""
    else: bulkbody=[]

    for container in client.containers.list(all=True):        
        cont={
            "name":container.name,
            "status":container.status,
            "image":container.attrs['Config']['Image'],
            "created":container.attrs['Created'],
            "started":container.attrs['State']["StartedAt"]
        }
        # if cont["status"]=="running":
        #     stats=container.stats(stream=False)
        #     cont["memory_used"]=stats["memory_stats"]["usage"]/1000000
        #     cont["memory_used_mb"]=int(stats["memory_stats"]["usage"]/1000000)
        #     cont["@timestamp"]=datetime.now().isoformat()

        #     if(('cpu_stats' in stats) and ('cpu_usage' in stats['cpu_stats'])
        #         and ('total_usage' in stats['cpu_stats']['cpu_usage'])):
        #             cpuvalue=stats['cpu_stats']['cpu_usage']['total_usage']
        #             precpuvalue=stats['precpu_stats']['cpu_usage']['total_usage']

        #             cpuDelta = cpuvalue -  precpuvalue;

        #             if('system_cpu_usage' in stats['cpu_stats']) and ('system_cpu_usage' in stats['precpu_stats']):
        #                 systemvalue=stats['cpu_stats']['system_cpu_usage']
        #                 presystemvalue=stats['precpu_stats']['system_cpu_usage']
        #                 systemDelta = systemvalue - presystemvalue;

        #                 if (systemDelta >0):

        #                     RESULT_CPU_USAGE = float(cpuDelta) / float(systemDelta) * 100.0

        #                     cont['cpu_percent']=round(RESULT_CPU_USAGE,2)
        #print(cont)
        id=container.name.replace(" ","").lower()

        if cont["status"]=="running":
            cont["result_icon"]="regular/check-circle>#0F9D58"
        else:
            cont["result_icon"]="exclamation-circle>#DB4437"


        action={}
        if elkversion<=6:
            action["index"] = {"_index": "docker_status","_type": "_doc","_id":id}
            bulkbody+=json.dumps(action)+"\r\n"
            bulkbody+=json.dumps(cont)+"\r\n"  

        elif elkversion==7:
            action["index"] = {"_index": "docker_status","_id":id}
            bulkbody+=json.dumps(action)+"\r\n"
            bulkbody+=json.dumps(cont)+"\r\n"  

        else:
            action["_op_type"] = "index"
            action["_index"] = "docker_status"
            action["_id"] = id
            action["_source"] = cont
            bulkbody.append(action)

    logger.info("BULK")
    if elkversion <= 7:
        res=es.bulk(bulkbody) 
        if res["errors"]:    
            logger.info(res)
    else:
        try:
            success, failed = bulk(es, bulkbody)
            logger.info(f"Bulk indexing successful: {success}, failed: {failed}")
        except Exception as e:
            logger.error("Error during bulk indexing: %s", str(e))
        
    logger.info(">>>Load Data............................")

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

    elkversion=getELKVersion(es)

    logger.info("AMQC_URL          :"+os.environ["AMQC_URL"])
    client = docker.from_env()

    lastrun=datetime.now()

    while True:
        time.sleep(5)
        try:
            if lastrun+timedelta(seconds=30)<datetime.now():
                load_data()
                lastrun=datetime.now()
        except:
            logger.error("Unable monitor docker.",exc_info=True)
                
        try:                      
            variables={"platform":"_/_".join(platform.uname()),"icon":"brands/docker"}
            variables["queue"]=",".join(QUEUE)  
            conn.send_life_sign(variables=variables)
        except:
            logger.error("Unable to send life sign.",exc_info=True)
            
    #app.run(threaded=True,host= '0.0.0.0')