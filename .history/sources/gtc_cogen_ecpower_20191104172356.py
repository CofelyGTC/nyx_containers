"""
GTC COGEN ECPOWER
====================================

Collections:
-------------------------------------


VERSION HISTORY
===============

* 04 Nov 2019 0.0.1 **PDE** First version


"""  
import re
import sys
import json
import time
import uuid
import pytz
import base64
import tzlocal
import platform
import requests
import traceback
import threading
import os,logging
import numpy as np
import pandas as pd
from elastic_helper import es_helper 
from dateutil.tz import tzlocal
from tzlocal import get_localzone

from functools import wraps
import datetime as dt
from datetime import datetime
from datetime import timedelta
#from lib import pandastoelastic as pte
from amqstompclient import amqstompclient
from logging.handlers import TimedRotatingFileHandler
from logstash_async.handler import AsynchronousLogstashHandler
from elasticsearch import Elasticsearch as ES, RequestsHttpConnection as RC

from selenium import webdriver
from selenium.webdriver.common.keys import Keys

import collections
import dateutil.parser


containertimezone=pytz.timezone(get_localzone().zone)

MODULE  = "GTC_COGEN_ECPOWER"
VERSION = "0.0.3"
QUEUE   = ["GTC_COGEN_ECPOWER_RANGE"]

class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, dt.datetime):
            return o.isoformat()

        elif isinstance(o, dt.time):
            return o.isoformat()

        return json.JSONEncoder.default(self, o)

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

def getDate(ts):
    #ts = int(ts)/1000
    #dt = datetime.fromtimestamp(ts)
    dt = ts
    dateStr = str(dt.year) + '-' + "{:02d}".format(dt.month) + '-' + "{:02d}".format(dt.day)
    return dateStr

def getMonth(dateStr):
    month = dateStr[:-3]
    return month

def getYear(dateStr):
    year = dateStr[:4]
    return year

def getECPowerCogens():
    global es
    cogens = es_helper.elastic_to_dataframe(es, 'cogen_parameters', query="source: ecpower")
    cogens = cogens [['ecid', 'name']]
    cogensTab = []
    for key, row in cogens.iterrows():
        rec = {"id": row['ecid'], "name": row['name']}
        cogensTab.append(rec)

    return cogensTab


def doTheWork(start):
    logger.info("Do the work ...")
    ids = getECPowerCogens()
    driver = webdriver.PhantomJS()
    driver.get("https://service.ecpower.dk/login")
    assert "EC POWER" in driver.title
    endtime=datetime.now().strftime("%d-%m-%Y %H%%3A%M")

    username = driver.find_element_by_id('brugernavn')
    username.send_keys(os.environ["ECPOWER_LOGIN"])
    password = driver.find_element_by_id("kodeord")
    password.send_keys(os.environ["ECPOWER_PWD"])

    loginbutton = driver.find_element_by_class_name('submit ')
    loginbutton.click()
    time.sleep(2)

    dt = datetime.now()
    dtstr = dt.strftime('%Y/%m/%d %H:%M:%S')

    finaldata = ''

    for cogen in ids:
        logger.info("===============>")
        logger.info(cogen["name"])
        tag = cogen['name'].replace(' ', '_')
        hours = 0
        starts = 0

        url="https://service.ecpower.dk/jsp/statistikgen2.jsp?anlaegid="+cogen["id"]+"&showresult=true&from=01-01-15+00%3A00&to="+endtime+"&B1=Get+data"
        driver.get(url)

        tables=driver.find_elements_by_xpath("//table")
        #logger.info(tables)
        for table in tables:
            logger.info("====>")
            html=table.get_attribute("innerHTML")        
            for htmlline in html.split("\n"):
                if "Total operating time:" in htmlline:
                    logger.info("H ==================================")
                    
                    indexhours=htmlline.find("hours")
                    if indexhours>0:
                        hours=htmlline[20:indexhours]
                        hours=int(re.sub("[^0-9]", "",hours))
                        logger.info("HOURS=%d " %(hours))

                if "Total number of generator starts:" in htmlline:
                    logger.info("S ==================================")
                    starts=int(re.sub("[^0-9]", "",htmlline))
                    logger.info("STARTS=%d " %(starts))


        rowdatahours = tag+'_hours,'+dtstr+',2,'+str(hours)+',0\n'
        rowdatastarts = tag+'_starts,'+dtstr+',2,'+str(starts)+',0\n'   
        finaldata+= rowdatahours
        finaldata+= rowdatastarts  
        time.sleep(2)

    logger.info(finaldata)
    outputfile = open('./ECPOWER/NyxAWS_'+dt.strftime('%Y%m%d_%H%M%S')+'.csv', 'w+')
    outputfile.write(finaldata)
    outputfile.close()



    driver.close()









def messageReceived(destination,message,headers):
    global es
    logger.info("==> "*10)
    logger.info("Message Received %s" % destination)
    logger.info(headers)

    msg = json.loads(message)
    start = datetime.fromtimestamp(int(msg['start']))
    stop = datetime.fromtimestamp(int(msg['stop']))

    while start <= stop:
        doTheWork(start)
        start = start + timedelta(1)





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
                    ,"heartbeats":(1200000,1200000),"earlyack":True}
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
    logger.info("AMQC_URL          :"+os.environ["AMQC_URL"])

    SECONDSBETWEENCHECKS=3600

    nextload=datetime.now()

    while True:
        time.sleep(5)
        try:            
            variables={"platform":"_/_".join(platform.uname()),"icon":"list-alt"}
            
            conn.send_life_sign(variables=variables)


            if (datetime.now() > nextload):
                try:
                    start = datetime.now()
                    start = start.replace(hour=0,minute=0,second=0, microsecond=0)
                    nextload=datetime.now()+timedelta(seconds=SECONDSBETWEENCHECKS)
                    doTheWork(start)
                except Exception as e2:
                    logger.error("Unable to load sites data.")
                    logger.error(e2,exc_info=True)

            
        except Exception as e:
            logger.error("Unable to send life sign.")
            logger.error(e)
    #app.run(threaded=True,host= '0.0.0.0')    