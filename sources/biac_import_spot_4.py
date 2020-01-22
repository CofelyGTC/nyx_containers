"""
BIAC IMPORT SPOT 4
====================================

Sends:
-------------------------------------

* /topic/BIAC_KIZEO_IMPORTED_2

Listens to:
-------------------------------------

Collections:
-------------------------------------

* **biac_spot_lot4** (Raw Data)

VERSION HISTORY
===============

* 21 Oct 2019 0.0.1 **VME** First commit
* 30 Oct 2019 0.0.2 **VME** Buf fixing r.text empty and better error log.
* 30 Oct 2019 1.0.0 **AMA** Use data get rest api exports_info function to get record ids
* 27 Nov 2019 1.0.1 **VME** Delete data before inserting to weakly handle the deletion of records in Kizeo + send message to kizeo month 2
* 03 Dec 2019 1.0.2 **VME** Delete data even when no data from kizeo 
* 05 Dec 2019 1.0.3 **VME** Bug fix
* 22 Jan 2020 1.0.4 **VME** delete by query instead of deleting indices to always have an existing indice in order to have a working generic table when no data
"""  
import re
import sys
import json
import time
import uuid
import pytz
import base64
import tzlocal
import calendar
import platform
import requests
import traceback
import threading
import os,logging
import numpy as np
import pandas as pd

import datetime as dt
from datetime import date
from pytz import timezone
from functools import wraps
from datetime import datetime
from datetime import timedelta
from elastic_helper import es_helper
from lib import pandastoelastic as pte
from lib import elastictopandas as etp
from amqstompclient import amqstompclient
from logging.handlers import TimedRotatingFileHandler
from logstash_async.handler import AsynchronousLogstashHandler
from elasticsearch import Elasticsearch as ES, RequestsHttpConnection as RC



MODULE  = "BIAC_SPOT4_IMPORTER"
VERSION = "1.0.3"
QUEUE   = []

localtz = timezone('Europe/Paris')

class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, dt.datetime):
            return o.isoformat()
            
        elif isinstance(o, dt.time):
            return o.isoformat()

#        elif isinstance(o, dttime):
#            return o.isoformat()

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

################################################################################
def messageReceived(destination,message,headers):
    global es
    logger.info("==> "*10)
    logger.info("Message Received %s" % destination)
    logger.info(headers)


#################################################
def loadKizeo():
    try:
        global es
        starttime = time.time()
        logger.info(">>> LOADING SPOTCHECKS LOT4")
        logger.info("==================")
        url_kizeo = 'https://www.kizeoforms.com/rest/v3'
        
        kizeo_user=os.environ["KIZEO_USER"]
        kizeo_password=os.environ["KIZEO_PASSWORD"]
        kizeo_company=os.environ["KIZEO_COMPANY"]


        payload = {
            "user": kizeo_user,
            "password": kizeo_password,
            "company": kizeo_company
            }

        r = requests.post(url_kizeo + '/login', json = payload)
        if r.status_code != 200:
            logger.error('Unable to reach Kizeo server. Code:'+str(r.status_code)+" Reason:"+str(r.reason))
            return

        response = r.json()
        token = response['data']['token']
        logger.info('>Token: '+str(token))

        r = requests.get(url_kizeo + '/forms?Authorization='+token)
        form_list = r.json()['forms']

        logger.info('>Form List: ')

        df_all=pd.DataFrame()
        for i in form_list:
            if 'SPOTCHECK ~ Lot 4' in i['name'] and 'test' not in i['name']:
                logger.info(i['name'])
                form_id = i['id']
                start=(datetime.now()+timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")
                logger.info("Start %s" %(start))            
                end = datetime(2019, 1, 1)
                logger.info("End %s" %(end))
                #post={"onlyFinished":False,"startDateTime":start,"endDateTime":end,"filters":[]}

                #r = requests.post(url_kizeo + '/forms/' + form_id + '/data/exports_info?Authorization='+token,post)
                r = requests.get(url_kizeo + '/forms/' + form_id + '/data/exports_info?Authorization='+token)

                if r.status_code != 200:
                    logger.info('something went wrong...')
                    logger.info(r.status_code, r.reason)
                elif r.text == '':
                    logger.info('Empty response')
                else:
                    ids=[]
                    for rec in r.json()["data"]:
                        ids.append(rec["id"])                    
                    
                    logger.info(ids)
                    payload={
                    "data_ids": ids
                    }
                    posturl=("%s/forms/%s/data/multiple/excel_custom" %(url_kizeo,form_id))
                    headers = {'Content-type': 'application/json','Authorization':token}

                    r=requests.post(posturl,data=json.dumps(payload),headers=headers)

                    if r.status_code != 200:
                        logger.info('something went wrong...')
                        logger.info(r.status_code, r.reason)

                    logger.info("Handling Form. Content Size:"+str(len(r.content)))
                    if len(r.content) >0:

                        file = open("./tmp/excel.xlsx", "wb")
                        file.write(r.content)
                        file.close()

                        df_all = df_all.append(pd.read_excel("./tmp/excel.xlsx"))

        if len(df_all) > 0:
            df_all['KPI']=df_all['KPI'].str.extract(r'([0-9]{3})').astype(int)
            df_all['datetime'] = df_all.apply(lambda row: localtz.localize(datetime.combine(row['Datum Controle'].date(), 
                                                                                    row['Uur'])), axis=1) 

            del df_all['Datum Controle']
            del df_all['Uur']

            df_all.columns = ['record_nummer', 'contract', 'kpi', 'madeBy', 'building', 'floor', 
                        'comment', 'check', 'conform', 'not_conform', 'percentage',
                        'qr_code_1', 'qr_code_1_cabin_name', 'qr_code_2', 'qr_code_2_cabin_name', 
                        'qr_code_3', 'qr_code_3_cabin_name', 'datetime']


            df_all['_timestamp'] = df_all['datetime']
            df_all['_index'] = 'biac_spot_lot4'
            df_all['_id'] = df_all['record_nummer']
            df_all['lot'] = 4

            try:
                # es.indices.delete('biac_spot_lot4')
                es.delete_by_query(index='biac_spot_lot4', doc_type='', body={"query":{"match_all": {}}})

                 
            except:
                logger.warn('unable to delete biac_spot_lot4')
                pass

            es_helper.dataframe_to_elastic(es, df_all)

            obj = {
                'start_ts': int(datetime(2019, 1, 1).timestamp()),
                'end_ts': int(datetime.now().timestamp())
            }
                 
            conn.send_message('/topic/BIAC_KIZEO_IMPORTED', json.dumps(obj))
            conn.send_message('/topic/BIAC_KIZEO_IMPORTED_2', json.dumps(obj))

        else:
            try:
                # es.indices.delete('biac_spot_lot4')
                es.delete_by_query(index='biac_spot_lot4', doc_type='', body={"query":{"match_all": {}}})
            except:
                logger.info('already no data')
                pass      

    except Exception as e:
        endtime = time.time()
        exc_type, exc_value, exc_traceback = sys.exc_info()
        logging.error('Ooops', exc_info=True)





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

    SECONDSBETWEENCHECKS=3600

    nextload=datetime.now()

    while True:
        time.sleep(5)
        try:            
            variables={"platform":"_/_".join(platform.uname()),"icon":"list-alt"}
            
            conn.send_life_sign(variables=variables)

            if (datetime.now() > nextload):
                try:
                    nextload=datetime.now()+timedelta(seconds=SECONDSBETWEENCHECKS)
                    loadKizeo()
                except Exception as e2:
                    logger.error("Unable to load kizeo.")
                    logger.error(e2,exc_info=True)
            
        except Exception as e:
            logger.error("Unable to send life sign.")
            logger.error(e)
