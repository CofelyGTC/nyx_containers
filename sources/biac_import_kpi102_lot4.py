"""
BIAC KPI 102
====================================

Sends:
-------------------------------------

* /topic/BIAC_KPI102_LOT4_IMPORTED

Listens to:
-------------------------------------

* /queue/KPI102_LOT4_IMPORT

Collections:
-------------------------------------

* **biac_kpi102_lot4** (Raw Data)
* **biac_kib_kpi102_lot4** (Heat map and horizontal bar stats)

VERSION HISTORY
===============

* 09 Sep 2019 0.0.1 **VME** First version
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

from functools import wraps

from datetime import date
from datetime import timedelta
from datetime import datetime
import datetime as dt

from elastic_helper import es_helper
from amqstompclient import amqstompclient
from logging.handlers import TimedRotatingFileHandler
from logstash_async.handler import AsynchronousLogstashHandler
from elasticsearch import Elasticsearch as ES, RequestsHttpConnection as RC



MODULE  = "BIAC_KPI102_LOT4_IMPORTER"
VERSION = "0.0.1"
QUEUE   = ["KPI102_LOT4_IMPORT"]

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
        

def compute_kib_index(es, df_all):
    df_week = df_all.copy()
    df_week['monday'] = df_week.apply( \
            lambda row: datetime.date(row['create_time'] - timedelta(days=row['create_time'].weekday())) \
                                     , axis=1)

    df_week[['create_time', 'monday']]


    df_kib = df_week[['monday', 'ronde_letter']]
    df_kib.drop_duplicates(inplace=True)
    df_kib.set_index('monday', inplace=True)


    df_kib=df_kib.pivot(columns='ronde_letter', values='ronde_letter')

    df_kib=df_kib.reset_index().rename_axis(None, axis=1).set_index('monday')

    df_kib#.loc[~df_kib.isnull()] = 1


    df_kib.loc[~df_kib['A'].isnull(),'A']= 1
    df_kib.loc[~df_kib['B'].isnull(),'B']= 1
    df_kib.loc[~df_kib['C'].isnull(),'C']= 1
    df_kib.fillna(0, inplace=True)
    df=pd.DataFrame(index=pd.date_range(start=min(df_kib.index) - timedelta(days=1) \
                                        , end=max(df_kib.index), freq='W'))
    df.index = df.index + timedelta(days=1)
    df.index.name='monday'
    df = df.merge(df_kib, how='left', left_index=True, right_index=True).fillna(0)
    
    bulk_body=''
    
    for index, row in df.iterrows():
        for letter in ['A', 'B', 'C']:
            obj= {
                '@timestamp': index,
                'ronde': letter,
                'rec_type': 'heatmap',
                'done' : 1,
                'globalpercentage': 100,
                'lot':4,
                'str_date': index.strftime('%d-%b-%Y')
            }
            
            if row[letter] == 0:
                obj['done'] = 0
                obj['globalpercentage'] = 0
        
            _id = 'heatmap_'+letter+'_'+str(index)
            _index = 'biac_kib_kpi102_lot4'
            
            
            action = {}
            action["index"] = {"_index": _index, "_type": "doc", "_id": _id}

            bulk_body += json.dumps(action) + "\r\n"
            bulk_body += json.dumps(obj, cls=DateTimeEncoder) + "\r\n"
    
    bulkres=es.bulk(bulk_body)

    
    
    return bulkres



#################################################
def loadKPI102():
    try:
        starttime = time.time()
        logger.info(">>> LOADING KPI102 LOT4")
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
            logger.error('Something went wrong...')
            logger.error(r.status_code, r.reason)
            return

        response = r.json()
        token = response['data']['token']
        logger.info('>Token: '+str(token))

        r = requests.get(url_kizeo + '/forms?Authorization='+token)
        form_list = r.json()['forms']

        logger.info('>Form List: ')
        #logger.info(form_list)

        df_all=pd.DataFrame()
        for i in form_list:
            if 'LOT 4 - HS Cabines ~ Wekelijkse Ronde ' in i['name']:
                logger.info('MATCH')
                logger.info(i['name'])
                
                form_id = i['id']
                r = requests.get(url_kizeo + '/forms/' + form_id + '/data/all?Authorization='+token)

                if r.status_code != 200:
                    print('something went wrong...')
                    print(r.status_code, r.reason)
        
                
                if len(r.content) >0 and len(r.json()['data'])>0:
                    
                    #print(r.content)
                        
                    df=pd.DataFrame(r.json()['data'])
                    df['ronde'] = i['name']
                    
                    df_all=df_all.append(df)

        if len(df_all) > 0:
            df_all['ronde_letter'] = df_all['ronde'].str \
                                    .replace(' \(Rondier\)', '') \
                                    .str.replace('LOT 4 - HS Cabines ~ Wekelijkse Ronde ','')
                                

            df_all['answer_time'] = pd.to_datetime(df_all['answer_time'], utc=False).dt.tz_localize('Europe/Paris')
            df_all['create_time'] = pd.to_datetime(df_all['create_time'], utc=False).dt.tz_localize('Europe/Paris')
            df_all['@timestamp'] = df_all['create_time']
            df_all['_index'] = 'biac_kpi102_lot4'
            df_all['ts'] = df_all['@timestamp'].values.astype(np.int64)  // 10 ** 9
            df_all['_id'] = df_all['record_number'].astype(str) + '_' + df_all['ronde_letter'] + '_' +  df_all['ts'].astype(str)
            df_all['lot'] = 4
            del df_all['ts']                           
            
            es_helper.dataframe_to_elastic(es, df_all)

            compute_kib_index(es, df_all)

            endtime = time.time()
            log_message("Import of KPI102 LOT4 from Kizeo finished. Duration: %d Records: %d." % (endtime-starttime, df_all.shape[0]))       


    


    except Exception as e:
        endtime = time.time()
        exc_type, exc_value, exc_traceback = sys.exc_info()
        traceback.print_tb(exc_traceback, limit=1, file=sys.stdout)
        # exc_type below is ignored on 3.5 and later
        traceback.print_exception(exc_type, exc_value, exc_traceback,
                              limit=2, file=sys.stdout)





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
                    nextload=datetime.now()+timedelta(seconds=SECONDSBETWEENCHECKS)
                    loadKPI102()
                except Exception as e2:
                    logger.error("Unable to load kizeo.")
                    logger.error(e2,exc_info=True)
            
        except Exception as e:
            logger.error("Unable to send life sign.")
            logger.error(e)
    #app.run(threaded=True,host= '0.0.0.0')