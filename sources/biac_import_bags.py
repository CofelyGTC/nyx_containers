"""
BIAC IMPORT BAGS
====================================

Sends:
-------------------------------------

* /topic/BIAC_BAGS_IMPORTED

Listens to:
-------------------------------------

* /queue/BAGS_IMPORT

Collections:
-------------------------------------

* **biac_bags** (Raw Data)
* **biac_month_bags** (Computed)


VERSION HISTORY
===============

* 23 Jul 2019 0.0.4 **VME** Code commented
* 25 Mar 2022 0.0.5 **PDB** Correction new date format
"""  
import re
import json
import time
import uuid
import base64
import platform
import threading
import os,logging

import pandas as pd

from logging.handlers import TimedRotatingFileHandler
from amqstompclient import amqstompclient
from datetime import datetime
from datetime import timedelta
from functools import wraps
from elasticsearch import Elasticsearch as ES, RequestsHttpConnection as RC
from logstash_async.handler import AsynchronousLogstashHandler
from lib import pandastoelastic as pte
import numpy as np
from sqlalchemy import create_engine


MODULE  = "BIAC_BAGS_IMPORTER"
VERSION = "0.0.5"
QUEUE   = ["BAGS_IMPORT"]

def sizeof_fmt(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)

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

    xlsbytes = base64.b64decode(message)
    f = open('./tmp/excel.xlsx', 'wb')
    f.write(xlsbytes)
    f.close()

    #xlsdf=pd.read_excel('./tmp/excel.xlsx', index_col=None)    
    #xlsname=headers["file"]

    df = None
    try:        
        file_name = headers["file"]
        

        site      = 'UNK'
        threshold = 0.5

        if 'dwell' not in file_name.lower() or '.xlsx' not in file_name:
            endtime = time.time()
            log_message("Import of file [%s] failed. Duration: %d Error: File name incorrect." % (headers["file"],(endtime-starttime))) 
            return
            

        if 'TRF' in file_name.upper():
            site = 'TRF'
            threshold = 1.5
        elif 'CI' in file_name.upper():
            site = 'CI'
            threshold = 0.5
            
        logger.info(site)

        df_global = pd.DataFrame()
        sheet_to_df_map = pd.read_excel('./tmp/excel.xlsx', sheet_name=None)

        for i in sheet_to_df_map:
            df_global=df_global.append(sheet_to_df_map[i])
            
        df_global['site'] = site
        df_global.columns = ['date', 'bags', 'less_than_15',
            '16_to_25', 'more_than_25',
            'percent_more_16', 'site']

        df_global['date'] = df_global['date'].apply(lambda x: datetime.strptime(x, '%d/%m/%Y'))    

        del df_global['percent_more_16']

        df_global['more_than_16'] = df_global['16_to_25'] + df_global['more_than_25']
        df_global['percent_recomputed'] = round(100*df_global['more_than_16'] / df_global['bags'], 2)


        df_global['color'] = 'green'
        df_global.loc[df_global['percent_recomputed'] > threshold , 'color'] = 'red'

        df_global['availability'] = 100 - df_global['percent_recomputed']

        df_glob2 = df_global.copy()
        df_glob2['color'] = df_glob2['color'].replace('green', 0).replace('red', 1)

        

        df_month = df_glob2.set_index('date').groupby(pd.Grouper(freq='M'))\
                .agg({'availability':'mean', 'color': 'sum'}).reset_index()\
                .rename(columns={'color':'bad_days'})

        df_month['date'] = df_month['date'] - pd.offsets.MonthBegin(1) 
        df_month['availability'] = round(df_month['availability'], 2)
        df_month['site'] = site

        df_global['percent_recomputed'] = df_global['percent_recomputed'].apply(lambda x: ('%f' % x).rstrip('0').rstrip('.'))
        df_global['availability'] = df_global['availability'].apply(lambda x: ('%f' % x).rstrip('0').rstrip('.'))
        df_month['availability'] = df_month['availability'].apply(lambda x: ('%f' % x).rstrip('0').rstrip('.'))

        logger.info(df_global.head())

        df_global['_id']    = df_global.apply(lambda row: row['site']+'_'+str(int(row['date'].timestamp()*1000)), axis=1)
        df_global['_index'] = 'biac_bags'

        df_month['_id']    = df_month.apply(lambda row: row['site']+'_'+str(int(row['date'].timestamp()*1000)), axis=1)
        df_month['_index'] = 'biac_month_bags'

        pte.pandas_to_elastic(es, df_global)
        pte.pandas_to_elastic(es, df_month)


        first_bags_ts = df_global['date'].min()
        last_bags_ts = df_global['date'].max()
        obj = {
                'start_ts': int(first_bags_ts.timestamp()),
                'end_ts': int(last_bags_ts.timestamp())
            }

        logger.info('sending message to /topic/BIAC_BAGS_IMPORTED')
        logger.info(obj)


        postgresqlurl=('postgresql://%s:%s@%s:%s/nyx' %(os.environ["PSQL_LOGIN"],os.environ["PSQL_PASSWORD"],os.environ["PSQL_URL"],os.environ["PSQL_PORT"]))
        #logger.info("SQL "*30)
        #logger.info(postgresqlurl)

        engine = create_engine(postgresqlurl)

        logger.info("*-"*200)


        df_global.to_sql("biac_bags", engine, if_exists='replace')
        df_month.to_sql("biac_bags_month", engine, if_exists='replace')

        logger.info("<== "*10)


        conn.send_message('/topic/BIAC_BAGS_IMPORTED', json.dumps(obj))
    except Exception as e:
        endtime = time.time()
        logger.error(e,exc_info=e)
        log_message("Import of file [%s] failed. Duration: %d Exception: %s." % (headers["file"],(endtime-starttime),str(e)))        


    endtime = time.time()    
    try:
        log_message("Import of file [%s] finished. Duration: %d Records: %d." % (headers["file"],(endtime-starttime),df_global.shape[0]))         
    except:
        log_message("Import of file [%s] finished. Duration: %d." % (headers["file"],(endtime-starttime)))    
    
        


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
           
            variables={"platform":"_/_".join(platform.uname()),"icon":"suitcase"}
            conn.send_life_sign(variables=variables)
        except Exception as e:
            logger.error("Unable to send life sign.")
            logger.error(e)
    #app.run(threaded=True,host= '0.0.0.0')
