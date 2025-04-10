"""
BIAC MONTH KIZEO 2
====================================

Sends:
-------------------------------------

Listens to:
-------------------------------------

* /topic/BIAC_KIZEO_IMPORTED_2

Collections:
-------------------------------------

* **biac_month_2_kizeo** (Computed Data)

VERSION HISTORY
===============

* 23 Jul 2019 0.0.2 **VME** Code commented
* 24 Jul 2019 0.0.3 **VME** Modification of agg to fill the requirements for BACFIR dashboards (Maximo)
* 25 Nov 2019 0.0.4 **VME** Adding lot4 (comes from another collection biac_spot_lot4)
* 27 Nov 2019 0.0.5 **VME** Bug fixing
* 04 Dec 2019 0.0.6 **VME** Fix buf when biac_spot_lot4 doesnt exist
* 09 Dec 2019 1.0.0 **VME** Replacing pte by es_helper
"""  
import re
import json
import time
import uuid
import base64
import tzlocal
import platform
import calendar
import threading
import os,logging
import numpy as np
import pandas as pd
import elasticsearch
from math import ceil

from datetime import date
from functools import wraps
from datetime import datetime
from datetime import timedelta
from elastic_helper import es_helper
import amqstomp as amqstompclient
from logging.handlers import TimedRotatingFileHandler
from logstash_async.handler import AsynchronousLogstashHandler
from elasticsearch import Elasticsearch as ES

MODULE  = "BIAC_MONTH_KIZEO_2"
VERSION = "1.1.1"
QUEUE   = ["/topic/BIAC_KIZEO_IMPORTED_2"]

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


def add_months(sourcedate, months):
    month = sourcedate.month - 1 + months
    year = sourcedate.year + month // 12
    month = month % 12 + 1
    day = min(sourcedate.day, calendar.monthrange(year,month)[1])
    return date(year, month, day)

def define_interval(dt_last_update):
    print('last_update          : '+str(dt_last_update))
    dt_last_update
    last_month = (add_months(dt_last_update,-1))    
    dt_start = datetime(last_month.year, last_month.month, 1, 0, 0, 0, 0)    

    dt_start = datetime(2019, 1, 1)
    
    return {
        'dt_start' : dt_start,
        'dt_end'   : dt_last_update
    } 

################################################################################
def messageReceived(destination,message,headers):
    global es
    starttime = time.time()
    logger.info("==> "*10)
    logger.info("Message Received %s" % destination)
    logger.info(headers)
    local_timezone = tzlocal.get_localzone()

    try: 
        logger.info('waiting 5 sec before doing the request to be sure date are correctly inserted by biac_import_kizeo.py')
        time.sleep(5)

        logger.info('message: '+str(message))

        obj    = json.loads(message)

        last_update    = datetime.fromtimestamp(obj['end_ts'])

        interval = define_interval(last_update)

        start_dt = interval['dt_start']
        end_dt   = interval['dt_end']

        logger.info(start_dt)
        logger.info(end_dt)

        df = es_helper.elastic_to_dataframe(es, index="biac_kizeo", scrollsize=1000, start=start_dt, end=end_dt,
                                    datecolumns=["@timestamp"])

        df['month'] = df['@timestamp'].dt.strftime('%Y-%m') 

        df_grouped = df.groupby(['lot', 'kpi', 'contract', 'screen_name', 'month']) \
                .agg({'check_conform':'sum', 'check_no_conform':'sum', 'check_number':'sum', '@timestamp':'max'}) \
                    .reset_index()

        df_grouped2 = df[df['contract']=='BACFIR'].groupby(['lot', 'kpi', 'contract', 'month']) \
                .agg({'check_conform':'sum', 'check_no_conform':'sum', 'check_number':'sum', '@timestamp':'max'}) \
                    .reset_index()

        df_grouped2['screen_name'] = 'BACFIR'
        df_grouped=df_grouped.append(df_grouped2)


        df_lot4 = pd.DataFrame()

        try:
            #handling lot4
            df_lot4 = es_helper.elastic_to_dataframe(es, index="biac_spot_lot4", query="kpi:302", scrollsize=1000, 
                                            start=start_dt, end=end_dt,
                                            datecolumns=["@timestamp"])
        except elasticsearch.NotFoundError:
            logger.warn('Index biac_spot_lot4 does not exist')
        

        if len(df_lot4) == 0:
            obj = {
                'lot': '4',
                'kpi': 302,
                'contract': 'DNBBA',
                'screen_name': 'DNBBA',
                'month': '2019-01',
                'conform': 0,
                'not_conform': 0,
                'check': 0,
                '@timestamp': datetime(2019, 1, 1, tzinfo=local_timezone),
            }

            df_lot4 = pd.DataFrame.from_dict({0: obj.values()}, orient='index', columns=obj.keys())

        else:
            df_lot4['screen_name'] = 'BACDNB'
            df_lot4['month'] = df_lot4['@timestamp'].dt.strftime('%Y-%m') 

        df_lot4['lot'] = df_lot4['lot'].astype(str)

        df_grouped_lot4 = df_lot4.groupby(['lot', 'kpi', 'contract', 'screen_name', 'month']) \
                .agg({'conform':'sum', 'not_conform':'sum', 'check':'sum', '@timestamp':'max'}) \
                    .reset_index()

        df_grouped_lot4=df_grouped_lot4.rename(columns={"conform": "check_conform", "not_conform": "check_no_conform", 
                                        "check": "check_number"})

        df_grouped=df_grouped.append(df_grouped_lot4)

        df_grouped['_index'] = 'biac_month_2_kizeo'

        df_grouped['_id']    = df_grouped.apply(lambda row: row['lot'] + '_' + str(row['kpi']) + '_' + \
                                                            row['contract'].lower() + '_' + row['screen_name'].lower() + '_' + \
                                                            str(int(row['@timestamp'].timestamp())), axis=1)


        df_grouped=df_grouped.rename(columns={"@timestamp": "last_update"})

        df_grouped['_id']    = df_grouped['_id'].str.replace(' ', '_')
        df_grouped['_id']    = df_grouped['_id'].str.replace('/', '_')

        df_grouped['percentage_conform'] = round(100*(df_grouped['check_conform'] / df_grouped['check_number']), 2).fillna(100)
        df_grouped['percentage_conform'] = df_grouped['percentage_conform'].apply(lambda x: ('%f' % x).rstrip('0').rstrip('.'))

        df_grouped['percentage_no_conform'] = round(100*(df_grouped['check_no_conform'] / df_grouped['check_number']), 2).fillna(0)
        df_grouped['percentage_no_conform'] = df_grouped['percentage_no_conform'].apply(lambda x: ('%f' % x).rstrip('0').rstrip('.'))

        df_grouped['percentage_conform'] = df_grouped['percentage_conform'].fillna(100)
        df_grouped['percentage_no_conform'] = df_grouped['percentage_no_conform'].fillna(0)

        logger.info(df_grouped)

        es_helper.dataframe_to_elastic(es, df_grouped)

    except Exception as e:
        endtime = time.time()
        logger.error(e, exc_info=True)
        log_message("Process month Kizeo failed. Duration: %d Exception: %s." % ((endtime-starttime),str(e)))        


    endtime = time.time()    
    try:
        log_message("Process month Kizeo finished. Duration: %d Records: %d." % ((endtime-starttime),df_grouped.shape[0]))         
    except:
        log_message("Process month Kizeo finished. Duration: %d." % ((endtime-starttime)))    

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
            variables={"platform":"_/_".join(platform.uname()),"icon":"list-alt"}
            
            conn.send_life_sign(variables=variables)
        except Exception as e:
            logger.error("Unable to send life sign.")
            logger.error(e)
    #app.run(threaded=True,host= '0.0.0.0')
