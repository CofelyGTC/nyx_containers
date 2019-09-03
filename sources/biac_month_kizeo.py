import json
import time
import uuid
import base64
import threading
import os,logging
import pandas as pd
import platform
import re

from logging.handlers import TimedRotatingFileHandler
from amqstompclient import amqstompclient
from datetime import datetime
from datetime import timedelta
from functools import wraps
from elasticsearch import Elasticsearch as ES, RequestsHttpConnection as RC
from logstash_async.handler import AsynchronousLogstashHandler
from lib import pandastoelastic as pte
from lib import elastictopandas as etp
import numpy as np
from math import ceil

MODULE  = "BIAC_MONTH_KIZEO"
VERSION = "0.0.5"
QUEUE   = ["/topic/BIAC_KIZEO_IMPORTED"]

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



def define_interval(dt_last_update):
    logger.info('last_update          : '+str(dt_last_update))
    
    week_last_update        = week_of_year(dt_last_update.timestamp()*1000)
    
    logger.info('week last update     : '+str(week_last_update))
    
    first_day_week          = dt_last_update - timedelta(days=dt_last_update.weekday())

    logger.info('first day week       : '+str(first_day_week))
    
    week_first_day_of_month = week_of_year(first_day_week.replace(day=1).timestamp()*1000)

    logger.info('week first day month : '+str(week_first_day_of_month))
    
    return {
        'week_start' : week_first_day_of_month,
        'week_end'   : week_last_update
    }

def week_of_year(ts):
    ts = int(ts) / 1000
    dt = datetime.utcfromtimestamp(ts)
    weekOfYear = dt.isocalendar()[1]
    return weekOfYear

def week_of_month(dt):
    """ Returns the week of the month for the specified date.
    """

    first_day = dt.replace(day=1)

    dom = dt.day
    adjusted_dom = dom + first_day.weekday()

    return int(ceil(adjusted_dom/7.0))

################################################################################
def messageReceived(destination,message,headers):
    global es
    starttime = time.time()
    logger.info("==> "*10)
    logger.info("Message Received %s" % destination)
    logger.info(headers)

    try: 
        logger.info('waiting 5 sec before doing the request to be sure date are correctly inserted by biac_import_kizeo.py')
        time.sleep(5)

        logger.info('message: '+str(message))

        obj    = json.loads(message)

        start  = datetime.fromtimestamp(obj['start_ts'])
        end    = datetime.fromtimestamp(obj['end_ts'])

        logger.info('start  : '+str(start))
        logger.info('end    : '+str(end))
        
        df     = etp.genericIntervalSearch(es,"biac_kizeo",query="*",start=start,end=end)

        logger.info('kizeo records retrieved: '+str(len(df)))

        df2 = df[['@timestamp','check_conform', 'lot', 'kpi', 'technic', 'percentage_conform',
            'check_no_conform', 'check_number', 'contract', 'screen_name']].copy()

        df2['week_of_year'] = df2['@timestamp'].apply(week_of_year)
        df2['datetime']     = pd.to_datetime(df2['@timestamp'], unit='ms')

        dt_last_update = max(df2['datetime'])
        interval       = define_interval(dt_last_update)

        df3 = df2[(df2['week_of_year']>=interval['week_start']) & (df2['week_of_year']<=interval['week_end'])]

        df_grouped = df3.groupby(['lot', 'kpi', 'contract', 'screen_name']) \
           .agg({'check_conform':'sum', 'check_no_conform':'sum', 'check_number':'sum'}).reset_index()

        df_grouped['percentage_conform'] = round(100*(df_grouped['check_conform'] / df_grouped['check_number']), 2)
        df_grouped['percentage_conform'] = df_grouped['percentage_conform'].apply(lambda x: ('%f' % x).rstrip('0').rstrip('.'))

        df_grouped['percentage_no_conform'] = round(100*(df_grouped['check_no_conform'] / df_grouped['check_number']), 2)
        df_grouped['percentage_no_conform'] = df_grouped['percentage_no_conform'].apply(lambda x: ('%f' % x).rstrip('0').rstrip('.'))

        df_grouped['last_update'] = dt_last_update.timestamp()*1000
        df_grouped['computation_period_fr'] = 'S' + str(interval['week_start']) + ' - S' + str(interval['week_end'])
        df_grouped['computation_period']    = 'W' + str(interval['week_start']) + ' - W' + str(interval['week_end'])

        df_grouped['_index'] = 'biac_month_kizeo'
        df_grouped['_id']    = df_grouped.apply(lambda row: row['lot'] + '_' + str(row['kpi']) + '_' + \
                                                    row['contract'].lower() + '_' + row['screen_name'].lower() + '_' + \
                                                    str(int(row['last_update'])), axis=1)

        df_grouped['_id']    = df_grouped['_id'].str.replace(' ', '_')
        df_grouped['_id']    = df_grouped['_id'].str.replace('/', '_')

        pte.pandas_to_elastic(es, df_grouped)

    except Exception as e:
        endtime = time.time()
        logger.error(e)
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
            variables={"platform":"_/_".join(platform.uname()),"icon":"list-alt"}
            
            conn.send_life_sign(variables=variables)
        except Exception as e:
            logger.error("Unable to send life sign.")
            logger.error(e)
    #app.run(threaded=True,host= '0.0.0.0')
