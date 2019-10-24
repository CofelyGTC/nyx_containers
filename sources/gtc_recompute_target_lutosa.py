"""
BIAC RECOMPUTE TARGETS
====================================


Sends:
-------------------------------------

Listens to:
-------------------------------------

* /queue/RECOMPUTE_TARGET_LUTOSA

Collections:
-------------------------------------



VERSION HISTORY
-------------------------------------

* 09 Oct 2019 0.0.1 **VME** Creation
* 16 Oct 2019 0.0.2 **VME** Bug fixing on targets when day off
* 23 Oct 2019 0.0.3 **VME** Get targets from cogen_parameters
"""

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
from elastic_helper import es_helper 
from functools import wraps
from elasticsearch import Elasticsearch as ES, RequestsHttpConnection as RC
from logstash_async.handler import AsynchronousLogstashHandler
import numpy as np
import collections
from datetime import timezone
from io import StringIO
from dateutil import tz
import dateutil.parser

from lib import cogenhelper as ch


import tzlocal # $ pip install tzlocal

VERSION="0.0.3"
MODULE="GTC_RECOMPUTE_TARGET_LUTOSA"
QUEUE=["RECOMPUTE_TARGET_LUTOSA"]

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
def compute_targets(year):
    global es
    target_prod_biogaz   = ch.get_targets(es)['biogas']
    target_prod_elec     = ch.get_targets(es)['elec']
    target_prod_heat     = ch.get_targets(es)['heat']
    target_runtime_cogen = ch.get_targets(es)['runtime']
    
    start, end = datetime(year, 1, 1), datetime(year, 12, 31, 23, 59, 59)

    logger.info(start)
    logger.info(end)
    
    df_calendar = es_helper.elastic_to_dataframe(es, 
                                                 start=start, 
                                                 end=end, 
                                                 index='nyx_calendar', 
                                                 query='type:LUTOSA',
                                                 datecolumns=['date'],
                                                 timestampfield='date')
    
    open_days = 365
    
    logger.info('size df_calendar: '+str(len(df_calendar)))
    if len(df_calendar) > 0:
        open_days = df_calendar.loc[df_calendar['on'], 'on'].count()
        # df_calendar['date']=pd.to_datetime(df_calendar['date'], utc=True).dt.tz_convert('Europe/Paris').dt.date
        df_calendar['date']=pd.to_datetime(df_calendar['date']).dt.date
        del df_calendar['_id']
        del df_calendar['_index']
    
    logger.info('opening days: '+str(open_days))
    
    daily_target_prod_biogaz   = target_prod_biogaz   / open_days
    daily_target_prod_elec     = target_prod_elec     / open_days
    daily_target_prod_heat     = target_prod_heat     / open_days
    daily_target_runtime_cogen = target_runtime_cogen / open_days
    
    df_daily_lutosa = es_helper.elastic_to_dataframe(es, 
                                                     start=start, 
                                                     end=end, 
                                                     index='daily_cogen_lutosa', 
                                                     datecolumns=['@timestamp'],
                                                     query='*')
    
    if len(df_daily_lutosa) == 0:
        return None
    
    if 'on' in df_daily_lutosa.columns:
        del df_daily_lutosa['on']
    
    df_daily_lutosa['date'] = pd.to_datetime(df_daily_lutosa['@timestamp']).dt.date
    
    if len(df_calendar) > 0:
        logger.info('merge calendar')
        df_daily_lutosa=df_daily_lutosa.merge(df_calendar[['date', 'on']], left_on='date', right_on='date', how='left')
    
    df_daily_lutosa['entry_biogas_thiopaq_target_MWh'] = 0
    df_daily_lutosa.loc[df_daily_lutosa['on'], 'entry_biogas_thiopaq_target_MWh'] = daily_target_prod_biogaz
    df_daily_lutosa.loc[df_daily_lutosa['on'], 'entry_biogas_thiopaq_ratio_target'] = round((df_daily_lutosa.loc[df_daily_lutosa['on'], 'entry_biogas_thiopaq_MWh'] / 
                                                               df_daily_lutosa.loc[df_daily_lutosa['on'], 'entry_biogas_thiopaq_target_MWh']), 2)
    
    df_daily_lutosa['daily_avail_motor_target_hour'] = 0
    df_daily_lutosa.loc[df_daily_lutosa['on'], 'daily_avail_motor_target_hour'] = daily_target_runtime_cogen
    df_daily_lutosa.loc[df_daily_lutosa['on'], 'daily_avail_motor_ratio_target'] = round((df_daily_lutosa.loc[df_daily_lutosa['on'], 'daily_avail_motor_hour'] / 
                                                               df_daily_lutosa.loc[df_daily_lutosa['on'], 'daily_avail_motor_target_hour']), 2)
    
    df_daily_lutosa['out_elec_cogen_target_MWh'] = 0
    df_daily_lutosa.loc[df_daily_lutosa['on'], 'out_elec_cogen_target_MWh'] = daily_target_prod_elec
    df_daily_lutosa.loc[df_daily_lutosa['on'], 'out_elec_cogen_ratio_target'] = round((df_daily_lutosa.loc[df_daily_lutosa['on'], 'out_elec_cogen_MWh'] / 
                                                               df_daily_lutosa.loc[df_daily_lutosa['on'], 'out_elec_cogen_target_MWh']), 2)
    
    df_daily_lutosa['out_therm_cogen_target_MWh'] = 0
    df_daily_lutosa.loc[df_daily_lutosa['on'], 'out_therm_cogen_target_MWh'] = daily_target_prod_heat
    df_daily_lutosa.loc[df_daily_lutosa['on'], 'out_therm_cogen_ratio_target'] = round((df_daily_lutosa.loc[df_daily_lutosa['on'], 'out_therm_cogen_MWh'] / 
                                                               df_daily_lutosa.loc[df_daily_lutosa['on'], 'out_therm_cogen_target_MWh']), 2)
    
    if 'on_x' in df_daily_lutosa.columns:
        del df_daily_lutosa['on_x']
    if 'on_y' in df_daily_lutosa.columns:
        del df_daily_lutosa['on_y']
    if 'weekday' in df_daily_lutosa.columns:
        del df_daily_lutosa['weekday']
    if 'date' in df_daily_lutosa.columns:
        del df_daily_lutosa['date']
    if 'type' in df_daily_lutosa.columns:
        del df_daily_lutosa['type']

    df_daily_lutosa.loc[df_daily_lutosa['on']==False, 'out_therm_cogen_ratio_target'] = np.nan
    df_daily_lutosa.loc[df_daily_lutosa['on']==False, 'out_elec_cogen_ratio_target'] = np.nan
    df_daily_lutosa.loc[df_daily_lutosa['on']==False, 'daily_avail_motor_ratio_target'] = np.nan
    df_daily_lutosa.loc[df_daily_lutosa['on']==False, 'entry_biogas_thiopaq_ratio_target'] = np.nan

    es_helper.dataframe_to_elastic(es, df_daily_lutosa)





################################################################################
def messageReceived(destination,message,headers):
    global es
    logger.info("==> "*10)
    logger.info("Message Received %s" % destination)
    logger.info(message)
    logger.info("<== "*10)
    compute_targets(int(message))

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
            conn.send_life_sign()
        except Exception as e:
            logger.error("Unable to send life sign.")
            logger.error(e)