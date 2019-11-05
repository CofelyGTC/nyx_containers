"""
BIAC COMPUTE ALARMS LUTOSA
====================================


Sends:
-------------------------------------

Listens to:
-------------------------------------

* /queue/COMPUTE_ALARMS_LUTOSA

Collections:
-------------------------------------



VERSION HISTORY
-------------------------------------

* 31 Oct 2019 0.0.1 **VME** Creation
* 03 Nov 2019 0.0.2 **VME** Adding text in alarm
"""

import sys
import traceback

import json
import time
import uuid
import json
import pytz
import base64
import tzlocal 
import urllib3
import datetime
import platform
import threading
import os,logging
import numpy as np
import collections
import pandas as pd
import datetime as dt
import dateutil.parser
from dateutil import tz
from io import StringIO
from functools import wraps
from datetime import datetime
from datetime import timezone
from itertools import groupby
from datetime import timedelta
from tzlocal import get_localzone
from elastic_helper import es_helper 
from amqstompclient import amqstompclient
from logging.handlers import TimedRotatingFileHandler
from logstash_async.handler import AsynchronousLogstashHandler
from elasticsearch import Elasticsearch as ES, RequestsHttpConnection as RC

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


VERSION="0.0.2"
MODULE="GTC_COMPUTE_ALARMS_LUTOSA"
QUEUE=["COMPUTE_ALARMS_LUTOSA"]

ALARM_LIST = [
    'Urg_Alm',
    'Mot_Alm',
    'Mot_PAl',
    'Det_Inc_Cog_Alm',
    'Det_Gaz_Cog_Alm',
    'Det_Inc_N0_Alm',
    'Det_Inc_N1_Alm',
    'Det_Gaz_Dael_Alm',
    'AnalyBiog_H2S_NH_Av_Cogen',
    'MqEau_Alm',
    'AnalyBiog_H2S_NTH_Av_Cogen',
    'Analy_Av_Thiopaq_Def',
    'Analy_Ap_Thiopaq_Def',
    'Analy_Av_Cogen_Def',
    'AH_HTL_TT13',
    'AH_HTL_TT04',
    'AH_LTL_TT01',
    'AB_EGL_DP01',
    'AH_HuilMot',
    'Tpo_Long_GN_Moteur'
]

NAME_TO_TEXT = {
    'Urg_Alm': 'Alarme arrêt d urgence',
    'Mot_Alm': 'Diane: Moteur en alarme',
    'Mot_PAl': 'Diane: Moteur en préalarme',
    'Det_Inc_Cog_Alm': 'Détection incendie caisson de la cogen',
    'Det_Gaz_Cog_Alm': 'Détection gaz caisson de la cogen',
    'Det_Inc_N0_Alm': 'Détection incendie niveau 0',
    'Det_Inc_N1_Alm': 'Détection incendie niveau +1',
    'Det_Gaz_Dael_Alm': 'Détection gaz Daelemans RDC & +1',
    'AnalyBiog_H2S_NH_Av_Cogen': 'Préalarme analyseur de biogaz: Niveau H2S haut avant cogen',
    'MqEau_Alm': 'Alarme maque eau circuit HT',
    'AnalyBiog_H2S_NTH_Av_Cogen': 'Alarme analyseur de biogaz: Niveau H2S très haut avant cogen',
    'Analy_Av_Thiopaq_Def': 'Défaut analyseur Biogaz avant Thiopaq',
    'Analy_Ap_Thiopaq_Def': 'Défaut analyseur Biogaz après Thiopaq',
    'Analy_Av_Cogen_Def': 'Défaut analyseur Biogaz avant Cogen',
    'AH_HTL_TT13': 'Arrêt moteur sur HTL-TT13: Haute température entrée eau échangeur gaz / eau',
    'AH_HTL_TT04': 'Arrêt moteur sur HTL-TT04: Haute température départ eau collecteur',
    'AH_LTL_TT01': 'Arrêt moteur sur LTL-TT01: Haute température entrée eau de refroidissement',
    'AB_EGL_DP01': 'Arrêt moteur sur EGL-DP01: Bas débit eau sortie échangeur gaz / eau',
    'AH_HuilMot': 'Arrêt moteur sur haute température d huile moteur',
    'Tpo_Long_GN_Moteur': 'Arrêt moteur sur temps de fonctionnement trop long 100% GN ou Biogaz trop faible (Reset nécessaire)'
 }

ALARM_LIST = ['LUTOSA_'+_ for _ in ALARM_LIST]
QUERY = 'name:'+' OR name:'.join(ALARM_LIST)

containertimezone=pytz.timezone(get_localzone().zone)

START = containertimezone.localize(datetime(2019, 10, 24))


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

class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, dt.datetime):
            return o.isoformat()
            
        elif isinstance(o, dt.time):
            return o.isoformat()

        return json.JSONEncoder.default(self, o)

################################################################################
def compute_alarms(alarm_name, df):
    alarm_array = []

    intervals=[ list(group) for key, group in groupby(df['value'].values.tolist())]
    index = 0

    for i in intervals:
        #logger.info('index: '+str(index))

        if index == 0:
            if df.iloc[index]['value'] == 1:
                try:
                    #logger.info('try get cache')
                    cache_alarm = es.get(index='gtc_alarm_cache', doc_type='doc',id=alarm_name)
                    alarm={}
                    alarm['start'] = datetime.fromisoformat(cache_alarm['_source']['start'])
                    alarm['end']   = df.iloc[index+len(i)-1]['datetime'].to_pydatetime() + timedelta(minutes=1) #+1 minute
                    alarm['name']  = alarm_name

                    if len(df) == (index+len(i)):
                        es.index(index='gtc_alarm_cache', doc_type='doc', id=alarm_name, body=alarm)
                        alarm['unfinished'] = 1
                        #logger.info('last alarm unfinished -> cached')
                    else:
                        try:
                            #logger.info('try delete cache')
                            es.delete(index='gtc_alarm_cache', doc_type='doc', id=alarm_name)
                        except:
                            logger.info('failed')
                            pass

                    alarm_array.append(alarm)
                except:
                    logger.info('failed')
                    pass

        else:
            if df.iloc[index]['value'] == 1:
                #logger.info('  --> '+str(df.iloc[index]['datetime'])+' --> duration: '+str(len(i)))

                alarm={}
                alarm['start'] = df.iloc[index]['datetime'].to_pydatetime()
                alarm['end']   = df.iloc[index+len(i)-1]['datetime'].to_pydatetime() + timedelta(minutes=1)
                alarm['name']  = alarm_name

                if len(df) == (index+len(i)):
                    alarm['unfinished'] = 1
                    es.index(index='gtc_alarm_cache', doc_type='doc', id=alarm_name,  body=alarm)
                    #logger.info('last alarm unfinished -> cached')

                alarm_array.append(alarm)

        index+=len(i)


    bulk_body=''
    for i in alarm_array:
        action = {}

        i['duration_minutes'] = int((i['end']-i['start']).total_seconds()/60)
        i['site'] = 'LUTOSA'
        i['site_name'] = i['name']
        i['name'] = i['name'].replace('LUTOSA_', '')
        
        
        try:
            i['text'] = NAME_TO_TEXT[i['name']]
        except:
            logger.warning(str(i['name'])+ ' not in NAME_TO_TEXT dict')


        action["index"] = {
            "_index": 'gtc_alarm', 
            "_type": "doc", 
            "_id": i['name'] + '_' +str(int(i['start'].timestamp())*1000)
        }

        bulk_body += json.dumps(action) + "\r\n"
        bulk_body += json.dumps(i, cls=DateTimeEncoder) + "\r\n"
        
    return bulk_body

def proceed_computation(start):
    logger.info('**********************************************************')
    logger.info('PROCEED COMPUTATION: '+str(start))
    bulkbody = ''
    flag = True
    end = start + timedelta(hours=2)
    
    while flag:
        if end > datetime.now(containertimezone):
            flag=False


        #bulkbody = ''    

        df_alarms=es_helper.elastic_to_dataframe(es, 
                                             index='opt_sites_data*', 
                                             query=QUERY, 
                                             start=start, 
                                             end=end)

        if len(df_alarms) == 0:
            logger.info('no alarms on this period -> add 2 hours to end')
            end = end + timedelta(hours=2)
        else:
            logger.info('-'+str(start))
            logger.info('-'+str(end))
            logger.info('-------')
            df_alarms['datetime'] = pd.to_datetime(df_alarms['@timestamp'], unit="ms", utc=True) \
                                                                        .dt.tz_convert('Europe/Paris')

            if len(df_alarms) == 0:
                logger.info('NO DATA')                                            

            df_alarms = df_alarms.sort_values('@timestamp')

            df = df_alarms[['@timestamp', 'datetime', 'client', 'name', 'value']]

            for alarm in df['name'].unique():
                #logger.info(alarm)
                df_filtered = df[df['name']==alarm]

                if len(df_filtered) > 0:
                    bulkbody += compute_alarms(alarm, df_filtered)



            if start == max(df['datetime']).to_pydatetime():
                logger.info('big hole')

                obj = {
                    'start': str(start),
                    'end': str(end),
                }

                conn.send_message('/topic/ALARMS_MISSING_DATA', json.dumps(obj))

                end   = end + timedelta(hours=2)
            else:
                start = max(df['datetime']).to_pydatetime()
                end   = start + timedelta(hours=2)
                

    if bulkbody != '':         
        bulkres=es.bulk(bulkbody)
        
        if(not(bulkres["errors"])):
            logger.info("BULK done without errors.")
        else:
            for item in bulkres["items"]:
                if "error" in item["index"]:
                    logger.info(item["index"]["error"])
    
    
    return start




################################################################################
def messageReceived(destination,message,headers):
    global es
    logger.info("==> "*10)
    logger.info("Message Received %s" % destination)
    logger.info(message)
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

    SECONDSBETWEENCHECKS=120
    es.indices.delete(index='gtc_alarm_cache', ignore=[400, 404]) 

    nextload=datetime.now()

    while True:
        time.sleep(5)
        try:            
            variables={"platform":"_/_".join(platform.uname()),"icon":"list-alt"}
            
            conn.send_life_sign(variables=variables)

            if (datetime.now() > nextload):
                try:
                    nextload=datetime.now()+timedelta(seconds=SECONDSBETWEENCHECKS)
                    SECONDSBETWEENCHECKS = 60
                    START = proceed_computation(START)
                    
                except Exception as e2:
                    logger.error("Unable to load kizeo.")
                    logger.error(e2,exc_info=True)
            
        except Exception as e:
            logger.error("Unable to send life sign.")
            logger.error(e)
