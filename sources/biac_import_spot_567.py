"""
BIAC IMPORT SPOT 567
====================================

Listens to:
-------------------------------------

* /queue/SPOT567_IMPORT

Collections:
-------------------------------------

* **biac_spot5** (Raw Data)
* **biac_spot5_monthly** (Computed Data)
* **biac_spot6** (Raw Data)
* **biac_spot6_monthly** (Computed Data)
* **biac_spot7** (Raw Data)
* **biac_spot7_monthly** (Computed Data)

VERSION HISTORY
===============

* 23 Jul 2019 0.0.4 **VME** Code commented
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
from pytz import timezone
from functools import wraps

import datetime as dt
from datetime import date
from datetime import datetime
from datetime import timedelta

from lib import pandastoelastic as pte
from lib import elastictopandas as etp
from amqstompclient import amqstompclient
from logging.handlers import TimedRotatingFileHandler
from logstash_async.handler import AsynchronousLogstashHandler
from elasticsearch import Elasticsearch as ES, RequestsHttpConnection as RC



MODULE  = "BIAC_SPOT567_IMPORTER"
VERSION = "0.0.4"
QUEUE   = ["SPOT567_IMPORT"]

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

def isNan(val):
    return val != val

def get_month_day_range(date):
    date=date.replace(hour = 0)
    date=date.replace(minute = 0)
    date=date.replace(second = 0)
    date=date.replace(microsecond = 0)
    first_day = date.replace(day = 1)
    last_day = date.replace(day = calendar.monthrange(date.year, date.month)[1])
    last_day=last_day+timedelta(1)
    last_day = last_day - timedelta(seconds=1)
    
    local_timezone = tzlocal.get_localzone()
    first_day=first_day.astimezone(local_timezone)
    last_day=last_day.astimezone(local_timezone)
    
    return first_day, last_day
        
def compute_kpi(df_reduced, lot, kpi_number, complaint_field='Totaal'):
    logger.info('kpi'+str(kpi_number)+' -> '+str(len(df_reduced)))

    
    _index='biac_spot_lot'+str(lot)
    ret_bulk = ''

    complaint_field = str(kpi_number)+ ' ' +complaint_field
    
    for index, row in df_reduced.iterrows():

        action = {}
        action["index"] = {"_index": _index, "_type": "doc", "_id": str(int(row['datetime_ctrl'].timestamp()))+'_kpi'+str(kpi_number)}
        
        obj = {
            '@timestamp': row['datetime_ctrl'],
            'kpi': kpi_number,
            'lot': lot
        }

        if (lot == 7) and (not isNan(row['Aantal controles'])) \
                      and (not isNan(row['Aantal niet conform'])) \
                      and (not isNan(row['Aantal conform'])):
            obj['check']= int(row['Aantal controles'])
            obj['conform']= int(row['Aantal conform'])
            obj['not_conform']= int(row['Aantal niet conform'])
            obj['percentage'] = round(100*(obj['conform'] / obj['check']), 2)

        elif (kpi_number == 7 or kpi_number == 11) and (not isNan(row[str(kpi_number)+' Totaal'])) \
                                                 and (not isNan(row[str(kpi_number)+' Niet juist'])) \
                                                 and (not isNan(row[str(kpi_number)+' Juist'])):
            obj['check']= int(row[str(kpi_number)+' Totaal'])
            obj['conform']= int(row[str(kpi_number)+' Juist'])
            obj['not_conform']= int(row[str(kpi_number)+' Niet juist'])
            obj['percentage'] = round(100*(obj['conform'] / obj['check']), 2)
        
        elif complaint_field in row and not isNan(row[complaint_field]):
            obj['not_conform']= int(row[complaint_field])

        if not isNan(row['Lokalisatie']):
            obj['location'] = row['Lokalisatie']
        
        if not isNan(row['Terminal']):
            obj['terminal'] = row['Terminal']

        if not isNan(row['Controle door']):
            obj['madeBy'] = row['Controle door']

        if not isNan(row['Uitleg van de uitgevoerde controle']):
            obj['comment'] = row['Uitleg van de uitgevoerde controle']
        
        if not isNan(row['Record number']):
            obj['record_number'] = row['Record number']

        ret_bulk += json.dumps(action, cls=DateTimeEncoder) + "\r\n"
        ret_bulk += json.dumps(obj, cls=DateTimeEncoder) + "\r\n"


    return ret_bulk

def compute_lot6(df):
    df['datetime_ctrl'] = df.apply(lambda row: localtz.localize(datetime.combine(row['Datum Controle'].date(), row['Uur'])), axis=1) 

    bulkbody=''

    lot = 6
        
    for i in df['KPI'].unique():
        df_reduced = df[df['KPI']==i]
        
        if i.startswith('4'):
            bulkbody+=compute_kpi(df_reduced, lot, 4)
        elif i.startswith('5'):
            bulkbody+=compute_kpi(df_reduced, lot, 5)
        elif i.startswith('7'):
            bulkbody+=compute_kpi(df_reduced, lot, 7)
            
            obj_kpi_7 = {
                'dt_min': min(df_reduced['datetime_ctrl']),
                'dt_max': max(df_reduced['datetime_ctrl']),
            }
            
            logger.info(obj_kpi_7)
        elif i.startswith('8'):
            bulkbody+=compute_kpi(df_reduced, lot, 8)
        elif i.startswith('9'):
            bulkbody+=compute_kpi(df_reduced, lot, 9)

    bulkres = es.bulk(bulkbody,request_timeout=30)
    logger.info("BULK DONE")


    if(not(bulkres["errors"])):     
        logger.info("BULK done without errors.")   
    else:
        for item in bulkres["items"]:
            if "error" in item["index"]:
                logger.info(item)
                
    handle_kpi_monthly(obj_kpi_7, lot=lot, kpi=7)

def compute_lot7(df):
    df['datetime_ctrl'] = df.apply(lambda row: localtz.localize(datetime.combine(row['Datum Controle'].date(), row['Uur'])), axis=1) 

    bulkbody=''

    lot = 7
        
    for i in df['KPI'].unique():
        df_reduced = df[df['KPI']==i]
        
        if i.startswith('3'):
            bulkbody+=compute_kpi(df_reduced, lot, 3)
        elif i.startswith('5'):
            bulkbody+=compute_kpi(df_reduced, lot, 5)
            obj_kpi_5 = {
                'dt_min': min(df_reduced['datetime_ctrl']),
                'dt_max': max(df_reduced['datetime_ctrl']),
            }
            
            logger.info(obj_kpi_5)
        elif i.startswith('6'):
            bulkbody+=compute_kpi(df_reduced, lot, 6)
            
    bulkres = es.bulk(bulkbody,request_timeout=30)
    logger.info("BULK DONE")


    if(not(bulkres["errors"])):     
        logger.info("BULK done without errors.")   
    else:
        for item in bulkres["items"]:
            if "error" in item["index"]:
                logger.info(item)
                
    handle_kpi_monthly(obj_kpi_5, lot=lot, kpi=5)


def compute_lot5(df):
    logger.info('compute lot 5 -> '+str(len(df)))
    df['datetime_ctrl'] = df.apply(lambda row: localtz.localize(datetime.combine(row['Datum Controle'].date(), row['Uur'])), axis=1) 

    bulkbody=''

    lot = 5
        
    for i in df['KPI'].unique():
        df_reduced = df[df['KPI']==i]
        
        if i.startswith('11'):
            bulkbody+=compute_kpi(df_reduced, lot, 11)
            obj_kpi_11 = {
                'dt_min': min(df_reduced['datetime_ctrl']),
                'dt_max': max(df_reduced['datetime_ctrl']),
            }

            logger.info(obj_kpi_11)
        elif i.startswith('13'):
            bulkbody+=compute_kpi(df_reduced, lot, 13, 'Fouten')
        elif i.startswith('16'):
            bulkbody+=compute_kpi(df_reduced, lot, 16, 'Verkeerd')
        elif i.startswith('17'):
            bulkbody+=compute_kpi(df_reduced, lot, 17, 'Afwijkingen')

    bulkres = es.bulk(bulkbody,request_timeout=30)
    logger.info("BULK DONE")


    if(not(bulkres["errors"])):     
        logger.info("BULK done without errors.")   
    else:
        for item in bulkres["items"]:
            if "error" in item["index"]:
                logger.info(item)
                
    handle_kpi_monthly(obj_kpi_11, lot=lot, kpi=11)


def create_default_record(kpi, lot):
    obj={
            'type': 'monthly',
            'kpi': kpi,
            'lot': lot,
            '@timestamp': localtz.localize(datetime.now()),
            'last_update': localtz.localize(datetime(2019, 1, 1)),
            'check': 0,
            'conform': 0,
            'not_conform': 0,
            'percentage': 100,
        }

    logger.info('create default record - lot :'+str(lot)+' - kpi : '+str(kpi))


    es.index(index='biac_spot_lot'+str(lot)+'_monthly', doc_type='doc', id=str(int(obj['@timestamp'].timestamp()))+'_kpi'+str(kpi), body=obj)



def handle_kpi_monthly(dt_obj, lot, kpi):
    time.sleep(3)
    dt_start=get_month_day_range(dt_obj['dt_min'])[0]
    dt_end=get_month_day_range(dt_obj['dt_max'])[1]

    res = etp.genericIntervalSearch(es,index='biac_spot_lot'+str(lot),query="kpi:"+str(kpi),start=dt_start,end=dt_end, datecolumns=['@timestamp'])

    if len(res) == 0:
        create_default_record(kpi, lot)
    else:
        res['month']=res['@timestamp'].dt.strftime('%Y-%m')

        df_agg=res[['month', 'check', 'conform', 'not_conform']].groupby('month').agg('sum')

        df_agg  


        for index, row in df_agg.iterrows():
            dt = localtz.localize(datetime.strptime(index, '%Y-%m'))
            logger.info(dt)

            obj={
                'last_update': dt,
                '@timestamp': localtz.localize(datetime.now()),
                'kpi': kpi,
                'lot': lot,
                'type': 'monthly',
            }

            for i in ['check', 'conform', 'not_conform']:
                obj[i] = int(row[i])

            obj['percentage'] = round(100*(obj['conform'] / obj['check']), 2) 
            logger.info(str(obj))

            es.index(index='biac_spot_lot'+str(lot)+'_monthly', doc_type='doc', id=str(int(dt.timestamp()))+'_kpi'+str(kpi), body=obj)


#################################################
def loadKizeo():
    try:
        starttime = time.time()
        logger.info(">>> LOADING LOT6")
        logger.info("==================")
        url_kizeo = 'https://www.kizeoforms.com/rest/v3'
        
        kizeo_user=os.environ["KIZEO_USER"]
        kizeo_password=os.environ["KIZEO_PASSWORD"]
        kizeo_company=os.environ["KIZEO_COMPANY"]

        create_default_record(kpi=11, lot=5)
        create_default_record(kpi=7, lot=6)
        create_default_record(kpi=5, lot=7)

        payload = {
            "user": kizeo_user,
            "password": kizeo_password,
            "company": kizeo_company
            }
        #logger.info(payload)

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
            logger.info(i['name'])
            kpi_list=['Spotcheck ~ Lot 7 Screening']
            kpi_list=['Spotcheck ~ Lot 5  Baggage handling', 'Spotcheck ~ Lot 6 Boarding Bridges', 'Spotcheck ~ Lot 7 Screening']
            
            if i['name'] in kpi_list:
                logger.info("=== SPOTCHECK FOUND")
                form_id = i['id']
                start=(datetime.now()+timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")
                logger.info("Start %s" %(start))            
                end=(datetime.now()-timedelta(days=60)).strftime("%Y-%m-%d %H:%M:%S")

                end = datetime(2019, 1, 1)
                logger.info("End %s" %(end))
                post={"onlyFinished":False,"startDateTime":start,"endDateTime":end,"filters":[]}
                
                #https://www.kizeoforms.com/rest/v3//forms/411820/data/exports_info
                r = requests.post(url_kizeo + '/forms/' + form_id + '/data/exports_info?Authorization='+token,post)

                if r.status_code != 200:
                    logger.error('something went wrong...')
                    logger.error(r.status_code, r.reason)

                logger.info(r.json())

                ids=r.json()['data']["dataIds"]
                
                logger.info(ids)
                payload={
                "data_ids": ids
                }
                posturl=("%s/forms/%s/data/multiple/excel_custom" %(url_kizeo,form_id))
                headers = {'Content-type': 'application/json','Authorization':token}
                
                r=requests.post(posturl,data=json.dumps(payload),headers=headers)
                
                if r.status_code != 200:
                    logger.error('something went wrong...')
                    logger.error(r.status_code, r.reason)

                #logger.info(r.content)
                
                logger.info("Handling Form. Content Size:"+str(len(r.content)))
                if len(r.content) >0:
                    
                    file = open("./tmp/excel.xlsx", "wb")
                    file.write(r.content)
                    file.close()
                    
                    df = pd.read_excel("./tmp/excel.xlsx")
                    
                    
                    
                    if i['name'] == 'Spotcheck ~ Lot 5  Baggage handling':
                        compute_lot5(df)
                    if i['name'] == 'Spotcheck ~ Lot 6 Boarding Bridges':
                        compute_lot6(df)
                    if i['name'] == 'Spotcheck ~ Lot 7 Screening':
                        compute_lot7(df)


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
