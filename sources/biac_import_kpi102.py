"""
BIAC KPI 102
====================================

Sends:
-------------------------------------

* /topic/BIAC_KPI102_IMPORTED

Listens to:
-------------------------------------

* /queue/KPI102_IMPORT

Collections:
-------------------------------------

* **biac_kpi102** (Raw Data)
* **biac_kib_kpi102** (Heat map and horizontal bar stats)
* **biac_month_kpi102** (Computed Data)

VERSION HISTORY
===============

* 29 May 2019 0.0.3 **AMA** Heat map added
* 04 Jun 2019 0.0.4 **AMA** Synchronized with VME
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
from datetime import datetime
from datetime import timedelta

from lib import pandastoelastic as pte
from lib import elastictopandas as etp
from amqstompclient import amqstompclient
from logging.handlers import TimedRotatingFileHandler
from logstash_async.handler import AsynchronousLogstashHandler
from elasticsearch import Elasticsearch as ES, RequestsHttpConnection as RC



MODULE  = "BIAC_KPI102_IMPORTER"
VERSION = "0.0.4"
QUEUE   = ["KPI102_IMPORT"]

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

def computeStats102():
    """
    Compute KPI 102 stats. It first deletes the collection biac_kib_kpi102 and then recreates it.
    """

    try:
        start,end=get_month_day_range(datetime.now())

        logger.info(">>> Compute Stats 102")
        es_index="biac_kib_kpi102"

        es.indices.delete(index=es_index, ignore=[400, 404]) 
        
        query={
            "size":1000,
            "query": {
                "bool": {
                "must": [
                    {
                    "match_all": {}
                    },
                    {
                    "range": {
                        "@timestamp": {
                        "gte": start.timestamp()*1000,
                        "lte": end.timestamp()*1000,
                        "format": "epoch_millis"
                        }
                    }
                    }
                ]
                }
            }
        }

        rondes=['1','2']
        rondestats={"1":{"done":0},"2":{"done":0}}    

        res=es.search(body=query,index="biac_kpi102*")
        logger.info("========+>"*10)

        dones=[0 for i in range(0,16)]

        if "hits" in res and "hits" in res["hits"]:
            for rec in res["hits"]["hits"]:
                if "ronde_number" in rec["_source"]:
                    print(rec["_source"]["ronde_number"])
                    dones[int(rec["_source"]["ronde_number"])-1]=1

        bulkbody=[]

        for i in range(0,16):
            action = {}
            action["index"] = {"_index": es_index,"_type": "doc"}
            bulkbody.append(json.dumps(action))  
            obj={"@timestamp":datetime.now().date().isoformat(),"done":dones[i],"ronde":i+1,"rec_type":"heatmap"}
            bulkbody.append(json.dumps(obj))  

        bulkbody
                        
        res=es.bulk("\r\n".join(bulkbody))
    except:
        logger.error("Unable to compute stats.",exc_info=True)

#######################################################################################
# get_month_day_range
#######################################################################################
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

#################################################
def loadKPI102():
    try:
        starttime = time.time()
        logger.info(">>> LOADING KPI102")
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
            if re.findall("LOT 2 - Maandelijkse ronde N° [0-9]*$", i['name'].strip()):
                logger.info('MATCH')
                logger.info(i['name'])
                
                form_id = i['id']
                start=(datetime.now()+timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")
                logger.info("Start %s" %(start))            
                end=(datetime.now()-timedelta(days=60)).strftime("%Y-%m-%d %H:%M:%S")
                logger.info("End %s" %(end))
                post={"onlyFinished":False,"startDateTime":start,"endDateTime":end,"filters":[]}

                r = requests.post(url_kizeo + '/forms/' + form_id + '/data/exports_info?Authorization='+token,post)

                if r.status_code != 200:
                    logger.error('something went wrong...')
                    logger.error(r.status_code, r.reason)
                    
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

                logger.info("Handling Form. Content Size:"+str(len(r.content)))
                if len(r.content) >0:
                    file = open("./tmp/"+i['name']+".xlsx", "wb")
                    file.write(r.content)
                    file.close()
                
                    df = pd.read_excel("./tmp/"+i['name']+".xlsx")
                    
                    logger.info(df)
                    df_all=df_all.append(df)

        if len(df_all) > 0:
            df_all['ronde_number'] = df_all['Ronde'].apply(lambda x: x.lower().replace('lot 2 - maandelijkse ronde n° ',''))
            print(df_all.columns)
            if len(df_all.columns)==4:
                df_all.columns=['ronde', '_timestamp', 'record_number', 'ronde_number']
            else:
                df_all.columns=['ronde', '_timestamp','answer_date','record_number', 'ronde_number']
            df_all

            df_all['_id'] = df_all['_timestamp'].astype(str) +'_'+ df_all['ronde_number']
            df_all['_index'] = 'biac_kpi102'


            pte.pandas_to_elastic(es, df_all)

            obj={
                'start': min(df_all['_timestamp']),
                'end': max(df_all['_timestamp']),
            }

            conn.send_message('/topic/BIAC_KPI102_IMPORTED', str(obj))

            time.sleep(3)
            compute_kpi102_monthly(obj['start'], obj['end'])

            endtime = time.time()
            log_message("Import of KPI102 from Kizeo finished. Duration: %d Records: %d." % (endtime-starttime, df_all.shape[0]))       


    


    except Exception as e:
        endtime = time.time()
        exc_type, exc_value, exc_traceback = sys.exc_info()
        traceback.print_tb(exc_traceback, limit=1, file=sys.stdout)
        # exc_type below is ignored on 3.5 and later
        traceback.print_exception(exc_type, exc_value, exc_traceback,
                              limit=2, file=sys.stdout)



def compute_kpi102_monthly(start, end):
    starttime = time.time()
    
    logger.info(start)
    logger.info(end)


    start = mkFirstOfMonth(start)
    end = mkLastOfMonth(end)

    logger.info(start)
    logger.info(end)

    df_kpi102 = etp.genericIntervalSearch(es, 'biac_kpi102', query='*', start=start, end=end)
    
    df_kpi102['month'] = pd.to_datetime(df_kpi102['@timestamp'], unit='ms').dt.strftime('%Y-%m')
    df_kpi102=df_kpi102.groupby(['month', 'ronde_number']).count()[['@timestamp']].rename(columns={'@timestamp': 'count'}).reset_index()
    df_kpi102=df_kpi102.groupby('month').count().reset_index()[['month', 'count']]
    df_kpi102['percent'] = round((df_kpi102['count']/16)*100, 2)
    df_kpi102['percent'] = df_kpi102['percent'].apply(lambda x: '{0:g}'.format(float(x)))
    df_kpi102['_timestamp'] = pd.to_datetime(df_kpi102['month'], format='%Y-%m')
    df_kpi102['_index'] = 'biac_month_kpi102'
    df_kpi102['_id'] = df_kpi102['month']
    
    pte.pandas_to_elastic(es, df_kpi102)


    endtime = time.time()
    log_message("Compute monthly KPI102 (process biac_import_kpi102.py) finished. Duration: %d Records: %d." % (endtime-starttime, df_kpi102.shape[0]))   

    

def mkDateTime(dateString,strFormat="%Y-%m-%d"):
    # Expects "YYYY-MM-DD" string
    # returns a datetime object
    eSeconds = time.mktime(time.strptime(dateString,strFormat))
    return datetime.fromtimestamp(eSeconds)

def formatDate(dtDateTime,strFormat="%Y-%m-%d"):
    # format a datetime object as YYYY-MM-DD string and return
    return dtDateTime.strftime(strFormat)

def mkFirstOfMonth2(dtDateTime):
    #what is the first day of the current month
    ddays = int(dtDateTime.strftime("%d"))-1 #days to subtract to get to the 1st
    delta = timedelta(days= ddays)  #create a delta datetime object
    return dtDateTime - delta                #Subtract delta and return

def mkFirstOfMonth(dtDateTime):
    #what is the first day of the current month
    #format the year and month + 01 for the current datetime, then form it back
    #into a datetime object
    return mkDateTime(formatDate(dtDateTime,"%Y-%m-01"))

def mkLastOfMonth(dtDateTime):
    dYear = dtDateTime.strftime("%Y")        #get the year
    dMonth = str(int(dtDateTime.strftime("%m"))%12+1)#get next month, watch rollover
    dDay = "1"                               #first day of next month
    nextMonth = mkDateTime("%s-%s-%s"%(dYear,dMonth,dDay))#make a datetime obj for 1st of next month
    delta = timedelta(seconds=1)    #create a delta of 1 second
    return nextMonth - delta                 #subtract from nextMonth and return

def add_months(sourcedate, months):
    month = sourcedate.month - 1 + months
    year = sourcedate.year + month // 12
    month = month % 12 + 1
    day = min(sourcedate.day, calendar.monthrange(year,month)[1])
    return date(year, month, day)


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
                    time.sleep(1)
                    computeStats102()
                except Exception as e2:
                    logger.error("Unable to load kizeo.")
                    logger.error(e2,exc_info=True)
            
        except Exception as e:
            logger.error("Unable to send life sign.")
            logger.error(e)
    #app.run(threaded=True,host= '0.0.0.0')
