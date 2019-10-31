"""
BIAC KPI 103
====================================
Reads kizeo FROM **LOT 1 - Gebouw 16 - Dagelijkse ronde** (Ronde 1) and **LOT 1 - Gebouw 702 (Ronde 2)- Dagelijkse ronde** via Kizeo REST API and stores it in Elastic Search collections:

Sends:
-------------------------------------

* /topic/BIAC_KPI103_IMPORTED

Listens to:
-------------------------------------

* /queue/KPI103_IMPORT

Collections:
-------------------------------------

* **biac_kpi103** (Raw Data)
* **biac_kib_kpi103** (Heat map and horizontal bar stats)
* **biac_month_kpi103** (Computed Data) NOT IN USE ANYMORE

VERSION HISTORY
-------------------------------------

* 20 May 2019 0.0.2 **AMA** First based on version 0.0.2 from VME KPI102
* 28 May 2019 0.0.5 **AMA** Stats collection added first day skipped
* 05 Jun 2019 0.0.6 **AMA** Fixed a bug that oprevenrts new record to be read
* 01 Aug 2019 0.0.7 **VME** Remove call to compute_kpi103_monthly. New Kizeo forms for weekend.
* 30 Oct 2019 0.0.8 **VME** Buf fixing r.text empty and better error log.
* 30 Oct 2019 1.0.0 **AMA** Use data get rest api exports_info function to get record ids
* 30 Oct 2019 1.0.1 **AMA** Fix a bug that added an additional day during the daylight saving month
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
from calendar import monthrange

from elastic_helper import es_helper 

from amqstompclient import amqstompclient
from dateutil.relativedelta import relativedelta
from logging.handlers import TimedRotatingFileHandler
from logstash_async.handler import AsynchronousLogstashHandler
from elasticsearch import Elasticsearch as ES, RequestsHttpConnection as RC



MODULE  = "BIAC_KPI103_IMPORTER"
VERSION = "1.0.0"
QUEUE   = ["KPI103_IMPORT"]

def get_days_already_passed(str_month):
    """
    Retrieves the day already elapsed in the given month as string.         

    Parameters
    ----------
    str_month
        targetmonth as string
    """
    month = datetime.strptime(str_month, '%Y-%m')
    cur_date = datetime.now()
    
    if cur_date < month:
        return 0
    elif cur_date == month:
        return 0
    else:
        if cur_date.month == month.month and cur_date.year == month.year:
            return cur_date.day
        else:
            return monthrange(month.year, month.month)[1]

###################################################################            
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

################################################################################
def messageReceived(destination,message,headers):
    global es
    logger.info("==> "*10)
    logger.info("Message Received %s" % destination)
    logger.info(headers)
        

#################################################
def loadKPI103():
    """
    Main function that reads the two forms from kizeo every hour.         
    """
    try:
        starttime = time.time()
        logger.info(">>> LOADING KPI103")
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
        #logger.info(form_list)

        df_all=pd.DataFrame()
        for i in form_list:
            if re.findall("LOT 1 - Gebouw .*$", i['name'].strip()):
                logger.info('MATCH')
                logger.info(i['name'])
                
                form_id = i['id']
                start=(datetime.now()+timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")
                logger.info("Start %s" %(start))            
                end=(datetime.now()-timedelta(days=60)).strftime("%Y-%m-%d %H:%M:%S")
                logger.info("End %s" %(end))
                #post={"onlyFinished":False,"startDateTime":start,"endDateTime":end,"filters":[]}

                #r = requests.post(url_kizeo + '/forms/' + form_id + '/data/exports_info?Authorization='+token,post)
                r = requests.get(url_kizeo + '/forms/' + form_id + '/data/exports_info?Authorization='+token)

                if r.status_code != 200:
                    logger.error('something went wrong...')
                    logger.error(r.status_code, r.reason)
                elif r.text == '':
                    logger.info('Empty response')
                else:

                    ids=[]
                    for rec in r.json()["data"]:
    #                    print(rec)
                        ids.append(rec["id"])
                    
                    
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
                    
                        df = pd.read_excel("./tmp/"+i['name']+".xlsx", header=None)
                                                                
                        logger.info(i["name"])
                        if "16" in i['name']:
                            ronde='16'
                        else:
                            ronde='702'
                        df=df[[1,2]]
                        df[5]=ronde
                        df_all=df_all.append(df)

                    
                    

        if len(df_all) > 0:
            df_all.columns=["date","creation_date","ronde_number"]
            
            df_all['_id']='biac_kpi103'+"_"+df_all['date'].dt.strftime("%d%B%Y")+"_"+df_all['ronde_number'].astype(str)#+"_"+df_all["operator"]
            df_all['_id'].apply(lambda x:x.lower().replace(" ",""))            
            df_all['_index'] = "biac_kpi103"

            df_all['creation_date']=df_all['creation_date'].dt.tz_localize(tz='Europe/Paris')
            df_all['date']=df_all['date'].dt.tz_localize(tz='Europe/Paris')


            es_helper.dataframe_to_elastic(es, df_all)  


            obj={
                    'start': min(df_all['date']),
                    'end': max(df_all['date']),
                    'ronde_number': ronde
            }

            # conn.send_message('/topic/BIAC_KPI103_IMPORTED', str(obj))

            time.sleep(3)
            # compute_kpi103_monthly(obj['start'], obj['end'])

            endtime = time.time()
            log_message("Import of KPI103 from Kizeo finished. Duration: %d Records: %d." % (endtime-starttime, df_all.shape[0]))       


    


    except Exception as e:
        logger.error("Unable to read Kizeo.",exc_info=True)
        endtime = time.time()
        exc_type, exc_value, exc_traceback = sys.exc_info()
        traceback.print_tb(exc_traceback, limit=1, file=sys.stdout)
        # exc_type below is ignored on 3.5 and later
        traceback.print_exception(exc_type, exc_value, exc_traceback,
                              limit=2, file=sys.stdout)


#################################################
def computeStats103():
    """
    Compute KPI 103 stats. It first deletes the collection biac_kib_kpi103 and then recreates it.
    """

    try:
        start,end=get_month_day_range(datetime.now())

        logger.info(">>> Compute Stats 103")
        es_index="biac_kib_kpi103"

        es.indices.delete(index=es_index, ignore=[400, 404]) 
        
        query={
            "aggs": {
                "time": {
                "date_histogram": {
                    "field": "date",
                    "interval": "1d",
                    "time_zone": "Europe/Berlin",
                    "min_doc_count": 1
                },
                "aggs": {
                    "ronde": {
                    "terms": {
                        "field": "ronde_number",
                        "size": 50,
                        "order": {
                        "_count": "desc"
                        }
                    }
                    }
                }
                }
            },
            "query": {
                "bool": {
                "must": [
                    {
                    "match_all": {}
                    },
                    {
                    "range": {
                        "date": {
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

        rondes=['16','702']
        rondestats={"16":{"done":0},"702":{"done":0}}    

        res=es.search(body=query,index="biac_kpi103*")

        records=res["aggregations"]["time"]["buckets"]



        objs={}
        
        for rec in records:  
            logger.info("========+>"*10)
            logger.info(rec)

            obj={}
            for ronde in rec["ronde"]["buckets"]:
                logger.info(ronde)
                obj[str(ronde["key"])]=1

            objs[rec["key_as_string"][0:10]]=obj

        startloop=start
        dates=[]
        
        while startloop <=end  and startloop.month==start.month:
            dates.append(startloop.strftime("%Y-%m-%d"))        
            startloop+=timedelta(days=1)
            


        bulkbody=[]
        
        
        for ronde in rondes:
            for date in dates:
                value=0
                if date in objs:
                    if ronde in objs[date]:
    #                    print(rondestats[ronde])
                        #rondestats[ronde]["done"]+=1                    
                        value=1                
                

                action = {}
                action["index"] = {"_index": es_index,
                        "_type": "doc"}
                bulkbody.append(json.dumps(action))  
                if ronde=="16":
                    finalronde="Ronde 16"
                else:
                    finalronde="Ronde 702"
                obj={"@timestamp":datetime.now().date().isoformat(),"done":value,"ronde":ronde,"finalronde":finalronde,"date":date,"rec_type":"heatmap"}
                bulkbody.append(json.dumps(obj))            

    #    bulkbody=[]
        dones=0
        notdones=0
        percentage=100
        
        if(datetime.now().date().day>1):
            startloop=start
            dates=[]
            
            while startloop.day <datetime.now().day:
                dates.append(startloop.strftime("%Y-%m-%d"))        
                startloop+=timedelta(days=1)

            for ronde in rondes:
                for date in dates:                    
                    value=0
                    if date in objs:
                        if ronde in objs[date]:
                            rondestats[ronde]["done"]+=1                    
                            value=1  
            for key in rondestats:
                notdones+=datetime.now().date().day-1-rondestats[key]["done"]
                dones+=rondestats[key]["done"]

            if dones+notdones>0:
                percentage=(dones*100)/(dones+notdones)
        

        for key in rondestats:
            rondestats[key]["notdone"]=datetime.now().date().day-rondestats[key]["done"]
            
            action = {}
            action["index"] = {"_index": es_index,"_type": "doc"}
            obj={"@timestamp":datetime.now().date().isoformat(),"value":rondestats[key]["done"]
                    ,"ronde":key,"rec_type":"stats_done","globalpercentage":percentage}        
            bulkbody.append(json.dumps(action))
            bulkbody.append(json.dumps(obj))            
            
            #obj=rondestats[key]
            
            if ronde=="16":
                finalronde="Ronde 16"
            else:
                finalronde="Ronde 702"


            obj={"@timestamp":datetime.now().date().isoformat(),"value":rondestats[key]["notdone"]
                    ,"ronde":key,"finalronde":finalronde,"rec_type":"stats_notdone","globalpercentage":percentage}
            
            bulkbody.append(json.dumps(action))
            bulkbody.append(json.dumps(obj))            

                        
        res=es.bulk("\r\n".join(bulkbody))
    except:
        logger.error("Unable to compute stats.",exc_info=True)

def compute_kpi103_monthly(start, end):
    """
    Called three seconds after an import with the first day and last day of the previous import.

    Parameters
    ----------
    start
        Date of the first imported record
    end
        Date of the last imported record
    """

    starttime = time.time()
    
    logger.info(start)
    logger.info(end)


    start = mkFirstOfMonth(start)
    end = mkLastOfMonth(end)

    logger.info(start)
    logger.info(end)
    
    df_kpi103 = etp.genericIntervalSearch(es, 'biac_kpi103', query='*', start=start, end=end,timestampfield="date")

    #logger.info(df_kpi103['date'].dt)
    

    df_kpi103['month'] = pd.to_datetime(df_kpi103['date2']).dt.strftime('%Y-%m')
    
    df_kpi103=df_kpi103.groupby(['month', 'ronde_number']).count()[['date']].rename(columns={'date': 'count'}).reset_index()


    min_date = datetime.strptime(min(df_kpi103['month']), '%Y-%m')
    max_date = datetime.strptime(max(df_kpi103['month']), '%Y-%m')

    months  = []

    current = min_date

    while current <= max_date:
        months.append(current.strftime('%Y-%m'))
        current += relativedelta(months=1)
        
    df_months = pd.DataFrame(months)
    df_months['join']=1
    df_months.columns=['month', 'join']

    rondes = [1, 2]
    df_rondes = pd.DataFrame(rondes)
    df_rondes['join']=1
    df_rondes.columns=['ronde_number', 'join']


    df_default=df_months.merge(df_rondes, left_on='join', right_on='join')
    del df_default['join']
    df_default['number_of_days'] = df_default['month'].apply(lambda x: get_days_already_passed(x))
    df_default['_id'] = df_default['month'] + '_' + df_default['ronde_number'].astype(str)

    df_kpi103['_id'] = df_kpi103['month'] + '_' + df_kpi103['ronde_number'].astype(str)
    del df_kpi103['month']
    del df_kpi103['ronde_number']
    df_merged = df_default.merge(df_kpi103, left_on='_id', right_on='_id', how="outer")
    df_merged = df_merged.fillna(0)
    df_merged['percent'] = round(100*df_merged['count'] / df_merged['number_of_days'], 2)
    df_merged['_index'] = 'biac_month_kpi103'
    df_merged['_timestamp'] = pd.to_datetime(df_merged['month'], format='%Y-%m')
    df_merged.columns=['month', 'ronde_number', 'number_of_days', '_id', 'ronde_done', 'percent', '_index', '_timestamp']
    df_merged['ronde_done'] = df_merged['ronde_done'].astype(int)

    pte.pandas_to_elastic(es, df_merged)

    endtime = time.time()
    log_message("Compute monthly KPI103 (process biac_import_kpi103.py) finished. Duration: %d Records: %d." % (endtime-starttime, df_merged.shape[0]))   
    

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
                    loadKPI103()
                    time.sleep(1)
                    computeStats103()

                except Exception as e2:
                    logger.error("Unable to load kizeo.")
                    logger.error(e2,exc_info=True)
            
        except Exception as e:
            logger.error("Unable to send life sign.")
            logger.error(e)
    