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
from functools import wraps
from elasticsearch import Elasticsearch as ES, RequestsHttpConnection as RC
from logstash_async.handler import AsynchronousLogstashHandler
from lib import pandastoelastic as pte
import numpy as np
import collections
from datetime import timezone
from io import StringIO
from dateutil import tz
import dateutil.parser


import tzlocal # $ pip install tzlocal


VERSION="1.0.5"
MODULE="GTC_SITES_DATA"
QUEUE=["GTC_SITES_DATA_temp1"]

query = {
  "aggs": {
    "2": {
      "terms": {
        "field": "name",
        "size": 5000,
        "order": {
          "_key": "desc"
        }
      },
      "aggs": {
        "1": {
          "top_hits": {
            "docvalue_fields": [
              {
                "field": "value",
                "format": "use_field_mapping"
              }
            ],
            "_source": "value",
            "size": 1,
            "sort": [
              {
                "@timestamp": {
                  "order": "desc"
                }
              }
            ]
          }
        },
        "3": {
          "top_hits": {
            "docvalue_fields": [
              {
                "field": "value_day",
                "format": "use_field_mapping"
              }
            ],
            "_source": "value_day",
            "size": 1,
            "sort": [
              {
                "@timestamp": {
                  "order": "desc"
                }
              }
            ]
          }
        }
      }
    }
  },
  "size": 0,
  "_source": {
    "excludes": []
  },
  "stored_fields": [
    "*"
  ],
  "script_fields": {},
  "docvalue_fields": [
    {
      "field": "@timestamp",
      "format": "date_time"
    }
  ],
  "query": {
    "bool": {
      "must": [
        {
          "query_string": {
            "query": "*",
            "analyze_wildcard": True,
            "default_field": "*"
          }
        },
        {
          "range": {
            "@timestamp": {
              "gte": "now-7d",
              "lte": "now",
              "format": "epoch_millis"
            }
          }
        }
      ],
      "filter": [],
      "should": [],
      "must_not": []
    }
  }
}

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

def getTimestamp(timeD):
    """ Returns the Unix timestamp of a datetime

    Parameters
    ----------
    dt
        Date on datetime format
    """
    dtt = timeD.timetuple()
    ts = int(time.mktime(dtt))
    return ts


################################################################################
def messageReceived(destination,message,headers):
    global es
    records=0
    starttime = time.time()
    logger.info("==> "*10)
    logger.info("Message Received %s" % destination)
    logger.info(headers)

    if "CamelFileNameOnly" in headers:
        headers["file"] = headers["CamelFileNameOnly"]

    if "file" in headers:
        logger.info("File:%s" %headers["file"])
        log_message("Import of file [%s] started." % headers["file"])

    filename = headers["CamelFileAbsolutePath"]
    mes=base64.b64decode(message)
    mes = mes.decode('utf-8')
    #logger.info("Message [%d]:%s" %(len(mes),mes))
    if((filename != None) and (len(mes)>10)):
        try:
            handleOneMessage(filename,mes)
        except Exception as e:
            logger.error("Unable to decode message")
            error = traceback.format_exc()
            logger.error(error)
            endtime = time.time()
            logger.error(e,exc_info=True)
            log_message("Import of file [%s] failed. Duration: %d Exception: %s." % (headers["file"],(endtime-starttime),str(e)))   


    #if len(reserrors)>0:
    #   log_message("Import of file [%s] failed. Duration: %d. %d records were not imported." % (headers["file"],(endtime-starttime),len(reserrors)))        

    endtime = time.time()    
    try:
        log_message("Import of file [%s] finished. Duration: %d Records: %d." % (headers["file"],(endtime-starttime),df.shape[0]))         
    except:
        log_message("Import of file [%s] finished. Duration: %d." % (headers["file"],(endtime-starttime)))    
    
        

    logger.info("<== "*10)


def utc_to_local(utc_dt):
    return utc_dt.replace(tzinfo=timezone.utc).astimezone(tz=tz.tzlocal())

def updateCache(meter,meterdate,curvalue):
    global lastvaluecache
    #print(meter)
    key=meter+meterdate.strftime('%Y-%m-%d %H:%M')
    lastvaluecache[meter]={"key":key,"value":curvalue}
    if((meterdate.hour==0) and (meterdate.minute==0)):
        dayvaluecache[meter]={"key":key,"value":curvalue}


def getCache(meter,meterdate,curvalue):
    global lastvaluecache, dayvaluecache
    last15=-1
    #last5=-1
    lastday=-1
    meterdateold=meterdate- timedelta(minutes=15)
    meterdateold2=meterdate- timedelta(minutes=5)
    key=meter+meterdateold.strftime('%Y-%m-%d %H:%M')
    key2=meter+meterdateold2.strftime('%Y-%m-%d %H:%M')

    if ((meter in lastvaluecache) and (lastvaluecache[meter]["key"]==key)):
      print('15 minutes')
      last15=curvalue-lastvaluecache[meter]["value"]

    elif ((meter in lastvaluecache) and (lastvaluecache[meter]["key"]==key2)):
      print('5 minutes')
      last15=curvalue-lastvaluecache[meter]["value"]
    #else:
    #  print('not compatible : '+ key + '  ' + lastvaluecache[meter]["key"])



    if((meterdate.hour==0) and (meterdate.minute < 5)):
      lastday=0
    else:
      key=meter+meterdate.strftime('%Y-%m-%d')+" 00:00"
      if((meter in dayvaluecache) and (dayvaluecache[meter]["key"]==key)):
        lastday=curvalue-dayvaluecache[meter]["value"]

    #logger.info(lastvaluecache)
    #logger.info(dayvaluecache)

    return [last15,lastday]

def decodeMetaData(fullpath):
    global site,contract
    namecols=fullpath.split("/")
    lennamecols=len(namecols)
    if(lennamecols>2):
        contract=namecols[lennamecols-2]
        site=namecols[lennamecols-1]
        sitecols=site.split("_")
        cleansite=""
        for i in range(0,len(sitecols)-2):
            cleansite+=sitecols[i]+"_"
        cleansite=cleansite.strip('_')
        cleansite=cleansite.replace('ENERGY_ICT_','')
        site=cleansite

        return 0
    return -1

def computeType(metername):

    if((metername.lower().find("eau")>=0) or (metername.lower().find("water")>=0)):
        return "water"
    if((metername.lower().find("tension")>=0) or (metername.lower().find("eclair")>=0) or (metername.lower().find("venti")>=0) or (metername.lower().find("elec")>=0)):
        return "elec"
    if((metername.lower().find("photo")>=0) or (metername.lower().find("zonnepanelen")>=0)):
        return "photo"
    if((metername.lower().find("gaz")>=0) or (metername.lower().find("gas")>=0)):
        return "gaz"
    return "NA"

def convertToDate(cell):
    try:
        return dateutil.parser.parse(cell)
    except:
        return None

def handleOneMessage(name,body):
    global es
    logger.info("===> HANDLE MESSAGE <===")
    if decodeMetaData(name)==0:
        logger.info("===> ENTER IF <===")
        indexname="OPT_SITES_DATA-%s" %(datetime.now().strftime("%Y-%m"))

        logger.info(lastvaluecache['Conso_Ecl_Ext'])
        logger.info(dayvaluecache['Conso_Ecl_Ext'])
        #logger.info(body)

        dataasio=StringIO(body)

        #logger.info(dataasio)
        nofsemicolons=body.count(';')
        nofcommas=body.count(',')

        if nofsemicolons>nofcommas:
            df=pd.read_csv(dataasio,sep=";",header=None)
        else:
            df=pd.read_csv(dataasio,sep=",",header=None)

        #df["date"]=df[1].apply(dateutil.parser.parse)
        df["date"]=df[1].apply(convertToDate)
        df=df.dropna()

        bulkbody=""

        #logger.info(df)

        indexname=indexname.lower()

        for index,row in df.iterrows():
            area_name=site+"_"+row[0]
            client_area_name=contract+"_"+area_name

            localdate=utc_to_local(row['date'])
            key=row[0]+localdate.strftime('%Y-%m-%d %H:%M')
            key2 = row[0]+localdate.strftime('%Y-%m-%d 00:00')
            print(key)
            if row[0] in lastvaluecache:
              if lastvaluecache[row[0]]["key"] != key:
                cacheres=getCache(row[0],localdate,row[3])
                ts=localdate.strftime('%s')+"000"

                newrec={"src":"gtc","client_area_name":client_area_name, "type":computeType(row[0]), "area_name":area_name, "client":contract,"area":site,"name":row[0],"@timestamp":ts
                            ,"date":localdate.strftime('%Y-%m-%d %H:%M:00'),"value":row[3],"value_15":cacheres[0],"value_day":cacheres[1]}

                updateCache(row[0],localdate,row[3])
                action={}
                id=(client_area_name+utc_to_local(row['date']).strftime('%Y%m%d%H%M')).lower()
                action["index"]={"_index":indexname,"_type":"doc","_id":id}
                bulkbody+=json.dumps(action)+"\r\n"
                bulkbody+=json.dumps(newrec)+"\r\n"
              else:
                logger.info('Duplicate File')
            else:
              print("new key")
              lastvaluecache[row[0]]= {"key": key, "value": 0}
              dayvaluecache[row[0]]= {"key": key2, "value": 0}


            first_alarm_ts = getTimestamp(localdate)
            obj = {
                    'start_ts': int(first_alarm_ts),
                    'area_name': area_name 
                }
            logger.info(obj)     
            conn.send_message('/topic/SITES_DATA_IMPORTED', json.dumps(obj))
        

        #logger.info("Final %s=" %(bulkbody))
        logger.info("Bulk ready.")
        if(bulkbody != ""):
            res = es.bulk(body=bulkbody)
            logger.info(res)
        else:
            logger.error("No BODY !!!")
        logger.info("Bulk gone.")
    
    
      
    

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
                ,"login":os.environ["AMQC_LOGIN"],"password":os.environ["AMQC_PASSWORD"]
                ,"heartbeats":(180000,180000),"earlyack":True}

lastvaluecache={}
dayvaluecache={}


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

    res=es.search(index='opt_*'
        ,size=0
        ,body=query
        )
    
    data = res['aggregations']['2']['buckets']
    
    for tag in data:
        tagkey = tag['key']
        value = tag['1']['hits']['hits'][0]['fields']['value'][0]
        timestamp = datetime.fromtimestamp(int(tag['1']['hits']['hits'][0]['sort'][0] / 1000))
        datestr = timestamp.strftime("%Y-%m-%d %H:%M")
        datestr2 = timestamp.strftime("%Y-%m-%d 00:00")
        
        if "fields" in tag['3']['hits']['hits'][0]:
          value_day = tag['3']['hits']['hits'][0]['fields']['value_day'][0]
          #print(str(tagkey)+': '+str(value) + ' , ' + str(value_day) + ' ' + datestr)
          key = tagkey+datestr
          key2 = tagkey+datestr2
          lastvaluecache[tagkey]= {"key": key, "value": value}
          dayvaluecache[tagkey]= {"key": key2, "value": value_day}

    logger.info(lastvaluecache)
    logger.info(dayvaluecache)

    conn=amqstompclient.AMQClient(server
    , {"name":MODULE,"version":VERSION,"lifesign":"/topic/NYX_MODULE_INFO"},QUEUE,callback=messageReceived)
#conn,listener= amqHelper.init_amq_connection(activemq_address, activemq_port, activemq_user,activemq_password, "RestAPI",VERSION,messageReceived)
    connectionparameters={"conn":conn}
    while True:
        time.sleep(5)
        try:            
            variables={"platform":"_/_".join(platform.uname()),"icon":"wrench"}
            conn.send_life_sign(variables=variables)
        except Exception as e:
            logger.error("Unable to send life sign.")
            logger.error(e)
    #app.run(threaded=True,host= '0.0.0.0')
