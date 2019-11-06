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

from elastic_helper import es_helper 



import tzlocal # $ pip install tzlocal


VERSION="1.1.1"
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
            cleanData(filename, mes)
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

def condType1(var1, var2):
    cond1 = var1 > 0
    cond2 = (var1-(var1 - 1)) < (30*var1/100)
    cond3 = var2 > 10
    
    if cond1 and cond2 and cond3:
        return var1
    else:
        return 0
    
def condType2(var1, var2):
    cond1 = var1 > 0
    cond2 = (var1-(var1 - 1)) < (50*var1/100)
    cond3 = var2 > 10
    
    if cond1 and cond2 and cond3:
        return var1
    else:
        return 0
    
def condType3(var1, var2, var3):
    cond1 = var1 > 0
    cond2 = (var1-(var1 - 1)) < var3
    cond3 = var2 > 10
    
    if cond1 and cond2 and cond3:
        return var1
    else:
        return 0

def cleanData(filename, mes):
    global es
    logger.info("===> CLEANING DATA <===")
    if decodeMetaData(filename)==0:
        logger.info("===> ENTER IF <===")
        indexname="OPT_CLEANED_DATA-%s" %(datetime.now().strftime("%Y-%m"))

    dataasio=StringIO(mes)
    nofsemicolons=mes.count(';')
    nofcommas=mes.count(',')

    if nofsemicolons>nofcommas:
        df=pd.read_csv(dataasio,sep=";",header=None)
    else:
        df=pd.read_csv(dataasio,sep=",",header=None)
    
    dfindexed = df.set_index(0)

    if contract == 'COGLTS':
        dfindexed = df.set_index(0)
        H2SApThiopaq = dfindexed.loc['LUTOSA_H2S_Ap_Thiopaq'].at[3]
        H2SAvCogen = dfindexed.loc['LUTOSA_H2S_Av_Cogen'].at[3]
        H2SAvThiopaq  = dfindexed.loc['LUTOSA_H2S_Av_Thiopaq'].at[3]

        CH4ApThiopaq = dfindexed.loc['LUTOSA_CH4_Ap_Thiopaq'].at[3]
        CH4AvCogen = dfindexed.loc['LUTOSA_CH4_Av_Cogen'].at[3]
        CH4AvThiopaq = dfindexed.loc['LUTOSA_CH4_Av_Thiopaq'].at[3]

        O2ApThiopaq = dfindexed.loc['LUTOSA_O2_Ap_Thiopaq'].at[3]
        O2AvCogen = dfindexed.loc['LUTOSA_O2_Av_Cogen'].at[3]
        O2AvThiopaq = dfindexed.loc['LUTOSA_O2_Av_Thiopaq'].at[3]

        DebitBiogazThiopaq = dfindexed.loc['COGLTS_BIOLTS_Valeur_Debit_Biogaz_Thiopaq'].at[3]
        DebitBiogazCogen = dfindexed.loc['COGLTS_BIOLTS_Valeur_Debit_Biogaz_Cogen'].at[3]

        H2SApThiopaq = condType1(H2SApThiopaq, DebitBiogazThiopaq)
        H2SAvCogen = condType3(H2SAvCogen, DebitBiogazCogen, 20)
        H2SAvThiopaq  = condType2(H2SAvThiopaq, DebitBiogazThiopaq)

        CH4ApThiopaq = condType3(CH4ApThiopaq, DebitBiogazThiopaq, 35)
        CH4AvCogen = condType3(CH4AvCogen, DebitBiogazCogen, 40)
        CH4AvThiopaq = condType3(CH4AvThiopaq, DebitBiogazThiopaq, 45)

        O2ApThiopaq = condType3(O2ApThiopaq, DebitBiogazThiopaq, 1.5)
        O2AvCogen = condType3(O2AvCogen, DebitBiogazCogen, 1.5)
        O2AvThiopaq = condType3(O2AvThiopaq, DebitBiogazThiopaq, 1.5)

        dfindexed.at['LUTOSA_H2S_Ap_Thiopaq', 3] = H2SApThiopaq 
        dfindexed.at['LUTOSA_H2S_Av_Cogen',3] = H2SAvCogen
        dfindexed.at['LUTOSA_H2S_Av_Thiopaq',3] = H2SAvThiopaq 

        dfindexed.at['LUTOSA_CH4_Ap_Thiopaq',3] = CH4ApThiopaq
        dfindexed.at['LUTOSA_CH4_Av_Cogen',3] = CH4AvCogen
        dfindexed.at['LUTOSA_CH4_Av_Thiopaq',3] = CH4AvThiopaq

        dfindexed.at['LUTOSA_O2_Ap_Thiopaq',3] = O2ApThiopaq
        dfindexed.at['LUTOSA_O2_Av_Cogen',3] = O2AvCogen
        dfindexed.at['LUTOSA_O2_Av_Thiopaq',3] = O2AvThiopaq

        datefile = dfindexed.at['LUTOSA_Cpt_Ther_HT', 1]
        param1 = dfindexed.at['LUTOSA_Cpt_Ther_HT', 2]
        param2 = dfindexed.at['LUTOSA_Cpt_Ther_HT', 4]
        dffinal = dfindexed.reset_index()
        fctThiopaqCond1 = 1 if dfindexed.loc['COGLTS_BIOLTS_Valeur_Debit_Biogaz_Thiopaq'].at[3] > 120 else 0
        fctThiopaqCond2 = 1 if (dfindexed.loc['COGLTS_BIOLTS_Valeur_Pression_Thiopaq_Entree'].at[3] > 15 and dfindexed.loc['COGLTS_BIOLTS_Valeur_Pression_Thiopaq_Entree'].at[3] < 36) else 0
        fctThiopaqCond3 = dfindexed.loc['LUTOSA_Etat_Autor_Biog'].at[3]
        fctThiopaqCondGlobal = 1 if fctThiopaqCond1 + fctThiopaqCond2 + fctThiopaqCond3 == 3 else 0

        consoDispos = dfindexed.loc['LUTOSA_Etat_Peleur1_Fct'].at[3] + dfindexed.loc['LUTOSA_Etat_Peleur2_Fct'].at[3] + dfindexed.loc['LUTOSA_Etat_Blancheur_1'].at[3] +dfindexed.loc['LUTOSA_Etat_Blancheur_2'].at[3]
        fctCogenCond1 = 1 if consoDispos >= 2 else 0
        fctCogenCond2 = 1 if dfindexed.loc['LUTOSA_TempRetour_Boucle_HT'].at[3] <= 80 else 0
        fctCogenCond3 = 1 if dfindexed.loc['COGLTS_BIOLTS_Valeur_Debit_Biogaz_Thiopaq'].at[3] > 220 else 0
        fctCogenCond4 = dfindexed.loc['LUTOSA_Moteur_Disponible'].at[3]
        fctCogenCond5 = 1 if dfindexed.loc['LUTOSA_H2S_Av_Cogen'].at[3] <= 5 else 0
        fctCogenCondGlobal = 1 if fctCogenCond1 + fctCogenCond2 + fctCogenCond3 + fctCogenCond4 + fctCogenCond5 == 5 else 0


        data = [{0: 'LUTOSA_Cond_Fct_1_Thiopaq',1:datefile, 2:param1, 3:fctThiopaqCond1, 4:param2},
              {0: 'LUTOSA_Cond_Fct_2_Thiopaq',1:datefile, 2:param1, 3:fctThiopaqCond2, 4:param2},
              {0: 'LUTOSA_Cond_Fct_3_Thiopaq',1:datefile, 2:param1, 3:fctThiopaqCond3, 4:param2},
              {0: 'LUTOSA_Cond_Fct_Global_Thiopaq',1:datefile, 2:param1, 3:fctThiopaqCondGlobal, 4:param2},
              {0: 'LUTOSA_Cond_Fct_1_Cogen',1:datefile, 2:param1, 3:fctCogenCond1, 4:param2},
              {0: 'LUTOSA_Cond_Fct_2_Cogen',1:datefile, 2:param1, 3:fctCogenCond2, 4:param2},
              {0: 'LUTOSA_Cond_Fct_3_Cogen',1:datefile, 2:param1, 3:fctCogenCond3, 4:param2},
              {0: 'LUTOSA_Cond_Fct_4_Cogen',1:datefile, 2:param1, 3:fctCogenCond4, 4:param2},
              {0: 'LUTOSA_Cond_Fct_5_Cogen',1:datefile, 2:param1, 3:fctCogenCond5, 4:param2},
              {0: 'LUTOSA_Cond_Fct_Global_Cogen',1:datefile, 2:param1, 3:fctCogenCondGlobal, 4:param2}]
        dffinal = dffinal.append(data,ignore_index=True,sort=True)
    else:
        dffinal = dfindexed.reset_index()
    
    dffinal = dffinal[[0,1,3]]
    dffinal.columns = ['name', 'date', 'value']

    dffinal['src'] = 'gtc'
    dffinal['area_name'] = site +'_'+ dffinal['name']
    dffinal['client_area_name'] = contract + '_' + dffinal['area_name']
    dffinal['client'] = contract
    dffinal['area'] = site
    dffinal["date"]=dffinal['date'].apply(convertToDate)
    dffinal["date"]=dffinal['date'].apply(lambda x: utc_to_local(x))
    dffinal['@timestamp'] = dffinal['date'].apply(lambda x: getTimestamp(x)*1000)
    dffinal['date'] = dffinal['date'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:00'))
    dffinal['_index'] = dffinal['date'].apply(lambda x: getIndex(x))
    dffinal['_id'] = dffinal.apply(lambda row: getId(row), axis=1)

    es_helper.dataframe_to_elastic(es, dffinal)
    



def getIndex(ts):
    return 'opt_cleaned_data-'+ts[:7]

def getId(row):
    _id = ('clean_'+row['client_area_name']+str(row['@timestamp'])).lower()
    return _id

def handleOneMessage(name,body):
    global es
    logger.info("===> HANDLE MESSAGE <===")
    if decodeMetaData(name)==0:
        logger.info("===> ENTER IF <===")
        indexname="OPT_SITES_DATA-%s" %(datetime.now().strftime("%Y-%m"))

        #logger.info(lastvaluecache['Conso_Ecl_Ext'])
        #logger.info(dayvaluecache['Conso_Ecl_Ext'])
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
        #logger.info(obj)     
    
        conn.send_message('/topic/SITES_DATA_IMPORTED', json.dumps(obj))
        

        #logger.info("Final %s=" %(bulkbody))
        logger.info("Bulk ready.")
        if(bulkbody != ""):
            res = es.bulk(body=bulkbody)
            #logger.info(res)
        else:
            logger.error("No BODY !!!")
        logger.info("Bulk gone.")
    
    
      
    
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
                    ,"login":os.environ["AMQC_LOGIN"],"password":os.environ["AMQC_PASSWORD"]
                    ,"heartbeats":(180000,180000),"earlyack":True}

    lastvaluecache={}
    dayvaluecache={}





  
    logger.info("AMQC_URL          :"+os.environ["AMQC_URL"])
      
      #>> ELK
    es=None
    logger.info (os.environ["ELK_SSL"])

    if os.environ["ELK_SSL"]=="true":
        host_params = {'host':os.environ["ELK_URL"], 'port':int(os.environ["ELK_PORT"]), 'use_ssl':True}
        es = ES([host_params], connection_class=RC, http_auth=(os.environ["ELK_LOGIN"], os.environ["ELK_PASSWORD"]),  use_ssl=True ,verify_certs=False)
    else:
        host_params="http://"+os.environ["ELK_URL"]+":"+os.environ["ELK_PORT"]
        es = ES(hosts=[host_params])

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
