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
import pytz
from dateutil.tz import tzlocal
from tzlocal import get_localzone

from elastic_helper import es_helper 



import tzlocal # $ pip install tzlocal


VERSION="0.0.1"
MODULE="GTC_CLEANED_DATA"
QUEUE=["GTC_CLEANED_DATA_RANGE"]

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

    logger.info("==> "*10)
    logger.info("Message Received %s" % destination)

    msg = json.loads(message)
    start = datetime.fromtimestamp(int(msg['start']))
    stop = datetime.fromtimestamp(int(msg['stop']))

    while start < stop:
        
        try:
            df=retrieve_raw_data(start)
            print(start)
            for datestr, df_date in df.groupby('date'):
                tempdf = df_date[['name']]
                tempdf[1] = df_date[['date']]
                tempdf[2] = 2
                tempdf[3] = df_date[['value']]
                tempdf[4] = 0
                tempdf = tempdf.reset_index(drop= True)
                tempdf = tempdf.rename({'name': 0}, axis='columns')
                datestr = datestr.replace('-','')
                datestr = datestr.replace(':','')
                datestr = datestr.replace(' ','_')
                filename = 'COGEN_NyxAWS_'+datestr+'.CSV'
                try:    
                    cleanData(filename, tempdf)
                except Exception as er:
                    logger.error('Cannot cleaned data for datetime : '+ datestr)
                    logger.error(er)
        except Exception as er:
            logger.error('Cannot cleaned data for datetime : '+ str(start))
            logger.error(er)

        start = start + timedelta(1)
    
def retrieve_raw_data(day):
    start_dt = datetime(day.year, day.month, day.day, 14, 30, 0)
    end_dt   = datetime(start_dt.year, start_dt.month, start_dt.day, 20, 59, 59)

    df_raw=es_helper.elastic_to_dataframe(es, index='opt_sites_data*', 
                                           query='client: COGLTS AND area_name: *CH4*', 
                                           start=start_dt, 
                                           end=end_dt,
                                           size=1000000)

    containertimezone=pytz.timezone(get_localzone().zone)
    df_raw['@timestamp'] = pd.to_datetime(df_raw['@timestamp'], \
                                               unit='ms', utc=True).dt.tz_convert(containertimezone)
    df_raw=df_raw.sort_values('@timestamp') 

    logger.info(df_raw)
    
    return df_raw

def utc_to_local(utc_dt):
    return utc_dt.replace(tzinfo=timezone.utc).astimezone(tz=tz.tzlocal())



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
    cond4 = var1 < 100
    
    if cond1 and cond2 and cond3 and cond4:
        return var1
    else:
        return 0

def getValueTag(tag, df):
    if tag in df.index:
        return df.loc[tag].at[3]
    else:
        return 0


def cleanData(filename, mes):
    global es
    #logger.info("===> CLEANING DATA <===")
    if decodeMetaData(filename)==0:
        #logger.info("===> ENTER IF <===")
        indexname="OPT_CLEANED_DATA-%s" %(datetime.now().strftime("%Y-%m"))

    #dataasio=StringIO(mes)
    #nofsemicolons=mes.count(';')
    #nofcommas=mes.count(',')

    #if nofsemicolons>nofcommas:
        #df=pd.read_csv(dataasio,sep=";",header=None)
    #else:
    #    df=pd.read_csv(dataasio,sep=",",header=None)
    
    df = mes
    
    dfindexed = df.set_index(0)
    site = 'H5BANI_ExportNyxAWS'
    contract = 'H5BANI'

    if contract == 'COGLTS':
        dfindexed = df.set_index(0)


        H2SApThiopaq = getValueTag('LUTOSA_H2S_Ap_Thiopaq', dfindexed)
        H2SAvCogen = getValueTag('LUTOSA_H2S_Av_Cogen', dfindexed)
        H2SAvThiopaq  = getValueTag('LUTOSA_H2S_Av_Thiopaq', dfindexed)

        CH4ApThiopaq = getValueTag('LUTOSA_CH4_Ap_Thiopaq', dfindexed)
        CH4AvCogen = getValueTag('LUTOSA_CH4_Av_Cogen', dfindexed)
        CH4AvThiopaq = getValueTag('LUTOSA_CH4_Av_Thiopaq', dfindexed)

        O2ApThiopaq = getValueTag('LUTOSA_O2_Ap_Thiopaq', dfindexed)
        O2AvCogen = getValueTag('LUTOSA_O2_Av_Cogen', dfindexed)
        O2AvThiopaq = getValueTag('LUTOSA_O2_Av_Thiopaq', dfindexed)

        DebitBiogazThiopaq = getValueTag('COGLTS_BIOLTS_Valeur_Debit_Biogaz_Thiopaq', dfindexed)
        DebitBiogazCogen = getValueTag('COGLTS_BIOLTS_Valeur_Debit_Biogaz_Cogen', dfindexed)

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
        fctThiopaqCond1 = 1 if getValueTag('COGLTS_BIOLTS_Valeur_Debit_Biogaz_Thiopaq', dfindexed) > 120 else 0
        fctThiopaqCond2 = 1 if (getValueTag('COGLTS_BIOLTS_Valeur_Pression_Thiopaq_Entree', dfindexed) > 15 and getValueTag('COGLTS_BIOLTS_Valeur_Pression_Thiopaq_Entree', dfindexed) < 36) else 0
        fctThiopaqCond3 = getValueTag('LUTOSA_Etat_Autor_Biog', dfindexed)
        fctThiopaqCondGlobal = 1 if fctThiopaqCond1 + fctThiopaqCond2 + fctThiopaqCond3 == 3 else 0

        consoDispos = getValueTag('LUTOSA_Etat_Peleur1_Fct', dfindexed) + getValueTag('LUTOSA_Etat_Peleur2_Fct', dfindexed) + getValueTag('LUTOSA_Etat_Blancheur_1', dfindexed) +getValueTag('LUTOSA_Etat_Blancheur_2', dfindexed)
        fctCogenCond1 = 1 if consoDispos >= 2 else 0
        fctCogenCond2 = 1 if getValueTag('LUTOSA_TempRetour_Boucle_HT', dfindexed) <= 80 else 0
        fctCogenCond3 = 1 if getValueTag('COGLTS_BIOLTS_Valeur_Debit_Biogaz_Thiopaq', dfindexed) > 220 else 0
        fctCogenCond4 = getValueTag('LUTOSA_Moteur_Disponible', dfindexed)
        fctCogenCond5 = 1 if getValueTag('LUTOSA_H2S_Av_Cogen', dfindexed) <= 5 else 0
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
    #site = 'LUTOSA_ExportNyxAWS'
    #contract = 'COGLTS'


    dffinal['src'] = 'gtc'
    dffinal['area_name'] = site +'_'+ dffinal['name']
    dffinal['client_area_name'] = contract + '_' + dffinal['area_name']
    dffinal['client'] = contract
    dffinal['area'] = site
    dffinal["date"]=dffinal['date'].apply(convertToDate)
    #dffinal["date"]=dffinal['date'].apply(lambda x: utc_to_local(x))
    dffinal['@timestamp'] = dffinal['date'].apply(lambda x: getTimestamp(x)*1000)
    dffinal['date'] = dffinal['date'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:00'))
    dffinal['_index'] = dffinal['date'].apply(lambda x: getIndex(x))
    dffinal['_id'] = dffinal.apply(lambda row: getId(row), axis=1)

    es_helper.dataframe_to_elastic(es, dffinal)
    #return dffinal



def getIndex(ts):
    return 'opt_cleaned_data-'+ts[:7]

def getId(row):
    _id = ('clean_'+row['client_area_name']+str(row['@timestamp'])).lower()
    return _id

      
    
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
