"""
BIAC IMPORT COGEN MAILS
====================================


Sends:
-------------------------------------

* /topic/GTC_COGEN_MAILS_IMPORTED

Listens to:
-------------------------------------

* /queue/COGEN_MAILS

Collections:
-------------------------------------



VERSION HISTORY
-------------------------------------

* 20 Sep 2019 1.0.1 **PDB** Creation
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
import csv


import tzlocal # $ pip install tzlocal

VERSION="0.0.4"
MODULE="GTC_COGEN_MAILS_IMPORTER"
QUEUE=["COGEN_MAILS2"]

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
def getTimestamp(timeD):
    #logger.info(timeD)
    ts = 0
    if timeD == 0:
        return 0
    else:
        try:
            dtt = timeD.timetuple()
            ts = int(time.mktime(dtt))
        except Exception as er:
            logger.error(er)
            logger.error(str(timeD))
        return ts

################################################################################

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

################################################################################

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

################################################################################

def convertToDate(cell):
    try:
        return dateutil.parser.parse(cell)
    except:
        return None

################################################################################

def utc_to_local(utc_dt):
    return utc_dt.replace(tzinfo=timezone.utc).astimezone(tz=tz.tzlocal())


################################################################################

def updateCache(meter,meterdate,curvalue):
    global lastvaluecache
    #print(meter)
    key=meter+meterdate.strftime('%Y-%m-%d %H:%M')
    lastvaluecache[meter]={"key":key,"value":curvalue}
    if((meterdate.hour==0) and (meterdate.minute==0)):
        dayvaluecache[meter]={"key":key,"value":curvalue}

################################################################################

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




################################################################################
def handleOneMotor(row,motor,datemail):
    logger.info("Handling motor:"+motor)
    logger.info("Serie:"+row[1])
    logger.info("Serie:"+row[2])

    indexname="OPT_SITES_DATA-%s" %(datetime.now().strftime("%Y-%m"))
    indexname = indexname.lower()
    id=(motor+"_"+row[1]+"_"+time.strftime('%Y%m%d%H%M',datemail)).lower()
    action={}
    action["index"]={"_index":indexname,"_type":"doc","_id":id}
    newrec={"src":"mail","client_area_name":"cogenmail_"+motor+"_"+row[1], "type":"cogen", "area_name":"cogenmail_"+motor, "client":"cogenmail","area":motor,"name":row[1]
            ,"@timestamp":int(datetime(*datemail[:6]).timestamp()*1000)
            ,"date":time.strftime('%Y-%m-%d %H:%M:00',datemail),"value":row[2],"value_day":row[2]}
    bulkbody=""
    bulkbody+=json.dumps(action)+"\r\n"
    bulkbody+=json.dumps(newrec)+"\r\n"

    return bulkbody

################################################################################

def handleOneMessage(name,body):
    if decodeMetaData(name)==0:
        indexname="OPT_SITES_DATA-%s" %(datetime.now().strftime("%Y-%m"))

        dataasio=StringIO(body)

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

        indexname=indexname.lower()

        for index,row in df.iterrows():
            area_name=site+"_"+row[0]
            client_area_name=contract+"_"+area_name

            localdate=utc_to_local(row['date'])
            cacheres=getCache(row[0],localdate,row[3])
            ts=localdate.strftime('%s')+"000"

            newrec={"src": "mail","client_area_name":client_area_name, "type":computeType(row[0]), "area_name":area_name, "client":contract,"area":site,"name":row[0],"@timestamp":ts
                        ,"date":localdate.strftime('%Y-%m-%d %H:%M:00'),"value":row[3],"value_15":cacheres[0],"value_day":cacheres[1]}

            updateCache(row[0],localdate,row[3])
            action={}
            id=(client_area_name+utc_to_local(row['date']).strftime('%Y%m%d%H%M')).lower()
            action["index"]={"_index":indexname,"_type":"doc","_id":id}
            bulkbody+=json.dumps(action)+"\r\n"
            bulkbody+=json.dumps(newrec)+"\r\n"

        logger.info("Final %s=" %(bulkbody))
        logger.info("Bulk ready.")
        if(bulkbody != ""):
            es.bulk(body=bulkbody)
        else:
            logger.error("No BODY !!!")
        logger.info("Bulk gone.")

################################################################################
def messageReceived(destination,message,headers):
    """
    Main function that reads the Excel file.         
    """
    global es
    starttime = time.time()
    logger.info("==> "*10)
    logger.info("Message Received %s" % destination)
    logger.info(headers)


    file_name  = 'default'


    if "CamelSplitAttachmentId" in headers:
        headers["file"] = headers["CamelSplitAttachmentId"]

    if "file" in headers:
        file_name = headers["file"]
        logger.info("File:%s" %file_name)
        log_message("Import of file [%s] started." %file_name)


    
    #filename = headers["file"]
    #mes=base64.b64decode(message)
    #message = message.decode('utf-8')
    mesin = message
    
    try:
        if("Date" in headers):
            maildatestr=headers["Date"]
            logger.info("Date:%s" %(maildatestr))

            #message=mesin.decode("utf-8", "ignore")
            logger.info("Message [%d]:%s" %(len(mesin),mesin))

            index1=message.find('\"')

            messagebody=""

            if index1>0:
                index2=message.rfind(' ',0,index1-1)
                if index2>0:
                    motor=message[index2+1:index1].replace("\n","").replace("\r","")
                    motor=motor.replace(' ','')
                    f = StringIO(message[index1:])
                    reader = csv.reader(f, delimiter=';')

                    if (maildatestr.index(',')>=0):
                        maildatestr=maildatestr[maildatestr.index(',')+1:]

                    if (maildatestr.index('+')>=0):
                        maildatestr=maildatestr[:maildatestr.index('+')]

                    maildatestr=maildatestr.strip()

                    try:
                        maildate=time.strptime(maildatestr,'%d %b %y %H:%M:%S')
                    except:
                        maildate=time.strptime(maildatestr,'%d %b %Y %H:%M:%S')
#                    maildate=time.strptime(maildatestr,'%d %b %y %H:%M:%S')

                    for row in reader:
                        if (row[0][0]>='0' and row[0][0]<='9'):
                            logger.info ('\t'.join(row))
                            messagebody+=handleOneMotor(row,motor,maildate)

            logger.info(messagebody)
            if(len(messagebody)>0):
                logger.info("Final %s=" %(messagebody))
                logger.info("Bulk ready.")
                res = es.bulk(body=messagebody)
                logger.info(res)
                logger.info("Bulk gone.")
            else:
                logger.error("No BODY !!!")       

 
    except Exception as e:
        endtime = time.time()
        logger.error(e)
        logger.error("Import of file [%s] failed. Duration: %d Exception: %s." % (headers["file"],(endtime-starttime),str(e)))        


    endtime = time.time()    
    try:
        log_message("Import of file [%s] finished. Duration: %d Records: %d." % (headers["file"],(endtime-starttime),df.shape[0]))         
    except:
        log_message("Import of file [%s] finished. Duration: %d." % (headers["file"],(endtime-starttime)))    
    
        

    logger.info("<== "*10)

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
                    ,"login":os.environ["AMQC_LOGIN"],"password":os.environ["AMQC_PASSWORD"],"heartbeats":(120000,120000),"earlyack":True}
    logger.info(server)                
    conn=amqstompclient.AMQClient(server
        , {"name":MODULE,"version":VERSION,"lifesign":"/topic/NYX_MODULE_INFO"},QUEUE,callback=messageReceived)
    #conn,listener= amqHelper.init_amq_connection(activemq_address, activemq_port, activemq_user,activemq_password, "RestAPI",VERSION,messageReceived)
    connectionparameters={"conn":conn}
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


    logger.info("AMQC_URL          :"+os.environ["AMQC_URL"])
    while True:
        time.sleep(5)
        try:            
            conn.send_life_sign()
        except Exception as e:
            logger.error("Unable to send life sign.")
            logger.error(e)
    #app.run(threaded=True,host= '0.0.0.0')
