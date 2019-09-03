"""
BIAC IMPORT INSTAWORK
====================================
Receives instawork excel files via the /queue/INSTAWORK_IMPORT.
There are 3 excel files:

* One for the lot 6 and lot 7
* Two for the lot 5

The data is cleaned in order to remove all the DGS records.

Sends:
-------------------------------------

* /topic/BIAC_INSTAWORK_IMPORTED

Listens to:
-------------------------------------

* /queue/INSTAWORK_IMPORT

Collections:
-------------------------------------

* **biac_kpi105** (Raw Data)
* **biac_kib_kpi105** (Heat map and horizontal bar stats)
* **biac_month_kpi105** (Computed Data)

VERSION HISTORY
===============

* 27 May 2019 0.1.7 **AMA** Fix a bug that occurs when the asset description column is interpreted as a float.
* 26 Jun 2019 0.1.9 **AMA** Added lot 7
""" 
import json
import time
import uuid
import json
import pytz
import base64
import platform
import threading
import os,logging
import pandas as pd
from logging.handlers import TimedRotatingFileHandler
from amqstompclient import amqstompclient
from datetime import datetime
from datetime import timedelta
from functools import wraps
from elasticsearch import Elasticsearch as ES, RequestsHttpConnection as RC
from logstash_async.handler import AsynchronousLogstashHandler
from lib import pandastoelastic as pte
import numpy as np


from datetime import datetime
import tzlocal # $ pip install tzlocal


VERSION="0.2.0"
MODULE="BIAC_INSTAWORK_IMPORTER"
QUEUE=["INSTAWORK_IMPORT"]

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

def tzaware_localize(dt: datetime, local_tz: str):
    """
    Takes a TZ-unaware datetime in local time *or* a TZ-aware datetime and turns
    it into a TZ-aware datetime in the provided timezone.
    :return: TZ-aware DateTime in the provided timezone
    """
    if dt.tzinfo is None:
        return pytz.timezone(local_tz).localize(dt)
    else:
        return dt.astimezone(pytz.timezone(local_tz))

def compute_status5(row):
    now=tzaware_localize(datetime.now(),"Europe/Paris")        

    if row["Order_Type"]=="PM":
        
        if not pd.isnull(row["Finish_Date"]):#row["Finish_Date"]!=None and not math.isnan(row["Finish_Date"]):
            diff=(row["Finish_Date"]-row["Reference_Date"])
            if diff<=timedelta(days=5):
                return "Done OK"
            else:
                return "Done NOK"        
        else:
            diff=(now)-(row["Reference_Date"].to_pydatetime())
            if diff<=timedelta(days=5):
                return "Not Done OK"
            else:
                return "Not Done NOK"        
    else:
        if not pd.isnull(row["Finish_Date"]):#!=None and not math.isnan(row["Finish_Date"]):
            diff=(row["Reference_Date"]-row["Finish_Date"])
            #logger.info(diff)
            
            if diff>=timedelta(days=0):
                return "Done OK"
            else:                
                return "Done NOK"        
        else:
            diff=(row["Reference_Date"].to_pydatetime()-(now))

            if diff>=timedelta(days=0):
                return "Not Done OK"
            else:
                logger.info("diff")
                return "Not Done NOK"  


def compute_status7(row):
    now=tzaware_localize(datetime.now(),"Europe/Paris")        

    if row["Order_Type"]=="PM":
        
        if not pd.isnull(row["Finish_Date"]):#row["Finish_Date"]!=None and not math.isnan(row["Finish_Date"]):
            diff=(row["Finish_Date"]-row["Reference_Date"])
            if diff<=timedelta(days=7):
                return "Done OK"
            else:
                return "Done NOK"        
        else:
            diff=(now)-(row["Reference_Date"].to_pydatetime())
            if diff<=timedelta(days=7):
                return "Not Done OK"
            else:
                return "Not Done NOK"        
    else:
        if not pd.isnull(row["Finish_Date"]):#!=None and not math.isnan(row["Finish_Date"]):
            diff=(row["Reference_Date"]-row["Finish_Date"])
            #logger.info(diff)
            
            if diff>=timedelta(days=0):
                return "Done OK"
            else:                
                return "Done NOK"        
        else:
            diff=(row["Reference_Date"].to_pydatetime()-(now))

            if diff>=timedelta(days=0):
                return "Not Done OK"
            else:
                logger.info("diff")
                return "Not Done NOK" 

################################################################################
def messageReceived(destination,message,headers):
    global es
    records=0
    starttime = time.time()
    logger.info("==> "*10)
    logger.info("Message Received %s" % destination)
    logger.info(headers)

    if "CamelSplitAttachmentId" in headers:
        headers["file"] = headers["CamelSplitAttachmentId"]

    if "file" in headers:
        logger.info("File:%s" %headers["file"])
        log_message("Import of file [%s] started." % headers["file"])
    
    xlsbytes = base64.b64decode(message)
    f = open('./tmp/excel.xlsx', 'wb')
    f.write(xlsbytes)
    f.close()

    #xlsdf=pd.read_excel('./tmp/excel.xlsx', index_col=None)    
    #xlsname=headers["file"]

    df = None
    lot=0


    if headers["file"].find("lot6")==0:
        lot=6
    elif headers["file"].find("lot5")==0:
        lot=5
    elif headers["file"].find("lot7")==0:
        lot=7        
    else:
        return

    if lot==6:
        logger.info(">>> USING compute_status7")
        compute_status=compute_status7
    else:
        logger.info(">>> USING compute_status5")
        compute_status=compute_status5

    ext=""
    if headers["file"].find("pa")>0:
        ext="_pa"
    elif headers["file"].find("mt")>0:
        ext="_mt"

    logger.info("******************")    
    logger.info("LOT:"+str(lot))
    logger.info("******************")



    try:
        es_index="biac_instawork"       
        #logger.info("Deleting index biac_instawork")
        #res=es.indices.delete(index=es_index, ignore=[400, 404]) 
        #logger.info(res)
        bulkbody = ''
        bulkres = ''
        reserrors= []
        

        df = pd.read_excel('./tmp/excel.xlsx')

#        df.columns=['Order', 'Asset_Description', 'Order_Description',
#                'Order_Type', 'Original_Execution_Date', 'Execution_Date',
#                'Finish_Date', 'Planning_Reason', 'Origin', 'Action', 'Note']

        df.columns=['Order', 'Asset_Description', 'Order_Description',
                'Order_Type', 'Reference_Date', 'Unused_Date',
                'Finish_Date', 'Rescheduling_Reason', 'Origin', 'Action', 'Note']
 

        df['Lot'] =  lot

        deletequery=""
        if lot==6 or lot==7:
            deletequery='{"size":10000,"query":{"bool":{"must":[{"term":{"Lot":{"value":"'+str(lot)+'"}}}]}},"_source":false}'
        elif lot==5:
            deletequery='{"size":10000,"query":{"bool":{"must":[{"term":{"Lot":{"value":"'+str(lot)+'"}}}, {"term":{"Ext":{"value":"'+ext.strip("_")+'"}}} ]}},"_source":false}'
        logger.info(deletequery)

        try:
            resdelete=es.delete_by_query(body=deletequery,index="biac_instawork")
            logger.info(">>>>>>>>>>>> DELETE")
            logger.info(resdelete)
        except Exception as e3:            
            logger.error(e3)   
            logger.error("Unable to delete records.")            

        logger.info("Sleeping 5 seconds")
        time.sleep(5)


        #print(df[['Reference_Date','Order']])
#        print(df.dtypes)
        #for i,x in df[['Reference_Date','Order']].iterrows():
        #    print(x)
         #   pd.to_datetime(x['Reference_Date'],dayfirst=True)
        df['Ext']=ext.strip("_")
        df['Reference_Date'] =  pd.to_datetime(df['Reference_Date'],dayfirst=True,errors='coerce')
        df['Unused_Date'] =  pd.to_datetime(df['Unused_Date'],dayfirst=True,errors='coerce')
        df['Finish_Date'] =  pd.to_datetime(df['Finish_Date'],dayfirst=True,errors='coerce')

#        df['Reference_Date2']=df['Reference_Date']+ pd.DateOffset(hours=1)
        df['Reference_Date']=df['Reference_Date'].dt.tz_localize(tz='Europe/Paris')
        df['Unused_Date']=df['Unused_Date'].dt.tz_localize(tz='Europe/Paris')
        df['Finish_Date']=df['Finish_Date'].dt.tz_localize(tz='Europe/Paris')

        df['Original_Order_Type']=df['Order_Type']
        df['Order_Type']=df['Order_Type'].apply(lambda x: x if x=="PM" else "CM")
        
        df["Status"]=df.apply(compute_status, axis=1)

        df['json'] = df.apply(lambda x: x.to_json(date_format="iso"), axis=1)
        #df['Reference_Date'].tz_convert('Europe/Paris')


        for index, row in df.iterrows():
            
            try:
                if "DGS" in str(row["Asset_Description"]):
                    logger.info("Skipping "+row["Asset_Description"])
                    continue
            except:
                logger.error("Unable to read.",exc_info=True)

            action = {}
            action["index"] = {"_index": es_index,
                "_type": "doc","_id":str(lot)+"_"+str(row["Order"])+ext}
            newrec=row["json"]
            bulkbody += json.dumps(action)+"\r\n"
            bulkbody += newrec + "\r\n"


            if len(bulkbody) > 512000:
                logger.info("BULK READY:" + str(len(bulkbody)))
                #logger.info(bulkbody)
                bulkres = es.bulk(bulkbody, request_timeout=30)
        #        bulkres={}
                logger.info("BULK DONE")
                bulkbody = ''
                if(not(bulkres["errors"])):
                    logger.info("BULK done without errors.")
                else:

                    for item in bulkres["items"]:
                        if "error" in item["index"]:
                            #imported_records -= 1
                            logger.info(item["index"]["error"])
                            reserrors.append(
                                {"error": item["index"]["error"], "id": item["index"]["_id"]})
            
        if len(bulkbody) > 0:
            logger.info("BULK READY FINAL:" + str(len(bulkbody)))
            bulkres = es.bulk(bulkbody)
            #logger.info(bulkbody)
            logger.info("BULK DONE FINAL")
            if(not(bulkres["errors"])):
                logger.info("BULK done without errors.")
            else:
                for item in bulkres["items"]:
                    if "error" in item["index"]:
                        #imported_records -= 1
                        logger.info(item["index"]["error"])
                        reserrors.append(
                            {"error": item["index"]["error"], "id": item["index"]["_id"]})
        time.sleep(5)
        conn.send_message('/topic/BIAC_INSTAWORK_IMPORTED', '{"lot":'+str(lot)+'}');
    except Exception as e:
        endtime = time.time()
        logger.error(e,exc_info=True)
        log_message("Import of file [%s] failed. Duration: %d Exception: %s." % (headers["file"],(endtime-starttime),str(e)))        

    if len(reserrors)>0:
        log_message("Import of file [%s] failed. Duration: %d. %d records were not imported." % (headers["file"],(endtime-starttime),len(reserrors)))        

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
                    ,"login":os.environ["AMQC_LOGIN"],"password":os.environ["AMQC_PASSWORD"]}

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

    curday="NA"

    logger.info("AMQC_URL          :"+os.environ["AMQC_URL"])
    while True:
        time.sleep(5)


        try:            
            variables={"platform":"_/_".join(platform.uname()),"icon":"wrench"}
            conn.send_life_sign(variables=variables)

            try:
                newday=datetime.now().strftime("%Y-%m-%d")
                if newday !=curday and datetime.now().hour>=8:
                    logger.info("Creating insta histo")
                    logger.info("====================")
                    curday=newday
                    reind={
                        "source": {
                            "index": "biac_instawork"
                        },
                        "dest": {
                            "index": "biac_histo_instawork"
                        }
                        ,
                        "script": {
                            "source": "ctx._source.histo_date='"+curday+"';ctx._id='"+curday+"_'+ctx._id",
                            "lang": "painless"
                        }
                        }
                    logger.info(reind)
                    
                    result = es.reindex(reind, wait_for_completion=True, request_timeout=300)
                    logger.info(result)
            except:
                logger.error("Unable to reindex instawork hist",exc_info=True)

        except Exception as e:
            logger.error("Unable to send life sign.")
            logger.error(e)
    #app.run(threaded=True,host= '0.0.0.0')
