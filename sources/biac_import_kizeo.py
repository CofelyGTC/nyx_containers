"""
BIAC IMPORT KIZEO
====================================

Sends:
-------------------------------------

* /topic/BIAC_KPI102_IMPORTED
* /topic/BIAC_KIZEO_IMPORTED_2

Listens to:
-------------------------------------

* /queue/KIZEO_IMPORT

Collections:
-------------------------------------

* **biac_kizeo** (Raw Data)
* **biac_month_kizeo** (Computed Data) DEPRECATED
* **biac_month_2_kizeo** (Computed Data)

VERSION HISTORY
===============

* 23 Jul 2019 0.0.19 **VME** Code commented
* 24 Jul 2019 0.0.20 **VME** Modification of screen name to fill the requirements for BACFIR dashboards (Maximo)
* 30 Oct 2019 0.0.21 **VME** Buf fixing r.text empty and better error log.
* 30 Oct 2019 1.0.0  **AMA** Use data get rest api exports_info function to get record ids
* 25 Nov 2019 1.0.1  **VME** change the end date of the message to other container that create monthly collections (replace max of the timestamp by now)
* 09 Dec 2019 1.0.2  **VME** Replacing pte by es_helper
"""  
import re
import sys
import json
import time
import uuid
import pytz
import base64
import tzlocal
import platform
import requests
import traceback
import threading
import os,logging
import numpy as np
import pandas as pd

from functools import wraps
from datetime import datetime
from datetime import timedelta
from elastic_helper import es_helper 
#from amqstompclient import amqstompclient
import amqstomp as amqstompclient
from logging.handlers import TimedRotatingFileHandler
from logstash_async.handler import AsynchronousLogstashHandler
from elasticsearch import Elasticsearch as ES



MODULE  = "BIAC_KIZEO_IMPORTER"
VERSION = "1.1.3"
QUEUE   = ["KIZEO_IMPORT"]

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
def extract_lot_number(str_lot_number):
    """
    Extract lot number. 
    lot3 -> 3

    Parameters
    ----------
    str_lot_number
        a string : lot[0-9]

    """
    tmp = str_lot_number.lower().replace(' ','') 
    m = re.match("lot([0-9])", tmp)
    if m:
        return m.groups()[0]
    
    return None

################################################################################
def extract_contract(row):
    """
    Extract contract from lot and technic. 

    Parameters
    ----------
    row
        a spotcheck record 
    """
    
    lot = int(row['lot'])
    
    if lot == 1:
        return 'BACHEA'
    if lot == 3:
        return 'BACEXT'
    if lot == 4:
        return 'BACDNB'

    if lot == 2:
        tmp = str(row['technic']).lower()
        
        if tmp == 'acces':
            return 'BACFIR'
        
        if tmp == 'fire fighting':
            return 'BACFIR'
        
        if tmp == 'cradle':
            return 'BACFIR'
        
        if tmp == 'sanitair':
            return 'BACSAN'
        
        if 'hvac' in tmp:
            return 'BACHVA'
        
        if 'hvac pa' in tmp:
            return 'BACSAN'
        
        if tmp == 'elektriciteit':
            return 'BACELE'
        
        
    
    return None

################################################################################
def extract_screen_name(row):
    """
    Extract screen name from lot and technic. 

    Parameters
    ----------
    row
        a spotcheck record 

    """
    
    lot = int(row['lot'])
    
    if lot == 1:
        return 'heating'
    if lot == 3:
        return 'ext_building'
    if lot == 4:
        return 'dnb'

    if lot == 2:
        tmp = str(row['technic']).lower()

        if tmp == 'acces':
            return 'access'
            
        if tmp == 'fire fighting':
            return 'fire'
            
        if tmp == 'cradle':
            return 'cradle'
            
        if tmp == 'sanitair':
            return 'sani'
        
        if 'hvac' in tmp:
            return 'hvacpb'
        
        if 'hvac pa' in tmp:
            return 'hvacpa'
        
        if tmp == 'elektriciteit':
            return 'elec'
        
        if 'hvac pb' in tmp:
            return 'hvacpb'
    
    return None

################################################################################
def clean_technic(str_tec):
    """
    clean technic string. 

    Parameters
    ----------
    str_tec
        the technic to clean
         

    """
    str_tec = str_tec.lower()
    if 'hvac' in str_tec:
            return 'hvac pb'  
    if 'hvac pa' in str_tec:
            return 'hvac pa'    
        
    if 'hvac pb' in str_tec:
        return 'hvac pb'

    if 'gondels' in str_tec:
        return 'cradle'
    return str_tec

################################################################################
def create_default_records(es): 
    """
    Create default record (01JAN2019) 0/0 100% in biac_month_kizeo. 

    Parameters
    ----------
    es
        elasticsearch connection 

    """
    
    kpi          = ['201', '202', '204', '205', '207', '208', '216', '217', '303']
    lot          = ['1', '2', '3', '4']
    technic      = ['acces', 'fire fighting', 'cradle', 'dnb', 'sanitair', 'hvac pa', 'elektriciteit', 'hvac pb', 'hvac']
    
    df_kpi       = pd.DataFrame({'kpi': kpi, 'merge': 1})
    df_lot       = pd.DataFrame({'lot': lot, 'merge': 1})
    df_tec       = pd.DataFrame({'technic': technic, 'merge': 1})
    
    df_all = df_tec.merge(df_lot, how='outer')
    df_all = df_all.merge(df_kpi, how='outer')
    del df_all['merge']

    df_all['screen_name'] = df_all.apply(extract_screen_name, axis=1)
    df_all['contract'] = df_all.apply(extract_contract, axis=1)
    df_all['check_conform'] = 0
    df_all['check_no_conform'] = 0
    df_all['check_number'] = 0
    df_all['computation_period'] = 'W1'
    df_all['computation_period_fr'] = 'S1'
    df_all['percentage_conform'] = 100
    df_all['percentage_no_conform'] = 0
    df_all['last_update'] = datetime(2019, 1, 1, 12).timestamp()*1000


    df_all['_index'] = 'biac_month_kizeo'
    df_all['_id'] = df_all.apply(lambda row: row['lot'] + '_' + str(row['kpi']) + '_' + str(row['screen_name']), axis=1)

    del df_all['technic']
    es_helper.dataframe_to_elastic(es, df_all)





def create_default_records_2(es): 
    """
    Create default record (01JAN2019) 0/0 100% in biac_month_2_kizeo. 

    Parameters
    ----------
    es
        elasticsearch connection 

    """
    logger.info('^¨'*80)
    logger.info('create_default_records_2')
    
    kpi          = ['201', '202', '203', '204', '205', '207', '208', '209', '210', '211', '213', '216', '217', '303']
    lot          = ['1', '2', '3', '4']
    technic      = ['acces', 'fire fighting', 'cradle', 'dnb', 'sanitair', 'hvac pa', 'elektriciteit', 'hvac pb', 'hvac']
    
    df_kpi       = pd.DataFrame({'kpi': kpi, 'merge': 1})
    df_lot       = pd.DataFrame({'lot': lot, 'merge': 1})
    df_tec       = pd.DataFrame({'technic': technic, 'merge': 1})
    
    df_all = df_tec.merge(df_lot, how='outer')
    df_all = df_all.merge(df_kpi, how='outer')
    del df_all['merge']

    df_all['screen_name'] = df_all.apply(extract_screen_name, axis=1)
    df_all['contract'] = df_all.apply(extract_contract, axis=1)
    df_all['check_conform'] = 0
    df_all['check_no_conform'] = 0
    df_all['check_number'] = 0
    df_all['month'] = '2019-01'
    
    df_all['percentage_conform'] = 100
    df_all['percentage_no_conform'] = 0
    local_timezone = tzlocal.get_localzone()

    
    df_all['last_update'] = pytz.timezone(str(local_timezone)).localize(datetime(2019, 1, 1))
    #local_timezone.localize(datetime(2019, 1, 1, 0, 0, 0, 0))
    
    df_all['_index'] = 'biac_month_2_kizeo'
    df_all['_id'] = df_all.apply(lambda row: row['lot'] + '_' + str(row['kpi']) + '_' + str(row['screen_name']), axis=1)
    del df_all['technic']

    es_helper.dataframe_to_elastic(es, df_all)

################################################################################
def messageReceived(destination,message,headers):
    global es
    logger.info("==> "*10)
    logger.info("Message Received %s" % destination)
    logger.info(headers)

    xlsbytes = base64.b64decode(message)
    f = open('./tmp/excel.xlsx', 'wb')
    f.write(xlsbytes)
    f.close()

    importKizeo(headers)


def importKizeo(headers):
    """
    read an excel (./tmp/excel.xlsx) and :
        - delete data in biac_kizeo based on the time range of the excel 
        - insert data in index biac_kizeo. 

    Parameters
    ----------
    headers
        the file name is stored in headers 

    """
    logger.info('=====> IMPORT KIZEO')
    starttime = time.time()
    reserrors=[]

    if "CamelSplitAttachmentId" in headers:
        headers["file"] = headers["CamelSplitAttachmentId"]

    if "file" in headers:
        logger.info("File:%s" %headers["file"])
        log_message("Import of file [%s] started." % headers["file"])


    df = None
    try:        

        create_default_records(es)
        create_default_records_2(es)
        file_name = headers["file"]

        logger.info('*******'*100)
        logger.info('openingexcel'*100)
        df = pd.read_excel('./tmp/excel.xlsx')
        logger.info(df.columns)
        logger.info(df)

        df.columns = ['record_number', 'lot_nummer', 'kpi', 'date', 'time', 'control_by',
            'spot_check', 'fire_scenario', 'emergency_power_test',
            'building_lot_1', 'floor_lot_1', 'building_lot_2', 'floor_lot_2',
            'building_lot_3', 'floor_lot_3', 'technic',
            'installation_type_sfb_201', 'installation_type_sfb_202',
            'installation_type_sfb_204', 'installation_type_sfb_205',
            'installation_type_sfb_207', 'installation_type_sfb_208',
             'check_explanation',
            'check_number', 'check_conform', 'check_no_conform',
            'percentage_conform']

        df['lot'] = df['lot_nummer'].apply(extract_lot_number)
        df['technic'] = df['technic'].apply(clean_technic)
        df['contract'] = df.apply(extract_contract, axis=1)
        df['screen_name'] = df.apply(extract_screen_name, axis=1)

        # combining datetime.date and datetime.time objects
        df['@timestamp'] = pd.to_datetime(df['date'].dt.strftime('%Y-%m-%d') + df['time'].astype(str), format = '%Y-%m-%d%H:%M:%S')

        df[['spot_check', 'fire_scenario', 'emergency_power_test']]=df[['spot_check', 'fire_scenario', 'emergency_power_test']].fillna('No')


        df[['building_lot_1', 'floor_lot_1', 'building_lot_2', 'floor_lot_2',
            'building_lot_3', 'floor_lot_3', 'technic',
            'installation_type_sfb_201', 'installation_type_sfb_202',
            'installation_type_sfb_204', 'installation_type_sfb_205',
            'installation_type_sfb_207', 'installation_type_sfb_208',
             'check_explanation']]= \
            df[['building_lot_1', 'floor_lot_1', 'building_lot_2', 'floor_lot_2',
                'building_lot_3', 'floor_lot_3', 'technic',
                'installation_type_sfb_201', 'installation_type_sfb_202',
                'installation_type_sfb_204', 'installation_type_sfb_205',
                'installation_type_sfb_207', 'installation_type_sfb_208',
                 'check_explanation']].fillna('')

        df[['check_number', 'check_conform', 'check_no_conform']]=df[['check_number', 'check_conform', 'check_no_conform']].fillna(0)


        del df['date']
        del df['time']
        
        df['filename'] = file_name
        df['importer'] = MODULE
        df['version']  = VERSION

        first_kizeo_ts = df['@timestamp'].min()
        last_kizeo_ts = df['@timestamp'].max()
        obj = {
                'start_ts': int(datetime(2019, 1, 1).timestamp()),
                'end_ts': int(datetime.now().timestamp())
            }

        logger.info(obj)

        deletequery={
            "query":{
                "range" : {
                    "@timestamp" : {
                    "gte" : int(first_kizeo_ts.timestamp())*1000,
                    "lte" : int(last_kizeo_ts.timestamp())*1000
                    }
                }
            }        
        }
        logger.info("Deleting records")
        try:
            resdelete=es.delete_by_query(body=deletequery,index="biac_kizeo")
            logger.info(resdelete)
        except Exception as e3:            
            logger.error(e3)   
            logger.error("Unable to delete records.")            

        
        logger.info("Sleeping 5 seconds")
        time.sleep(5)

        bulkbody = ''
        bulkres = ''

        for index, row in df.iterrows():
            es_index = 'biac_kizeo'
            #es_id    = row['lot']+'_'+str(row['kpi'])+'_'+str(int(row['@timestamp'].timestamp()*1000))
            action = {}
            action["index"] = {"_index": es_index}
            
            #action["index"] = {"_index": es_index,
            #    "_type": "doc", "_id": es_id}
            
            newrec = json.loads(row.to_json())
            
            bulkbody += json.dumps(action)+"\r\n"
            bulkbody += json.dumps(newrec) + "\r\n"
            

            if len(bulkbody) > 512000:
                logger.info("BULK READY:" + str(len(bulkbody)))
                bulkres = es.bulk(body=bulkbody, request_timeout=30)
                logger.info("BULK DONE")
                bulkbody = ''
                if(not(bulkres["errors"])):
                    logger.info("BULK done without errors.")
                else:

                    for item in bulkres["items"]:
                        if "error" in item["index"]:
                            logger.info(item["index"]["error"])
                            reserrors.append(
                                {"error": item["index"]["error"], "id": item["index"]["_id"]})




        if len(bulkbody) > 0:
            logger.info("BULK READY FINAL:" + str(len(bulkbody)))
            bulkres = es.bulk(body=bulkbody)
            logger.info("BULK DONE FINAL")
            if(not(bulkres["errors"])):
                logger.info("BULK done without errors.")
            else:
                for item in bulkres["items"]:
                    if "error" in item["index"]:
                        print(item["index"]["error"])
                        logger.info(item["index"]["error"])
                        reserrors.append(
                            {"error": item["index"]["error"], "id": item["index"]["_id"]})

        

        logger.info('sending message to /topic/BIAC_KIZEO_IMPORTED')
        logger.info(obj)




        conn.send_message('/topic/BIAC_KIZEO_IMPORTED', json.dumps(obj))
        conn.send_message('/topic/BIAC_KIZEO_IMPORTED_2', json.dumps(obj))
    except Exception as e:
        endtime = time.time()



        
        exc_type, exc_value, exc_traceback = sys.exc_info()
        traceback.print_tb(exc_traceback, limit=1, file=sys.stdout)
        # exc_type below is ignored on 3.5 and later
        traceback.print_exception(exc_type, exc_value, exc_traceback,
                              limit=2, file=sys.stdout)



        log_message("Import of file [%s] failed. Duration: %d Exception: %s." % (headers["file"],(endtime-starttime),str(e)))        

    if len(reserrors)>0:
        log_message("Import of file [%s] failed. Duration: %d. %d records were not imported." % (headers["file"],(endtime-starttime),len(reserrors)))        

    endtime = time.time()    
    try:
        log_message("Import of file [%s] finished. Duration: %d Records: %d." % (headers["file"],(endtime-starttime),df.shape[0]))         
    except:
        log_message("Import of file [%s] finished. Duration: %d." % (headers["file"],(endtime-starttime)))    
    
        

    logger.info("<== "*10)

#################################################
def loadKizeo():
    """
    Reach the kizeo Rest API and retrieve data from 01/01/2019 to now + 30 days. 
    Save data in ./tmp/excel.xlsx
    
    """
    logger.info(">>> LOADING KIZEO")
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

    for i in form_list:
        if i['name'] == 'SPOTCHECK ~ Loten 1-2-3 (KPI200 & KPI300)':
            logger.info("=== SPOTCHECK FOUND")
            form_id = i['id']

            start=(datetime.now()+timedelta(days=30)).strftime("%Y-%m-%d %H:%M:%S")
            logger.info("Start %s" %(start))            
            end=datetime(2019, 1, 1).strftime("%Y-%m-%d %H:%M:%S")
            logger.info("End %s" %(end))
            #post={"onlyFinished":False,"startDateTime":start,"endDateTime":end,"filters":[]}
            
            #1r = requests.post(url_kizeo + '/forms/' + form_id + '/data/exports_info?Authorization='+token,post)
            r = requests.get(url_kizeo + '/forms/' + form_id + '/data/exports_info?Authorization='+token)


            if r.status_code != 200:
                logger.error('something went wrong...')
                logger.error(r.status_code, r.reason)
            elif r.text == '':
                logger.info('Empty response')
            else:
            #    logger.info(r.json())

                #ids=r.json()['data']["dataIds"]
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
                    
                    file = open("./tmp/excel.xlsx", "wb")
                    file.write(r.content)
                    file.close()

                    importKizeo({"file":"RestAPI"})





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
        host_params=os.environ["ELK_URL"]
        es = ES([host_params], http_auth=(os.environ["ELK_LOGIN"], os.environ["ELK_PASSWORD"]), verify_certs=False)
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
                    loadKizeo()
                except Exception as e2:
                    logger.error("Unable to load kizeo.")
                    logger.error(e2,exc_info=True)
            
        except Exception as e:
            logger.error("Unable to send life sign.")
            logger.error(e)
    #app.run(threaded=True,host= '0.0.0.0')
