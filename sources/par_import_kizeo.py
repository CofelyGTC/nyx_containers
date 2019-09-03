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

from pytz import timezone

from functools import wraps
from datetime import datetime
from datetime import timedelta
from dateutil.parser import parse
from lib import pandastoelastic as pte
from lib import elastictopandas as etp
from amqstompclient import amqstompclient
from logging.handlers import TimedRotatingFileHandler
from logstash_async.handler import AsynchronousLogstashHandler
from elasticsearch import Elasticsearch as ES, RequestsHttpConnection as RC



MODULE  = "KIZEO_IMPORTER"
VERSION = "0.0.25"
QUEUE   = ["KIZEO_IMPORT"]

localtz = timezone('Europe/Paris')

def extractUser(hist):
    foundname="NA"
    state="NA"
    historynumber=0
    lastline=hist.split("\n")[-1:][0]
    historynumber=len(hist.split("\n"))

    finda=lastline.find("à")

    if finda !=-1:
        findle=lastline.find(" le ",finda)
        if findle !=-1:
            foundname=lastline[finda+2:findle].strip(" ")
            state="Underway"
        else:
            foundname="NA"

    else:
        findpar=lastline.find(" par ")
        if findpar !=-1:
            findle=lastline.find(" le ",findpar)
            if findle !=-1:
                foundname=lastline[findpar+5:findle].strip(" ")
                state="Closed"
            else:
                foundname="NA"
    
    return {"final_user":foundname,"wo_state":state,"number_of_history":historynumber}

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



def get_building_from_equipment(equipmentValue):

    equipmentValue = equipmentValue.upper()
    
    rslt = re.search("^[A-Z]{2,3}(?=\.)", equipmentValue)
    if rslt:
        return rslt.group(0)
    else:
        return "N/A"


def get_floor_from_equipment(equipmentValue):
    regex_list = [
        "(?<=\.)[0-9-]{1}[0-9]{1}(?=[A-Z]{1}[0-9]{3})", # LOCAL : .12G110 / .-1E240
        "(?<=\.)[0-9-]{1}[0-9]{1}(?=[A-Z]{1})",         # ETAGE ZONE : .12G / .-1E
        "(?<=\.[A-Z]){1}[0-9]{3}(?=\.)",                # ESCALIER1 : .E650. / .J620.
        "(?<=\.[A-Z]{1})[A-Z]{2}[0-9]{1}(?=\.)",        # ESCALIER2 : .ACV6. / .BCV4.
        "(?<=\.)[0-9-]{2}(?=\.)",                       # ETAGE : .-1. / .-5.
        "(?<=\.)[P]{1}[0-9]{1}(?=\.)"                   # ETAGE : .P1. / .P5.
    ]
    
    equipmentValue = equipmentValue.upper()
    
    gotMatch = False
    for regex in regex_list:
        rslt = re.search(regex, equipmentValue)
        if rslt:
             gotMatch = True
             break

    if gotMatch:
        return rslt.group(0)
    else:
        return "N/A"

def get_location_from_equipment(equipmentValue):
    regex_list = [
        "(?<=\.)[0-9-]{1}[0-9]{1}[A-Z]{1}[0-9]{3}",     # LOCAL : .12G110 / .-1E240
        "(?<=\.)[0-9-]{1}[0-9]{1}[A-Z]{1}",             # ETAGE ZONE : .12G / .-1E
        "(?<=\.)[A-Z]{1}[0-9]{3}(?=\.)",                # ESCALIER1 : .E650. / .J620.
        "(?<=\.)[A-Z]{1}[A-Z]{2}[0-9]{1}(?=\.)",        # ESCALIER2 : .ACV6. / .BCV4.
        "(?<=\.)[A-Z]{1}(?=\.)",                        # ZONE : .B. / .G.
        "(?<=\.)NORD|SUD|EXT(?=\.)",                    # ZONE2 : .NORD. / .SUD. / .EXT.
        "(?<=\.)[0-9-]{2}(?=\.)",                       # ETAGE : .-1. / .-5.
        "(?<=\.)[P]{1}[0-9]{1}(?=\.)"                   # ETAGE : .P1. / .P5.
    ]
    
    equipmentValue = equipmentValue.upper()
    
    gotMatch = False
    for regex in regex_list:
        rslt = re.search(regex, equipmentValue)
        if rslt:
             gotMatch = True
             break

    if gotMatch:
        return rslt.group(0)
    else:
        return "N/A"

def get_zone_from_equipment(equipmentValue):
    regex_list = [
        "(?<=\.[0-9-]{1}[0-9]{1})[A-Z]{1}(?=[0-9]{3})", # LOCAL : .12G110 / .-1E240
        "(?<=\.[0-9-]{1}[0-9]{1})[A-Z]{1}",             # ETAGE ZONE : .12G / .-1E
        "(?<=\.)[A-Z]{1}(?=[0-9]{3}\.)",                # ESCALIER1 : .E650. / .J620.
        "(?<=\.)[A-Z]{1}(?=[A-Z]{2}[0-9]{1}\.)",        # ESCALIER2 : .ACV6. / .BCV4.
        "(?<=\.)[A-Z]{1}(?=\.)"                         # ZONE : .B. / .G.
    ]
    
    equipmentValue = equipmentValue.upper()
    
    gotMatch = False
    for regex in regex_list:
        rslt = re.search(regex, equipmentValue)
        if rslt:
             gotMatch = True
             break

    if gotMatch:
        return rslt.group(0)
    else:
        return "N/A"

def compute_history(ot_number, history):
    bulk_ret = ''

    for i in history.split('\n'):
        #print(i)

        obj = {
            'history': i,
            'ot_number': ot_number
        }

        mode_type = i.split(' ')[0].lower()

        if mode_type == 'transféré':
            obj['type'] = 'transfer'

            regex = ".*par (.*) à (.*) le (.*)"

            m=re.match(regex, i)
            if m:
           #     print(m.group(1))
            #    print(m.group(2))
             #   print(m.group(3))

                obj['user'] = m.group(1).lower()
                obj['to']   = m.group(2).lower()
                obj['datetime'] = m.group(3)

        elif mode_type == 'enregistré':
            obj['type'] = 'save'

            regex = ".*par (.*) le (.*)"

            m=re.match(regex, i)
            if m:
         #       print(m.group(1))
          #      print(m.group(2))

                obj['user'] = m.group(1).lower()
                obj['datetime'] = m.group(2)

        else:
            logger.info('other type:'+mode_type)



        if 'datetime' in obj:        
            ts = int(localtz.localize(datetime.strptime(obj['datetime'], '%Y-%m-%d %H:%M:%S')).timestamp())
            obj['datetime'] = localtz.localize(datetime.strptime(obj['datetime'], '%Y-%m-%d %H:%M:%S')).isoformat()

            action = {}
            action['index'] = {"_index": "parl_history_kizeo",
                "_type": "doc", "_id": str(ot_number)+'_'+str(ts)}

            bulk_ret+=json.dumps(action)+"\r\n"
            bulk_ret+=json.dumps(obj, ensure_ascii=False)+"\r\n"
            
    return bulk_ret

def delete_es_records(es, df_id_kizeo, start_dt, end_dt):
    df_id_es=etp.genericIntervalSearch(es,'parl_kizeo',start=start_dt,end=end_dt,timestampfield="update_time_dt",\
                          _source=['_id'])
    
    if len(df_id_es) > 0:
        df_merge=df_id_kizeo.merge(df_id_es, how='right', on='_id')
        df_to_delete=df_merge[df_merge.isnull().T.any()]
        
        logger.info('id to delete: '+str(len(df_to_delete)))
        if len(df_to_delete) > 0:
            bulkbody = ''
            
        
            for i in df_to_delete['_id']:
                logger.info('id to delete:'+str(i))
                bulkbody+= json.dumps({"delete" : { "_index" : "parl_kizeo", "_type": "doc", "_id" : str(i) } })+'\r\n'

            bulkres=es.bulk(bulkbody)


def handle_autocontrole(str_autocontrole):
    #print(str_autocontrole)
    try:
        return localtz.localize(datetime.strptime(str_autocontrole, '%Y-%m-%d %H:%M')).isoformat()
    except:
        try:
            return localtz.localize(datetime.strptime(str_autocontrole, '%Y-%m-%d')).isoformat()
        except:
            flag=True
            #print('failed to parse')
        
        
    return None

def computeEquipementRecord(eqp):
    split=eqp.split(".")
    if len(split)==3:
        return {"comp_building":split[0],"comp_equipment":split[1],"comp_team":split[2]}
    if len(split)==2:
        return {"comp_building":split[0],"comp_equipment":split[1]}
    else:
        return {}


################################################################################
def messageReceived(destination,message,headers):
    global es
    logger.info("==> "*10)
    logger.info("Message Received %s" % destination)
    logger.info(message)
    logger.info(headers)

    mesobj=json.loads(message)
    
    if 'days_to_resync' in mesobj:
        days_to_resync = mesobj['days_to_resync']

        logger.info('days_to_resync: '+str(days_to_resync))
        importKizeo(days_to_resync = days_to_resync)


def importKizeo(days_to_resync = 10):
    logger.info('=====> IMPORT KIZEO')
    starttime = time.time()
    reserrors=[]

    log_message("Import of Kizeo started.")        

    res=None

    try:
        res=loadKizeo(days_to_resync = days_to_resync)
        #logger.info(res)
    except Exception as e:
        logger.error(e)
        endtime = time.time()    

        log_message("Import of kizeo failed. Duration: %d Exception: %s." % ((endtime-starttime),str(e)))        

    endtime = time.time()    

    if res !=None and res["errors"]:
        log_message("Import of kizeo failed. Duration: %d. Some records were not imported." % ((endtime-starttime)))        

    try:
        log_message("Import of kizeo finished. Duration: %d Records: %d." % ((endtime-starttime),df.shape[0]))         
    except:
        log_message("Import of kizeo finished. Duration: %d." % ((endtime-starttime)))    
    
        

    logger.info("<== "*10)

#################################################
def loadKizeo(days_to_resync = 10):
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
    # logger.info(payload)

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
    logger.info(form_list)

    

    end_dt   = datetime.now()
    start_dt = end_dt-timedelta(days= days_to_resync)


    for i in form_list:
        if i['name'] == '800 PE - OT_CM_PE':
            logger.info("=== FORMS FOUND")
            form_id = i['id']


            postbody={                                        
                'filters': [
                {
                    "field": "_update_time",
                    "operator": ">=",
                    "type": "simple",
                    #"val": "2015-04-10 00:00:00"
                    "val": start_dt.strftime("%Y-%m-%d %H:%M:%S")
                }
                ],
                'order': [{
                    'col': '_id',
                    'type': 'asc'
                    }
                ]
            }
            r = requests.post(url_kizeo + '/forms/'+form_id+'/data/advanced?Authorization='+token,json.dumps(postbody))
            if r.status_code != 200:
                logger.error('Something went wrong...')
                logger.error(r.status_code, r.reason)
                return

            bulkbody=""

            df_id_kizeo = pd.DataFrame(r.json()["data"])[['_id','n_d_ot', '_update_time']].sort_values('_update_time')

            logger.info("Records: %d" %(len(r.json()["data"])))
            for rec in r.json()["data"]:
                #logger.info(rec)
                newkeys={}
                for key in rec:                    
                    if key.find("date")==0 or key.find("_time")>=0:
                        #logger.info(key)
                        try:
                            newkeys[key+"_dt"]=parse(rec[key]).isoformat()
                            
                        except Exception as e:
                            #logger.info(e,exc_info=True)
                            pass
                            
                rec.update(newkeys)
                rec.update(computeEquipementRecord(rec.get("equipement","")))
                
                es_index = 'parl_kizeo'
                
                action = {}
                action["index"] = {"_index": es_index,
                    "_type": "doc","_id":rec["_id"]}
                                
                
                newrec={}
                for key in rec:
                    newrec[key.strip("_")]=rec[key]

                if "history" in newrec:
                    newrec.update(extractUser(newrec["history"]))
                    bulk_history = compute_history(newrec['n_d_ot'], newrec['history'])

                    if bulk_history != '':
                        bulkbody += bulk_history

                else:
                    logger.info("*="*100)
                    logger.info(rec)

                
                if 'equipement' in newrec:
                    newrec['floor']    = get_floor_from_equipment(newrec['equipement'])
                    newrec['building'] = get_building_from_equipment(newrec['equipement'])
                    newrec['location'] = get_location_from_equipment(newrec['equipement'])
                    newrec['zone']     = get_zone_from_equipment(newrec['equipement'])

                if 'debut_autocontole' in newrec:
                    newrec['debut_autocontrole'] = newrec.pop('debut_autocontole')

                    
                for j in ['debut_autocontrole', 'fin_autocontrole']:    
                    if j in newrec:
                        newrec[j] = handle_autocontrole(newrec[j])
                
                        if newrec[j] is None:
                            del newrec[j]




                date_fields= ["date_de_creation_new__dt", "date_limite_d_intervention_dt", "update_answer_time_dt", "date_limite_de_depannage_dt", "date_limite_de_reparation_dt", 
                "create_time_dt", "update_time_dt", "pull_time_dt"]

                for field in date_fields:
                    if field in newrec:
                        try: 
                            newrec[field] = localtz.localize(datetime.strptime(newrec[field], "%Y-%m-%dT%H:%M:%S")).isoformat()
                        except: 
                            logger.info('unable to localize this field: '+field)
                            logger.info('unable to localize this field: '+newrec[field])
                            logger.info('unable to localize this field: '+str(type(newrec[field])))

                bulkbody += json.dumps(action)+"\r\n"
                bulkbody += json.dumps(newrec) + "\r\n"
                
                
                if len(bulkbody) > 512000:
                    bulkres=es.bulk(bulkbody)
                    bulkbody=''
                    if(not(bulkres["errors"])):
                        logger.info("BULK done without errors.")
                    else:
                        for item in bulkres["items"]:
                            if "error" in item["index"]:
                                logger.info(item["index"]["error"])
                            
            #print(bulkbody)
            if len(bulkbody) > 0:
                bulkres=es.bulk(bulkbody)
                if(not(bulkres["errors"])):
                    logger.info("BULK done without errors.")
                else:
                    for item in bulkres["items"]:
                        if "error" in item["index"]:
                            logger.info(item["index"]["error"])



            time.sleep(5)

            delete_es_records(es, df_id_kizeo, start_dt, end_dt)

            return bulkres

#           




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

    SECONDSBETWEENCHECKS=240

    nextload=datetime.now()

    while True:
        time.sleep(5)
        try:            
            variables={"platform":"_/_".join(platform.uname()),"icon":"list-alt"}
            
            conn.send_life_sign(variables=variables)

            if (datetime.now() > nextload):
                try:
                    nextload=datetime.now()+timedelta(seconds=SECONDSBETWEENCHECKS)
                    importKizeo()
                except Exception as e2:
                    logger.error("Unable to load kizeo.")
                    logger.error(e2,exc_info=True)
            
        except Exception as e:
            logger.error("Unable to send life sign.")
            logger.error(e)
    #app.run(threaded=True,host= '0.0.0.0')
