import re
import sys

import json
import time
import uuid
import json
import base64
import platform
import traceback
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


import tzlocal # $ pip install tzlocal


VERSION="0.0.4"
MODULE="PAR_IMPORT_COSWIN"
QUEUE=["COSWIN_IMPORT"]

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

def compute_type_inf(date_diff):
    if date_diff < 0:
        return 'hors délai'
    if date_diff <= 14:
        return 'délai <= 14J'
    if date_diff <= 21:
        return 'délai <= 21J'
    if date_diff <= 41:
        return 'délai <= 41J'

    return 'planifié'

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

def create_summary_records(df):

    es.indices.delete(index="parl_summary_coswin", ignore=[400, 404])

    array = []
    technics = ['ELE', 'HVA', 'INC', 'SAN']

    today = datetime.now().date()

    seven_week_ago = today - timedelta(days=7*7)
    three_week_from_now = today + timedelta(days=3*7)

    for tec in technics:
        for i in pd.date_range(seven_week_ago, three_week_from_now, freq="1w").date:
            week_field=str(i.year)+' S'+str(i.isocalendar()[1]).zfill(2)

            obj = {
                'week_field'            : week_field,
                'equipment_function_fx' : tec,
                'value_count'           : df[(df['equipment_function_fx']==tec) & (df['user_status']=='CREE') & 
                                            (df['job_type'].str.contains('PRE.*A')) & (df['week_field']==week_field)].shape[0],
                'user_status'           : 'CREE',
                'type'                  : 'summary_planning'
            }

            array.append(obj)

            obj = {
                'week_field'            : week_field,
                'equipment_function_fx' : tec,
                'value_count'           : df[(df['equipment_function_fx']==tec) & (df['user_status']!='CREE') & 
                                            (df['job_type'].str.contains('PRE.*A')) & (df['week_field']==week_field)].shape[0],
                'user_status'           : 'TERM',
                'type'                  : 'summary_planning'
            }

            array.append(obj)
        
    df_summary_planning=pd.DataFrame(array)
    df_summary_planning['_id'] = df_summary_planning['equipment_function_fx']+'-'+df_summary_planning['user_status']+'-'+df_summary_planning['week_field']
    df_summary_planning['_index'] = 'parl_summary_coswin'
    pte.pandas_to_elastic(es, df_summary_planning)

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
    

    logger.info(message[:1000])

    xlsbytes = base64.b64decode(message)
    f = open('./tmp/excel.xlsx', 'wb')
    f.write(xlsbytes)
    f.close()

    #xlsdf=pd.read_excel('./tmp/excel.xlsx', index_col=None)    
    #xlsname=headers["file"]

    df = None
    

    try:
        
        bulkbody = ''
        bulkres = ''
        reserrors= []
        

        df = pd.read_excel('./tmp/excel.xlsx')


        df['_id']    = df['WORK_ORDER']
        df['_index'] = 'parl_coswin'


        date_columns = ['REPORT_DATE', 'CREATION_DATE', 'SCHEDULE_DATE', 'TARGET_DATE', 'LAST_UPDATE', 
                        'START_DATE', 'END_DATE', 'SEF_date_start-feedback', 'SEF_date_end-feedback', 
                        'SEF_date_status-TERM', 'FX_target-date']
        for i in date_columns:
            df[i] = pd.to_datetime(df[i]).dt.tz_localize('Europe/Paris')

        for i in df.columns:
            if i not in date_columns:
                df[i] = df[i].fillna('')


        today = datetime.now().date()


        df['target_date_day'] = df['TARGET_DATE'].dt.date
        df['date_diff'] = (df['target_date_day'] - today).dt.days
        df['type_inf'] = df['date_diff'].apply(lambda x: compute_type_inf(x))
        del df['target_date_day']
        df['week_field'] = df['SCHEDULE_DATE'].apply(lambda x: str(x.year)+' S'+str(x.week).zfill(2))
            
        df['_timestamp'] = pd.to_datetime(df['CREATION_DATE'])


        columns_to_keep = ['WORK_ORDER', 'USER_STATUS', 'EQUIPMENT', 'EQUIPMENT_DESCRIPTION',
            'SEF_job_description', 'FX_job', 'FX_job_frequency', 'JOB_TYPE',
            'FX_job_class', 'FX_function', 'SUPERVISOR',
            'FX_supervisor', 'EQUIPEMENT_LEVEL', 'ACTION_ENTITY', 'ACTUAL_HOURS',
            'SEF_hours_ENCOUR', 'SEF_hours_FINDEF', 'SEF_hours_SLFCTR',
            'SEF_count_ENCOUR', 'SEF_count_FINDEF', 'SEF_count_SLFCTR',
            'EQUIPEMENT_STATUS', 'PREVIOUS_WORK_ORDER', 'SEF_child_work_orders',
            'FX_priority', 'SEF_job_request_code', 'SEF_employee_allocated',
            'SEF_employee_feedback', 'FX_building', 'FX_floor', 'FX_zone',
            'FX_location', 'LOCATION', 'REPORT_DATE', 'SCHEDULE_DATE',
            'TARGET_DATE', 'FX_target-date', 'CREATOR', 'CREATION_DATE',
            'LAST_UPDATE', 'START_DATE', 'END_DATE', 'SEF_date_start-feedback',
            'SEF_date_end-feedback', 'SEF_date_status-TERM',
            'SEF_date_status-PERINP', 'FX_error', '_id', '_index', 'date_diff',
            'type_inf', 'week_field', '_timestamp']

        column_list = ['WORK_ORDER', 'USER_STATUS', 'EQUIPMENT', 'EQUIPMENT_DESCRIPTION',
            'JOB_DESCRIPTION_FX', 'JOB_FX', 'JOB_FREQUENCY_FX', 'JOB_TYPE',
            'JOB_CLASS_FX', 'EQUIPMENT_FUNCTION_FX', 'SUPERVISOR',
            'SUPERVISOR_FX', 'EQUIPMENT_LEVEL', 'ACTION_ENTITY', 'ACTUAL_HOURS',
            'ACTUAL_HOURS_ENCOUR_FX', 'ACTUAL_HOURS_FINDEF_FX', 'ACTUAL_HOURS_SLFCTR_FX',
            'COUNT_ENCOUR_FX', 'COUNT_FINDEF_FX', 'COUNT_SLFCTR_FX',
            'EQUIPMENT_STATUS', 'WORK_ORDER_PREVIOUS', 'WORK_ORDERS_CHILD_FX',
            'PRIORITY_FX', 'JOB_REQUEST_FX', 'EMPLOYEE_ALLOCATED_FX',
            'EMPLOYEE_FEEDBACK_FX', 'LOCATION_BUILDING_FX', 'LOCATION_FLOOR_FX', 'LOCATION_ZONE_FX',
            'LOCATION_FX', 'LOCATION', 'REPORT_DATE', 'SCHEDULE_DATE',
            'TARGET_DATE', 'TARGET_DATE_FX', 'CREATOR', 'CREATION_DATE',
            'LAST_UPDATE', 'START_DATE', 'END_DATE', 'START_DATE_FX',
            'END_DATE_FX', 'USER_STATUS_TERM_DATE_FX',
            'USER_STATUS_PERINP_DATE_FX', 'ERROR_FX', '_id', '_index', 'date_diff',
            'type_inf', 'week_field', '_timestamp']
        

        if 'SEF_DI_problem' in df.columns:
            columns_to_keep.append('SEF_DI_problem')
            column_list.append('di_problem_fx')

        if 'FEEDBACK_NOTE' in df.columns:
            columns_to_keep.append('FEEDBACK_NOTE')
            column_list.append('feedback_note')


        df=df[columns_to_keep]

        

        column_list=[x.lower() for x in column_list]

        df.columns = column_list

        create_summary_records(df)
        pte.pandas_to_elastic(es, df)
        

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
    while True:
        time.sleep(5)
        try:            
            variables={"platform":"_/_".join(platform.uname()),"icon":"wrench"}
            conn.send_life_sign(variables=variables)
        except Exception as e:
            logger.error("Unable to send life sign.")
            logger.error(e)
    #app.run(threaded=True,host= '0.0.0.0')
