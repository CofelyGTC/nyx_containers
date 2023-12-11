"""
BIAC IMPORT MAXIMO
====================================
Reads Maximo Excels files imported from MaxBI by mails to "maximorepport@cofelygtc.com" transfered to AQMC by Camelworker treated and stored into an ElasticSearch collection

Sends:
-------------------------------------

* /topic/BIAC_MAXIMO_IMPORTED

Listens to:
-------------------------------------

* /queue/MAXIMO_IMPORT

Collections:
-------------------------------------

WORKTYPE PM
* **biac_maximo** (Raw Data / Modified when a new file append)
* **biac_maximo_monthly** (Computed Data for Current Month / Modified when a new file append)
* **biac_histo_maximo** (Raw Data historicly saved by day / Never Change)


WORKTYPE ADM
* **biac_503maximo** (Raw Data / Modified when a new file append) 
* **biac_503histo_maximo** (Raw Data historicly saved by day / Never Change)

VERSION HISTORY
-------------------------------------

* 03 Jun 2019 1.0.20 **PDB** Finished version for basic applications
* 01 Aug 2019 2.0.1 **AMA** Add Historical system
* 01 Aug 2019 2.1.0 **AMA** Filter worktypes PM and ADM to two different collections.
* 18 Dec 2019 2.1.1 **VME** Added the KPI 503 field
* 18 Dec 2019 2.1.2 **VME** Added the KPI503computed field
* 13 Jan 2022 2.1.7 **PDB** Adapt to new Maximo Exporter
"""

import re
import json
import time
import uuid
import base64
import threading
import os,logging
import numpy as np
import pandas as pd


from functools import wraps
from datetime import datetime
from datetime import timedelta
from amqstompclient import amqstompclient
from dateutil.relativedelta import relativedelta
from logging.handlers import TimedRotatingFileHandler
from logstash_async.handler import AsynchronousLogstashHandler
from elasticsearch import Elasticsearch as ES, RequestsHttpConnection as RC

from elastic_helper import es_helper 

VERSION="2.1.11"
MODULE="BIAC_MAXIMO_IMPORTER"
QUEUE=["MAXIMO_IMPORT"]

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
def getDisplayStart(now):
    start = ''
    if 1 <= now.day < 15:
        month = 12
        year = now.year - 1
        if now.month != 1:
            month = now.month -1
            year = now.year
        start = datetime(year, month, 1, 0, 0, 0, 0)
    else:
        start = datetime(now.year, now.month, 1, 0, 0, 0, 0)

    #logger.info('Display start : ' + str(start))

    return int(start.timestamp())

################################################################################
def getDisplayDate503():
    now = datetime.now()
    start_month = datetime(now.year, now.month, 1)
    return  start_month - relativedelta(months=4), start_month - relativedelta(months=3)

################################################################################
def compute_str_months(dt):
    today = datetime.now().date()
    start_month = datetime(today.year, today.month, 1).date()
    end_month = start_month + relativedelta(months=+1)
    
    # print(f"comute_str_months, start : {start_month}  - end : {end_month}")
    if start_month <= dt and dt < end_month:
        return '+0m'
    else:
        offset = -1
        i = 0
        if dt >= end_month:
            offset = 1
        
            while dt >= end_month:
                i += 1
                end_month += relativedelta(months=offset)
            
            return f"-{i}m"
        else:
            while dt < start_month:
                i += 1
                start_month += relativedelta(months=offset)
                
            
            return f"+{i}m"


################################################################################

def extract_screen_name(row):

    """
    Defined the screen where the data must be displayed based on lot and technic

    Parameters
    ----------
    raw
        All the row that conatins the workorder information
    """
    
    lot = int(row['lot'])
    
    if lot == 1:
        return 'heating'
    if lot == 3:
        return 'ext_building'

    if lot == 2:
        tmp = str(row['technic']).lower()

        if tmp == 'acces':
            return 'access'
        
        if tmp == 'fire fighting':
            return 'fire'
        
        if tmp == 'access':
            return 'access'
        
        if tmp == 'cradle':
            return 'cradle'

        if tmp == 'sanitair':
            return 'sani'
        
        if 'hvac pa' in tmp:
            return 'hvacpa'
        
        if tmp == 'elektriciteit':
            return 'elec'
        
        if 'hvac pb' in tmp:
            return 'hvacpb'
    if lot == 4:
        return 'dnb'
    
    return None

################################################################################

def set_technic(contract, team):
    #team = row['Pm execution team']
    #contract = row['Contract']
    team = str(int(team))
    logger.info('Technic Team: '+ str(team))

    if contract == 'BACELE':
        return 'elektriciteit'
    if contract == 'BACHEA':
        return 'heating'
    if contract == 'BACEXT':
        return 'ext_building'
    if contract == 'BACFIR':
        if team == "6199":
            return 'acces'
        elif team == '6596':
            return 'cradle'
        else:
            return 'fire fighting'
    if contract == "BACSAN" and team == "6483" or contract == "BACSAN" and team == "6197":
        return 'sanitair'
    if contract == 'BACSAN':
        return 'hvac pa'
    if contract == 'BACHVA':
        return 'hvac pb'
    if contract == 'BACDNB':
        return 'dnb'

    return None 


################################################################################

def set_lot(contract):
    #contract=row['Contract']
    
    if contract == 'BACHEA':
        return '1'
    if contract == 'BACEXT':
        return '3'
    if contract == 'BACDNB':
        return '4'
    
    return '2'

################################################################################

def getDisplayStop(now):
    """
    Defined the date until the workerorder must be displayed on the screens        

    Parameters
    ----------
    now
        unix timestamp taked at the start of importation
    """
    datestop = now - timedelta(days=13)
    datestop = datestop.replace(hour=0)
    datestop = datestop.replace(minute=0)
    datestop = datestop.replace(second=0)
    datestop = datestop.replace(microsecond=0)
    stop = int(datestop.timestamp())
    #logger.info('Display stop :' + str(stop))
    return stop

##################################################################################

def getDisplayKPI302(now):
    """
    Defined the date until the workerorder must be used in the calculation of KPI302   

    Parameters
    ----------
    now
        unix timestamp taked at the start of importation
    """
    KPI302Start = datetime(now.year, now.month, 1, 0, 0, 0, 0)
    start = (int(KPI302Start.timestamp()) - 3601 )
    #logger.info('Start KPI 302:' + str(start))
    return start

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
        if '.xlsx' in file_name:
            log_message("Import of file [%s] started." %file_name)
        else:
            log_message("Import of file [%s] not done (Bad File Format)." %file_name)
            return 0
        


    
    
    histo_date = datetime.now().date()
    flag_histo = False
    regex = r'_([0-9]{8})\.'
    z = re.findall(regex, file_name)
    if z:
        flag_histo = True
        histo_date = datetime.strptime(z[0], "%Y%m%d").date()

    flag_already_histo_for_today = False
    try:
        dataframe=es_helper.elastic_to_dataframe(es,index="biac_histo_maximo"
                                        ,size=1
                                        ,query='histo_date:'+str(histo_date))

        if len(dataframe) > 0:
            flag_already_histo_for_today=True
    except:
        pass
    
    logger.info('Histo flag: '+str(flag_histo))
    logger.info('Histo date            : '+str(histo_date))
    logger.info('Already histo for date: '+str(histo_date)+' -> '+str(flag_already_histo_for_today))


    xlsbytes = base64.b64decode(message)
    f = open('./tmp/excel.xlsx', 'wb')
    f.write(xlsbytes)
    f.close()

    df = None
    try:        
        sheet = 'WorkOrdersListBAC.rdl'
        xl = pd.ExcelFile('./tmp/excel.xlsx')

        logger.info(xl.sheet_names)
        df = pd.read_excel('./tmp/excel.xlsx', sheet_name=sheet)
        newheaders = df.iloc[0]
        logger.info(newheaders)
        #df = df[1:]
        #df.columns = newheaders

        bulkbody = ''
        bulkres = ''
        reserrors= []

        df.fillna(0,inplace=True)

        if not flag_histo:
            es.indices.delete(index="biac_maximo", ignore=[400, 404])
            es.indices.delete(index="biac_503maximo", ignore=[400, 404])
            time.sleep(3)
            

        logger.info("step 1")  
        logger.info(df.head())
        logger.info(df.columns)  
        df = df[df['Contract'] !=0]  

        logger.info("step 2")

        df['Contract'] = df['Contract'].apply(lambda x: x.upper())

        logger.info("step 3")

        df['lot'] = df['Contract'].apply(lambda x: set_lot(x))

        logger.info("step 4")

        df['technic'] = df.apply(lambda row: set_technic(row['Contract'], row['Pm execution team']), axis=1)
        
        logger.info("step 5")
            
        df['screen_name'] = df.apply(extract_screen_name, axis=1)
        df[['lot', 'technic', 'Contract', 'Pm execution team']]

        logger.info("step 6")

        now = datetime.now()
        displayStart = getDisplayStart(now)
        displayStop = getDisplayStop(now)

        start_dt_503, stop_dt_503 = getDisplayDate503()

        for index, row in df.iterrows():
            woid  = row[0]
            contract= row[1]
            worktype = row[2]
            wosummary = row[3]
            status = row[4]
            getTsDate = lambda cpltd: getTimestamp(cpltd) if cpltd != None and cpltd != np.nan and not isinstance(cpltd, str) else 0
            completedDate = getTsDate(row[5])
            pmExecutionTeam = row['Pm execution team']
            woExecutionTeam = row['Wo execution team']
            
            displaykpi302Start = getDisplayKPI302(now)
            nowts = int(now.timestamp())

            display = 0
            displaykpi302 = 0

            row['ScheduleStart'] = row['ScheduleStart'].replace(hour=12, minute=0)
            if woExecutionTeam == np.nan:
                woExecutionTeam = 0
            targetStart = getTsDate(row['TargetStart'])
            scheduledStart = getTsDate(row['ScheduleStart'])
            actualStartMax = getTsDate(row['ActualStart max'])
            actualStart = getTsDate(row['ActualStart'])
            KPI301 = row['KPI301']
            assetDescription = row['Asset description']
            routeDescription = row['Route description']
            actualFinishMax = getTsDate(row['ActualFinish max'])
            actualFinish = getTsDate(row['ActualFinish'])
            KPI302 = row['KPI302']
            KPI503 = ""
            KPI503computed =" "
            if "KPI503" in row:
                KPI503 = row['KPI503']
                KPI503computed = row['KPI503']

            overdue = 0

            ispm=(worktype=="PM")
            issafe=(worktype=="SAFE" or worktype=="GRIS")


            try:

                if ispm or issafe:
                    if displayStart <= scheduledStart < displayStop and KPI503 != "Done OK":
                        display = 1
                    elif scheduledStart < displayStart:
                        overdue = 1
                else:
                    if start_dt_503.timestamp() <= scheduledStart < stop_dt_503.timestamp() and KPI503 != "Done OK":
                        display = 1
                        KPI503computed = 'New overdue'
                    elif scheduledStart < start_dt_503.timestamp():
                        overdue = 1
                        KPI503computed = 'Overdue'
                        

            except Exception as e:
                logger.warning('Scheduled start not int : ' + str(scheduledStart) +  '-' + str(row['ScheduleStart']))
                logger.info(e)
                
            try:    
                if displaykpi302Start <= actualStart < nowts:
                    displaykpi302 = 1

            except Exception as e:
                logger.info(e)
            
            difference = now - row['ScheduleStart']
            days = int(difference /timedelta(days=1))
            strDays = '+'+str(days)+'j'
            
            
            sevendays = getTimestamp(now - timedelta(days=7))
            fourthteendays = getTimestamp(now - timedelta(days=14))


            if displayStart <= scheduledStart < fourthteendays:
                strDays = '+14j'


            if fourthteendays <= scheduledStart < sevendays:
                if KPI301 == 'Not Started NOK' or KPI301 == 'Started NOK':
                    strDays ='+14j'
                else:
                    strDays = '+7j'

                
            if strDays == '+14j' and KPI301 == 'Started OK':
                strDays = '+15j'

            future = 1

            if nowts < scheduledStart:
                future = 0



            newrec = {
                "woid": woid,
                "contract": contract,
                "worktype": worktype,
                "wosummary": wosummary,
                "status": status,
                "completedDate": completedDate*1000,
                "pmExecutionTeam": pmExecutionTeam,
                "woExecutionTeam": woExecutionTeam,
                "targetStart": targetStart*1000,
                "scheduledStart": scheduledStart*1000,
                "Scheduled": scheduledStart*1000,
                "actualStartMax": actualStartMax*1000,
                "Started": actualStart*1000,
                "future": future,
                "actualStart": actualStart*1000,
                "displayKPI301": display,
                "displayKPI302": displaykpi302,
                "technic": row.technic,
                "lot": row.lot,
                "screen_name": row.screen_name,
                "strDays": strDays,
                "KPI301": KPI301,
                "assetDescription": assetDescription,
                "routeDescription": routeDescription,
                "overdue": overdue,
                "actualFinishMax": actualFinishMax*1000,
                "actualFinish": actualFinish*1000,
                "KPI302": KPI302,
                "KPI503": KPI503,
                "KPI503computed": KPI503computed,
            }

            

            if not ispm or not issafe:
                dt = datetime.fromtimestamp(scheduledStart).date()
                newrec['strMonths'] = compute_str_months(dt)

            if not flag_histo:
                es_id = woid+'_'+str(scheduledStart)
                action = {}
                if ispm:
                    action["index"] = {"_index": 'biac_maximo',
                        "_type": "doc", "_id": es_id}
                elif issafe:
                    action["index"] = {"_index": 'biac_safemaximo',
                        "_type": "doc", "_id": es_id}
                else:
                    action["index"] = {"_index": 'biac_503maximo',
                        "_type": "doc", "_id": es_id}

                bulkbody += json.dumps(action)+"\r\n"
                bulkbody += json.dumps(newrec) + "\r\n"


            if not flag_already_histo_for_today:
                es_id = woid+'_'+str(scheduledStart)+'_'+(histo_date.strftime('%Y-%m-%d'))
                action = {}
                if ispm:
                    action["index"] = {"_index": 'biac_histo_maximo',
                        "_type": "doc", "_id": es_id}
                elif issafe:
                    action["index"] = {"_index": 'biac_safehisto_maximo',
                        "_type": "doc", "_id": es_id}
                else:
                    action["index"] = {"_index": 'biac_503histo_maximo',
                        "_type": "doc", "_id": es_id}

                newrec['histo_date_dt'] = str(histo_date)
                newrec['histo_date'] = histo_date.strftime('%Y-%m-%d')

                bulkbody += json.dumps(action)+"\r\n"
                bulkbody += json.dumps(newrec) + "\r\n"
            

            if len(bulkbody) > 512000:
                logger.info("BULK READY:" + str(len(bulkbody)))
                bulkres = es.bulk(bulkbody, request_timeout=30)
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
            bulkres = es.bulk(bulkbody)
            logger.info(bulkres)
            logger.info("BULK DONE FINAL")
            if(not(bulkres["errors"])):
                logger.info("BULK done without errors.")
            else:
                for item in bulkres["items"]:
                    if "error" in item["index"]:
                        logger.info(item["index"]["error"])
                        reserrors.append(
                            {"error": item["index"]["error"], "id": item["index"]["_id"]})

        time.sleep(3)

        first_alarm_ts = getTimestamp(df['ScheduleStart'].min())
        last_alarm_ts = getTimestamp(df['ScheduleStart'].max())
        obj = {
                'start_ts': int(first_alarm_ts),
                'end_ts': int(last_alarm_ts)
            }
        logger.info(obj)


        if not flag_histo:
            conn.send_message('/topic/BIAC_MAXIMO_IMPORTED', json.dumps(obj))
            conn.send_message('/topic/BIAC_MAXIMOBIS_IMPORTED', json.dumps(obj))
    except Exception as e:
        endtime = time.time()
        logger.error(e)
        log_message("Import of file [%s] failed. Duration: %d Exception: %s." % (headers["file"],(endtime-starttime),str(e)))        


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
