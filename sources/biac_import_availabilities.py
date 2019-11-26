"""
BIAC IMPORT AVAILABILITIES
====================================
Reads Honeywell Availabilities Excels files sended by Honeywell by mails to "bacavailabilities@cofelygtc.com", 
transfered to AQMC by BiacMails module, sorted by a NodeRed flow treated and stored into an ElasticSearch collection

Sends:
-------------------------------------

* /topic/BIAC_AVAILABILITY_IMPORTED
* /topic/BIAC_AVAILABILITY_LOT5_IMPORTED
* /topic/BIAC_AVAILABILITY_LOT7_IMPORTED

Listens to:
-------------------------------------

* /queue/BIAC_FILE_6_BoardingBridge
* /queue/BIAC_FILE_6_PCA
* /queue/BIAC_FILE_6_400HZ
* /queue/BIAC_FILE_5_tri
* /queue/BIAC_FILE_7_screening

Collections:
-------------------------------------

* **biac_availability-yyyy.mm** (Raw Data)
* **biac_month_availability-yyyy** (Computed Data for Current Month / Modified when a new file append)

VERSION HISTORY
-------------------------------------

* 01 Feb 2019 1.0.1  **PDB** First Version
* 01 Jun 2019 1.0.18 **PDB** Add Lot7 importation
* 19 Nov 2019 1.0.19 **VME** Bug fixing on dates
"""

import json
import time
import uuid
import base64
import threading
import os,logging
import pandas as pd
import re
import platform


from logging.handlers import TimedRotatingFileHandler
from amqstompclient import amqstompclient
from datetime import datetime
from datetime import timedelta
from functools import wraps
from elasticsearch import Elasticsearch as ES, RequestsHttpConnection as RC
from logstash_async.handler import AsynchronousLogstashHandler
from lib import pandastoelastic as pte
import numpy as np
from math import ceil


VERSION="1.0.19"
MODULE="BIAC_IMPORT_AVAILABILITIES"
QUEUE=["/queue/BIAC_FILE_6_BoardingBridge","/queue/BIAC_FILE_6_PCA","/queue/BIAC_FILE_6_400HZ", "/queue/BIAC_FILE_5_tri", "/queue/BIAC_FILE_7_screening"]
INDEX_PATTERN = "biac_availability"


#####################################################

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


def week_of_month(dt):
    """ Returns the week of the month for the specified date.

    Parameters
    ----------
    dt
        Date on datetime format
    """

    first_day = dt.replace(day=1)

    dom = dt.day
    adjusted_dom = dom + first_day.weekday()

    return int(ceil(adjusted_dom/7.0))


def week_of_year(ts):
    """ Returns the week of the year for the specified date.

    Parameters
    ----------
    ts
        Unix timestamp
    """
    #print('Timestamp:' + str(ts))
    ts = int(ts) / 1000
    dt = datetime.utcfromtimestamp(ts)
    weekOfYear = dt.isocalendar()[1]
    return weekOfYear


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
    """
    Main function that reads the Excel file.         
    """
    global es
    records=0
    starttime = time.time()
    imported_records=0
    reserrors = dict()
    logger.info("==> "*10)
    logger.info("Message Received %s" % destination)
    logger.info(headers)
    decodedmessage = base64.b64decode(message)

    if "file" in headers:
        headers['CamelFileNameOnly'] = headers['file']
        log_message("Import of file [%s] started." % headers["file"])


    #####################
    # TO MODIFY ########
    lot = headers['lot']
    filename = headers['CamelFileNameOnly']
    category = headers['category']
    #####################

    f = open('dataFile.xlsm', 'w+b')
    f.write(decodedmessage)
    f.close()

    file = 'dataFile.xlsm'
    """ ef = pd.ExcelFile(file)
    dfs = []
    for sheet in ef.sheet_names:
        # print(sheet)
        df = pd.read_excel(file, sheetname=sheet)
        # print(df)
        dfs.append(df)

    dfdef = dfs[0]

    dfdata = dfs[1] """



    badeq = [
    'gtx_ana',
    'gtx_aws',
    'gtx_eac',
    'gtx_eds',
    'gtx_ema',
    'gtx_eq',
    'gtx_etd',
    'gtx_kro',
    'gtx_md',
    'gtx_opt',
    'gtx_rx',
    'gtx_samd',
    'gtx_td',
    'gtx_trs'
    ]


    excel = pd.ExcelFile(file)

    try:
        dfdef = pd.read_excel(file, sheet_name='REPDEF')
    except:
        print('unable to read REPDEF sheet')
        
    try:
        dfdata = pd.read_excel(file, sheet_name='REPORT', index_col=5, header=7)
    except:
        print('unable to read REPORT sheet')

    newcolumns = []
    for col in dfdata.columns:
        if str(col) != "nan" and str(col) != "NaT" and str(col) != "EQT" and \
        str(col) != "OBW" and str(col) != "KPI" and "AVERAGE" not in str(col) and \
        "Unnamed" not in str(col):
            newcolumns.append(col)


    dfdata    = dfdata[newcolumns]
    serie_obj  = dfdata.loc['OBJ'].copy()
    dfdata.drop(index='OBJ', inplace=True)

    dfdata.dropna(axis=0, how='all', inplace=True)

    # if dfdata.index[0] != 1:
    #     print('we expect the file to start at 1 day/week of year -> we drop the first line')
    #     dfdata.drop(index=dfdata.index[0], inplace=True)


    print(dfdata)

    if lot == '7':
        dfdata = dfdata.fillna(100)

    dfdata.dropna(inplace=True)
    #objectives = dfdata.iloc[0]


    res = dfdata.to_json(orient="values")
    #print(objectives)
    idrepport = dfdef.get_value(0, 'id_report')
    interval = dfdef.get_value(0, 'interval')
    startDate = dfdef.get_value(0, 'g_start_date')
    stopDate = dfdef.get_value(0, 'g_end_date')
    report_type = dfdef.get_value(0, 'report_type')
    name = dfdef.get_value(0, 'name').lower()
    name = name.replace('%', '')

    first_day_year = startDate.to_pydatetime().replace(
        month=1, day=1, hour=0, minute=0, second=0)



    cur_year = first_day_year.year

    year = []
    old = 0

    for i in dfdata.index:
        if i < old:
            cur_year += 1

        year.append(cur_year)
        old = i


    dfdata['year'] = year

    dfdata.reset_index(inplace=True)

    if interval == 'week':
        # dfdata['dt'] = dfdata['EQ'].apply(
        #     lambda x: first_day_year + ((x-1)*timedelta(days=7)))
        dfdata['dt'] = dfdata.apply(
            lambda row:  datetime(int(row['year']), 1, 1) + timedelta(days=7*(row['EQ']-1)), axis=1)

    # elif interval == 'day' and lot=='6':
    #     # dfdata['dt'] = dfdata['EQ'].apply(
    #     #     lambda x: startDate + ((x)*timedelta(days=1)))
    #     dfdata['dt'] = dfdata.apply(
    #         lambda row:  datetime(int(row['year']), 1, 1) + timedelta(days=row['EQ']), axis=1)

    elif interval == 'day':
        # dfdata['dt'] = dfdata['EQ'].apply(
        #     lambda x: startDate + ((x-1)*timedelta(days=1)))
        dfdata['dt'] = dfdata.apply(
            lambda row:  datetime(int(row['year']), 1, 1) + timedelta(days=(row['EQ']-1)), axis=1)


    del dfdata['year']

    dfdata.set_index('dt', inplace=True)

    dfdata = dfdata.resample('1d').pad()

    dfdata.reset_index(inplace=True)

    dfdata['week_of_month'] = dfdata.dt.apply(week_of_month)
    dfdata['_index'] = dfdata.dt.map(
        lambda x: INDEX_PATTERN+'-'+x.strftime('%Y.%m'))
    dfdata['year_month'] = dfdata.dt.map(lambda x: x.strftime('%Y-%m'))
    dfdata['@timestamp'] = dfdata.dt.values.astype(np.int64) // 10 ** 6
    dfdata.set_index('dt', inplace=True)
    dfdata.columns = map(str.lower, dfdata.columns)

    regex = r"^gt[zb]_"

    if lot != '6':
        regex = r"^gt[xh]"

    if lot == '6':
        for index, col in dfdata.iteritems():
            last
            cpt = 0
            if re.match(regex, index):
                print(len(col))
                for item in col.iteritems():
                    td = item[0]
                    if item[1] == 0:
                        lastwas0 = 1
                        dfdata.at[td, index] = 100
                        if cpt != 0:
                            dfdata.at[td-timedelta(days=1), index] = 100
                    if cpt < (len(col)- 1) and lastwas0 == 1 and item[1] != 0:
                        dfdata.at[td, index] = 100
                        #item[1] = 100
                        lastwas0 = 0
                    cpt +=1

    # print(dfdata)

    bulkbody = ''
    bulkres = ''


    for index, row in dfdata.iterrows():
        # print(index)
        # print(row)

        week_of_the_month = row['week_of_month']
        es_index = row['_index']
        ts = row['@timestamp']
        year_month = row['year_month']

        for i in row.index:
            if re.match(regex, i):
                equipment = i.replace('%', '')
                #objective = objectives.get_value(1, i)
                # print('it s an equipment: '+i+'  value: '+str(row[i])+'   objective: '+str(objective))
                weekOfYear = week_of_year(ts)

                es_id = str(idrepport) + '_' + i + '_' + str(ts)

                action = {}
                action["index"] = {"_index": es_index,
                    "_type": "doc", "_id": es_id}

                rowInterval = 0

                if interval == 'week':
                    rowInterval = int(row['eq'])
                elif interval == 'day':
                    rowInterval = int(row['eq'])


                displayReport = 1

                if lot == '7' and i in badeq:
                    displayReport = 0

                newrec = {
                    "@timestamp": ts,
                    "reportID": int(idrepport),
                    "category": category,
                    "reportType": int(report_type),
                    "startDate": int(getTimestamp(startDate)*1000),
                    "stopDate": int(getTimestamp(stopDate)*1000),
                    "interval": interval,
                    "equipment": i,
                    "cleanEquipement": i[4:],
                    "lot": lot,
                    "filename": filename,
                    "objective": 98,
                    "numInterval": rowInterval,
                    "value": round(float(row[i]),2),
                    "year_month": row['year_month'],
                    "weekOfMonth": week_of_the_month,
                    "weekOfYear": weekOfYear,
                    "lastWeek": 0,
                    "displayReport": displayReport
                }

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
                            imported_records -= 1
                            logger.info(item["index"]["error"])
                            reserrors.append(
                                {"error": item["index"]["error"], "id": item["index"]["_id"]})


    if len(bulkbody) > 0:
        logger.info("BULK READY FINAL:" + str(len(bulkbody)))
        bulkres = es.bulk(bulkbody)
        logger.info("BULK DONE FINAL")
        if(not(bulkres["errors"])):
            logger.info("BULK done without errors.")
        else:
            for item in bulkres["items"]:
                if "error" in item["index"]:
                    imported_records -= 1
                    logger.info(item["index"]["error"])
                    reserrors.append(
                        {"error": item["index"]["error"], "id": item["index"]["_id"]})


    time.sleep(3)
    first_alarm_ts = int(dfdata['@timestamp'].min())
    last_alarm_ts = int(dfdata['@timestamp'].max())+10800000
    obj = {
            'start_ts': int(first_alarm_ts),
            'end_ts': int(last_alarm_ts)
        }

    if len(reserrors)>0:
        log_message("Import of file [%s] failed. Duration: %d. %d records were not imported." % (headers["file"],(endtime-starttime),len(reserrors)))        

    endtime = time.time()    
    try:
        log_message("Import of file [%s] finished. Duration: %d Records: %d." % (headers["file"],(endtime-starttime),df.shape[0]))         
    except:
        log_message("Import of file [%s] finished. Duration: %d." % (headers["file"],(endtime-starttime)))

    logger.info('sending message to /topic/BIAC_AVAILABILITY_IMPORTED')
    logger.info(obj)

    if lot == '5':
        conn.send_message('/topic/BIAC_AVAILABILITY_LOT5_IMPORTED', json.dumps(obj))
    elif lot == '7':
        conn.send_message('/topic/BIAC_AVAILABILITY_LOT7_IMPORTED', json.dumps(obj))
    else:
        conn.send_message('/topic/BIAC_AVAILABILITY_IMPORTED', json.dumps(obj))




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
            variables={"platform":"_/_".join(platform.uname()),"icon":"clipboard-check"}
            
            conn.send_life_sign(variables=variables)
        except Exception as e:
            logger.error("Unable to send life sign.")
            logger.error(e)
    #app.run(threaded=True,host= '0.0.0.0')
