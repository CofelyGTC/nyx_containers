"""
ULG COMPUTE TIMESHEET
====================================

Collections:
-------------------------------------


VERSION HISTORY
===============

* 14 Dec 2022 1.0.1 **PDE** First version



"""  

import json
import time
import uuid
import base64
import threading
import os,logging
import regex
import numpy as np
import pandas as pd
from elastic_helper import es_helper 
from dateutil.tz import tzlocal
from tzlocal import get_localzone
import requests
import platform
from datetime import timedelta
import pytz

from logging.handlers import TimedRotatingFileHandler
from amqstompclient import amqstompclient
from datetime import datetime
from functools import wraps
from elasticsearch import Elasticsearch as ES, RequestsHttpConnection as RC
from logstash_async.handler import AsynchronousLogstashHandler

VERSION="1.0.4"
MODULE="ULG_COMPUTE_TIMESHEET"
QUEUE=["ULG_COMPUTE_TIMESHEET"]

################################################################################
def messageReceived(destination,message,headers):
    global es
    logger.info("==> "*10)
    logger.info("Message Received %s" % destination)
    logger.info(message)
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

class DateTimeEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, dt.datetime):
            return o.isoformat()

        elif isinstance(o, dt.time):
            return o.isoformat()

        return json.JSONEncoder.default(self, o)

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
    
def getDay(ts):
    dt = datetime.fromtimestamp(ts/1000)
    day = dt.strftime('%Y-%m-%d')
    return day
    


def getConge(ts):
    
    isConge = False
    start = ts-timedelta(hours=2)
    end = ts+timedelta(hours=2)
    query = "isHolliday: true"
    dfConge = es_helper.elastic_to_dataframe(es, 'ulg_hollidays', query=query, start=start, end=end)
    if dfConge.shape[0] > 0:
        logger.info(dfConge)
        isConge = True
    return isConge

def getCongeULG(ts):
    
    isConge = False
    start = ts-timedelta(hours=2)
    end = ts+timedelta(hours=2)
    query = "isULGHolliday: true"
    dfConge = es_helper.elastic_to_dataframe(es, 'ulg_hollidays', query=query, start=start, end=end)
    if dfConge.shape[0] > 0:
        logger.info(dfConge)
        isConge = True
    return isConge
    
def getCongeOffice(ts):
    
    isConge = False
    start = ts-timedelta(hours=2)
    end = ts+timedelta(hours=2)
    query = "isOfficeHolliday: true"
    dfConge = es_helper.elastic_to_dataframe(es, 'ulg_hollidays', query=query, start=start, end=end)
    if dfConge.shape[0] > 0:
        logger.info(dfConge)
        isConge = True
    return isConge    
        
def ulg_computed():
    global es

    start = datetime.now()-timedelta(days=1)
    end = start + timedelta(days=15)
    regulars = es_helper.elastic_to_dataframe(es, 'horaires_ulg_regular')
    circuit = 'Stat.général'
    DB = 'XX'
    roomRegular = regulars[regulars['circuit'] == circuit].reset_index(drop = True)
    fromULG = es_helper.elastic_to_dataframe(es, 'horaires_ulg_import', start=start, end=end)
    df = es_helper.elastic_to_dataframe(es, 'ulg_circuits')
    df = df.sort_values(by=['bat_circuit']).reset_index(drop=True)
    circuits = df['bat_circuit'].unique().tolist()
    
    
    bulkbody = ""

    for circuit in circuits:
        DB = 'XX'
        deroCovid = False
        deroCovidMins = 0
        deroCovidMorning = False
        deroCovidMorningMins = 0
        deroConge = False
        deroCongeULG = False
        deroCongeOffice = False
        nextGen = False
        oldGenDecimal = False
        logger.info(circuit)

        start = datetime.now()-timedelta(days=1)
        end = start + timedelta(days=15)
        roomsdf = df[df['bat_circuit'] == circuit]
        roomsdf = roomsdf.sort_values(by=['code_identification']).reset_index(drop = True)
        rooms = roomsdf['code_identification'].unique().tolist()
        roomRegular = regulars[regulars['circuit'] == circuit].reset_index(drop = True)
        regularHoraire = ''
        if roomRegular.shape[0] > 0:
            print(roomRegular)
            regularHoraire = json.loads(roomRegular.loc[0].to_json())

        while start <= end:
            action = {}
            newRec = {}
            #print(start)
            startDay = start.replace(hour=0, minute=0, second=0, microsecond=0)
            endDay = start.replace(hour=23, minute=59, second=59, microsecond=999999)
            fromULG = es_helper.elastic_to_dataframe(es, 'horaires_ulg_import', start=startDay, end=endDay)
            horaireULG = pd.DataFrame()
            if fromULG.shape[0] > 0:
                horaireULG = fromULG[fromULG['id_room'].isin(rooms)].sort_values(by=['start_datetime', 'end_datetime'])
            start1 = startDay.timestamp()
            start2 = startDay.timestamp()
            end1 = startDay.timestamp()
            end2 = startDay.timestamp()

            if regularHoraire != '':
                #print(regularHoraire)
                #regularHoraire = json.loads(regularHoraire)
                DB = regularHoraire['DB']
                deroCovid = regularHoraire['dero_covid']
                deroCovidMins = regularHoraire['dero_covid_mins']
                deroCovidMorning = regularHoraire['dero_covid_morning']
                deroCovidMorningMins = regularHoraire['dero_covid_morning_mins']
                deroConge = regularHoraire['dero_holidays']
                deroCongeULG = regularHoraire['dero_holidays_ulg']
                deroCongeOffice = regularHoraire['dero_holidays_office']
                nextGen = regularHoraire['nextGen']
                oldGenDecimal = regularHoraire['oldGenDecimal']
                if startDay.strftime('%w') == '0':
                    print('Dimanche')
                    start1 = startDay.replace(hour = int(regularHoraire['sundayON1'][:2]), minute= int(regularHoraire['sundayON1'][-2:]), second=0).timestamp()
                    start2 = startDay.replace(hour = int(regularHoraire['sundayON2'][:2]), minute= int(regularHoraire['sundayON1'][-2:]), second=0).timestamp()
                    end1 = startDay.replace(hour = int(regularHoraire['sundayOFF1'][:2]), minute= int(regularHoraire['sundayOFF1'][-2:]), second=0).timestamp()
                    end2 = startDay.replace(hour = int(regularHoraire['sundayOFF2'][:2]), minute= int(regularHoraire['sundayOFF2'][-2:]), second=0).timestamp()

                elif startDay.strftime('%w') == '1':
                    print('Lundi')
                    start1 = startDay.replace(hour = int(regularHoraire['mondayON1'][:2]), minute= int(regularHoraire['mondayON1'][-2:]), second=0).timestamp()
                    start2 = startDay.replace(hour = int(regularHoraire['mondayON2'][:2]), minute= int(regularHoraire['mondayON2'][-2:]), second=0).timestamp()
                    end1 = startDay.replace(hour = int(regularHoraire['mondayOFF1'][:2]), minute= int(regularHoraire['mondayOFF1'][-2:]), second=0).timestamp()
                    end2 = startDay.replace(hour = int(regularHoraire['mondayOFF2'][:2]), minute= int(regularHoraire['mondayOFF2'][-2:]), second=0).timestamp()

                elif startDay.strftime('%w') == '2':
                    print('Mardi')
                    start1 = startDay.replace(hour = int(regularHoraire['tuesdayON1'][:2]), minute= int(regularHoraire['tuesdayON1'][-2:]), second=0).timestamp()
                    start2 = startDay.replace(hour = int(regularHoraire['tuesdayON2'][:2]), minute= int(regularHoraire['tuesdayON2'][-2:]), second=0).timestamp()
                    end1 = startDay.replace(hour = int(regularHoraire['tuesdayOFF1'][:2]), minute= int(regularHoraire['tuesdayOFF1'][-2:]), second=0).timestamp()
                    end2 = startDay.replace(hour = int(regularHoraire['tuesdayOFF2'][:2]), minute= int(regularHoraire['tuesdayOFF2'][-2:]), second=0).timestamp()
                elif startDay.strftime('%w') == '3':
                    print('Mercredi')
                    start1 = startDay.replace(hour = int(regularHoraire['wednesdayON1'][:2]), minute= int(regularHoraire['wednesdayON1'][-2:]), second=0).timestamp()
                    start2 = startDay.replace(hour = int(regularHoraire['wednesdayON2'][:2]), minute= int(regularHoraire['wednesdayON2'][-2:]), second=0).timestamp()
                    end1 = startDay.replace(hour = int(regularHoraire['wednesdayOFF1'][:2]), minute= int(regularHoraire['wednesdayOFF1'][-2:]), second=0).timestamp()
                    end2 = startDay.replace(hour = int(regularHoraire['wednesdayOFF2'][:2]), minute= int(regularHoraire['wednesdayOFF2'][-2:]), second=0).timestamp()
                elif startDay.strftime('%w') == '4':
                    print('Jeudi')
                    start1 = startDay.replace(hour = int(regularHoraire['thursdayON1'][:2]), minute= int(regularHoraire['thursdayON1'][-2:]), second=0).timestamp()
                    start2 = startDay.replace(hour = int(regularHoraire['thursdayON2'][:2]), minute= int(regularHoraire['thursdayON2'][-2:]), second=0).timestamp()
                    end1 = startDay.replace(hour = int(regularHoraire['thursdayOFF1'][:2]), minute= int(regularHoraire['thursdayOFF1'][-2:]), second=0).timestamp()
                    end2 = startDay.replace(hour = int(regularHoraire['thursdayOFF2'][:2]), minute= int(regularHoraire['thursdayOFF2'][-2:]), second=0).timestamp()
                elif startDay.strftime('%w') == '5':
                    print('Vendredi')
                    start1 = startDay.replace(hour = int(regularHoraire['fridayON1'][:2]), minute= int(regularHoraire['fridayON1'][-2:]), second=0).timestamp()
                    start2 = startDay.replace(hour = int(regularHoraire['fridayON2'][:2]), minute= int(regularHoraire['fridayON2'][-2:]), second=0).timestamp()
                    end1 = startDay.replace(hour = int(regularHoraire['fridayOFF1'][:2]), minute= int(regularHoraire['fridayOFF1'][-2:]), second=0).timestamp()
                    end2 = startDay.replace(hour = int(regularHoraire['fridayOFF2'][:2]), minute= int(regularHoraire['fridayOFF2'][-2:]), second=0).timestamp()
                elif startDay.strftime('%w') == '6':
                    print('Samedi')
                    start1 = startDay.replace(hour = int(regularHoraire['saturdayON1'][:2]), minute= int(regularHoraire['saturdayON1'][-2:]), second=0).timestamp()
                    start2 = startDay.replace(hour = int(regularHoraire['saturdayON2'][:2]), minute= int(regularHoraire['saturdayON2'][-2:]), second=0).timestamp()
                    end1 = startDay.replace(hour = int(regularHoraire['saturdayOFF1'][:2]), minute= int(regularHoraire['saturdayOFF1'][-2:]), second=0).timestamp()
                    end2 = startDay.replace(hour = int(regularHoraire['saturdayOFF2'][:2]), minute= int(regularHoraire['saturdayOFF2'][-2:]), second=0).timestamp()


            if horaireULG.shape[0] > 0:
                horaireULG = horaireULG.reset_index(drop=True)
                #print('hooraireULG '+circuit)
                for index, row in horaireULG.iterrows():
                    startRow = startDay.replace(hour = int(row['start_time'][:2]), minute=int(row['start_time'][-5:-3]), second=0).timestamp()
                    endRow = startDay.replace(hour = int(row['end_time'][:2]), minute=int(row['end_time'][-5:-3]), second=0).timestamp()
                    if regularHoraire == '' and index ==0:
                        start1 = startRow
                        end1 = endRow

                    if start2 != end2: 
                        if startRow < start1:
                            start1 = startRow

                        if startRow >= end1 and startRow < start2:
                            start2 = startRow

                        if endRow > end1 and endRow <= start2:
                            end1 = endRow

                        if endRow > end2:
                            end2 = endRow
                    else:
                        if index == 0 and start1 == end1:
                            start1 = startRow
                        if startRow < start1:
                            start1 = startRow
                        if endRow > end1:
                            end1 = endRow
                            
                            
            
            if deroCovid:
                if end2 > end1:
                    end2 += deroCovidMins*60
                    if int(end2) >= startDay.timestamp()+86400:
                        end2 = (startDay + timedelta(days=1)-timedelta(minutes=1)).timestamp()
                elif end1 > end2:
                    end1 += deroCovidMins*60
                    if int(end1) >= startDay.timestamp()+86400:
                        end1 = (startDay + timedelta(days=1)-timedelta(minutes=1)).timestamp()
                    
            if deroCovidMorning:
                startdt = datetime.fromtimestamp(start1)
                if startdt.hour != 0:
                    start1 -= deroCovidMorningMins*60   
            
            isConge = getConge(startDay)
            isCongeULG = getCongeULG(startDay)
            isCongeOffice = getCongeOffice(startDay)
            
            if (deroConge and isConge) or (deroCongeULG and isCongeULG) or (deroCongeOffice and isCongeOffice):
                start1=startDay.timestamp()
                start2=startDay.timestamp()
                end1=startDay.timestamp()
                end2=startDay.timestamp()

            ts = int(startDay.timestamp()*1000)
            idCircuit = circuit.replace('/', '_')
            idCircuit = idCircuit.replace(' ', '_')
            idCircuit = idCircuit.replace('.', '_')
            _id = idCircuit+'_'+str(ts)

            action["index"]={"_index":"ulg_horaires_computed","_type":"doc","_id":_id}
            newRec = {"@timestamp": ts, "circuit": circuit, "DB": DB, "start1": int(start1*1000), "start2": int(start2*1000), "end1": int(end1*1000), "end2": int(end2*1000), 'nextGen': nextGen, 'oldGenDecimal': oldGenDecimal}

            bulkbody+=json.dumps(action)+"\r\n"
            bulkbody+=json.dumps(newRec)+"\r\n"






            start = start+timedelta(days=1)
    

    res = es.bulk(body=bulkbody)
    return res
    
def ulg_compute_alone(circuit):
    global es



    start = datetime.now()-timedelta(days=1)
    end = start + timedelta(days=15)
    regulars = es_helper.elastic_to_dataframe(es, 'horaires_ulg_regular')
    #circuit = 'Stat.général'
    DB = 'XX'
    roomRegular = regulars[regulars['circuit'] == circuit].reset_index(drop = True)
    fromULG = es_helper.elastic_to_dataframe(es, 'horaires_ulg_import', start=start, end=end)
    
    
    
    bulkbody = ""


    DB = 'XX'
    deroCovid = False
    deroCovidMins = 0
    deroCovidMorning = False
    deroCovidMorningMins = 0
    deroConge = False
    deroCongeULG = False
    deroCongeOffice = False
    nextGen = False
    oldGenDecimal = False
    logger.info(circuit)

    df = es_helper.elastic_to_dataframe(es, 'ulg_circuits')
    df = df.sort_values(by=['bat_circuit']).reset_index(drop=True)
    circuits = df['bat_circuit'].unique().tolist()

    start = datetime.now()-timedelta(days=1)
    end = start + timedelta(days=15)
    roomsdf = df[df['bat_circuit'] == circuit]
    roomsdf = roomsdf.sort_values(by=['code_identification']).reset_index(drop = True)
    rooms = roomsdf['code_identification'].unique().tolist()
    roomRegular = regulars[regulars['circuit'] == circuit].reset_index(drop = True)
    regularHoraire = ''
    if roomRegular.shape[0] > 0:
        logger.info("++++++++++++++++++++++++++++++++++++++++")
        logger.info(roomRegular)
        logger.info("++++++++++++++++++++++++++++++++++++++++")
        regularHoraire = json.loads(roomRegular.loc[0].to_json())

    while start <= end:
        action = {}
        newRec = {}
        #print(start)
        startDay = start.replace(hour=0, minute=0, second=0, microsecond=0)
        endDay = start.replace(hour=23, minute=59, second=59, microsecond=999999)
        fromULG = es_helper.elastic_to_dataframe(es, 'horaires_ulg_import', start=startDay, end=endDay)
        horaireULG = pd.DataFrame()
        if fromULG.shape[0] > 0:
            horaireULG = fromULG[fromULG['id_room'].isin(rooms)].sort_values(by=['start_datetime', 'end_datetime'])
            logger.info(horaireULG)
        start1 = startDay.timestamp()
        start2 = startDay.timestamp()
        end1 = startDay.timestamp()
        end2 = startDay.timestamp()

        if regularHoraire != '':
            #print(regularHoraire)
            #regularHoraire = json.loads(regularHoraire)
            DB = regularHoraire['DB']
            deroCovid = regularHoraire['dero_covid']
            deroCovidMins = regularHoraire['dero_covid_mins']
            deroCovidMorning = regularHoraire['dero_covid_morning']
            deroCovidMorningMins = regularHoraire['dero_covid_morning_mins']
            deroConge = regularHoraire['dero_holidays']
            deroCongeULG = regularHoraire['dero_holidays_ulg']
            deroCongeOffice = regularHoraire['dero_holidays_office']
            nextGen = regularHoraire['nextGen']
            oldGenDecimal = regularHoraire['oldGenDecimal']
            if startDay.strftime('%w') == '0':
                print('Dimanche')
                start1 = startDay.replace(hour = int(regularHoraire['sundayON1'][:2]), minute= int(regularHoraire['sundayON1'][-2:]), second=0).timestamp()
                start2 = startDay.replace(hour = int(regularHoraire['sundayON2'][:2]), minute= int(regularHoraire['sundayON1'][-2:]), second=0).timestamp()
                end1 = startDay.replace(hour = int(regularHoraire['sundayOFF1'][:2]), minute= int(regularHoraire['sundayOFF1'][-2:]), second=0).timestamp()
                end2 = startDay.replace(hour = int(regularHoraire['sundayOFF2'][:2]), minute= int(regularHoraire['sundayOFF2'][-2:]), second=0).timestamp()

            elif startDay.strftime('%w') == '1':
                print('Lundi')
                start1 = startDay.replace(hour = int(regularHoraire['mondayON1'][:2]), minute= int(regularHoraire['mondayON1'][-2:]), second=0).timestamp()
                start2 = startDay.replace(hour = int(regularHoraire['mondayON2'][:2]), minute= int(regularHoraire['mondayON2'][-2:]), second=0).timestamp()
                end1 = startDay.replace(hour = int(regularHoraire['mondayOFF1'][:2]), minute= int(regularHoraire['mondayOFF1'][-2:]), second=0).timestamp()
                end2 = startDay.replace(hour = int(regularHoraire['mondayOFF2'][:2]), minute= int(regularHoraire['mondayOFF2'][-2:]), second=0).timestamp()

            elif startDay.strftime('%w') == '2':
                print('Mardi')
                start1 = startDay.replace(hour = int(regularHoraire['tuesdayON1'][:2]), minute= int(regularHoraire['tuesdayON1'][-2:]), second=0).timestamp()
                start2 = startDay.replace(hour = int(regularHoraire['tuesdayON2'][:2]), minute= int(regularHoraire['tuesdayON2'][-2:]), second=0).timestamp()
                end1 = startDay.replace(hour = int(regularHoraire['tuesdayOFF1'][:2]), minute= int(regularHoraire['tuesdayOFF1'][-2:]), second=0).timestamp()
                end2 = startDay.replace(hour = int(regularHoraire['tuesdayOFF2'][:2]), minute= int(regularHoraire['tuesdayOFF2'][-2:]), second=0).timestamp()
            elif startDay.strftime('%w') == '3':
                print('Mercredi')
                start1 = startDay.replace(hour = int(regularHoraire['wednesdayON1'][:2]), minute= int(regularHoraire['wednesdayON1'][-2:]), second=0).timestamp()
                start2 = startDay.replace(hour = int(regularHoraire['wednesdayON2'][:2]), minute= int(regularHoraire['wednesdayON2'][-2:]), second=0).timestamp()
                end1 = startDay.replace(hour = int(regularHoraire['wednesdayOFF1'][:2]), minute= int(regularHoraire['wednesdayOFF1'][-2:]), second=0).timestamp()
                end2 = startDay.replace(hour = int(regularHoraire['wednesdayOFF2'][:2]), minute= int(regularHoraire['wednesdayOFF2'][-2:]), second=0).timestamp()
            elif startDay.strftime('%w') == '4':
                print('Jeudi')
                start1 = startDay.replace(hour = int(regularHoraire['thursdayON1'][:2]), minute= int(regularHoraire['thursdayON1'][-2:]), second=0).timestamp()
                start2 = startDay.replace(hour = int(regularHoraire['thursdayON2'][:2]), minute= int(regularHoraire['thursdayON2'][-2:]), second=0).timestamp()
                end1 = startDay.replace(hour = int(regularHoraire['thursdayOFF1'][:2]), minute= int(regularHoraire['thursdayOFF1'][-2:]), second=0).timestamp()
                end2 = startDay.replace(hour = int(regularHoraire['thursdayOFF2'][:2]), minute= int(regularHoraire['thursdayOFF2'][-2:]), second=0).timestamp()
            elif startDay.strftime('%w') == '5':
                print('Vendredi')
                start1 = startDay.replace(hour = int(regularHoraire['fridayON1'][:2]), minute= int(regularHoraire['fridayON1'][-2:]), second=0).timestamp()
                start2 = startDay.replace(hour = int(regularHoraire['fridayON2'][:2]), minute= int(regularHoraire['fridayON2'][-2:]), second=0).timestamp()
                end1 = startDay.replace(hour = int(regularHoraire['fridayOFF1'][:2]), minute= int(regularHoraire['fridayOFF1'][-2:]), second=0).timestamp()
                end2 = startDay.replace(hour = int(regularHoraire['fridayOFF2'][:2]), minute= int(regularHoraire['fridayOFF2'][-2:]), second=0).timestamp()
            elif startDay.strftime('%w') == '6':
                print('Samedi')
                start1 = startDay.replace(hour = int(regularHoraire['saturdayON1'][:2]), minute= int(regularHoraire['saturdayON1'][-2:]), second=0).timestamp()
                start2 = startDay.replace(hour = int(regularHoraire['saturdayON2'][:2]), minute= int(regularHoraire['saturdayON2'][-2:]), second=0).timestamp()
                end1 = startDay.replace(hour = int(regularHoraire['saturdayOFF1'][:2]), minute= int(regularHoraire['saturdayOFF1'][-2:]), second=0).timestamp()
                end2 = startDay.replace(hour = int(regularHoraire['saturdayOFF2'][:2]), minute= int(regularHoraire['saturdayOFF2'][-2:]), second=0).timestamp()


        if horaireULG.shape[0] > 0:
            horaireULG = horaireULG.reset_index(drop=True)
            #print('hooraireULG '+circuit)
            for index, row in horaireULG.iterrows():
                startRow = startDay.replace(hour = int(row['start_time'][:2]), minute=int(row['start_time'][-5:-3]), second=0).timestamp()
                endRow = startDay.replace(hour = int(row['end_time'][:2]), minute=int(row['end_time'][-5:-3]), second=0).timestamp()
                if regularHoraire == '' and index ==0:
                    start1 = startRow
                    end1 = endRow

                if start2 != end2: 
                    if startRow < start1:
                        start1 = startRow

                    if startRow >= end1 and startRow < start2:
                        start2 = startRow

                    if endRow > end1 and endRow <= start2:
                        end1 = endRow

                    if endRow > end2:
                        end2 = endRow
                else:
                    if index == 0 and start1 == end1:
                        start1 = startRow
                    if startRow < start1:
                        start1 = startRow
                    if endRow > end1:
                        end1 = endRow
        
        
        logger.info("*******************************************")
        logger.info(startDay)     
        logger.info(startDay.timestamp()+86400)
        logger.info("*******************************************")
        if deroCovid:
            logger.info("######################################")
            logger.info("DERO COVID")
            logger.info(end1)
            logger.info(end2)
            logger.info("######################################")
            #logger.warning('DeroCovid')
            #logger.warning('End1: '+str(end1))
            #logger.warning('End2: '+str(end2))
            if end2 > end1:
                end2 += deroCovidMins*60
                if int(end2) >= startDay.timestamp()+86400:
                    end2 = (startDay + timedelta(days=1)-timedelta(minutes=1)).timestamp()
            elif end1 > end2:
                end1 += deroCovidMins*60
                if int(end1) >= startDay.timestamp()+86400:
                    end1 = (startDay + timedelta(days=1)-timedelta(minutes=1)).timestamp()
                
        if deroCovidMorning:
            startdt = datetime.fromtimestamp(start1)
            if startdt.hour != 0:
                start1 -= deroCovidMorningMins*60   
        
        isConge = getConge(startDay)
        isCongeULG = getCongeULG(startDay)
        isCongeOffice = getCongeOffice(startDay)
        
        if (deroConge and isConge) or (deroCongeULG and isCongeULG) or (deroCongeOffice and isCongeOffice):
            start1=startDay.timestamp()
            start2=startDay.timestamp()
            end1=startDay.timestamp()
            end2=startDay.timestamp()

        ts = int(startDay.timestamp()*1000)
        idCircuit = circuit.replace('/', '_')
        idCircuit = idCircuit.replace(' ', '_')
        idCircuit = idCircuit.replace('.', '_')
        _id = idCircuit+'_'+str(ts)

        action["index"]={"_index":"ulg_horaires_computed","_type":"doc","_id":_id}
        newRec = {"@timestamp": ts, "circuit": circuit, "DB": DB, "start1": int(start1*1000), "start2": int(start2*1000), "end1": int(end1*1000), "end2": int(end2*1000), 'nextGen': nextGen, 'oldGenDecimal': oldGenDecimal}

        bulkbody+=json.dumps(action)+"\r\n"
        bulkbody+=json.dumps(newRec)+"\r\n"






        start = start+timedelta(days=1)
    

    res = es.bulk(body=bulkbody)
    return res

def messageReceived(destination,message,headers):
    global es
    logger.info("==> "*10)
    logger.info("Message Received %s" % destination)
    logger.info(headers)
    logger.info(message)

    s = message.replace("\'", "\"")
    msg = json.loads(s)

    logger.info(msg)
    #start = datetime.fromtimestamp(int(msg['start']))
    #stop = datetime.fromtimestamp(int(msg['stop']))

    if 'circuit' in msg:
        ulg_compute_alone(msg['circuit'])

    else:
        ulg_computed()
        



if __name__ == '__main__':    
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
    dfConge = None
    dfCongeULG = None
    dfCongeOffice = None
    logger.info (os.environ["ELK_SSL"])
    logger.info(datetime.now())
    logger.info(datetime.now().timestamp())

    if os.environ["ELK_SSL"]=="true":
        host_params = {'host':os.environ["ELK_URL"], 'port':int(os.environ["ELK_PORT"]), 'use_ssl':True}
        es = ES([host_params], connection_class=RC, http_auth=(os.environ["ELK_LOGIN"], os.environ["ELK_PASSWORD"]),  use_ssl=True ,verify_certs=False)
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
                    start = datetime.now()
                    start = start.replace(hour=0,minute=0,second=0, microsecond=0)
                    nextload=datetime.now()+timedelta(seconds=SECONDSBETWEENCHECKS)
                    ulg_computed()
                except Exception as e2:
                    logger.error("Unable to load sites data.")
                    logger.error(e2,exc_info=True)

        except Exception as e:
            logger.error("Unable to send life sign.")
            logger.error(e)
    #app.run(threaded=True,host= '0.0.0.0')