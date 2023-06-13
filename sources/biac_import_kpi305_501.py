"""
BIAC IMPORT KPI 305 501
====================================

This process listens to two queues, decodes the file included in each message in order to process KPI 305 or 501.
The monthly collection is used for the dashboard with a query equal to: "active:1" in order to retrieve the most recent values.

Listens to:
-------------------------------------

* /queue/BIAC_EXCELS_KPI305
* /queue/BIAC_EXCELS_KPI501


Collections:
-------------------------------------

* **biac_kpi305** (Raw Data)
* **biac_kpi501** (Raw Data)
* **biac_month_kpi305** (Aggregated Data)
* **biac_month_kpi501** (Aggregated Data)



VERSION HISTORY
===============

* 24 Oct 2019 0.0.2 **AMA** Do no longer crash with empty data frames. Add the lot4 if it does not exist yet.
* 12 Nov 2019 1.0.0 **AMA** Match report author and incoming 501/305 value without the white spaces (Van Der Veken issue)
* 09 Dec 2019 1.0.1 **VME** Replacing pte by es_helper
* 10 Dec 2019 1.0.2 **VME** Fix bug due to typo
* 10 Dec 2019 1.0.3 **VME** Undo 1.0.2 that was not a typo
* 11 Dec 2019 1.0.4 **VME** Fix a bug 1.0.4 that prevented lot 1 to work properly
* 13 Jan 2020 1.0.5 **AMA** Load second 501 sheet
"""  

import re
import json
import time
import uuid
import base64
import platform
import threading
import os,logging
import numpy as np
import pandas as pd

from datetime import date
from functools import wraps
from datetime import datetime
from datetime import timedelta
from lib import reporthelper as rp
from elastic_helper import es_helper 
from amqstompclient import amqstompclient
from dateutil.relativedelta import relativedelta
from logging.handlers import TimedRotatingFileHandler
from logstash_async.handler import AsynchronousLogstashHandler
from elasticsearch import Elasticsearch as ES, RequestsHttpConnection as RC


VERSION="1.0.6"
MODULE="BIAC_KPI_305_501_IMPORTER"
QUEUE=["BIAC_EXCELS_KPI305","BIAC_EXCELS_KPI501"]

goodmonth="NA"
rps=None

def reorderMonth(x):
    if x.find("-")==4:
        y=x.split("-")
        return y[1]+"-"+y[0]
    return x

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

def computeReport(row):
    logger.info("Compute Report")
    logger.info("<%s> => <%s>" % (row["CofelyResp"],row["BACService"]))
    
    
    res=rps.getKPI500Config(row["CofelyResp"],row["BACService"])
    if res==None:
        logger.error("BAD" *100)
        return "NA"
    else:
        return res["key"]

def computeReport305(row):
    logger.info("Compute Report")
    logger.info("<%s> => <%s>" % (row["CofelyResp"],row["BACService"]))
    if row['Lot'] == 'Lot 1':
        res = {}
        res["key"] = 'Lot1 (BACHEA)'
    elif row['Lot'] == 'Lot 3':
        res = {}
        res["key"] = 'Lot3 (BACEXT)'
    else:
        res=rps.getKPI500Config(row["CofelyResp"],row["BACService"])
    if res==None:
        logger.error("BAD" *100)
        return "NA"
    else:
        return res["key"]        

def compute305():
    logger.info(">>> COMPUTE 305 STARTED")
    global goodmonth
    time.sleep(3)

    orgdf = es_helper.elastic_to_dataframe(es,"biac_kpi305",query="*",
                                            start=datetime.now()-timedelta(days=365),end=datetime.now()+timedelta(days=365),
                                            sort=None,timestampfield="CheckMonth")
    # orgdf=etp.genericIntervalSearch(es,"biac_kpi305",query="*",doctype="doc",start=datetime.now()-timedelta(days=365),end=datetime.now()+timedelta(days=365),sort=None,timestampfield="CheckMonth")

    months=orgdf["Month"].unique()

    keys=orgdf["key"].unique()

    logger.info(keys)

    alllot={}

    for month in months:
        for key in keys:
            
            regex = r"\(([A-Z]*)\)"
            
            result = re.search(regex, key, re.MULTILINE)
            if result:            
                lot=result.group(1)
            else:
                lot="NA"
            if not lot+month in alllot:
                alllot[lot+month]={"Positive":0,"Negative":0,"Month":month,"key":lot}
                
            
            logger.info("Loading "+month+" Key "+key)
            onekeydf=orgdf[(orgdf["key"]==key) & (orgdf["Month"]==month)]
            good=onekeydf["CountPositives"].sum()
            bad=onekeydf["CountNC"].sum()
            
            alllot[lot+month]["Positive"]+=int(good)
            alllot[lot+month]["Negative"]+=int(bad)
                        
            logger.info("==> %d %d" %(bad,good))
    #        alllot[key+month]={"Positive":int(good),"Negative":int(bad),"Month":month,"key":key}
            
    bulkbody=""     

    es_index="biac_month_kpi305"

    for key in alllot:
        action={}
            
        action["index"] = {"_index": es_index,
            "_type": "doc"
            ,"_id":(alllot[key]["key"]+alllot[key]["Month"]).replace("(","").replace(")","").replace(" ","").lower()
                        }
        
        alllot[key]["Total"]=alllot[key]["Positive"]+alllot[key]["Negative"]
        if alllot[key]["Total"]>0:
            alllot[key]["KPI"]=round(alllot[key]["Positive"]*100/alllot[key]["Total"],2)
        else:
            alllot[key]["KPI"]=100
        
        newrec=json.dumps(alllot[key])
        bulkbody += json.dumps(action)+"\r\n"
        bulkbody += newrec + "\r\n"
            
    res=es.bulk(bulkbody)
    if res["errors"]:
        logger.error("Error in bulk")
        logger.info(res)

    logger.info(">>>>Set Active Fields")
    logger.info("Reset active records ")
    logger.info("=====================")
    time.sleep(3)

    updatequery={
        "script": {
            "inline": "ctx._source.active=0",
            "lang": "painless"
        },
        'query': {'bool': {'must': [{'query_string': {'query': '*'}}]}}
    }
    logger.info("*="*30)
    logger.info(json.dumps(updatequery))

    try:
        resupdate=es.update_by_query(body=updatequery,index="biac_kpi305")
        logger.info(resupdate)
    except Exception as e3:            
        logger.error(e3)   
        logger.error("Unable to update records biac_month_kpi305.") 

    try:
        resupdate=es.update_by_query(body=updatequery,index="biac_month_kpi305")
        logger.info(resupdate)
    except Exception as e3:            
        logger.error(e3)   
        logger.error("Unable to update records biac_month_kpi305.") 

    # Activating current month
    
    logger.info("Waiting for update...")    
    time.sleep(3)
    logger.info("Done")

    updatequery={
        "script": {
            "inline": "ctx._source.active=1",
            "lang": "painless"
        },
        'query': {'bool': {'must': [{'query_string': {'query': 'Month: "'+goodmonth+'"'}}]}}
    }

    logger.info("Update active records")
    logger.info(updatequery)
    try:
        resupdate=es.update_by_query(body=updatequery,index="biac_kpi305")
        logger.info(resupdate)
    except Exception as e3:            
        logger.error(e3)   
        logger.error("Unable to update records biac_month_kpi305.") 

    try:
        resupdate=es.update_by_query(body=updatequery,index="biac_month_kpi305")
        logger.info(resupdate)
    except Exception as e3:            
        logger.error(e3)   
        logger.error("Unable to update records biac_month_kpi305.") 
    logger.info(">>> COMPUTE 305 FINISHED")

def compute501():
    logger.info(">>> COMPUTE 501 STARTED")
    global goodmonth
    time.sleep(5)
    logger.info(">>> COMPUTE 501 WAIT")

    orgdf=es_helper.elastic_to_dataframe(es,"biac_kpi501",query="*",
                                            start=datetime.now()-timedelta(days=365),end=datetime.now()+timedelta(days=365),
                                            sort=None,timestampfield="CheckMonth")
    # orgdf=etp.genericIntervalSearch(es,"biac_kpi501",query="*",doctype="doc",start=datetime.now()-timedelta(days=365),end=datetime.now()+timedelta(days=365),sort=None,timestampfield="CheckMonth")

    months=orgdf["Month"].unique()

    logger.info(">>> KEYS")
    keys=orgdf["key"].unique()
    keys=list(orgdf["key"].unique())
    if "Lot4 (BACDNB)"  not in keys:
        keys.append("Lot4 (BACDNB)")

    logger.info(keys)

    alllot={}

    for month in months:
        for key in keys:
            lot=key[0:4]
            if not lot+month in alllot:
                alllot[lot+month]={"OK":0,"NOK":0,"Month":month,"key":lot}
                
            
            logger.info("Loading "+month+" Key "+key)
            onekeydf=orgdf[(orgdf["key"]==key) & (orgdf["Month"]==month)]
            good=onekeydf["MonitorOK"].sum()
            bad=onekeydf["MonitorNOK"].sum()
            
            alllot[lot+month]["OK"]+=int(good)
            alllot[lot+month]["NOK"]+=int(bad)
                        
            logger.info("==> %d %d" %(bad,good))
            alllot[key+month]={"OK":int(good),"NOK":int(bad),"Month":month,"key":key}
            
    bulkbody=""     

    es_index="biac_month_kpi501"

    for key in alllot:
        action={}
        if len(alllot[key]["key"])==4:
            alllot[key]["key"]+=" (All)"
            
        action["index"] = {"_index": es_index,
            "_type": "doc"
            ,"_id":(alllot[key]["key"]+alllot[key]["Month"]).replace("(","").replace(")","").replace(" ","").lower()
                        }
        
        alllot[key]["Total"]=alllot[key]["OK"]+alllot[key]["NOK"]
        if alllot[key]["Total"]>0:
            alllot[key]["KPI"]=round(alllot[key]["OK"]*100/alllot[key]["Total"],2)
        else:
            alllot[key]["KPI"]=100
        
        newrec=json.dumps(alllot[key])
        bulkbody += json.dumps(action)+"\r\n"
        bulkbody += newrec + "\r\n"

    res=es.bulk(bulkbody)
    if res["errors"]:
        logger.error("Error in bulk")
        logger.info(res)


    logger.info(">>>>Set Active Fields")
    logger.info("Reset active records ")
    logger.info("=====================")
    time.sleep(3)

    updatequery={
        "script": {
            "inline": "ctx._source.active=0",
            "lang": "painless"
        },
        'query': {'bool': {'must': [{'query_string': {'query': '*'}}]}}
    }
    logger.info("*="*30)
    logger.info(json.dumps(updatequery))

    try:
        resupdate=es.update_by_query(body=updatequery,index="biac_kpi501")
        logger.info(resupdate)
    except Exception as e3:            
        logger.error(e3)   
        logger.error("Unable to update records biac_month_kpi501.") 

    try:
        resupdate=es.update_by_query(body=updatequery,index="biac_month_kpi501")
        logger.info(resupdate)
    except Exception as e3:            
        logger.error(e3)   
        logger.error("Unable to update records biac_month_kpi501.") 

    # Activating current month
    
    logger.info("Waiting for update...")    
    time.sleep(3)
    logger.info("Done")

    updatequery={
        "script": {
            "inline": "ctx._source.active=1",
            "lang": "painless"
        },
        'query': {'bool': {'must': [{'query_string': {'query': 'Month: "'+goodmonth+'"'}}]}}
    }

    logger.info("Update active records")
    logger.info(updatequery)
    try:
        resupdate=es.update_by_query(body=updatequery,index="biac_kpi501")
        logger.info(resupdate)
    except Exception as e3:            
        logger.error(e3)   
        logger.error("Unable to update records biac_month_kpi501.") 

    try:
        resupdate=es.update_by_query(body=updatequery,index="biac_month_kpi501")
        logger.info(resupdate)
    except Exception as e3:            
        logger.error(e3)   
        logger.error("Unable to update records biac_month_kpi501.") 

    logger.info(">>> COMPUTE 501 FINISHED")    

################################################################################
def messageReceived(destination,message,headers):
    global es
    global goodmonth

    records=0
    starttime = time.time()
    logger.info("==> "*10)
    logger.info("Message Received %s" % destination)
    logger.info(headers)

    

    result="failed"

    if "CamelSplitAttachmentId" in headers:
        headers["file"] = headers["CamelSplitAttachmentId"]

    if "file" in headers:
        logger.info("File:%s" %headers["file"])
        log_message("Import of file [%s] started." % headers["file"])
        file=headers["file"]

    filedate=file.split(".")[0][-7:]
    goodmonth=filedate.split("-")[1]+"-"+filedate.split("-")[0]
    
    logger.info("MONTH(BEF)     :"+goodmonth)

    gm=datetime.strptime(goodmonth,"%m-%Y")
    gm=gm- relativedelta(months=1)
    goodmonth=datetime.strftime(gm,"%m-%Y")
    logger.info("MONTH(AFT)     :"+goodmonth)


    xlsbytes = base64.b64decode(message)
    f = open('./tmp/excel.xlsx', 'wb')
    f.write(xlsbytes)
    f.close()
    orgfile=file
    file = './tmp/excel.xlsx'

    
    if "501" in destination:
        try:
            #dfdata = pd.read_excel(file, sheet_name='Sheet1')
            dfdatas = pd.ExcelFile(file)

            sheettoload=""
            #for sheet in dfdatas.sheet_names:
            #    if "Lot" in sheet or "Sheet1" in sheet:
            #        sheettoload=sheet
            #        break
            sheettoload = dfdatas.sheet_names[1]

            if sheettoload=="":
                logger.info("No worksheet to load...")
            else:
                logger.info("Loading :"+sheettoload)
               
                dfdata = pd.read_excel(file, sheet_name=sheettoload)
                logger.info(dfdata.shape)
                logger.info(dfdata.head())


                if dfdata.shape[1]==38:
                    newcols=['Month', 'BACID', 'SRPresentation', 'SendDate', 'TypeOfReport',
                            'ReportNumber', 'ReportDate', 'Building', 'Material', 'ExtraData',
                            'Label', 'MonitorOKYN', 'x1', 'Label2', 'LinkPeriod2',
                            'SendDate2', 'Status', 'ReportDate2',
                            'CheckDateSend', 'CheckStatus', 'CheckReportDate',
                            'Month_BacID', 'CheckMonth', 'GlobalCheck', 'CountC',
                            'CountCR', 'CountNC', 'CountPositives', 'Count', 'Dept', 'SubDept',
                            'BACService', 'Company', 'CofelyResp', 'Lot', 'Organism',
                            'MonitorNOK', 'MonitorOK']
                elif dfdata.shape[1]==40:
                    newcols=['Month', 'BACID', 'SRPresentation', 'SendDate', 'TypeOfReport',
                            'ReportNumber', 'ReportDate', 'Building', 'Material', 'ExtraData',
                            'Label','Note','Supervisor', 'MonitorOKYN', 'x1', 'Label2', 'LinkPeriod2',
                            'SendDate2', 'Status', 'ReportDate2',
                            'CheckDateSend', 'CheckStatus', 'CheckReportDate',
                            'Month_BacID', 'CheckMonth', 'GlobalCheck', 'CountC',
                            'CountCR', 'CountNC', 'CountPositives', 'Count', 'Dept', 'SubDept',
                            'BACService', 'Company', 'CofelyResp', 'Lot', 'Organism',                        
                            'MonitorNOK', 'MonitorOK']
                elif dfdata.shape[1]==50:
                    dfdata.columns=[
                            'Month', 'ControleOrg', 'Avid', 'QRcode', 'SRPresentation', 'SendDate', 'TypeOfReport',
                            'ReportNumber', 'ReportDate', 'Building', 'Material', 'ExtraData',
                            'Label', 'Remarque', 'NoteStr', 'MonitorOKYN', 'LinkPeriod2', 'x1', 'x2', 'x3', 'x4', 'x5', 'x6', 
                            'SendDate2', 'Status', 'ReportDate2',
                            'CheckDateSend', 'CheckStatus', 'CheckReportDate',
                            'Month_BacID', 'CheckMonth', 'x7', 'x8', 'GlobalCheck', 'CountC',
                            'CountCR', 'CountNC', 'CountPositives', 'Count', 'Dept', 'SubDept',
                            'BACService', 'Company', 'CofelyResp', 'Lot', 'Organism', 'x9', 'x10',
                            'MonitorNOK', 'MonitorOK']
                    newcols=[
                            'Month', 'ControleOrg', 'Avid', 'QRcode', 'SRPresentation', 'SendDate', 'TypeOfReport',
                            'ReportNumber', 'ReportDate', 'Building', 'Material', 'ExtraData',
                            'Label', 'Remarque', 'NoteStr', 'MonitorOKYN', 'LinkPeriod2',
                            'SendDate2', 'Status', 'ReportDate2',
                            'CheckDateSend', 'CheckStatus', 'CheckReportDate',
                            'Month_BacID', 'CheckMonth', 'GlobalCheck', 'CountC',
                            'CountCR', 'CountNC', 'CountPositives', 'Count', 'Dept', 'SubDept',
                            'BACService', 'Company', 'CofelyResp', 'Lot', 'Organism',
                            'MonitorNOK', 'MonitorOK', 'BAC ID'] 
                    dfdata = dfdata[['Month', 'ControleOrg', 'Avid', 'QRcode', 'SRPresentation', 'SendDate', 'TypeOfReport',
                            'ReportNumber', 'ReportDate', 'Building', 'Material', 'ExtraData',
                            'Label', 'Remarque', 'NoteStr', 'MonitorOKYN', 'LinkPeriod2',
                            'SendDate2', 'Status', 'ReportDate2',
                            'CheckDateSend', 'CheckStatus', 'CheckReportDate',
                            'Month_BacID', 'CheckMonth', 'GlobalCheck', 'CountC',
                            'CountCR', 'CountNC', 'CountPositives', 'Count', 'Dept', 'SubDept',
                            'BACService', 'Company', 'CofelyResp', 'Lot', 'Organism',
                            'MonitorNOK', 'MonitorOK']]        
                    dfdata['BAC ID'] = dfdata['Month_BacID'].apply(lambda x: x.split('_')[1])     
                else:                         # MARCH 2019
                    newcols=['Month', 'ControlOrg', 'Avid', 'QRCode', 'SRPresentation', 'SendDate', 'TypeOfReport','ReportNumber', 'ReportDate', 'Building', 'Material', 'ExtraData','Label','Note','Supervisor','MonitorOKYN', 'LinkPeriod2','Latest_Status','Count of "Opmerkingen"','Count of "Inbreuken"','theorical global status','delta global status','detail quality check','Organism', 'ReportDate2','Status', 'SendDate2','CheckDateSend', 'CheckStatus', 'CheckReportDate','Month_BacID', 'CheckMonth','Report_Type_Prio','Column1', 'GlobalCheck', 'CountC','CountCR', 'CountNC', 'CountPositives', 'Count', 'Dept', 'SubDept','BACService', 'Company', 'CofelyResp', 'Lot', 'Organisme','Building2','DocName','SupervisorNOK','SupervisorOK','Sub Technic','text_VL','text_sending date','text_Report date','text_Global status vs VL','text_Org.','text_Quality check']
                dfdata.to_excel('./kpi501.xlsx')                            
                logger.info(dfdata)
                dfdata.columns=newcols

                dfdata["Month"]=dfdata["Month"].apply(reorderMonth)

                if not dfdata.empty:


                    regex = r"Lot[0-4]"
                    
                    matches = re.finditer(regex, orgfile, re.MULTILINE)
                    lot="NA"
                    for matchNum, match in enumerate(matches, start=1):    
                        lot=match.group()
                        break
                    logger.info("Lot:"+lot)

                    if "4" in str(lot):
                        dfdata["key"]="Lot4 (BACDNB)"                    
                    elif "1" in str(lot):
                        dfdata["key"]="Lot1 (BACHEA)"                    
                    else:
                        dfdata["key"]=dfdata.apply(computeReport,axis=1)


                    dfdata["FileLot"]=lot

        #            dfdata["_id"]=dfdata["Month_BacID"]
                    dfdata["_index"]="biac_kpi501"
                    dfdata["SRPresentation"]=pd.to_datetime(dfdata["SRPresentation"],dayfirst=True)

                    dfdata=dfdata.fillna("")

        #            logger.info(dfdata["FileLot"])

                    for month in dfdata["Month"].unique():
                            deletequery={
                                    "query":{
                                        "bool": {
                                            "must": [
                                            {
                                                "query_string": {
                                                "query": "Month: "+month
                                                }
                                            },
                                            {
                                                "query_string": {
                                                "query": "FileLot: "+lot
                                                }
                                            }
                                            ]
                                        }            
                                    }   
                                }
                            logger.info("Deleting records")
                            logger.info(deletequery)
                            try:
                                resdelete=es.delete_by_query(body=deletequery,index="biac_kpi501")
                                logger.info(resdelete)
                            except Exception as e3:            
                                logger.error(e3)   
                                logger.error("Unable to delete records.")            

                    time.sleep(3)
                    #DELETE COLUMNS WITH MIXED CONTENTS
                    del dfdata["SRPresentation"]
                    del dfdata["ReportDate"]
                    es_helper.dataframe_to_elastic(es, dfdata)
                else:
                    logger.info("Empty Data")        
            compute501()



            conn.send_message('/topic/BIAC_KPI501_IMPORTED', {})
            result="finished"
        except Exception as e:
            endtime = time.time()
            logger.error(e,exc_info=e)
            log_message("Import of file [%s] failed. Duration: %d Exception: %s." % (headers["file"],(endtime-starttime),str(e)))        
    else:
        try:
            dfdata = pd.read_excel(file, sheet_name='Sheet1')        

            if dfdata.shape[1]==36:
                newcols=['Month', 'BACID', 'SRPresentation', 'SendDate', 'TypeOfReport',
                    'ReportNumber', 'ReportDate', 'Building', 'Material', 'ExtraData',
                    'Label', 'MonitorOKYN', 'x1', 'Label2', 'LinkPeriod2',
                    'SendDate2', 'Status', 'ReportDate2',
                    'CheckDateSend', 'CheckStatus', 'CheckReportDate',
                    'Month_BacID', 'CheckMonth', 'GlobalCheck', 'CountC',
                    'CountCR', 'CountNC', 'CountPositives', 'Count', 'Dept', 'SubDept',
                    'BACService', 'Company', 'CofelyResp', 'Lot', 'Organism']
            elif dfdata.shape[1]==42:  # MARCH 2019
                newcols=['Month', 'BACID', 'SRPresentation', 'SendDate', 'TypeOfReport',
                    'ReportNumber', 'ReportDate', 'Building', 'Material', 'ExtraData',
                    'Label','Note','Supervisor','MonitorOKYN', 'x1', 'Label2', 'LinkPeriod2',
                    'SendDate2', 'Status', 'ReportDate2',
                    'CheckDateSend', 'CheckStatus', 'CheckReportDate',
                    'Month_BacID', 'CheckMonth', 'GlobalCheck', 'CountC',
                    'CountCR', 'CountNC', 'CountPositives', 'Count', 'Dept', 'SubDept',
                    'BACService', 'Company', 'CofelyResp', 'Lot', 'Organism',
                    'Building2','DocName','SupervisorNOK','SupervisorOK']
            elif dfdata.shape[1]==52:  # JANUARY 2023
                newcols=['Month', 'ControlOrg', 'Avid', 'QRCode', 'SRPresentation', 'SendDate', 'TypeOfReport',
                    'ReportNumber', 'ReportDate', 'Building', 'Material', 'ExtraData',
                    'Label','Note','Supervisor','MonitorOKYN', 'LinkPeriod2',
                    'Latest_Status'
                    ,'Count of "Opmerkingen"','Count of "Inbreuken"','theorical global status','delta global status','detail quality check','Organism'
                    , 'ReportDate2',
                    'Status', 'SendDate2',
                    'CheckDateSend', 'CheckStatus', 'CheckReportDate',
                    'Month_BacID', 'CheckMonth','Report_Type_Prio','Column1', 'GlobalCheck', 'CountC',
                    'CountCR', 'CountNC', 'CountPositives', 'Count', 'Dept', 'SubDept',
                    'BACService', 'Company', 'CofelyResp', 'Lot', 'Organisme',
                    'Building2','DocName','SupervisorNOK','SupervisorOK','Sub Technic']                    
            else:
                newcols=['Month', 'BACID', 'SRPresentation', 'SendDate', 'TypeOfReport',
                    'ReportNumber', 'ReportDate', 'Building', 'Material', 'ExtraData',
                    'Label','Note','Supervisor','MonitorOKYN', 'x1', 'Label2', 'LinkPeriod2',
                    'SendDate2', 'Status', 'ReportDate2',
                    'CheckDateSend', 'CheckStatus', 'CheckReportDate',
                    'Month_BacID', 'CheckMonth', 'GlobalCheck', 'CountC',
                    'CountCR', 'CountNC', 'CountPositives', 'Count', 'Dept', 'SubDept',
                    'BACService', 'Company', 'CofelyResp', 'Lot', 'Organism']


            dfdata.columns=newcols

            dfdata["key"]=dfdata.apply(computeReport305,axis=1)

            

            #dfdata["_id"]=dfdata["Month_BacID"]
            dfdata["_index"]="biac_kpi305"
            dfdata["SRPresentation"]=pd.to_datetime(dfdata["SRPresentation"],dayfirst=True)

#DELETE COLUMNS WITH MIXED CONTENTS
            del dfdata["ReportDate"]
            del dfdata["SendDate"]

            dfdata=dfdata.fillna("")

            logger.info(dfdata)


            for month in dfdata["Month"].unique():
                    deletequery={
                            "query":{
                                "bool": {
                                    "must": [
                                    {
                                        "query_string": {
                                        "query": "Month: "+month
                                        }
                                    }
                                    ]
                                }            
                            }   
                        }
                    logger.info("Deleting records")
                    logger.info(deletequery)
                    try:
                        resdelete=es.delete_by_query(body=deletequery,index="biac_kpi305")
                        logger.info(resdelete)
                    except Exception as e3:            
                        logger.error(e3)   
                        logger.error("Unable to delete records.")            

            time.sleep(3)
            logger.info("****="*30)
            dfdata=dfdata.fillna("")
            logger.info(dfdata)
            logger.info(dfdata.dtypes)
            logger.info("****="*30)

            es_helper.dataframe_to_elastic(es, dfdata)        
            compute305()

            conn.send_message('/topic/BIAC_KPI305_IMPORTED', {})
            result="finished"
        except Exception as e:
            endtime = time.time()
            logger.error(e,exc_info=e)
            log_message("Import of file [%s] failed. Duration: %d Exception: %s." % (headers["file"],(endtime-starttime),str(e)))        
    


    endtime = time.time()    
    try:
        log_message("Import of file [%s] %s. Duration: %d Records: %d." % (headers["file"],result,(endtime-starttime),df.shape[0]))         
    except:
        log_message("Import of file [%s] %s. Duration: %d." % (headers["file"],result,(endtime-starttime)))    
    
        

    logger.info("<== "*10)

rps=None

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

    rps=rp.ReportStructure(es)

    logger.info("AMQC_URL          :"+os.environ["AMQC_URL"])
    while True:
        time.sleep(5)
        try:            
            variables={"platform":"_/_".join(platform.uname()),"icon":"shield-alt"}
            conn.send_life_sign(variables=variables)
        except Exception as e:
            logger.error("Unable to send life sign.")
            logger.error(e)
    #app.run(threaded=True,host= '0.0.0.0')
