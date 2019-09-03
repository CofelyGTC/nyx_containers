import json
import time
import uuid
import json
import math
import pytz
import base64
#import tzlocal
import calendar
import platform
import threading
import os,logging
import pandas as pd
from dateutil.tz import tzlocal
from tzlocal import get_localzone
from sqlalchemy import create_engine
from lib import elastictopandas as etp

from dateutil.relativedelta import relativedelta
from logging.handlers import TimedRotatingFileHandler
from amqstompclient import amqstompclient
from datetime import datetime
from datetime import timedelta
from functools import wraps
from elasticsearch import Elasticsearch as ES, RequestsHttpConnection as RC
from logstash_async.handler import AsynchronousLogstashHandler
from lib import pandastoelastic as pte
import numpy as np


VERSION="0.3.12"
MODULE="BIAC_MONTH_INSTAWORK"
QUEUE=["/topic/BIAC_INSTAWORK_IMPORTED"]



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



def get_month_day_range(date):
    date=date.replace(hour = 0)
    date=date.replace(minute = 0)
    date=date.replace(second = 0)
    first_day = date.replace(day = 1)
    last_day = date.replace(day = calendar.monthrange(date.year, date.month)[1])
    last_day=last_day+timedelta(1)
    last_day=last_day-timedelta(hours=(datetime.now().hour-datetime.utcnow().hour))
    return first_day, last_day

def compute_days_ago_flat5(row):
    today=datetime.now().date()
    
    val=((today)-row["Reference_Date"].date()).days
    a=math.floor(val)
    if a>=5:
        return 5
    return a 

def compute_days_ago_flat7(row):
    today=datetime.now().date()
    
    val=((today)-row["Reference_Date"].date()).days
    a=math.floor(val)
    if a>=7:
        return 7
    return a     

def compute_days_ago5(row):
    today=datetime.now().date()
    
    #val=(today-row["Reference_Date"].to_pydatetime()).days
    val=((today)-row["Reference_Date"].date()).days
    a=math.floor(val)
    if a>=5:
        return 5
    return a    

def compute_days_ago7(row):
    today=datetime.now().date()
    
    #val=(today-row["Reference_Date"].to_pydatetime()).days
    val=((today)-row["Reference_Date"].date()).days
    a=math.floor(val)
    if a>=7:
        return 7
    return a   

def compute_days_future5(row):
    #print(">>> COMPUTE")
    #print(row["Reference_Date"])

    today=datetime.now().date()

    val=(row["Reference_Date"].date()-(today)).days
    #print(row["Reference_Date"]+timedelta(hours=(datetime.now().hour-datetime.utcnow().hour)))
    #print(val)
    if(math.floor(val/5)*5==30):
     #   print(">D28")
        return (">D25")
    #print("D+%d->D+%d" % (math.floor(val/7)*7,math.floor((val+7)/7)*7))
    return ("D+%d->D+%d" % (math.floor(val/5)*5,math.floor((val+5)/5)*5))

def compute_days_future7(row):
    #print(">>> COMPUTE")
    #print(row["Reference_Date"])

    today=datetime.now().date()

    val=(row["Reference_Date"].date()-(today)).days
    #print(row["Reference_Date"]+timedelta(hours=(datetime.now().hour-datetime.utcnow().hour)))
    #print(val)
    if(math.floor(val/7)*7==28):
     #   print(">D28")
        return (">D28")
    #print("D+%d->D+%d" % (math.floor(val/7)*7,math.floor((val+7)/7)*7))
    return ("D+%d->D+%d" % (math.floor(val/7)*7,math.floor((val+7)/7)*7))


def compute(query,lot):
    log_message("Instawork computation started.")

    starttimeproc = time.time()
    reserrors=[]        

    today=(datetime.now()-timedelta(days=0)).replace(hour = 0).replace(minute = 0).replace(second = 0).replace(microsecond = 0)

    if lot=="6":
        logger.info(">>> USING compute_status7")
        compute_status=compute_status7
        compute_days_ago=compute_days_ago7
        compute_days_future=compute_days_future7
        compute_days_ago_flat=compute_days_ago_flat7
        endtime=today-timedelta(days=7)-timedelta(minutes=1)
    else:
        logger.info(">>> USING compute_status5")
        compute_status=compute_status5
        compute_days_ago=compute_days_ago5
        compute_days_future=compute_days_future7 # USE A 7 days limit
        compute_days_ago_flat=compute_days_ago_flat5
        endtime=today-timedelta(days=5)-timedelta(minutes=1)
    
    starttime,endofmonth=get_month_day_range(endtime)
    starttimecm,endofmonthcm=get_month_day_range(today)

    #overduestarttime=get_month_day_range(starttime-timedelta(days=1))[0]
    overduestarttime=starttime- relativedelta(years=1)
    logger.info("today")
    logger.info(overduestarttime)
    logger.info("overduestarttime")
    logger.info(overduestarttime)
    logger.info("starttime")
    logger.info (starttime)
    logger.info("endtime")
    logger.info (endtime)
    logger.info("starttimecm")
    logger.info (starttimecm)
    logger.info("endofmonthcm")
    logger.info (endofmonthcm)
    logger.info("endofmonth")
    logger.info (endofmonth)

    df2=etp.genericIntervalSearch(es,"biac_instawork",query="Order_Type: PM"+query,doctype="doc",start=starttime,end=endtime,sort=None,timestampfield="Reference_Date"
                ,datecolumns=["Reference_Date","Unused_Date","Finish_Date"])
    

    if(df2.shape[0]>0):
        
        df2["Status"]=df2.apply(compute_status, axis=1)
        df2["Computation_Start_Date"]=starttime
        df2["Computation_End_Date"]=endtime
        df2["Record_Type"]="record"
        df2["DaysAgoInt"]=df2.apply(compute_days_ago,axis=1)

    df2cm=etp.genericIntervalSearch(es,"biac_instawork",query="Order_Type: CM"+query,doctype="doc",start=starttimecm,end=today,sort=None,timestampfield="Reference_Date"
                ,datecolumns=["Reference_Date","Unused_Date","Finish_Date"])
    

    #logger.info(df2cm)
    if(df2cm.shape[0]>0):
        df2cm["Status"]=df2cm.apply(compute_status, axis=1)
        df2cm["Computation_Start_Date"]=starttime
        df2cm["Computation_End_Date"]=datetime.now()
        df2cm["Record_Type"]="record"
        df2cm["DaysAgoInt"]=df2cm.apply(compute_days_ago,axis=1)

        logger.info(df2cm[df2cm["Status"]!="Done OK"][["Order","Status"]])

    # KPI 2

    dfkpi=df2.append(df2cm,sort=False)
    
    logger.info(dfkpi)
    logger.info("dfkpi-"*100)


    if dfkpi.shape[0]>0:
        dfkpi["Good"]=dfkpi["Status"].apply(lambda x:1 if "Done OK" in x else 0)
        dfkpi["Records"]=1
        #logger.info(dfkpi[["Order_Type","Status",'Good','Records']])

        dfkpi2=dfkpi.groupby(['Order_Type']).agg({'Good':['sum'],'Records':['sum']})
        #logger.info(dfkpi2)
        dfkpi2=dfkpi2.reset_index()
        dfkpi2.columns=['_'.join(tuple(map(str, t))) for t in dfkpi2.columns.values]
        dfkpi2["Percentage"]=dfkpi2["Good_sum"]/dfkpi2["Records_sum"]
        dfkpi2=dfkpi2.set_index('Order_Type_')
        dfkpi2ht=dfkpi2.to_dict('index')

        if "PM" not in dfkpi2ht:
            dfkpi2ht["PM"]={'Good_sum': 1, 'Records_sum': 1, 'Percentage': 1}
        if "CM" not in dfkpi2ht:
            dfkpi2ht["CM"]={'Good_sum': 1, 'Records_sum': 1, 'Percentage': 1}

    else:
        dfkpi2ht={}
        dfkpi2ht["PM"]={'Good_sum': 0, 'Records_sum': 0, 'Percentage': 0}
        dfkpi2ht["CM"]={'Good_sum': 0, 'Records_sum': 0, 'Percentage': 0}

    computation=datetime.now()
    # COMPUTE OVERDUE
    dfoverdue=etp.genericIntervalSearch(es,"biac_instawork",query="Order_Type: PM"+query,doctype="doc",start=overduestarttime,end=starttime,sort=None,timestampfield="Reference_Date"
            ,datecolumns=["Reference_Date","Unused_Date","Finish_Date"])

    if len(dfoverdue)>0:
        dfoverdue["Status"]=dfoverdue.apply(compute_status, axis=1)

        dfoverdue2=dfoverdue[dfoverdue["Status"]=="Not Done NOK"]
        dfoverdue3=dfoverdue2.groupby('Order_Type').size().reset_index(name='counts')
        dfoverdue3=dfoverdue3.set_index('Order_Type')
        #logger.info("Overdue>>> "*10)
        #logger.info(dfoverdue2)
        #logger.info("Overdue=== "*10)
        #logger.info(dfoverdue3)
        overdueht=dfoverdue3.to_dict('index')
    else:
        overdueht={}
    

    
    # COMPUTE TO BE COMPLETED THIS MONTH
    logger.info("A Completed>>> "*10)
    curstarttime,curendofmonth=get_month_day_range(datetime.now())
    logger.info(curendofmonth)
    
    
    dftobecompleted=etp.genericIntervalSearch(es,"biac_instawork",query="Order_Type: PM"+query,doctype="doc",start=today+timedelta(days=1),end=curendofmonth,sort=None,timestampfield="Reference_Date"
        ,datecolumns=["Reference_Date","Unused_Date","Finish_Date"])

    #logger.info(dftobecompleted[["Reference_Date","Order"]])
    #logger.info(dftobecompleted[["Reference_Date"]].unique())
    # test=dftobecompleted.copy()
    # test["Date"]=test["Reference_Date"].dt.strftime("%d-%b-%Y")
    # test2=test.groupby("Date").agg({'Date':['count']}).rename(columns={'Date': 'count'}).reset_index()
    # logger.info(test2)
    # test2=test2.reset_index()
    # logger.info(test2.columns)
    # logger.info(test2["count"].sum())
    #logger.info(test2[["Date"]].unique())


    if(dftobecompleted.shape[0]>0):
        
        dftobecompleted["Status"]=dftobecompleted.apply(compute_status, axis=1)


        dftobecompleted2=dftobecompleted[(dftobecompleted["Status"]=="Not Done NOK") | (dftobecompleted["Status"]=="Not Done OK")]

        dftobecompleted3=dftobecompleted2.groupby('Order_Type').size().reset_index(name='counts')
        dftobecompleted3=dftobecompleted3.set_index('Order_Type')
        tobecompletedht=dftobecompleted3.to_dict('index')

    

    else:
        tobecompletedht={}

    # MONTH TO DATE
    
    dfmonthtodate=etp.genericIntervalSearch(es,"biac_instawork",query="Order_Type: PM"+query,doctype="doc",start=starttimecm,end=today+timedelta(days=1)-timedelta(seconds=1)
            ,sort=None,timestampfield="Reference_Date",datecolumns=["Reference_Date","Unused_Date","Finish_Date"])

    
    # print("==========>"*10)
    # logger.info(starttime)
    # logger.info(starttimecm)
    # logger.info(today+timedelta(days=1)-timedelta(seconds=1))
    # print(dfmonthtodate)
    # print("==========>"*10)
    if(dfmonthtodate.shape[0]>0):


        dfmonthtodate["Status"]=dfmonthtodate.apply(compute_status, axis=1)        
        dfmonthtodate["DaysAgoInt"]=dfmonthtodate.apply(compute_days_ago_flat,axis=1)
        dfmonthtodate["Record_Type"]="monthtodate"
        



    # DATE TO MONTH CM
    dfmonthtodatecm=etp.genericIntervalSearch(es,"biac_instawork",query="Order_Type: CM"+query,doctype="doc",start=today,end=endofmonth+timedelta(days=31*12),sort=None,timestampfield="Reference_Date"
            ,datecolumns=["Reference_Date","Unused_Date","Finish_Date"])
    logger.info("===> CM"*30)
    logger.info(dfmonthtodatecm)

    
    if(dfmonthtodatecm.shape[0]>0):

        dfmonthtodatecm["Status"]=dfmonthtodatecm.apply(compute_status, axis=1)
        dfmonthtodatecm["DaysAgoStr"]=dfmonthtodatecm.apply(compute_days_future,axis=1)

        dfmonthtodatecm["Record_Type"]="monthtodate"

    # LAST YEAR NOT DONE
    cmnotdone=0

    dflastyearcm=etp.genericIntervalSearch(es,"biac_instawork",query="Order_Type: CM"+query,doctype="doc",start=curstarttime-timedelta(days=365),end=curstarttime,sort=None,timestampfield="Reference_Date"
            ,datecolumns=["Reference_Date","Finish_Date"])
    logger.info("===>"*30)
    if(dflastyearcm.shape[0]>0):

        dflastyearcm["Status"]=dflastyearcm.apply(compute_status, axis=1)
        
        dflastyearcm=dflastyearcm[dflastyearcm["Status"].str.contains("Not Done")]
        
        cmnotdone=len(dflastyearcm)

    # LAST YEAR NOT DONE VINCOTTE
    cmnotdonevin=0

    dflastyearcm=etp.genericIntervalSearch(es,"biac_instawork",query="Order_Type: CM  AND Origin:Vinçotte"+query,doctype="doc",start=curstarttime-timedelta(days=365),end=curstarttime,sort=None,timestampfield="Reference_Date"
            ,datecolumns=["Reference_Date","Finish_Date"])
    logger.info("===>"*30)
    if(dflastyearcm.shape[0]>0):
        dflastyearcm['Reference_Date'] =  pd.to_datetime(dflastyearcm['Reference_Date'])
        dflastyearcm['Finish_Date'] =  pd.to_datetime(dflastyearcm['Finish_Date'])

        dflastyearcm["Status"]=dflastyearcm.apply(compute_status, axis=1)
        
        dflastyearcm=dflastyearcm[dflastyearcm["Status"].str.contains("Not Done")]
        
        cmnotdonevin=len(dflastyearcm)        

    bulkbody = ''
    bulkres = ''
    reserrors= []
    es_index="biac_month_instawork"

    logger.info("Deleting index biac_instawork_month for the current lot:"+lot)

    deletequery='{"size":10000,"query":{"bool":{"must":[{"term":{"Lot":{"value":"'+lot+'"}}}]}},"_source":false}'
    logger.info(deletequery)
    try:
        resdelete=es.delete_by_query(body=deletequery,index="biac_month_instawork")
        logger.info(resdelete)
    except Exception as e3:            
        logger.error(e3)   
        logger.error("Unable to delete records.")            

    logger.info("Sleeping 5 seconds")
    time.sleep(5)


    #res=es.indices.delete(index=es_index, ignore=[400, 404]) 

    #SUMMARY ROWS

    logger.info(dfkpi2ht)
    summary=[]
    for ordertype in ["CM","PM","FUM","CMVIN"]:
        action = {}
        action["index"] = {"_index": es_index,
            "_type": "doc"}
        overdue=overdueht.get(ordertype,{'counts': 0})["counts"]
        tobecompleted=tobecompletedht.get(ordertype,{'counts': 0})["counts"]    
        
        kpi2=dfkpi2ht.get(ordertype,{"Percentage":1})["Percentage"]*100
        
        

        if ordertype=="PM":
            sumstart=datetime.timestamp(starttime)*1000
            sumend=datetime.timestamp(endtime)*1000
            
            
        else:            
            sumstart=datetime.timestamp(starttimecm)*1000
            if datetime.now().day>1:
                sumend=datetime.timestamp(datetime.now()-timedelta(days=1))*1000
            else:
                sumend=datetime.timestamp(datetime.now())*1000
            

        

        # newrec={"Record_Type":"summary","Reference_Date":datetime.timestamp(starttime)*1000,"Computation_Start_Date":sumstart,
        #         "Computation_End_Date":sumend,"Order_Type":ordertype,
        #     "Overdue":overdue,"To_Be_Completed":tobecompleted,"Computation_Execution_Date":datetime.timestamp(computation)*1000
        #     ,"KPI2":kpi2,"Lot":int(df2["Lot"].unique()[0])}
        
        newrec={"Record_Type":"summary","Reference_Date":datetime.timestamp(starttime)*1000,"Computation_Start_Date":sumstart,
                "Computation_End_Date":sumend,"Order_Type":ordertype,
            "Overdue":overdue,"To_Be_Completed":tobecompleted,"Computation_Execution_Date":datetime.timestamp(computation)*1000
                ,"KPI2":kpi2,"Lot":lot}

        if ordertype=="CM":
            newrec["KPI2VIN"]=dfkpi2ht.get("CMVIN",{"Percentage":1})["Percentage"]*100


        if ordertype=="CM":
            newrec["CM_Not_Done"]=cmnotdone
            newrec["CM_Not_Done_VIN"]=cmnotdonevin
        if ordertype=="CMVIN":
            newrec["CM_Not_Done"]=cmnotdonevin            
        

        summary.append(newrec)
        bulkbody += json.dumps(action)+"\r\n"
        bulkbody += json.dumps(newrec) + "\r\n"

    dfsum=pd.DataFrame(summary)

    dfsum['Computation_Start_Date'] =  pd.to_datetime(dfsum['Computation_Start_Date']/1000, unit='s')
    dfsum['Computation_End_Date'] =  pd.to_datetime(dfsum['Computation_End_Date']/1000, unit='s')
    dfsum['Computation_Execution_Date'] =  pd.to_datetime(dfsum['Computation_Execution_Date']/1000, unit='s')

    
    #WRITE RECORS  
    full=dfmonthtodate.append(df2,sort=False).append(df2cm,sort=False).append(dfmonthtodatecm,sort=False)
    
    try:
        full["id"]=full["_id"]
    except Exception as e:
        print(e)
        
    try:
        del full["_id"]
    except Exception as e:
        print(e)

    try:    
        del full["_index"]
    except Exception as e:
        print(e)

    full['json'] = full.apply(lambda x: x.to_json(), axis=1)
        
    for index, row in full.iterrows():
        
        action = {}
        action["index"] = {"_index": es_index,
            "_type": "doc"}
        newrec=row["json"]
        bulkbody += json.dumps(action)+"\r\n"
        bulkbody += newrec + "\r\n"

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

    postgresqlurl=('postgresql://%s:%s@%s:%s/nyx' %(os.environ["PSQL_LOGIN"],os.environ["PSQL_PASSWORD"],os.environ["PSQL_URL"],os.environ["PSQL_PORT"]))
    logger.info("SQL "*30)
    #logger.info(postgresqlurl)

    engine = create_engine(postgresqlurl)

    logger.info("*-"*200)


    full.to_sql("insta_work_kpi", engine, if_exists='replace')
    dfsum.to_sql("insta_work_sum", engine, if_exists='replace')


    endtime = time.time()

    if len(reserrors)>0:
        log_message("Instawork computation failed. Duration: %d. %d records were not saved." % ((endtime-starttimeproc),len(reserrors)))        
    
    log_message("Instawork computation finished. Duration: %d." % ((endtime-starttimeproc))) 

################################################################################
def messageReceived(destination,message,headers):
    global es
    records=0
    logger.info("==> "*10)
    logger.info("Message Received %s" % destination)
    logger.info(headers)
    logger.info(message)
    mesobj=json.loads(message)
    lot=mesobj["lot"]


    logger.info("*************")
    logger.info("Compute Lot "+str(lot))
    logger.info("*************")
    try:
        compute(" AND Lot:"+str(lot),str(lot))   
    except Exception as e:
        logger.error("Unable to compute Lot "+str(lot),exc_info=True)

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
                ,"heartbeats":(120000,120000),"earlyack":True}
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
    while True:
        time.sleep(5)
        try:            
            variables={"platform":"_/_".join(platform.uname()),"icon":"wrench"}
            conn.send_life_sign(variables=variables)
        except Exception as e:
            logger.error("Unable to send life sign.")
            logger.error(e)
    #app.run(threaded=True,host= '0.0.0.0')
