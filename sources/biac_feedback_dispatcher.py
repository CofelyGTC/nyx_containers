"""
BIAC FEEDBACK DISPATCHER
====================================
This modules checks the biac_feedback_status in order to find a final report that has not been sent.
In order to be distributed, the docx and xlsx feedback files must have been received.

Sends:
-------------------------------------

* /queue/NYX_REPORT_STEP1
* /queue/NYX_LOG

Listens to:
-------------------------------------


Collections:
-------------------------------------


VERSION HISTORY
===============

* 08 Aug 2019 0.0.1 **AMA** First Version
* 19 Aug 2019 0.0.2 **AMA** Title of the report is set
* 22 Aug 2019 0.0.3 **AMA** Rename the report type parameter
* 11 Sep 2019 0.0.4 **AMA** Changed the result ready check duration
* 24 Sep 2019 0.0.5 **AMA** Improved the NL translation
* 25 Sep 2019 0.0.6 **AMA** Changed time period to 60 days -> now
* 16 Dec 2019 0.0.7 **VME** Adding tempo (24 hours) before sending the mail once xlsx and docx are here
* 06 Jan 2019 0.0.8 **VME** Changing the .py to launch depending on the report
""" 
import json
import time
import uuid
import json
import pytz
import base64
import tzlocal # $ pip install tzlocal
import platform
import calendar
import threading
import os,logging
import numpy as np
import pandas as pd
from functools import wraps
from datetime import datetime
from datetime import timedelta
from elastic_helper import es_helper 
from lib import pandastoelastic as pte
from amqstompclient import amqstompclient
from logging.handlers import TimedRotatingFileHandler
from logstash_async.handler import AsynchronousLogstashHandler
from elasticsearch import Elasticsearch as ES, RequestsHttpConnection as RC


VERSION="0.0.8"
MODULE="BIAC_FEEDBACK_DISPATCHER"
QUEUE=[]

#######################################################################################
# getEntityObj
#######################################################################################
def getEntityObj(es,entity):
    res=es.search(index="biac_entity",body={"size":100}) 
    for row in res["hits"]["hits"]:
        if row["_source"]["key"]==entity:
            return row["_source"]
    return None

#######################################################################################
# get_month_day_range
#######################################################################################
def get_month_day_range(date):
    date=date.replace(hour = 0)
    date=date.replace(minute = 0)
    date=date.replace(second = 0)
    date=date.replace(microsecond = 0)
    first_day = date.replace(day = 1)
    last_day = date.replace(day = calendar.monthrange(date.year, date.month)[1])
    last_day=last_day+timedelta(1)
    last_day = last_day - timedelta(seconds=1)
    
    local_timezone = tzlocal.get_localzone()
    first_day=first_day.astimezone(local_timezone)
    last_day=last_day.astimezone(local_timezone)
    
    return first_day, last_day

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



def checkCommentsStatus():
    logger.info(">>>>>>>> CHECKING FEEDBACK STATUS")
    start_dt,end_dt=get_month_day_range(datetime.now()-timedelta(days=60))
    end_dt = datetime.now()
    df = es_helper.elastic_to_dataframe(es,query="xlsx: true AND docx: true AND ((-_exists_:sent) OR sent:false)", 
                                    index="biac_feedback_status", start=start_dt, end=end_dt,
                                    timestampfield="reportdate", datecolumns=['xlsx_date', 'docx_date', 'reportdate'])


    if not df.empty:
        now = datetime.now(pytz.timezone('Europe/Paris'))
        date_window = (now - timedelta(hours=3))

        if 'xlsx_date' not in df.columns:
            df['xlsx_date'] = None

        df['xlsx_date'] = df['xlsx_date'].fillna(date_window)

        if 'docx_date' not in df.columns:
            df['docx_date'] = None

        df['docx_date'] = df['docx_date'].fillna(date_window)

        df['last_update'] = df.apply(lambda row: max(row['docx_date'], row['xlsx_date']), axis=1)

        df = df[df['last_update'] <= date_window]

        if not df.empty:
            for index, row in df.iterrows():
                id=row["_id"]
                logger.info("Executing record:"+id)
                rec=es.get(index="biac_feedback_status",doc_type="doc",id=id)["_source"]            
                logger.info(rec)
                rec["sent"]=True            

                creds={"token":"feedbackdispatcher",
                    "user":{"firstname":"Feedback",
                            "lastname":"Dispatcher",
                            "id":"FeedbackDispatcher",
                            "login":"FeedbackDispatcher",
                            "user":"FeedbackDispatcher",
                            "language":"en",
                            "privileges": ["admin"]
                            
                            }}

                report={
                        "description" : "Generates the report of the",
                        "title":"Feedback KPI report ",
                        "exec" : "./reports/pythondef/Lot2KPI.py",
                        "icon" : "plane-departure",
                        "output" : [
                        "docx"
                        ],
                        "parameters" : [
                        {
                            "name" : "param1",
                            "title" : "Date",
                            "type" : "date",
                            "value" : row['reportdate'].isoformat()
                        },
                        {                    
                            "name" : "param2",
                            "title" : "Contract / Technic",
                            "type" : "combo",
                            "value" : row["reporttype"]
                        },
                        {
                            "name" : "param3",
                            "title" : "Type",
                            "type" : "text",
                            "value" : "Final"
                        }
                        ]
                    }

                report['title'] += row['reporttype']
                report['description'] += row['reporttype']

                if row['reporttype'] == 'Lot4 (BACDNB)':
                    report['exec'] = './reports/pythondef/Lot4KPI.py'
                elif row['reporttype'] == 'Lot5':
                    report['exec'] = './reports/pythondef/Lot5KPI.py'
                elif row['reporttype'] == 'Lot6':
                    report['exec'] = './reports/pythondef/Lot6KPI.py'
                elif row['reporttype'] == 'Lot7':
                    report['exec'] = './reports/pythondef/Lot7KPI.py'

                maanden=['Januari',
                    'Februari',
                    'Maart',
                    'April',
                    'Mei',
                    'Juni',
                    'Juli',
                    'Augustus',
                    'September',
                    'Oktober',
                    'November',
                    'December']

                task={
                        "mailAttachmentName": "KPI rapport ${KPI} ${DATE}-val",
                        "attachmentName" : "KPI rapport ${KPI} ${DATE}-val",
                        "icon" : "file-excel",
                        "mailSubject" : "SLA-KPI gevalideerde rapport ${DATE} ${YEAR} ${KPI}",
                        "mailTemplate" : """
                        <body>
                        <h2>KPI gevalideerde rapport ${KPI} </h2>
                        <br/>
                        Goedemorgen,<br/>
                        <br/>
                        Hierbij het gevalideerde rapport van de KPI voor ${KPI}, met inbegrip van de commentaren en de nieuwe berekende scores.<br/>  
                        Percentagewijzigingen, zoals deze tijdens de maandelijkse vergadering door BAC geaccepteerd werden, zijn in de overzichtstabel aangepast.
                        De titel werd door 'Gevalideerd' vervangen en de naam van het rapport werd in "val"  veranderd.
                        Het gevalideerde rapport moet behouden worden.

                        <br/>      

                        <br/>
                        Mvg,<br/>
                        <br/>
                        <br/>
                        <img border="0" width="81" height="42" style="width:.8437in;height:.4375in" id="CofelyLogo" src="cid:cofely_logo.png" alt="cofely_logo.png">
                        <br/>
                        </body>                                        
                        """,
                                "mailingList" : [                            
                                ],          
                            }
                entity=getEntityObj(es,row["reporttype"])
                for dest in entity["header"]["list"]:
                    if "mail" in dest:                    
                        task["mailingList"].append(dest["mail"])

                #task=json.loads(json.dumps(task).replace("${KPI}",entity["title"]).replace("${DATE}",start_dt.strftime("%B")))
                task=json.loads(json.dumps(task).replace("${KPI}",entity["title"]).replace("${DATE}",maanden [row['reportdate'].month-1]).replace("${YEAR}",str(row['reportdate'].year)))

                message={
                        "id":"id_" + str(uuid.uuid4()),
                        "creds":creds,
                        "report":report,
                        "privileges":["admin"],
                        "task":task,
                        "entity":entity,
                        "mailAttachmentName":task["mailAttachmentName"],
                        "mailSubject":task["mailSubject"]
                }
                logger.info(message)
                conn.send_message("/queue/NYX_REPORT_STEP1",json.dumps(message))


                es.index(index="biac_feedback_status",doc_type="doc",id=id,body=rec)




    logger.info("<<<<<<<< CHECKING FEEDBACK STATUS")



################################################################################
def messageReceived(destination,message,headers):
    global es
    records=0
    starttime = time.time()
    logger.info("==> "*10)
    logger.info("Message Received %s" % destination)
    logger.info(headers)        

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
    lastrun=datetime.now()

    while True:
        time.sleep(5)


        try:            
            variables={"platform":"_/_".join(platform.uname()),"icon":"comments"}
            conn.send_life_sign(variables=variables)

            try:
                
                if lastrun < datetime.now():
                    lastrun = datetime.now()+timedelta(seconds=60)
                    checkCommentsStatus()
                    
            except:
                logger.error("Unable to check comments status",exc_info=True)

        except Exception as e:
            logger.error("Unable to send life sign.")
            logger.error(e)
    #app.run(threaded=True,host= '0.0.0.0')
