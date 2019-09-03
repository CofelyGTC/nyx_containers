import json
import time
import uuid
import base64
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


VERSION="1.0.14"
MODULE="BIAC_MAXIMO_CALCULATED"
QUEUE=["/topic/BIAC_MAXIMO_IMPORTED"]


########### QUERIES BODY #######################

query = {
  "range": {
    "scheduledStart": {
      "gte": "01/12/2018",
      "lt": "01/01/2019",
      "format": "dd/MM/yyyy HH:mm"
    }
  }
}

query302 = {
  "range": {
    "actualStart": {
      "gte": "01/12/2018",
      "lt": "01/01/2019",
      "format": "dd/MM/yyyy"
    }
  }
}

startquery = {
         "match": {
            "KPI301": {
              "query": "Started OK"
              }
          }
        }

doneokkpiquery = {
         "match": {
            "KPI302": {
              "query": "Done OK"
              }
          }
        }
getquery = {'query': {'bool': {'must': [{'range': {'scheduledStart': {'format': 'dd/MM/yyyy',
       'gte': '01/01/2019',
       'lt': '03/01/2019'}}}]}}}


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


################################################################################
def getTimestamp(timeD):
    logger.info(timeD)
    ts = 0
    try:
        dtt = timeD.timetuple()
        ts = int(time.mktime(dtt))
    except Exception as er:
        logger.error(er)
        logger.error(str(timeD))
    return ts


################################################################################
def getGTECurrentmonth(now):
    month = now.month
    year = now.year
    returndate = datetime(year, month, 1)
    return returndate


################################################################################
def getLTECurrentmonth(now):
    month = now.month
    year = now.year
    if month != 12:
        month = month + 1
    else:
        month = 1
        year = year +1
    returndate = datetime(year, month, 1)
    return returndate

################################################################################
def getGTELastmonth(now):
    month = now.month
    year = now.year
    if month == 1:
        month = 12
        year = year - 1
    else:
        month = month -1
    returndate = datetime(year, month, 1)
    return returndate


################################################################################
def getLTELastmonth(now):
    month = now.month
    year = now.year
    returndate = datetime(year, month, 1)
    return returndate


################################################################################
def getDisplayStart(now):
    start = ''
    if 1 < now.day < 15:
        month = 12
        year = now.year - 1
        if now.month != 1:
            month = now.month -1
            year = now.year
        start = datetime(year, month, 1, 0, 0, 0, 0)
    else:
        start = datetime(now.year, now.month, 1, 0, 0, 0, 0)

    return int(start.timestamp())

################################################################################
def getDisplayStop(now):
    stop = ''
    datestop = now - timedelta(days=14)
    datestop = datestop.replace(hour=0)
    datestop = datestop.replace(minute=0)
    datestop = datestop.replace(second=0)
    datestop = datestop.replace(microsecond=0)
    return datestop

################################################################################
def getDisplayKPI302(now):
    KPI302Start = datetime(now.year, now.month, 1, 0, 0, 0, 0)
    #return int(KPI302Start.timestamp())
    return KPI302Start

################################################################################
def getQueryFuture2(now):
    day = 1
    month = now.month
    year = now.year
    if month == 12:
        month = 1
        year += 1
    else:
        month += 1

    stopfuture = datetime(year, month, day)
    stopdeltafuture = stopfuture - timedelta(days=1)

    strQueryFuture = str(stopdeltafuture.day).zfill(2)+'/'+str(stopdeltafuture.month).zfill(2)+'/'+str(stopdeltafuture.year) + " 22:00"

    return strQueryFuture

################################################################################
def messageReceived(destination,message,headers):
    global es
    records=0
    starttime = time.time()
    logger.info("==> "*10)
    logger.info("Message Received %s" % destination)
    logger.info(headers)

    now = datetime.now()
    gtedate = ''
    ltedate= ''


    if now.day > 14:
        gtedate = getGTECurrentmonth(now)
        ltedate = getLTECurrentmonth(now)
    else:
        gtedate = getGTELastmonth(now)
        ltedate = getLTELastmonth(now)

    ltedate = now - timedelta(days=14)
    gtedatequery = gtedate - timedelta(days=1)
    query['range']['scheduledStart']['lt'] = ltedate.strftime("%d/%m/%Y") + " 22:00"
    query['range']['scheduledStart']['gte'] = gtedatequery.strftime("%d/%m/%Y") + " 22:00"

    es.indices.delete(index="biac_maximo_monthly", ignore=[400, 404])

    screens = ["ext_building", "fire fighting", "sani", "hvacpa", "heating", "hvacpb", "elec", "access","cradle", "dnb", "bacfir"]

    for screen in screens:

        logger.info("calculate contract : " + screen)
        
        bulkbody = ''

        contractQuery = {
            "match": {
            "screen_name": {
              "query": screen
              }
          }
        }

        if screen == 'bacfir':
            contractQuery = {
            "match": {
            "contract": {
              "query": 'BACFIR'
              }
          }
        }

        contract = ''
        lot = 0
        
        if screen == "ext_building":
            contract='BACEXT'
            lot = 3
        if screen == "fire fighting":
            contract = "BACFIR"
            lot = 2
        if screen == "bacfir":
            contract = "BACFIR"
            lot = 2
        if screen == "cradle":
            contract = "BACFIR"
            lot = 2
        if screen == "access":
            contract = "BACFIR"
            lot = 2
        if screen ==  "sani":
            contract = "BACSAN"
            lot = 2
        if screen ==  "hvacpa":
            contract = "BACSAN"
            lot = 2
        if screen ==  "heating":
            contract = "BACHEA"
            lot = 1
        if screen ==  "hvacpb":
            contract = "BACHVA"
            lot = 2
        if screen ==  "elec":
            contract = "BACELE"
            lot = 2
        if screen == "dnb":
            contract ='BACDNB'
            lot = 4

        #if contract == "HVACVPA":
        #    contractQuery = {
        #    "match": {
        #    "pmExecutionTeam": {
        #      "query": "6200"
        #      }
        #  }
        #}

        #if contract == "SANI":
        #    contractQuery = {
        #        "bool": {
        #        "should": [
        #            {
        #            "match_phrase": {
        #                "pmExecutionTeam": "6197"
        #            }
        #            },
        #            {
        #            "match_phrase": {
        #                "pmExecutionTeam": "6483"
        #            }
        #            }
        #        ],
        #        "minimum_should_match": 1
        #    }
        #}
        technic = screen

        if screen == 'access':
            technic = 'acces'

        getquery =  {
            "query": {
                "bool": {
                "must": [query, contractQuery]
                }
            }
        }

        logger.info(getquery)

        datatotal = es.search(index="biac_maximo", body=getquery, size=10000)

        getokquery = {
            "query": {
                "bool": {
                "must": [query, startquery, contractQuery]
                }
            }
        }

        logger.info(getokquery)

        dataok = es.search(index="biac_maximo", body=getokquery, size=10000)

        oksize = len(dataok['hits']['hits'])
        size = len(datatotal['hits']['hits'])
        ts = str(int(now.timestamp()))
        bulkbody = ''
        bulkres = ''
        action = {}
        action["index"] = {"_index": "biac_maximo_monthly", "_type": "doc", "_id": 'KPI301'+screen+ts}
        average = 1
        if size != 0:
            average = oksize / size
        percent = average * 100

        gtedate = gtedate.replace(hour=0, minute=0, second=0, microsecond=0)

        ltedate = ltedate.replace(hour = 0, minute = 0, second = 0, microsecond = 0)

        newrec = {
            "@timestamp": int(ts)*1000,
            "kpi": "KPI301",
            "total": size,
            "totalok": oksize,
            "average": average,
            "percent": percent,
            "contract": contract,
            "technic": technic,
            "screen_name": screen,
            "lot": lot,
            "dateStart": int((gtedate.timestamp())*1000),
            "dateStop": int((ltedate.timestamp()+3601)*1000)
        }
        bulkbody += json.dumps(action)+"\r\n"
        bulkbody += json.dumps(newrec) + "\r\n"
        logger.info(bulkbody)
        bulkres = es.bulk(bulkbody)
        bulkbody = ''
        bulkres = ''
        doneokquery = getokquery
        donenokquery = getokquery
        doneokkpiquery = {
            "match": {
                "KPI302": {
                "query": "Done OK"
                }
            }
            }

        dateStartStr = '01/'+str(now.month).zfill(2)+'/'+str(now.year)
        dateStopStr=  str(now.day).zfill(2)+'/'+str(now.month).zfill(2)+'/'+str(now.year)
        dtDateStop = datetime.strptime(dateStopStr, '%d/%m/%Y')
        dtplusone = dtDateStop + timedelta(days=1)
        dateStopStr = str(dtplusone.day).zfill(2)+'/'+str(dtplusone.month).zfill(2)+'/'+str(dtplusone.year)



        query302['range']['actualStart']['lt'] = dateStopStr
        query302['range']['actualStart']['gte'] = dateStartStr

        doneokquery['query']['bool']['must'][0]= query302
        doneokquery['query']['bool']['must'][1]= doneokkpiquery
        donenokquery = {
            "query": {
                "bool": {
                "must": [query302, contractQuery],
                "should": [
                {
                "match_phrase": {
                    "KPI302": "Done NOK"
                }
                },
                {
                "match_phrase": {
                    "KPI302": "Not Done NOK"
                }
                },
                {
                "match_phrase": {
                    "KPI302": "Done OK"
                }
                },
                {
                "match_phrase": {
                    "KPI302": "Not Finished NOK"
                }
                }
            ],
            "minimum_should_match": 1
                }
            }
        }

        logger.info(doneokquery)
        logger.info(donenokquery)

        datadoneok =  es.search(index="biac_maximo", body=doneokquery, size=1000)
        datatotal =  es.search(index="biac_maximo", body=donenokquery, size=1000)

        #logger.info(datadoneok)
        #logger.info(datatotal)

        doneok = len(datadoneok['hits']['hits'])
        donenok = len(datatotal['hits']['hits'])
        bulkbody = ''
        bulkres = ''
        action = {}
        action["index"] = {"_index": "biac_maximo_monthly", "_type": "doc", "_id": 'KPI302'+screen+ts}
        average = 1 
        if donenok != 0:
            average = doneok / donenok
        percent = average*100
        strQueryFuture = str(now.day).zfill(2)+'/'+str(now.month).zfill(2)+'/'+str(now.year) + " 22:00"
        strQueryFuture2 = getQueryFuture2(now)

        queryFuture = {
            "range": {
                "scheduledStart": {
                "gte": strQueryFuture,
                "lt": strQueryFuture2,
                "format": "dd/MM/yyyy HH:mm"
                }
            }
        }

        kpifutur = {
            "match": {
                "KPI301": {
                "query": "Not Started OK"
                }
            }
            }

        getquery =  {
            "query": {
                "bool": {
                "must": [queryFuture, kpifutur, contractQuery]
                }
            }
        }

        logger.info(getquery)

        datafuture = es.search(index="biac_maximo", body=getquery, size=10000)
        future = len(datafuture['hits']['hits'])

        dateStart = time.mktime(datetime.strptime(dateStartStr, "%d/%m/%Y").timetuple())
        dateStop = time.mktime(datetime.strptime(dateStopStr, "%d/%m/%Y").timetuple())

        newrec = {
            "@timestamp": int(ts)*1000,
            "kpi": "KPI302",
            "total": donenok,
            "totalok": doneok,
            "average": average,
            "percent": percent,
            "contract": contract,
            "technic": technic,
            "screen_name": screen,
            "lot": lot,
            "future": future,
            "dateStart": int(dateStart)*1000,
            "dateStop": int(dateStop-3700)*1000
        }

        bulkbody += json.dumps(action)+"\r\n"
        bulkbody += json.dumps(newrec) + "\r\n"

        logger.info(bulkbody)
        bulkres = es.bulk(bulkbody)







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


if __name__ == '__main__':
    logger.info("AMQC_URL          :"+os.environ["AMQC_URL"])
    while True:
        time.sleep(5)
        try:
            conn.send_life_sign()
        except Exception as e:
            logger.error("Unable to send life sign.")
            logger.error(e)
    #app.run(threaded=True,host= '0.0.0.0')
