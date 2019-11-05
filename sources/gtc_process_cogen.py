"""
GTC SITES COMPUTED
====================================

Collections:
-------------------------------------


VERSION HISTORY
===============

* 20 Sep 2019 0.0.1 **PDE** First version


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
from elastic_helper import es_helper 
from dateutil.tz import tzlocal
from tzlocal import get_localzone

from functools import wraps
import datetime as dt
from datetime import datetime
from datetime import timedelta
#from lib import pandastoelastic as pte
from amqstompclient import amqstompclient
from logging.handlers import TimedRotatingFileHandler
from logstash_async.handler import AsynchronousLogstashHandler
from elasticsearch import Elasticsearch as ES, RequestsHttpConnection as RC

import collections
import dateutil.parser


containertimezone=pytz.timezone(get_localzone().zone)

MODULE  = "GTC_PROCESS_COGEN"
VERSION = "0.0.8"
QUEUE   = ["GTC_PROCESS_COGEN_RANGE"]

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

def getDate(ts):
    #ts = int(ts)/1000
    #dt = datetime.fromtimestamp(ts)
    dt = ts
    dateStr = str(dt.year) + '-' + "{:02d}".format(dt.month) + '-' + "{:02d}".format(dt.day)
    return dateStr

def getMonth(dateStr):
    month = dateStr[:-3]
    return month

def getYear(dateStr):
    year = dateStr[:4]
    return year

#*******************************************************************************
#    PCI Query constant
#*******************************************************************************

pciquery={
  "size": 1000,
  "query": {
    "bool": {
      "must": [
        {
          "query_string": {
            "analyze_wildcard": True,
            "query": "*"
          }
        },
        {
          "match": {
            "area.keyword": {
              "query": "brussels"
            }
          }
        },
        {
          "range": {
            "@timestamp": {
              "gte": 1429443861271,
              "lte": 1492602261271,
              "format": "epoch_millis"
            }
          }
        }
      ],
      "must_not": []
    }
  },
  "_source": {
    "excludes": []
  },
  "aggs": {
    "2": {
      "date_histogram": {
        "field": "@timestamp",
        "interval": "1M",
        "time_zone": "Europe/Berlin",
        "min_doc_count": 1
      },
      "aggs": {
        "3": {
          "terms": {
            "field": "area.keyword",
            "size": 5,
            "order": {
              "1": "desc"
            }
          },
          "aggs": {
            "1": {
              "avg": {
                "field": "value"
              }
            }
          }
        }
      }
    }
  }
}

#*******************************************************************************
#    Hours Query
#*******************************************************************************

hoursquery={
  "size": 0,
  "query": {
    "bool": {
      "must": [
        {
          "query_string": {
            "query": "name: LUTOSA_Cpt_Hres_FctMoteur",
            "analyze_wildcard": True
          }
        },
        {
          "range": {
            "@timestamp": {
              "gte": 1491894138351,
              "lte": 1493103738352,
              "format": "epoch_millis"
            }
          }
        }
      ],
      "must_not": []
    }
  },
  "_source": {
    "excludes": []
  },
  "aggs": {
    "2": {
      "date_histogram": {
        "field": "@timestamp",
        "interval": "1d",
        "time_zone": "Europe/Berlin",
        "min_doc_count": 1
      },
      "aggs": {
        "1": {
          "min": {
            "field": "value"
          }
        },
        "3": {
          "max": {
            "field": "value"
          }
        }
      }
    }
  }
}

#*******************************************************************************
#    Search Query
#*******************************************************************************

searchquery={
  "size": 0,
  "query": {
    "bool": {
      "must": [
        {
          "query_string": {
            "query": "client: COGLTS AND name: LUTOSA_Cpt_Elec_OutToMoteur",
            "analyze_wildcard": True
          }
        },
        {
          "range": {
            "@timestamp": {
              "gte": 1489486928723,

              "lte": 1492075328723,
              "format": "epoch_millis"
            }
          }
        }
      ],
      "must_not": []
    }
  },
  "_source": {
    "excludes": []
  },
  "aggs": {
    "1": {
      "date_histogram": {
        "field": "@timestamp",
        "interval": "1d",
        "time_zone": "Europe/Berlin",
        "min_doc_count": 1
      },
      "aggs": {
        "2": {
          "min": {
            "field": "value"
          }
        },
        "3": {
          "max": {
            "field": "value"
          }
        }
      }
    }
  }
}

#*******************************************************************************
#    Power Query
#*******************************************************************************


powerquery={
  "size": 0,
  "query": {
    "bool": {
      "must": [
        {
          "query_string": {
            "query": "name: MAGNA_DB201_H_FCT",
            "analyze_wildcard": True
          }
        },
        {
          "range": {
            "@timestamp": {
              "gte": 1493372161772,
              "lte": 1494581761772,
              "format": "epoch_millis"
            }
          }
        }
      ],
      "must_not": []
    }
  },
  "_source": {
    "excludes": []
  },
  "aggs": {
    "2": {
      "date_histogram": {
        "field": "@timestamp",
        "interval": "1d",
        "time_zone": "Europe/Berlin",
        "min_doc_count": 1
      },
      "aggs": {
        "1": {
          "max": {
            "field": "value"
          }
        },
        "3": {
          "avg": {
            "field": "value"
          }
        },
        "4": {
          "min": {
            "field": "value"
          }
        }
      }
    }
  }
}

#*******************************************************************************
#    Life Sign Query
#*******************************************************************************

lifesignquery={
  "size": 0,
  "query": {
    "bool": {
      "must": [
        {
          "query_string": {
            "query": "name: NOH_DB201_Runhours_43589_1 AND value: [1 TO *] AND value_day: [0 TO *]",
            "analyze_wildcard": True
          }
        },
        {
          "range": {
            "@timestamp": {
              "gte": 1496354400000,
              "lte": 1496416885599,
              "format": "epoch_millis"
            }
          }
        }
      ],
      "must_not": []
    }
  },
  "_source": {
    "excludes": []
  },
  "aggs": {
    "1": {
      "max": {
        "field": "@timestamp"
      }
    }
  }
}
#===================================================================================
def loadPCICurve(area,starttime,endtime):
    global es,pciquery

    print ("Load PCI Curve for:"+area+" "+str(starttime)+" => "+str(endtime))

    starttime=starttime-31*24*3600

    pciquery["query"]["bool"]["must"][1]["match"]["area.keyword"]["query"]=area
    pciquery["query"]["bool"]["must"][2]["range"]["@timestamp"]["gte"]=int(starttime)
    pciquery["query"]["bool"]["must"][2]["range"]["@timestamp"]["lte"]=int(endtime)

    print (pciquery)
    res=es.search(index="scp",body=pciquery)
    results=[]
    for row in res["aggregations"]["2"]["buckets"]:
        date = datetime.fromtimestamp(row["key"]/1000).date()
        results.append([date,row["3"]["buckets"][0]["1"]["value"]])

    print (results)
    pcidf=pd.DataFrame(data=results,columns=["date","value"])
    return pcidf

#===================================================================================
def loadCurve(query,starttime,endtime):

    query=query+" AND value_day:[0 TO 1000000]"
    logger.info("==============> Loading Curve:"+query)

    global es,searchquery

    searchquery["query"]["bool"]["must"][0]["query_string"]["query"]=query+""
    searchquery["query"]["bool"]["must"][1]["range"]["@timestamp"]["gte"]=int(starttime)
    searchquery["query"]["bool"]["must"][1]["range"]["@timestamp"]["lte"]=int(endtime)

    logger.info(searchquery)

    res=es.search(index="opt_sites_data*",body=searchquery)
    newcurve=[]
    for bucket in res['aggregations']['1']['buckets']:
        newcurve.append([datetime.fromtimestamp(bucket['key']/1000),bucket['3']['value']-bucket['2']['value']])

    return newcurve

#===================================================================================
def loadCurveGroup(client,area,group,starttime,endtime):
    logger.info("#################")

    gazindf=None
    gazincurves=[]
    gazinlabels=[]

    pciincurves=[]
    pciinlabels=[]

    formulas=[]

    pcidf=None
    # METER IN
    for gazin in group:
        logger.info("Loading:"+gazin["name"])
        req="client: "+client+ " AND name: \""+gazin["name"]+"\" AND area: "+area
        curve=loadCurve(req,starttime,endtime)
        gazinlabels.append(gazin["label"])
        gazincurves.append(curve)

        pciexists=False

        if("pcisource" in gazin):
            logger.info("-----------------------------")
            logger.info("- PCI SOURCE FOUND:" +  gazin["pcisource"])

            pcidf=loadPCICurve(gazin["pcisource"],starttime-(24*3600*122*1000),endtime)
            idx = pd.date_range(datetime.fromtimestamp((starttime-(24*3600*62*1000))/1000)
                                                           ,datetime.fromtimestamp((endtime+(24*3600*1000))/1000))
            pcidf['date'] = pd.to_datetime(pcidf['date'], format='%Y-%m-%d').dt.date
            pcidf.index=pcidf["date"]
            del pcidf["date"]

            pcidf = pcidf.reindex(idx, fill_value=0,method='pad')
            pcidf["index2"]=pcidf.index
            pcidf["index2"]=pcidf["index2"].dt.date
            pcidf.index=pcidf["index2"]
            del pcidf["index2"]
            #print (pcidf)
            pciexists=True

        elif("pci" in gazin):
            logger.info("----------------")
            logger.info("- PCI FOUND")

            pcidf=pd.DataFrame(data=gazin["pci"],columns=["date","value"])
            pcidf['date'] = pd.to_datetime(pcidf['date'], format='%Y-%m-%d')

            idx = pd.date_range(min(datetime.fromtimestamp(starttime/1000),min(pcidf["date"]))
                                ,max(datetime.fromtimestamp(endtime/1000),max(pcidf["date"])))
            pcidf.index=pcidf["date"]
            del pcidf["date"]

            pcidf = pcidf.reindex(idx, fill_value=0,method='pad')
            pciexists=True

        if(pciexists):
            pciinlabels.append(gazin["label"]+"_scp")
            pciincurves.append(pcidf)
            formula={"x1":(gazin["label"]+"_scp"),"x2":(gazin["label"]),"target":(gazin["label"].replace(" m3","")+" kWh")}
            logger.info("Formula: "+(gazin["label"]+"_scp")+" * "+(gazin["label"]))
#            logger.info(formula)
            formulas.append(formula)

    gazindf=pd.DataFrame(data=gazincurves[0],columns=["date",gazinlabels[0]])
    gazindf.index=gazindf["date"]
    del gazindf["date"];


    for i in range(1,len(gazinlabels)):
        gazindf2=pd.DataFrame(data=gazincurves[i],columns=["date",gazinlabels[i]])
        gazindf2.index=gazindf2["date"]
        del gazindf2["date"]
        gazindf= pd.concat([gazindf,gazindf2], join='inner', axis=1)

    if(len(pciinlabels)>0):
        pcidf=pciincurves[0]
        pcidf.columns={pciinlabels[0]}
        for i in range(1,len(pciinlabels)):
            pciincurves[i].columns={pciinlabels[i]}
            pcidf= pd.concat([pcidf,pciincurves[i]], join='inner', axis=1)
        gazindf= pd.concat([gazindf,pcidf], join='inner', axis=1)

    for form in formulas:
        logger.info("Running:")
        logger.info(form)
        gazindf[form["target"]]=gazindf[form["x1"]]*gazindf[form["x2"]]


    return gazindf

#===================================================================================
def loadCogen(cogenname,starttime,endtime):
    global es

    logger.info("LOAD COGEN:"+cogenname+" ST:"+str(starttime.timestamp())+" EN:"+str(endtime.timestamp()))
    logger.info("-"*100)

    st=starttime.timestamp()*1000;
    et=endtime.timestamp()*1000;

    try:
        cog=es.get(index="cogen_parameters",doc_type="doc", id=cogenname)

    except:
        logger.info("Unable to find COGEN:"+cogenname)
        return [None,None]

    setSearchQueryField(cog["_source"]["valuefield"])

#    logger.info(cog["_source"])
    client=cog["_source"]["client"]
    area=cog["_source"]["area"]
    logger.info("Client:"+client)
    logger.info("Area:"+area)

    gazindfok=False

    if len(cog["_source"]["gaz_in"])>0:
        gazindf=loadCurveGroup(client,area,cog["_source"]["gaz_in"],st,et)
        gazindfok=True

    if len(cog["_source"]["elec_out"])>0:
        elecoutdf=loadCurveGroup(client,area,cog["_source"]["elec_out"],st,et)

    if len(cog["_source"]["heat_out"])>0:
        heatoutdf=loadCurveGroup(client,area,cog["_source"]["heat_out"],st,et)

    if(gazindfok):
        finaldf= pd.concat([gazindf,elecoutdf,heatoutdf], join='inner', axis=1)
    else:
#        print (starttime)
        idx = pd.date_range(starttime.replace(hour=0, minute=0, second=0, microsecond=0),endtime.replace(hour=0, minute=0, second=0, microsecond=0))
        finaldf=pd.DataFrame(data=idx,columns=["date"])
#        finaldf["date2"]=finaldf["date"].dt.date
#        finaldf["date3"]=finaldf["date2"].dt
        finaldf.index=finaldf["date"]
        del finaldf["date"];
        #del finaldf["date2"];
        #logger.info( "******"*30)
        #logger.info (starttime)
        #logger.info (finaldf)

    isoptibox=False
    if("isoptibox" in cog["_source"]):
        isoptibox=True



    if("usagehours" in cog["_source"]):
        if (("usagehoursuseoptimizcollection" in cog["_source"]) and (cog["_source"]["usagehoursuseoptimizcollection"])):
            logger.info ("Load hours using Optimiz collection...")
            hoursdf=loadUsageHours("name: "+cog["_source"]["usagehours"]+" AND client: (COGEN OR ECPOWER) AND area: NyxAWS",st,et,1000000,False)
            finaldf= pd.concat([finaldf,hoursdf], join='outer', axis=1)
        else:
            if( isoptibox):
                hoursdf=loadUsageHours("code: "+cog["_source"]["usagehours"]+" AND area: \""+area+"\"",st,et,1000000000,True)
                finaldf= pd.concat([finaldf,hoursdf], join='outer', axis=1)
            else:
                hoursdf=loadUsageHours("name: "+cog["_source"]["usagehours"]+" AND client: "+client+" AND area: "+area,st,et,1000000,False)
                finaldf= pd.concat([finaldf,hoursdf], join='outer', axis=1)

    if("startstops" in cog["_source"]):
        if (("startsstopsuseoptimizcollection" in cog["_source"]) and (cog["_source"]["startsstopsuseoptimizcollection"])):
            logger.info ("Load starts / stops using Optimiz collection...")
            startsdf=loadStartStops("name: "+cog["_source"]["startstops"]+" AND client: (COGEN OR ECPOWER) AND area: NyxAWS",st,et,1000000, False)
            finaldf= pd.concat([finaldf,startsdf], join='outer', axis=1)
        else:
            if( isoptibox):
                startsdf=loadStartStops("code: "+cog["_source"]["startstops"]+" AND area: \""+area+"\"",st,et,1000000000, True)
                finaldf= pd.concat([finaldf,startsdf], join='outer', axis=1)
            else:
                startsdf=loadStartStops("name: "+cog["_source"]["startstops"]+" AND client: "+client+" AND area: "+area,st,et,1000000, False)
                finaldf= pd.concat([finaldf,startsdf], join='outer', axis=1)

    if("power" in cog["_source"]):
        if (("poweruseoptimizcollection" in cog["_source"]) and (cog["_source"]["poweruseoptimizcollection"])):
            logger.info ("Load power using Optimiz collection...")
            powerdf=loadPower("name: "+cog["_source"]["power"]+" AND client: (COGEN OR ECPOWER) AND area: NyxAWS",st,et)
            finaldf= pd.concat([finaldf,powerdf], join='outer', axis=1)
        else:
            startsdf=loadPower("name: "+cog["_source"]["power"]+" AND client: "+client+" AND area: "+area,st,et)
            finaldf= pd.concat([finaldf,powerdf], join='outer', axis=1)




    if (client =='cogenmail'):

        logger.info("Interpolate missing values...")
        #logger.info(finaldf)
        finaldf["Hours_Index"]=finaldf["Hours_Index"].interpolate()
        finaldf["Starts_Index"]=finaldf["Starts_Index"].interpolate()

        todelete=0
        for i in range(finaldf.shape[0]-1,-1,-1):
            #logger.info(finaldf["Hours"][i])
            if(np.isnan(finaldf["Hours"][i])):
                #logger.info("NAN")
                todelete+=1
            else:
                break

        if(todelete>0):
            finaldf=finaldf[:-todelete]



        curhourindex=None
        curstartindex=None
        newhours=[]
        newstarts=[]
        for index,row in finaldf.iterrows():
            if not(np.isnan(row["Hours_Index"])):
                if(curhourindex !=None):
                    newhours.append(int(row["Hours_Index"]-curhourindex))
                else:
                    newhours.append(0)
                curhourindex=row["Hours_Index"]
            else:
                newhours.append(0)

            if not(np.isnan(row["Starts_Index"])):
                if(curstartindex !=None):
                    newstarts.append(int(row["Starts_Index"]-curstartindex))
                else:
                    newstarts.append(0)
                curstartindex=row["Starts_Index"]
            else:
                newstarts.append(0)
        finaldf["Hours"]=newhours
        finaldf["Starts_Stops"]=newstarts

        #finaldf=finaldf[:-1]

    else:
        finaldf=finaldf[1:]
    #finaldf=finaldf[:-1]
    return [cog["_source"],finaldf]

#===================================================================================
def setSearchQueryField(newfield):
#    logger.info("Value field:"+newfield)
    searchquery["aggs"]["1"]["aggs"]["2"]["min"]["field"]=newfield
    searchquery["aggs"]["1"]["aggs"]["3"]["max"]["field"]=newfield


#===================================================================================
def getCogenNames():
    logger.info("GET ALL COGENS")
    rescogen=[]
    querycogens = {
      "query": {
        "exists": {
          "field": "order"
        }
      }
    }
    cogens=es.search(index="cogen_parameters", body=querycogens, size=1000)

#    print (cogens)

    for cogen in cogens["hits"]["hits"]:
        if cogen["_source"]["active"]:
#            logger.info("* "+cogen["_id"]+" added.")
            rescogen.append(cogen["_id"])
        else:
            logger.info("* "+cogen["_id"]+" skipped.")
#            rescogen.append(cogen["_id"])


    return rescogen

#===================================================================================
def computePerformance(onecogendf):
    logger.info("Compute Performance")
#    logger.info(onecogendf)
    onecogendf["Total Out kWh"]=0
    onecogendf["Total In kWh"]=0

    for col in onecogendf.columns:
        if(col.find("Out kWh")!=-1) and (col.find("Total")==-1):
                onecogendf["Total Out kWh"]=onecogendf["Total Out kWh"]+onecogendf[col]
        if(col.find("In kWh")!=-1) and (col.find("Total")==-1):
                onecogendf["Total In kWh"]=onecogendf["Total In kWh"]+onecogendf[col]

    try:
        onecogendf["Performance"]=onecogendf["Total Out kWh"]/onecogendf["Total In kWh"]
    except:
        logger.error("Unable to compute performance. Division by 0 ?")
        onecogendf["Performance"]=0

    onecogendf.replace(np.inf, np.nan, inplace=True)
    onecogendf["Performance"].fillna(0, inplace=True)
    return onecogendf

#===================================================================================
def loadPower(query,startt,entt):
    global powerquery

    logger.info("==============> Loading power:"+query+" AND value:[0 TO *]")

    powerquery["query"]["bool"]["must"][0]["query_string"]["query"]=query+" AND value:[0 TO *]"
    powerquery["query"]["bool"]["must"][1]["range"]["@timestamp"]["gte"]=int(startt)
    powerquery["query"]["bool"]["must"][1]["range"]["@timestamp"]["lte"]=int(entt)

    res=es.search(body=powerquery,index="opt_sites_data*")

    newcurve=[]
    for bucket in res['aggregations']['2']['buckets']:
#        newcurve.append([datetime.fromtimestamp(bucket['key']/1000),bucket['3']['value']-bucket['2']['value']],bucket['3']['value'])
        newcurve.append([datetime.fromtimestamp(bucket['key']/1000),bucket['4']['value'],bucket['3']['value'],bucket['1']['value']])

    powerdf=pd.DataFrame(newcurve,columns=["date","Min Power kW","Power kW","Max Power kW"])

    powerdf.index=powerdf["date"]
    del powerdf["date"];

#    print (powerdf)

    return powerdf

#===================================================================================
def loadUsageHours(query,startt,entt,limit,isoptibox):
    global hoursquery

    logger.info("==============> Loading usage hours:"+query+" AND value:[1 TO "+str(limit)+"] ")

    hoursquery["query"]["bool"]["must"][0]["query_string"]["query"]=query+" AND value:[1 TO "+str(limit)+"]"
    hoursquery["query"]["bool"]["must"][1]["range"]["@timestamp"]["gte"]=int(startt)
    hoursquery["query"]["bool"]["must"][1]["range"]["@timestamp"]["lte"]=int(entt)

    if isoptibox:
        res=es.search(body=hoursquery,index="opt_optibox*")
    else:
        res=es.search(body=hoursquery,index="opt_sites_data*")

#    print(hoursquery)
#    print(res)

    newcurve=[]
    for bucket in res['aggregations']['2']['buckets']:
#        newcurve.append([datetime.fromtimestamp(bucket['key']/1000),bucket['3']['value']-bucket['2']['value']],bucket['3']['value'])
        newcurve.append([datetime.fromtimestamp(bucket['key']/1000),bucket['3']['value']-bucket['1']['value'],bucket['3']['value']])

    hoursdf=pd.DataFrame(newcurve,columns=["date","Hours","Hours_Index"])
    if isoptibox:
        hoursdf["Hours"]=hoursdf["Hours"]/60
        hoursdf["Hours_Index"]=hoursdf["Hours_Index"]/60

    hoursdf.index=hoursdf["date"]
    del hoursdf["date"];


    return hoursdf



#===================================================================================
def loadStartStops(query,startt,entt,limit, isoptibox):
    global hoursquery

    if limit<100000: # not optibox
        logger.info ("==============> Loading start / stops:"+query+" AND value:[1 TO *] AND value_day:[0 *]")
        hoursquery["query"]["bool"]["must"][0]["query_string"]["query"]=query+" AND value:[1 TO *] AND value_day:[0 *]"
    else:
        logger.info ("==============> Loading start / stops:"+query+" AND value:[1 TO *]")
        hoursquery["query"]["bool"]["must"][0]["query_string"]["query"]=query+" AND value:[1 TO *]"
    hoursquery["query"]["bool"]["must"][1]["range"]["@timestamp"]["gte"]=int(startt)
    hoursquery["query"]["bool"]["must"][1]["range"]["@timestamp"]["lte"]=int(entt)


    if isoptibox:
        res=es.search(body=hoursquery,index="opt_optibox*")
    else:
        res=es.search(body=hoursquery,index="opt_sites_data*")

#    print (res)

    newcurve=[]
    for bucket in res['aggregations']['2']['buckets']:
#        newcurve.append([datetime.fromtimestamp(bucket['key']/1000),bucket['3']['value']-bucket['2']['value']],bucket['3']['value'])
        newcurve.append([datetime.fromtimestamp(bucket['key']/1000),bucket['3']['value']-bucket['1']['value'],bucket['3']['value']])

    hoursdf=pd.DataFrame(newcurve,columns=["date","Starts_Stops","Starts_Index"])

    hoursdf.index=hoursdf["date"]
    del hoursdf["date"];

#    print (hoursdf)

    return hoursdf

#===================================================================================
def insertIntoELK(onecogendf,definition):
    global es
    #es2 = Elasticsearch(hosts=["http://esnodebal:9200"])
    # insert in ELASTIC SEARCH


    messagebody=""

    logger.info("Push into ELK...")
#    logger.info(definition)

    for index,row in onecogendf.iterrows():
        obj={}
        obj["loc"]=definition["location"]
        obj["client"]=definition["client"]
        obj["name"]=definition["name"]

        if(row["Hours_Index"]==0):
            continue

        if("contract" in definition):
            obj["Contract"]=definition["contract"]
        if("project" in definition):
            obj["Project"]=definition["project"]
        if("brand" in definition):
            obj["Brand"]=definition["brand"]
        if("expectedpower" in definition):
            obj["ExpectedPower"]=definition["expectedpower"]
        for col in onecogendf.columns:
            cleancol=col.replace(" ","_")
            if(col=="Hours"):
                if(row[col]>24):
                    obj[cleancol]=-1
                else:
                    obj[cleancol]=row[col]
            elif(col=="Starts_Stops"):
                if(row[col]>30):
                    obj[cleancol]=-1
                else:
                    obj[cleancol]=row[col]
            else:
                obj[cleancol]=row[col]


        action={}
        id=obj["name"]+"_"+str(index).replace(" ","").replace(":","")
        action["index"]={"_index":"cogen_computed","_type":"doc","_id":id}
        messagebody+=json.dumps(action)+"\r\n";
        obj["@timestamp"]=int(index.timestamp())*1000
        messagebody+=json.dumps(obj)+"\r\n"


    if True and len(messagebody)>0:
        res=es.bulk(messagebody)
        #print (res)
        try:
            es.bulk(messagebody)
        except:
            logger.error("Unable to push in Optimiz2")
    else:
        logger.info("BYPASSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSSS")
        print (messagebody)


def computeLastLifeSign():
    global es
    logger.info("Compute Life Sign")
    allcogens=getCogenNames()

    messagebody=""

    for cogenname in allcogens:
        logger.info("=>"+cogenname)

#        if cogenname != 'dumoulin':
#            continue


        try:
            cog=es.get(index="cogen_parameters",doc_type="doc",id=cogenname)

#            logger.info(cog)

            isoptibox=False
            if("isoptibox" in cog["_source"]):
                isoptibox=True
            query="NONONONONNO"

            if (("usagehoursuseoptimizcollection" in cog["_source"]) and (cog["_source"]["usagehoursuseoptimizcollection"])):
                query="name: "+cog["_source"]["usagehours"]+" AND client: (COGEN OR ECPOWER) AND area: NyxAWS"
            else:
                if( isoptibox):
                    query="code: "+cog["_source"]["usagehours"]+" AND area: \""+cog["_source"]["area"]+"\""
                    logger.info("Query:"+query)
                else:
                    query="name: "+cog["_source"]["usagehours"]+" AND client: "+cog["_source"]["client"]+" AND area: "+cog["_source"]["area"]



            lifesignquery["query"]["bool"]["must"][0]["query_string"]["query"]=query+""
            lifesignquery["query"]["bool"]["must"][1]["range"]["@timestamp"]["gte"]=int((datetime.now()- timedelta(days=2)).timestamp())*1000
            lifesignquery["query"]["bool"]["must"][1]["range"]["@timestamp"]["lte"]=int(datetime.now().timestamp())*1000

#            print(lifesignquery)

            res=es.search(index="opt_*",body=lifesignquery)
#            logger.info(res)
#            logger.info(res["aggregations"]["1"]["value"])

            action={}
            id=cogenname
            obj={}
            action["index"]={"_index":"cogen_lifesign","_type":"doc","_id":id}
            messagebody+=json.dumps(action)+"\r\n";
            if(res["aggregations"]["1"]["value"]!=None):
                obj["@timestamp"]=int(res["aggregations"]["1"]["value"])
            else:
                obj["@timestamp"]=int((datetime.now()- timedelta(days=3)).timestamp())*1000
            obj["project"]=cog["_source"]["project"]
            messagebody+=json.dumps(obj)+"\r\n"
        except Exception as excep:
            logger.error("Unable to find COGEN:"+cogenname)
            logger.error(excep)
            #return None
#    logger.info(messagebody)
    #es2 = Elasticsearch(hosts=["http://optimiz2.cofelygtc.com:9200"])
    res2=es.bulk(messagebody)
    #logger.info(res2)

def calcCogen():
    global es
    logger.info("Calc Cogen ...")
    end = datetime.now()
    start = end - timedelta(days=30)

    cogens = es_helper.elastic_to_dataframe(es, 'cogen_computed', query='*', start=start, end=end)
    cogengrouped = cogens.groupby(['Contract', 'client', 'name']).agg({'@timestamp': 'max', 'Hours_Index': 'max', 'Starts_Index': 'max'}).reset_index()
    cogengrouped = cogengrouped.set_index('name')
    params = es.search("cogen_parameters", size='10000')

    for cog in params['hits']['hits']:
        name = cog['_source']['name']
        if name in cogengrouped.index:
            cogstats = cogengrouped.loc[name, :]
            cog['_source']['Hours'] = cogstats['Hours_Index']
            cog['_source']['Starts'] = cogstats['Starts_Index']
            cog['_source']['LastUpdate'] = int(time.time()*1000)
            cog['_source']['modifyBy'] = 'GTC'

    bulkbody = ''
    action={}
    for cog in params['hits']['hits']:
        action["index"]={"_index":cog['_index'],"_type":"doc","_id":cog['_id']}
#        print(action)
        newrec = cog['_source']
#        print(newrec)
        bulkbody+=json.dumps(action)+"\r\n"
        bulkbody+=json.dumps(newrec)+"\r\n"

    res = es.bulk(body=bulkbody)
#    logger.info(res)





def doTheWork(start):
    logger.info("Do the work ...")
    allcogens=getCogenNames()

    for cogenname in allcogens:
#        if cogenname != 'noh':
#        if cogenname != 'calypso':#cogenname != 'sportoase_beringen':
#            continue
#multires=loadCogen("lutosa",datetime(2017, 4, 4),datetime(2017, 4, 24))
#        multires=loadCogen("sportoase_beringen",datetime(2017, 4, 4),datetime(2017, 4, 24))
#multires=loadCogen("sportoase_braine",datetime(2017, 4, 4),datetime(2017, 4, 24))
        multires=loadCogen(cogenname, datetime.today() - timedelta(days=40),datetime.now())

        cogendf=multires[1]
#        print (cogendf)

        cogendf=computePerformance(cogendf)
        cogendf.fillna(0, inplace=True)
#        logger.info(cogendf)

        insertIntoELK(cogendf,multires[0])











def messageReceived(destination,message,headers):
    global es
    logger.info("==> "*10)
    logger.info("Message Received %s" % destination)
    logger.info(headers)

    msg = json.loads(message)
    start = datetime.fromtimestamp(int(msg['start']))
    stop = datetime.fromtimestamp(int(msg['stop']))

    while start <= stop:
        doTheWork(start)
        start = start + timedelta(1)





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
                    ,"login":os.environ["AMQC_LOGIN"],"password":os.environ["AMQC_PASSWORD"]
                    ,"heartbeats":(1200000,1200000),"earlyack":True}
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

    SECONDSBETWEENCHECKS=3600
    SECONDSBETWEENKEEPALIVE=900

    nextload=datetime.now()
    keepalivenextload=datetime.now()

    while True:
        time.sleep(5)
        try:            
            variables={"platform":"_/_".join(platform.uname()),"icon":"list-alt"}
            
            conn.send_life_sign(variables=variables)

            if (datetime.now() > keepalivenextload):
                try:
                    keepalivenextload=datetime.now()+timedelta(seconds=SECONDSBETWEENKEEPALIVE)
                    computeLastLifeSign()
                    calcCogen()
                except Exception as e2:
                    logger.error("Unable to check keepalive.")
                    logger.error(e2,exc_info=True)

            if (datetime.now() > nextload):
                try:
                    start = datetime.now()
                    start = start.replace(hour=0,minute=0,second=0, microsecond=0)
                    nextload=datetime.now()+timedelta(seconds=SECONDSBETWEENCHECKS)
                    doTheWork(start)
                except Exception as e2:
                    logger.error("Unable to load sites data.")
                    logger.error(e2,exc_info=True)

            
        except Exception as e:
            logger.error("Unable to send life sign.")
            logger.error(e)
    #app.run(threaded=True,host= '0.0.0.0')