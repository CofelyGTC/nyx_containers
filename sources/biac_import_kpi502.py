"""
BIAC KPI 502
====================================
Expects a 502 compatible Excel File encoded as base 64 on the queue (BIAC_EXCELS_KPI502).
There must be as many sheets as there are lots and they should be named as follows:

* Lot 1
* Lot 2
* Lot 3
* Lot 4

The name of the file is decoded to determine the month. A month is subtracted to the file name.
For example:  SafetyRegister-KPI502-Lots_1_2_3_4-2019-09.xlsx is the safety register used for August 2019.
Note that all lots use the same overdue periods for Inbreuken and Opmerkingen except lot 4.


Sends:
-------------------------------------


Listens to:
-------------------------------------

* /queue/BIAC_EXCELS_KPI502

Collections:
-------------------------------------

* **biac_kpi502** (Raw Data)

VERSION HISTORY
--------------------------------------

* 09 Sep 2019 0.0.9 **AMA** Lot 4 overdue durations for Opmerkingen and Inbreuken moved from 12 months /6 months  to 6 months / 3 months
* 16 Sep 2019 0.1.0 **AMA** Do no longer keep data below < Mai 2018
* 19 Sep 2019 0.1.1 **AMA** Do no longer keep data below < Mai 2018
* 23 Sep 2019 0.1.2 **AMA** Fix a bug that prevented the original collection to be erased properly
* 10 Dec 2010 1.0.1 **AMA** Use elastic helper
* 06 Jan 2020 1.0.3 **AMA** Create the new Kibana collection
* 07 Jan 2020 1.0.6 **AMA** Lot 4 added in the new Kibana collection
* 17 Feb 2020 1.1.0 **AMA** Use the new 502 computation method (new overdues)
* 03 Mar 2020 1.2.0 **AMA** DOn't cound old overdues in percentages
* 12 May 2021 1.2.1 **PDB** Fix bug in reference Date
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

from elastic_helper import es_helper 
from lib import reporthelper as rp
from logging.handlers import TimedRotatingFileHandler
import amqstomp as amqstompclient
from datetime import datetime
from datetime import date
from datetime import timedelta
from functools import wraps
from dateutil.relativedelta import relativedelta
from logstash_async.handler import AsynchronousLogstashHandler
from elasticsearch import Elasticsearch as ES
from time import sleep


VERSION="1.3.1"
MODULE="BIAC_KPI502_IMPORTER"
QUEUE=["BIAC_EXCELS_KPI502","/topic/RECOMPUTE_502"]

goodmonth="NA"

def fixDate(row):
    try:
        x = int(row['Reference Date'].timestamp()*1000)
    except Exception as er:
        print(er)
        #print(row)
        month = row['Month']
        newdt = datetime(int(month[:4]),int(month[-2:]),1)
        print(newdt)
        x = int(newdt.timestamp()*1000)
    finally:
        print(x)
        return x    

def computeOverdue(row):
    """This function determines if a record is overdued. It depends on the lot and record type.
    Returns:
        String -- The month as string or overdue
    """
    global goodmonth
    try:
        curmonth=datetime.strptime(row["Month"],"%Y-%m")
    except:
        #print("===========++>>>>"*30)
        #print(row["Month"])
        return row["Month"]
    gm=datetime.strptime(goodmonth,"%m-%Y")
#    #print(gm)    

    if row["Sheet"] == "Lot 4":
        if row["ShortStatus"]==5:
            gm=gm- relativedelta(months=2)
        else:
            gm=gm- relativedelta(months=5)
        
    else:
        if row["ShortStatus"]==5:
            gm=gm- relativedelta(months=5)
        else:
            gm=gm- relativedelta(months=11)
        


#    #print("GM= %s" %(gm))    
    if curmonth>=gm:
        return row["Month"]
    else:
        return "OVERDUE" 

def computeShortStatus(status):
    """Determines a status based on the flemish status.
    Returns:
        int -- 5 for Inbreuk,4 for Opmerkingen, 0 otherwise
    """
    if status=="Inbreuk":
        return 5
    elif status=="Opmerking":
        return 4
    else:
        return 0

def compute_previous_months(month,year,number,skip=0):
    ret=[]
    
    while(number>0):
        if skip<=0:
            ret.append("%d-%s" %(year,str(month).zfill(2)))
        month-=1
        if month==0:
            month=12
            year-=1
        skip-=1
        number-=1
    return ret

def log_message(message):
    global conn,goodmonth

    message_to_send={
        "message":message,
        "@timestamp":datetime.now().timestamp() * 1000,
        "module":MODULE,
        "version":VERSION
    }
    logger.info("LOG_MESSAGE")
    logger.info(message_to_send)
    conn.send_message("/queue/NYX_LOG",json.dumps(message_to_send))


def get_id(month, rowid):
    month = re.sub('[-]', '', month)
    rowid = rowid.replace(' ', '')
    return (rowid+month).replace("-","").lower()

def computeReport(row):
    if row["Sheet"]=="Lot 1":
        return "Lot1 (BACHEA)"
    if row["Sheet"]=="Lot 3":
        return "Lot3 (BACEXT)"    
    if row["Sheet"]=="Lot 4":
        return "Lot4 (BACDNB)"        
    
    res=rps.getKPI500Config(row['Cc 5'], row['Sub-technic'])
    if row['Sub-technic'] == "SANI":
        logger.info("####"*100)
        logger.info(row['Cc 5'])
        logger.info(res)
        logger.info("####"*100)
    if res==None:
        logger.info(" %s => %s  "%(row['Cc 5'], row['Sub-technic']))
        return "NA"
    logger.info(res['key'])
    return res['key']

################################################################################
def recomputeKPI502():
    logger.info("Creating Kibana Dashboard 502")
    keys="Lot1 (BACHEA),Lot2 ELEC (BACELE),Lot2 FIRE (BACFIR),Lot2 ACCESS (BACFIR),Lot2 CRADLE (BACFIR),Lot2 HVAC PB/NT (BACHVA),Lot2 SANI (BACSAN),Lot2 HVAC PA (BACSAN),Lot3 (BACEXT),Lot4 (BACDNB)".split(",")
    es.indices.delete(index='biac_kpi502_kib', ignore=[400, 404])
    time.sleep(1)

    for key in keys:
        logger.info("Computing %s" %(key))
        try:
            compute502barchart_v3(key)
        except Exception as err:
            logger.error(err)





def generate_kibana_dash(kib_df,rec_type,key):
    bulkbody=[]

    for index,row in kib_df.iterrows():
        action = {}
        action["index"] = {"_index": "biac_kpi502_kib"}
        record={"type":rec_type,"month":row["Month"],"key":key,"value":0,"label":"","active":1}
        bulkbody.append(json.dumps(action))
        bulkbody.append(json.dumps(record))   
        if "OVERDUE" not in row["Month"]:
            record={"type":rec_type,"month":row["Month"],"key":key,"value":row["Cur"],"label":"Current","active":1}
            bulkbody.append(json.dumps(action))
            bulkbody.append(json.dumps(record))   

            if row["Good"]>0:
                record={"type":rec_type,"month":row["Month"],"key":key,"value":row["Good"],"label":"Previous","active":1}
                bulkbody.append(json.dumps(action))
                bulkbody.append(json.dumps(record))   
        else:
            if row["Cur"]>0:        
                record={"type":rec_type,"month":row["Month"],"key":key,"value":row["Cur"],"label":"Overdue","active":1}
                bulkbody.append(json.dumps(action))
                bulkbody.append(json.dumps(record))   
            if row["NewOverdue"]>0:        
                record={"type":rec_type,"month":row["Month"],"key":key,"value":row["NewOverdue"],"label":"New Overdue","active":1}
                bulkbody.append(json.dumps(action))
                bulkbody.append(json.dumps(record))   


    res=es.bulk(body=bulkbody)

def compute502barchart_v3(reporttype):

    #print('==============> KPI502 bar graph')
    #print(reporttype)

    cur_active=es_helper.elastic_to_dataframe(es,"biac_kpi502","active:1 AND key: \""+reporttype+"\"")
    #print(cur_active)
    startdate=datetime.strptime(cur_active["filedate"].iloc[0],"%Y-%m")

    
    start=startdate#-timedelta(days=2)

    start_overdue_4=start+ relativedelta(months=-13)
    start_overdue_4=str(start_overdue_4.year)+"-"+(str(start_overdue_4.month)).zfill(2)

    start_overdue_5=start+ relativedelta(months=-7)
    start_overdue_5=str(start_overdue_5.year)+"-"+(str(start_overdue_5.month)).zfill(2)


    #print(start_overdue_5)
#    startminusonemonth=start-timedelta(days=32)
    startminusonemonth=start-relativedelta(months=1)

    filedate=str(start.year)+"-"+(str(start.month)).zfill(2)
    filedateminusonemonth=str(startminusonemonth.year)+"-"+(str(startminusonemonth.month)).zfill(2)
    #print("FileDate: %s" %(filedate))

    queryadd=" AND key: \""+reporttype.replace("Lot4 (DNB)","Lot4 (BACDNB)")+"\""
    if "All" in reporttype:
        queryadd=" AND  Lot: \"Lot 2\""

        #print(queryadd)

    query='(ShortStatusFU: Actievereist OR ValueCount:0) AND filedate:'+filedate+queryadd
    queryminusonemonth='(ShortStatusFU: Actievereist OR ValueCount:0) AND filedate:'+filedateminusonemonth+queryadd

    #print(query)
    #print(queryminusonemonth)

    #print('Step 1')

    cur_df=es_helper.elastic_to_dataframe(es,"biac_kpi502",query)
    #print('Step 2')
    prev_df=es_helper.elastic_to_dataframe(es,"biac_kpi502",queryminusonemonth)

    #print('Step 3')


    cur_df_4=cur_df[cur_df["ShortStatus"]==4].copy()
    cur_df_4_gr=cur_df_4.groupby("MonthFU").agg({"ValueCount":["sum"]})
    cur_df_4_gr.columns=["value"]
     
    #print('Step 4')

    prev_df_4=prev_df[prev_df["ShortStatus"]==4].copy()
    prev_df_4_gr=prev_df_4.groupby("MonthFU").agg({"ValueCount":["sum"]})
    prev_df_4_gr.columns=["value"]
    #print('Step 5')

    merge_4_gr=cur_df_4_gr.merge(prev_df_4_gr,how="left", left_index=True, right_index=True)
    merge_4_gr.columns=["Cur","Prev"]
    merge_4_gr.fillna(0,inplace=True)
    merge_4_gr["Good"]=merge_4_gr["Prev"]-merge_4_gr["Cur"]
    merge_4_gr["Good"]=merge_4_gr["Good"].apply(lambda x:0 if x<0 else x)
    merge_4_gr["Month"]=merge_4_gr.index
    merge_4_gr["Month"]=merge_4_gr["Month"].apply(lambda x:" "+str(x) if x=="OVERDUE" else x)
    merge_4_gr=merge_4_gr.sort_values("Month",ascending=False).reset_index(drop=True)
    merge_4_gr.loc[merge_4_gr["Month"].str.contains("OVERDUE")>0,"Good"]=0

    merge_4_gr["Total"]=merge_4_gr["Cur"]+merge_4_gr["Good"]            

    cur_df_4[(cur_df_4["MonthFU"]=="OVERDUE") & (cur_df_4["Month"]==start_overdue_4)]
    newoverdue_4=cur_df_4[(cur_df_4["MonthFU"]=="OVERDUE") & (cur_df_4["Month_"]==start_overdue_4)].shape[0]

    merge_4_gr["NewOverdue"]=0
    merge_4_gr.loc[merge_4_gr["Month"].str.contains("OVERDUE"),"NewOverdue"]=newoverdue_4
    merge_4_gr.loc[merge_4_gr["Month"].str.contains("OVERDUE"),"Cur"]=merge_4_gr.loc[merge_4_gr["Month"].str.contains("OVERDUE"),"Cur"]-newoverdue_4

    cur_df_5=cur_df[cur_df["ShortStatus"]==5].copy()
    cur_df_5_gr=cur_df_5.groupby("MonthFU").agg({"ValueCount":["sum"]})
    cur_df_5_gr.columns=["value"]

    prev_df_5=prev_df[prev_df["ShortStatus"]==5].copy()
    prev_df_5_gr=prev_df_5.groupby("MonthFU").agg({"ValueCount":["sum"]})
    prev_df_5_gr.columns=["value"]

    merge_5_gr=cur_df_5_gr.merge(prev_df_5_gr,how="left", left_index=True, right_index=True)
    merge_5_gr.columns=["Cur","Prev"]
    merge_5_gr.fillna(0,inplace=True)
    merge_5_gr["Good"]=merge_5_gr["Prev"]-merge_5_gr["Cur"]
    merge_5_gr["Good"]=merge_5_gr["Good"].apply(lambda x:0 if x<0 else x)
    merge_5_gr["Month"]=merge_5_gr.index
    merge_5_gr["Month"]=merge_5_gr["Month"].apply(lambda x:" "+str(x) if x=="OVERDUE" else x)
    merge_5_gr=merge_5_gr.sort_values("Month",ascending=False).reset_index(drop=True)
    merge_5_gr.loc[merge_5_gr["Month"].str.contains("OVERDUE")>0,"Good"]=0


    merge_5_gr["Total"]=merge_5_gr["Cur"]+merge_5_gr["Good"]  
    newoverdue_5=cur_df_5[(cur_df_5["MonthFU"]=="OVERDUE") & (cur_df_5["Month_"]==start_overdue_5)].shape[0]

    merge_5_gr["NewOverdue"]=0
    merge_5_gr.loc[merge_5_gr["Month"].str.contains("OVERDUE"),"NewOverdue"]=newoverdue_5
    merge_5_gr.loc[merge_5_gr["Month"].str.contains("OVERDUE"),"Cur"]=merge_5_gr.loc[merge_5_gr["Month"].str.contains("OVERDUE"),"Cur"]-newoverdue_5

    generate_kibana_dash(merge_4_gr,4,reporttype)
    generate_kibana_dash(merge_5_gr,5,reporttype)

################################################################################
def messageReceived(destination,message,headers):
    global es,goodmonth
    records=0
    starttime = time.time()
    logger.info("==> "*10)
    logger.info("Message Received %s" % destination)


    if destination=="/topic/RECOMPUTE_502":
        try:
            recomputeKPI502()
        except Exception as er:
            logger.error(er)  
        return
    
    logger.info(headers)

    filepath=""
    file=""

    if "CamelSplitAttachmentId" in headers:
        headers["file"] = headers["CamelSplitAttachmentId"]

    if "file" in headers:
        logger.info("File:%s" %headers["file"])        
        log_message("Import of file [%s] started." % headers["file"])
        file=headers["file"]

    #file= './tmp/Safety Register - KPI502 - Lots_1_2_3 - 2019-04.xlsx'
    #file="Safety Register - KPI502 - Lots_1_2_3 - 2019-0.xlsx"


    logger.info("FILE      :"+file)
    filename=file
    logger.info("FILE NAME :"+filename)
    filedate=file[1:].split(".")[0][-7:]
    goodmonth=filedate.split("-")[1]+"-"+filedate.split("-")[0]
    logger.info("MONTH(BEF)     :"+goodmonth)

    gm=datetime.strptime(goodmonth,"%m-%Y")
    gm=gm- relativedelta(months=1)
    goodmonth=datetime.strftime(gm,"%m-%Y")
    

    xlsbytes = base64.b64decode(message)
    f = open('./tmp/excel.xlsx', 'wb')
    f.write(xlsbytes)
    f.close()
    filename = './tmp/excel.xlsx'

    # exit()

    #filename = './tmp/KPI502 - Lot2- 2019-01.xlsx'
    #file="KPI502 - Lot2- 2019-01.xlsx"


    #filename = './tmp/KPI502 - Lot2 - 2019-03.xlsx'
    

#    logger.info("MONTH(AFT)     :"+goodmonth)

    try:
        logger.info("Opening XLS...")
        dfdata=pd.DataFrame()
        for sheet in ["Lot 1","Lot 2","Lot 3","Lot 4"]:
            logger.info(">>>>>>>>>> LOADING:"+sheet)
            try:
                dfd = pd.read_excel(filename, sheet_name=sheet,skiprows=2,index_col=0)
            except:
                logger.error("Unable to read sheet.")
                continue
            dfd["Sheet"]=sheet
            #logger.info(dfd.describe())
            dfdata=dfdata.append(dfd)#,ignore_index = True)
            #break
        
        #dfdata=dfdata.reset_index()
        
        logger.info("Done.")
        #dfdata = dfdata.drop('Unnamed: 0', axis=1)
        dfdata=dfdata.reset_index()
        del dfdata["index"]
        ##print(dfdata.columns)
        logger.info(dfdata.columns)
        #newcols=['Month','SRid','Status','Opm. number','Definition','Building','Floor','Place','Technic','Sub-technic','materials','AssetCode','Device','BACid','Frequency','Control','Report','Report Date','Reference Date','Reference Year','Reference source date','Last shipment','Repeat','Point of interest','KPI timing','GroupNum','Type','FB Name','FB date','FB','Orig label','Orig Definition','Control organism','To','Cc 1','Cc 2','Cc 3','Cc 4','Cc 5','Cc 6','Cc 7','Cc 8','Cc 9','BAC Dept','BAC Sub Dept','BAC Service','Group','Contractor','Lot','KPI Type','ShortStatus','ShortStatusFU','ConcatShortStatus&OrigStatus','LongStatus','Classification nr (O/I)','Classification (O/I)','Show graph','Report link','Status (M-1)','Report Date (M-1)','FB Date (M-1)','Deleted vs M-1','MonthFU','Sheet']
        newcols=['Month','SRid','Status','Opm. number','Definition','Building','Floor','Place','Technic','Sub-technic','materials','AssetCode','Device','BACid','Frequency','Control','Report','Report Date','Reference Date','Reference Year','Reference source date','Last shipment','Repeat','Point of interest','KPI timing','GroupNum','Type','FB Name','FB date','FB','Orig label','Orig Definition','Control organism','To','Cc 1','Cc 2','Cc 3','Cc 4','Cc 5','Cc 6','Cc 7','Cc 8','Cc 9','BAC Dept','BAC Sub Dept','BAC Service','Group','Contractor','Lot','KPI Type','ShortStatus','ShortStatusFU','ConcatShortStatus&OrigStatus','LongStatus','Classification nr (O/I)','Classification (O/I)','Show graph','Report link','Status (M-1)','Report Date (M-1)','FB Date (M-1)','Deleted vs M-1',"KPI Type Nr","CheckArchived",'Sheet']
        logger.info(newcols)
        dfdata.columns = newcols
        dfdata.to_excel("./testExcel.xlsx")
        dfdata["KPI Type Nr"]=502


        dfdata["MonthFU"]=dfdata.apply(computeOverdue,axis=1)


        dfdata2 = dfdata.reset_index(drop=True)
        dfdata2.fillna('', inplace=True)
        dfdata2=dfdata2.drop(dfdata2[dfdata2["Month"]==''].index)
        dfdata2['_index'] = "biac_kpi502"
        #dfdata2['_timestamp'] = dfdata2['Reference Date'].apply(lambda x: getTimestamp(x)*1000)
        #dfdata2['_timestamp'] = dfdata2['Reference Date'].apply(lambda x: int(x.timestamp()*1000))
        dfdata2['Reference Date'] = dfdata2['Reference Date'].apply(lambda x: x if not x=="" else (datetime.now()-timedelta(days=31)) )
        dfdata2['_timestamp'] = dfdata2.apply(lambda row: fixDate(row), axis=1)
        dfdata2['_id'] = dfdata2.apply(lambda row: get_id(row['Month'], row['SRid']), axis=1)
        dfdata2['_id']=dfdata2['_id'].apply(lambda x:goodmonth+"-"+x)
        logger.info(dfdata2)
        
        filedate=file[1:].split(".")[0][-7:]
        dfdata2["filedate"]=filedate
        dfdata2['key'] = dfdata2.apply(computeReport , axis=1)
        dfdata2.to_excel("./tmp/kpi502_tmp.xlsx")

        dfdata2["ShortStatus"]=dfdata2["LongStatus"].apply(computeShortStatus)    
        logger.info(len(dfdata2))
        dfdata2=dfdata2[(dfdata2["ShortStatus"]==4) | (dfdata2["ShortStatus"]==5)]
        logger.info(len(dfdata2))
        

        dfdata2.drop_duplicates('_id', inplace=True)

        dfdata2["Month_"]=dfdata2["Month"]

        logger.info("BEFORE CLEANING")
        logger.info(dfdata2.shape)

        for x in [str(x) for x in range(2010,2018)]:
            dfdata2=dfdata2[~dfdata2["Month_"].str.contains(x)]
        
        dfdata2=dfdata2[~dfdata2["Month_"].isin(["2018-01","2018-02","2018-03","2018-04"])]

        logger.info("AFTER CLEANING")
        logger.info(dfdata2.shape)

        dfdata2["Month"]=goodmonth 
        
        dfdata2["ValueCount"]=1   
        

        logger.info(filedate)
        #res4=compute_previous_months(int(filedate.split("-")[1]),int(filedate.split("-")[0]),12,skip=0)
        res4=compute_previous_months(int(goodmonth.split("-")[0]),int(goodmonth.split("-")[1]),12,skip=0)
        res4lot4=compute_previous_months(int(goodmonth.split("-")[0]),int(goodmonth.split("-")[1]),6,skip=0)
        res4table=[]
        
        print(dfdata2['Reference Date'])


        dfdata2.to_excel('./new502.xlsx')

        for key in dfdata2['key'].unique():
            #print("===>"*30)
            #print(key)
            if key.startswith("Lot4"):
                res4touse=res4lot4
                #print("LOT4"*100)
            else:
                res4touse=res4
            for rec in res4touse:                
                res4table.append({"Month":goodmonth,"MonthFU":rec,"ShortStatus":4,"Status":4,"ValueCount":0,"_id":goodmonth+rec+"-S4-"+str(hash(key))
                                ,"_index":"biac_kpi502","key":key,"filedate":filedate})
                

        res4tabledf=pd.DataFrame(res4table)  
        res4tabledf.to_excel('./tablesKeys.xlsx')
        es_helper.dataframe_to_elastic(es,res4tabledf)
        
        #A/0
        #res5=compute_previous_months(int(filedate.split("-")[1]),int(filedate.split("-")[0]),6,skip=0)
        res5=compute_previous_months(int(goodmonth.split("-")[0]),int(goodmonth.split("-")[1]),6,skip=0)
        res5lot4=compute_previous_months(int(goodmonth.split("-")[0]),int(goodmonth.split("-")[1]),3,skip=0)
        res5table=[]
        for key in dfdata2['key'].unique():
            if key.startswith("Lot4"):
                res5touse=res5lot4
                #print("LOT4"*100)
            else:
                res5touse=res5
            for rec in res5touse:
                res5table.append({"Month":goodmonth,"MonthFU":rec,"ShortStatus":5,"Status":5,"ValueCount":0,"_id":goodmonth+rec+"-S5-"+str(hash(key))
                                    ,"_index":"biac_kpi502","key":key,"filedate":filedate})



# DELETE OLD DATA
        deletequery={
            "query":{
                "bool": {
                    "must": [
                    {
                        "query_string": {
                        "query": "Month: \""+goodmonth+"\""
                        }
                    }
                    ]
                }            
            }   
        }
        logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Deleting ORIGINALS records")
        logger.info(deletequery)
        try:
            resdelete=es.delete_by_query(body=deletequery,index="biac_kpi502")
            logger.info(resdelete)
        except Exception as e3:            
            logger.info(e3)   
            logger.info("Unable to delete records.") 
        
        logger.info("Waiting for deletion to finish")
        time.sleep(3)

# WRITE DATA
        res5tabledf=pd.DataFrame(res5table)  
        es_helper.dataframe_to_elastic(es,res5tabledf)
        
        es_helper.dataframe_to_elastic(es,dfdata2)
        

        ## NOW COMPUTE MONTH

        logger.info("Waiting for records to be written...")
        time.sleep(3)

        deletequery={
            "query":{
                "bool": {
                    "must": [
                    {
                        "query_string": {
                        "query": "Month: \""+goodmonth+"\""
                        }
                    }
                    ]
                }            
            }   
        }
        logger.info(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> Deleting records")
        logger.info(deletequery)
        try:
            resdelete=es.delete_by_query(body=deletequery,index="biac_month_kpi502")
            logger.info(resdelete)
        except Exception as e3:            
            logger.info(e3)   
            logger.info("Unable to delete records.") 
        
        logger.info("Waiting for deletion to finish")
        time.sleep(3)

        goodmonth_dt = datetime.strptime(goodmonth, '%m-%Y')
        
        


        df_kpi502 = es_helper.elastic_to_dataframe(es,"biac_kpi502",query='ShortStatusFU: Actievereist AND Month: '+goodmonth)
        df_kpi502_4= df_kpi502[df_kpi502["ShortStatus"]==4]


        df_kpi502_4_overdue=df_kpi502_4[df_kpi502_4["MonthFU"].str.contains("OVERDUE")].shape[0]

        logger.info("4 overdue:%d" %(df_kpi502_4_overdue))

        kpi502_4=df_kpi502_4_overdue/df_kpi502_4.shape[0]
        kpi502_4

        df_kpi502_5= df_kpi502[df_kpi502["ShortStatus"]==5]

        df_kpi502_5_overdue=df_kpi502_5[df_kpi502_5["MonthFU"].str.contains("OVERDUE")].shape[0]

        kpi502_5=df_kpi502_5_overdue/df_kpi502_5.shape[0]

        logger.info("5 overdue:%d" %(df_kpi502_5_overdue))

        #df_kpi502_4_overdue
        df_kpi502.shape[0]

        df_kpi502_4_obj={"total":df_kpi502_4.shape[0],"overdue":df_kpi502_4_overdue}
        df_kpi502_5_obj={"total":df_kpi502_5.shape[0],"overdue":df_kpi502_5_overdue}

        df_kpi502=dfdata2.copy()

        recs=[]
        recsfire=[]
        recsokko=[]


        for key in df_kpi502.key.unique():
            #logger.info(key)
            #goodmonth=filedate.split('-')[1]+"-"+filedate.split('-')[0]
            newrec={"key":key,"type":"summary","_id":goodmonth+"_"+key,"_index":"biac_month_kpi502","filedate":filedate,"Month":goodmonth}
            
            
            if key.startswith("Lot4"):
                limitmonth4=(goodmonth_dt-relativedelta(months=6)).strftime('%Y-%m')        
                limitmonth5=(goodmonth_dt-relativedelta(months=3)).strftime('%Y-%m')        
            else:
                limitmonth4=(goodmonth_dt-relativedelta(months=12)).strftime('%Y-%m')        
                limitmonth5=(goodmonth_dt-relativedelta(months=6)).strftime('%Y-%m')   

            logger.info(">>>>>KEY:"+key)
            logger.info(">>>>>LIMIT4:"+limitmonth4)
            logger.info(">>>>>LIMIT5:"+limitmonth5)

            df_kpi502_4= df_kpi502[(df_kpi502["ShortStatus"]==4)  & (df_kpi502["key"]==key)]

            newoverdues_4=0
            oldoverdues_4=0

            for index,row in df_kpi502_4.iterrows():
                newrec_okko={"key":key,"type":"stat","_index":"biac_month_kpi502","filedate":filedate,"Month":goodmonth}
                if "OVERDUE" in row["MonthFU"]:
                    if row["Month_"]==limitmonth4:
                        newrec_okko["sub_type"]="new_overdue_remarks"
                        newoverdues_4+=1
                    else:
                        newrec_okko["sub_type"]="overdue_remarks"
                        oldoverdues_4+=1
                    
                else:
                    newrec_okko["sub_type"]="ok_remarks"
                newrec_okko["@timestamp"]=datetime.utcnow().isoformat()
                recsokko.append(newrec_okko)
                    
                    
                    

            logger.info(">>>>>"+str(df_kpi502_4[df_kpi502_4["Month_"]==limitmonth4].shape[0]))
            #df_kpi502_4_overdue=df_kpi502_4[df_kpi502_4["MonthFU"].str.contains("OVERDUE")].shape[0]
            df_kpi502_4_overdue=newoverdues_4

            kpi502_4_percentage=1
            if df_kpi502_4.shape[0]-oldoverdues_4>0:
                kpi502_4_percentage=(df_kpi502_4.shape[0]-oldoverdues_4-newoverdues_4)/(df_kpi502_4.shape[0]-oldoverdues_4)
            else:
                kpi502_4_percentage=1
                
            df_kpi502_4_obj={"total":df_kpi502_4.shape[0],"overdue":df_kpi502_4_overdue,"percentage":round(kpi502_4_percentage*100,2)}

            newrec["Total_Remarks"]=df_kpi502_4.shape[0]
            newrec["Overdues_Remarks"]=df_kpi502_4_overdue

            newrec["Old_Overdues_Remarks"]=oldoverdues_4
            newrec["New_Overdues_Remarks"]=newoverdues_4


            newrec["KPI_Remarks"]=round(kpi502_4_percentage*100,2)
            
            
            df_kpi502_5= df_kpi502[(df_kpi502["ShortStatus"]==5)  & (df_kpi502["key"]==key)]
            
            newoverdues_5=0
            oldoverdues_5=0

            for index,row in df_kpi502_5.iterrows():
                newrec_okko={"key":key,"type":"stat","_index":"biac_month_kpi502","filedate":filedate,"Month":goodmonth}
                if "OVERDUE" in row["MonthFU"]:
                    
                    if row["Month_"]==limitmonth5:
                        newrec_okko["sub_type"]="new_overdue_breaches"
                        newoverdues_5+=1
                    else:
                        newrec_okko["sub_type"]="overdue_breaches"
                        oldoverdues_5+=1
                else:
                    newrec_okko["sub_type"]="ok_breaches"
                
                newrec_okko["@timestamp"]=datetime.utcnow().isoformat()
                recsokko.append(newrec_okko)
            
            #df_kpi502_5_overdue=df_kpi502_5[df_kpi502_5["MonthFU"].str.contains("OVERDUE")].shape[0]
            df_kpi502_5_overdue=newoverdues_5

            kpi502_5_percentage=1
            if df_kpi502_5.shape[0]-oldoverdues_5>0:
                kpi502_5_percentage=(df_kpi502_5.shape[0]-oldoverdues_5-newoverdues_5)/(df_kpi502_5.shape[0]-oldoverdues_5)
            else:
                kpi502_5_percentage=1


        #    logger.info(df_kpi502_5)
            newrec["Total_Breaches"]=df_kpi502_5.shape[0];
            newrec["Overdues_Breaches"]=df_kpi502_5_overdue;
            newrec["KPI_Breaches"]=round(kpi502_5_percentage*100,2);

            newrec["Old_Overdues_Breaches"]=oldoverdues_5
            newrec["New_Overdues_Breaches"]=newoverdues_5


            newrec["Total"]=df_kpi502_4.shape[0]+df_kpi502_5.shape[0];
            newrec["Overdues"]=df_kpi502_4_overdue+df_kpi502_5_overdue;
            percentage=0
            if df_kpi502_4.shape[0]+df_kpi502_5.shape[0]>0:        
                percentage=(df_kpi502_4_overdue+df_kpi502_5_overdue)/(df_kpi502_4.shape[0]+df_kpi502_5.shape[0])
            
            newrec["KPI"]=round(percentage*100,2);
            if "BACFIR" not in key:
                recs.append(newrec)
            else:
                recsfire.append(newrec)
            

        pdfire=pd.DataFrame(recsfire)
        firerec=json.loads(pdfire.sum().to_json())
        firerec["filedate"]=filedate
        firerec["Month"]=goodmonth
        firerec["type"]="summary"
        firerec["_index"]="biac_month_kpi502"
        firerec["key"]="ALL (BACFIR)"
        if firerec["Total_Breaches"]-firerec["Old_Overdues_Breaches"]>0:
            firerec["KPI_Breaches"]=round(100*(firerec["Total_Breaches"]-firerec["Old_Overdues_Breaches"]-firerec["New_Overdues_Breaches"])/(firerec["Total_Breaches"]-firerec["Old_Overdues_Breaches"]),2)
        else:
            firerec["KPI_Breaches"]=100
            
        if firerec["Total_Remarks"]>0:
            firerec["KPI_Remarks"]=round(100*(firerec["Total_Remarks"]-firerec["Old_Overdues_Remarks"]-firerec["New_Overdues_Remarks"])/(firerec["Total_Remarks"]-firerec["Old_Overdues_Remarks"]),2)
        else:
            firerec["KPI_Remarks"]=100

        df_month_kpi502=pd.DataFrame(recs+[firerec])

        es_helper.dataframe_to_elastic(es,df_month_kpi502)

        df_month_kpi502_stats=pd.DataFrame(recsokko)
        es_helper.dataframe_to_elastic(es,df_month_kpi502_stats)

    except Exception as e:
        endtime = time.time()
        logger.error(e,exc_info=True)
        log_message("Import of file [%s] failed. Duration: %d Exception: %s." % (file,(endtime-starttime),str(e)))        


    
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
        resupdate=es.update_by_query(body=updatequery,index="biac_kpi502")
        logger.info(resupdate)
    except Exception as e3:            
        logger.error(e3)   
        logger.error("Unable to update records biac_month_kpi502.") 

    try:
        resupdate=es.update_by_query(body=updatequery,index="biac_month_kpi502")
        logger.info(resupdate)
    except Exception as e3:            
        logger.error(e3)   
        logger.error("Unable to update records biac_month_kpi502.") 

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
        resupdate=es.update_by_query(body=updatequery,index="biac_kpi502")
        logger.info(resupdate)
    except Exception as e3:            
        logger.error(e3)   
        logger.error("Unable to update records biac_month_kpi502.") 

    try:
        resupdate=es.update_by_query(body=updatequery,index="biac_month_kpi502")
        logger.info(resupdate)
    except Exception as e3:            
        logger.error(e3)   
        logger.error("Unable to update records biac_month_kpi502.") 

    try:
        recomputeKPI502()
    except Exception as er:
        logger.error(er)    

    endtime = time.time()    
    try:
        log_message("Import of file [%s] finished. Duration: %d Records: %d." % (file,(endtime-starttime),df.shape[0]))         
    except:
        log_message("Import of file [%s] finished. Duration: %d." % (file,(endtime-starttime)))    


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
