"""
GTC SITES COMPUTED
====================================

Collections:
-------------------------------------


VERSION HISTORY
===============

* 04 Sep 2019 0.0.2 **PDE** First version
* 04 Sep 2019 0.0.3 **PDE** Adding **VME**'s functions
* 05 Sep 2019 0.0.7 **VME** Adding fields to the lutosa cogen records (ratios elec, heat, biogaz, gaznat, w/o zeros...)
* 06 Sep 2019 0.0.8 **VME** fix bug with indices not at starting day 

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

MODULE  = "GTC_SITES_COMPUTED"
VERSION = "0.0.16"
QUEUE   = ["GTC_SITES_COMPUTED_RANGE"]

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

def getIndex(dateStr):
    _index = 'opt_sites_computed-'+dateStr[:-3]
    return _index

def getCustomMin(minrow):
    minrow = minrow[minrow!=0]
    if np.isnan(minrow.min()):
        return 0
    else:
        return minrow.min()

def calc_dispo_thiopaq(raw):
    condition = 0
    if raw['value_debit'] >= 120 and raw['value_pression'] >= 15 and raw['value_pression'] < 36:
        condition = 1
        
    return condition

def get_dispo_thiopaq(df_raw):
    df_debit_biogaz = df_raw[df_raw['area_name']=='LUTOSA_ExportNyxAWS_COGLTS_BIOLTS_Valeur_Debit_Biogaz_Thiopaq'][['@timestamp', 'value']]
    df_debit_biogaz.columns = ['@timestamp', 'value_debit']
    df_pression_thiopaq = df_raw[df_raw['area_name']=='LUTOSA_ExportNyxAWS_COGLTS_BIOLTS_Valeur_Pression_Thiopaq_Entree'][['@timestamp', 'value']]
    df_pression_thiopaq.columns = ['@timestamp', 'value_pression']
    df_fct_thiopaq = df_raw[df_raw['area_name']=='LUTOSA_ExportNyxAWS_LUTOSA_Etat_Thiopaq_Fct'][['@timestamp', 'value']]
    df_fct_thiopaq.columns = ['@timestamp', 'value_fct']

    df_computed = df_debit_biogaz.set_index('@timestamp').join(df_pression_thiopaq.set_index('@timestamp').join(df_fct_thiopaq.set_index('@timestamp')))
    df_computed['condition_fct'] = df_computed.apply(lambda raw: calc_dispo_thiopaq(raw), axis=1)

    tps_total_fct = df_computed['value_fct'].sum()
    tps_normal_dispo = df_computed['condition_fct'].sum()
    dispo = tps_total_fct / tps_normal_dispo
    if dispo > 1:
        dispo = 1
    
    return dispo



def compute_fct_thiopaq(df_raw):
    df_debit = df_raw[df_raw['area_name']=='LUTOSA_ExportNyxAWS_COGLTS_BIOLTS_Valeur_Debit_Biogaz_Thiopaq'][['@timestamp', 'value']]
    df_debit.columns = ['@timestamp', 'value_debit']
    df_h2s_thiopaq = df_raw[df_raw['area_name']=='LUTOSA_ExportNyxAWS_LUTOSA_H2S_Ap_Thiopaq'][['@timestamp', 'value']]
    df_h2s_thiopaq.columns = ['@timestamp', 'value_h2s']
    df_pression_thiopaq = df_raw[df_raw['area_name']=='LUTOSA_ExportNyxAWS_COGLTS_BIOLTS_Valeur_Pression_Thiopaq_Entree'][['@timestamp', 'value']]
    df_pression_thiopaq.columns = ['@timestamp', 'value_pression']
    df_computed = df_debit.set_index('@timestamp').join(df_h2s_thiopaq.set_index('@timestamp').join(df_pression_thiopaq.set_index('@timestamp')))
    df_computed['value'] = df_computed.apply(lambda raw: calc_fct_thiopaq(raw), axis=1)
    sumvalue = df_computed['value'].mean()*24
    return sumvalue

def calc_fct_thiopaq(raw):
    debit = 1 if raw['value_debit'] > 120 else 0
    h2s = 1 if raw['value_h2s'] < 5000 else 0
    pression =  1 if raw['value_pression'] >= 15 and raw['value_pression'] <= 40 else 0
    result = debit * h2s * pression
    #print(raw)
    return result

def get_tps_fct_thiopaq(df_raw):
    df_fct = df_raw[df_raw['area_name']=='LUTOSA_ExportNyxAWS_LUTOSA_Etat_Thiopaq_Fct'][['@timestamp', 'value']]
    fct = df_fct['value'].mean()*24
    return fct

def get_tps_fct_real_thiopaq(fct_max, tps_fct):
    cond_1 = 1 if fct_max != 0 else 0
    cond_2 = 1
    if cond_1 == 0:
        cond_2 = 0
    else:
        cond_2 = tps_fct / fct_max
        if cond_2 > 1:
            cond_2 = 1
            
    result = cond_1 * cond_2
    return result
    

def get_tps_fct_surpresseur(df_raw):
    df_surp1 = df_raw[df_raw['area_name']=='LUTOSA_ExportNyxAWS_LUTOSA_Etat_Surp1_Fct'][['@timestamp', 'value']]
    df_surp1.columns = ['@timestamp', 'surp1']
    df_surp2 = df_raw[df_raw['area_name']=='LUTOSA_ExportNyxAWS_LUTOSA_Etat_Surp2_Fct'][['@timestamp', 'value']]
    df_surp2.columns = ['@timestamp', 'surp2']
    
    df_computed = df_surp1.set_index('@timestamp').join(df_surp2.set_index('@timestamp'))
    df_computed['value'] = df_computed.apply(lambda raw: calc_surpr(raw), axis=1)
    return df_computed['value'].mean()*24

def calc_surpr(raw):
    if raw['surp1'] == 1 or raw['surp2'] == 1:
        return 1
    else:
        return 0
    
def get_total_biogaz_thiopaq(df_raw):
    df_debit = df_raw[df_raw['area_name']=='LUTOSA_ExportNyxAWS_COGLTS_BIOLTS_Valeur_Debit_Biogaz_Thiopaq'][['@timestamp', 'value']]
    df_debit['filtre120220'] = df_debit['value'].apply(lambda x: getFiltre(x , 120))
    df_debit['filtre220330'] = df_debit['value'].apply(lambda x: getFiltre(x , 220))
    df_debit['filtre330600'] = df_debit['value'].apply(lambda x: getFiltre(x , 330))
    results = dict()
    results['120'] = df_debit['filtre120220'].sum() / 60
    results['220'] = df_debit['filtre220330'].sum() / 60
    results['330'] = df_debit['filtre330600'].sum() / 60
    return results

def getFiltre(value, filtre):
    if filtre == 120:
        if value >=120 and value <= 220:
            return value
    elif filtre == 220:
        if value >=220 and value <= 330:
            return value
    elif filtre == 330:
        if value >=330 and value <= 600:
            return value

    return 0

def retrieve_raw_data(day):
    start_dt = datetime(day.year, day.month, day.day)
    end_dt   = datetime(start_dt.year, start_dt.month, start_dt.day, 23, 59, 59)

    df_raw=es_helper.elastic_to_dataframe(es, index='opt_sites_data*', 
                                           query='*', 
                                           start=start_dt, 
                                           end=end_dt,
                                           scrollsize=10000,
                                           size=1000000)

    containertimezone=pytz.timezone(get_localzone().zone)
    logger.info('Data retrieved')
    logger.info(df_raw)
    df_raw['@timestamp'] = pd.to_datetime(df_raw['@timestamp'], \
                                               unit='ms', utc=True).dt.tz_convert(containertimezone)
    df_raw=df_raw.sort_values('@timestamp') 
    
    return df_raw

def compute_avail_debit_entree_thiopac(df_raw):
    df_debit_biogaz = df_raw[df_raw['area_name']=='LUTOSA_ExportNyxAWS_COGLTS_BIOLTS_Valeur_Debit_Biogaz_Thiopaq'][['@timestamp', 'value']]
    
    df_debit_biogaz['bit'] = 0
    df_debit_biogaz.loc[(df_debit_biogaz['value']>220) & (df_debit_biogaz['value']<600), 'bit'] = 1
    
    percent_value = sum(df_debit_biogaz['bit']) / df_debit_biogaz.shape[0]
    
    entry_biogaz_thiopaq = df_debit_biogaz['value'].mean()*24
    
    return percent_value, entry_biogaz_thiopaq, df_debit_biogaz

def compute_gaznat_entry(df_raw):
    df_debit_gaznat = df_raw[df_raw['area_name']=='LUTOSA_ExportNyxAWS_COGLTS_BIOLTS_Valeur_Debit_GazNat_Thiopaq'][['@timestamp', 'value']]
    entry_gaznat = df_debit_gaznat['value'].mean()*24
    return entry_gaznat

def compute_entry_biogaz_cogen(df_raw):
    df_entree_biogaz = df_raw[df_raw['area_name']=='LUTOSA_ExportNyxAWS_COGLTS_BIOLTS_Valeur_Debit_Biogaz_Cogen'][['@timestamp', 'value']]
    
    df_entree_biogaz['corrected_value'] = 0
    df_entree_biogaz.loc[(df_entree_biogaz['value']>0) & (df_entree_biogaz['value']<1000), 'corrected_value'] = df_entree_biogaz['value'] 
    entry_biogaz_cogen = df_entree_biogaz['corrected_value'].mean()*24
    
    return entry_biogaz_cogen, df_entree_biogaz

def compute_avail_moteur(df_raw):
    df_moteur = df_raw[df_raw['area_name']=='LUTOSA_ExportNyxAWS_LUTOSA_Etat_Mot_Fct']
    
    percent_value = sum(df_moteur['value']) / df_moteur.shape[0]
    
    return percent_value

def compute_avail_puissance(df_raw):
    df_puissance = df_raw[df_raw['area_name']=='LUTOSA_ExportNyxAWS_LUTOSA_Cpt_Elec_OutToMoteur'][['@timestamp', 'value']]
    df_puissance['diff'] = df_puissance['value'].diff()
    df_puissance['diff'] = df_puissance['diff'].fillna(0)
    df_puissance['bit'] = 0
    df_puissance.loc[(df_puissance['diff']>0), 'bit'] = 1

    percent_value = sum(df_puissance['bit']) / df_puissance.shape[0]
    return percent_value, df_puissance

def compute_out_cogen(df_raw):
    df_out_elec = df_raw[df_raw['area_name']=='LUTOSA_ExportNyxAWS_LUTOSA_Cpt_Ther_HT'][['@timestamp', 'value']]
    df_out_elec=df_out_elec[df_out_elec['value']!=0]
    out_elec = max(df_out_elec['value']) - min(df_out_elec['value'])

    df_out_to_moteur = df_raw[df_raw['area_name']=='LUTOSA_ExportNyxAWS_LUTOSA_Cpt_Elec_OutToMoteur'][['@timestamp', 'value']]
    df_out_to_moteur=df_out_to_moteur[df_out_to_moteur['value']!=0]
    out_to_moteur = max(df_out_to_moteur['value']) - min(df_out_to_moteur['value'])

    return out_elec, out_to_moteur

def create_obj(day, df_raw):
    #df_raw = retrieve_raw_data(day)
    
    percent_value, entry_biogaz_thiopaq, df_debit_biogaz = compute_avail_debit_entree_thiopac(df_raw)
    
    df_debit_biogaz.tail()

    obj_report_cogen = {
        'in_biogaz_thiopaq': entry_biogaz_thiopaq,
        'daily_avail'      : percent_value,
        'daily_avail_hour'  : percent_value*24,
    }
    
    entry_gaznat = compute_gaznat_entry(df_raw)
    obj_report_cogen['in_gaznat_cogen'] = entry_gaznat
    #obj_report_cogen['in_gaznat_cogen_kWh'] = obj_report_cogen['in_gaznat_cogen'] * 11.5
    obj_report_cogen['in_gaznat_cogen_kWh'] = obj_report_cogen['in_gaznat_cogen'] * 10.42
    obj_report_cogen['in_gaznat_cogen_MWh'] = obj_report_cogen['in_gaznat_cogen_kWh'] / 1000
    
    entry_biogaz_cogen, df_entree_biogaz = compute_entry_biogaz_cogen(df_raw)
    obj_report_cogen['in_biogaz_cogen'] = entry_biogaz_cogen
    obj_report_cogen['in_biogaz_chaudiere'] = obj_report_cogen['in_biogaz_thiopaq'] - obj_report_cogen['in_biogaz_cogen']
    #obj_report_cogen['in_biogaz_cogen_kWh'] = obj_report_cogen['in_biogaz_cogen'] * 5.98
    obj_report_cogen['in_biogaz_cogen_kWh'] = obj_report_cogen['in_biogaz_cogen'] * 6.1656
    obj_report_cogen['in_biogaz_cogen_MWh'] = obj_report_cogen['in_biogaz_cogen_kWh'] / 1000
    
    obj_report_cogen['in_total_cogen_kWh'] = obj_report_cogen['in_biogaz_cogen_kWh'] + obj_report_cogen['in_gaznat_cogen_kWh']
    
    percent_value = compute_avail_moteur(df_raw)
    obj_report_cogen['daily_avail_moteur'] = percent_value
    obj_report_cogen['daily_avail_moteur_hour'] = percent_value*24
    
    percent_value = compute_avail_puissance(df_raw)[0]
    obj_report_cogen['daily_avail_puissance'] = percent_value
    obj_report_cogen['daily_avail_puissance_hour'] = percent_value*24
    
    if  obj_report_cogen['daily_avail'] == 0:
        obj_report_cogen['daily_avail_moteur_real'] = 0
        obj_report_cogen['daily_avail_puissance_real'] = 0
    else:  
        if obj_report_cogen['daily_avail_moteur'] > obj_report_cogen['daily_avail']:
            obj_report_cogen['daily_avail_moteur_real'] = 1
        else:
            obj_report_cogen['daily_avail_moteur_real'] = obj_report_cogen['daily_avail_moteur'] / obj_report_cogen['daily_avail']

        if obj_report_cogen['daily_avail_puissance'] > obj_report_cogen['daily_avail']:
            obj_report_cogen['daily_avail_puissance_real'] = 1
        else:
            obj_report_cogen['daily_avail_puissance_real'] = obj_report_cogen['daily_avail_puissance'] / obj_report_cogen['daily_avail']

    
    out_elec, out_to_moteur = compute_out_cogen(df_raw)
    obj_report_cogen['out_elec_kWh'] = out_elec
    obj_report_cogen['out_moteur_kWh'] = out_to_moteur
    obj_report_cogen['out_total_kWh'] = obj_report_cogen['out_elec_kWh'] + obj_report_cogen['out_moteur_kWh']
    
    
    
    if obj_report_cogen['in_total_cogen_kWh'] == 0: 
        obj_report_cogen['total_efficiency'] = 0
        obj_report_cogen['elec_efficiency'] = 0
        obj_report_cogen['heat_efficiency'] = 0

        obj_report_cogen['gaznat_ratio'] = 0
        obj_report_cogen['biogaz_ratio'] = 0
        
    else:
        obj_report_cogen['total_efficiency'] = obj_report_cogen['out_total_kWh'] / obj_report_cogen['in_total_cogen_kWh']
        obj_report_cogen['elec_efficiency'] = obj_report_cogen['out_elec_kWh'] / obj_report_cogen['in_total_cogen_kWh']
        obj_report_cogen['heat_efficiency'] = obj_report_cogen['out_moteur_kWh'] / obj_report_cogen['in_total_cogen_kWh']
    
        obj_report_cogen['gaznat_ratio'] = obj_report_cogen['in_gaznat_cogen_kWh'] / obj_report_cogen['in_total_cogen_kWh']
        obj_report_cogen['biogaz_ratio'] = obj_report_cogen['in_biogaz_cogen_kWh'] / obj_report_cogen['in_total_cogen_kWh']
    
        obj_report_cogen['total_efficiency_wo_zero'] = obj_report_cogen['out_total_kWh'] / obj_report_cogen['in_total_cogen_kWh']
        obj_report_cogen['elec_efficiency_wo_zero'] = obj_report_cogen['out_elec_kWh'] / obj_report_cogen['in_total_cogen_kWh']
        obj_report_cogen['heat_efficiency_wo_zero'] = obj_report_cogen['out_moteur_kWh'] / obj_report_cogen['in_total_cogen_kWh']
    
        obj_report_cogen['gaznat_ratio_wo_zero'] = obj_report_cogen['in_gaznat_cogen_kWh'] / obj_report_cogen['in_total_cogen_kWh']
        obj_report_cogen['biogaz_ratio_wo_zero'] = obj_report_cogen['in_biogaz_cogen_kWh'] / obj_report_cogen['in_total_cogen_kWh']
    
    
    if obj_report_cogen['out_total_kWh'] == 0: 
        obj_report_cogen['heat_ratio'] = 0
        obj_report_cogen['elec_ratio'] = 0
        
    else:
        obj_report_cogen['heat_ratio'] = obj_report_cogen['out_moteur_kWh'] / obj_report_cogen['out_total_kWh']
        obj_report_cogen['elec_ratio'] = obj_report_cogen['out_elec_kWh'] / obj_report_cogen['out_total_kWh']
    
        obj_report_cogen['heat_ratio_wo_zero'] = obj_report_cogen['out_moteur_kWh'] / obj_report_cogen['out_total_kWh']
        obj_report_cogen['elec_ratio_wo_zero'] = obj_report_cogen['out_elec_kWh'] / obj_report_cogen['out_total_kWh']


    
    fct_max_thiopaq_min = compute_fct_thiopaq(df_raw)
    obj_report_cogen['fct_max_thiopaq'] = fct_max_thiopaq_min
    fct_max_thiopaq_min_avail = fct_max_thiopaq_min/24
    obj_report_cogen['fct_max_thiopaq_min_avail'] = fct_max_thiopaq_min_avail

    tps_fct_thiopaq_min = get_tps_fct_thiopaq(df_raw)
    obj_report_cogen['tps_fct_thiopaq'] = tps_fct_thiopaq_min
    tps_fct_thiopaq_avail = tps_fct_thiopaq_min /24
    obj_report_cogen['tps_fct_thiopaq_avail'] = tps_fct_thiopaq_avail

    tps_fct_surpresseur_min = get_tps_fct_surpresseur(df_raw)
    obj_report_cogen['tps_fct_surpresseur'] =  tps_fct_surpresseur_min
    tps_fct_surpresseur_avail = tps_fct_surpresseur_min / 24
    obj_report_cogen['tps_fct_surpresseur_avail'] = tps_fct_surpresseur_avail

    tps_fct_real_thiopaq = get_tps_fct_real_thiopaq(fct_max_thiopaq_min, tps_fct_thiopaq_min) 
    obj_report_cogen['tps_fct_real_thiopaq'] = tps_fct_real_thiopaq
    tps_fct_real_surpresseur = get_tps_fct_real_thiopaq(fct_max_thiopaq_min, tps_fct_surpresseur_min)
    obj_report_cogen['tps_fct_real_surpresseur'] = tps_fct_real_surpresseur
    total_biogaz_thiopaq = get_total_biogaz_thiopaq(df_raw)
    obj_report_cogen['total_biogaz_thiopaq'] = total_biogaz_thiopaq
    total_biogaz_thiopaq_120 = total_biogaz_thiopaq['120']
    obj_report_cogen['total_biogaz_thiopaq_120'] = total_biogaz_thiopaq_120
    total_biogaz_thiopaq_220 = total_biogaz_thiopaq['220']
    obj_report_cogen['total_biogaz_thiopaq_220'] = total_biogaz_thiopaq_220
    total_biogaz_thiopaq_330 = total_biogaz_thiopaq['330']
    obj_report_cogen['total_biogaz_thiopaq_330'] = total_biogaz_thiopaq_330 
    total_biogaz_thiopaq_120_600 = total_biogaz_thiopaq_120 + total_biogaz_thiopaq_220 + total_biogaz_thiopaq_330
    obj_report_cogen['total_biogaz_thiopaq_120_600'] = total_biogaz_thiopaq_120_600
    

    dispo_thiopaq = get_dispo_thiopaq(df_raw)
    obj_report_cogen['dispo_thiopaq'] = dispo_thiopaq
    obj_report_cogen['dispo_thiopaq_prct'] = dispo_thiopaq * 100



    

    
    
    
    
    return obj_report_cogen


def save_tags_to_computed(es, obj):
    list_of_tag = ['in_biogaz_thiopaq', 'in_gaznat_cogen', 
               'in_gaznat_cogen_kWh', 'in_biogaz_cogen', 'in_biogaz_chaudiere', 'in_biogaz_cogen_kWh', 
               'in_total_cogen_kWh', 'out_elec_kWh', 
               'out_moteur_kWh', 'out_total_kWh']


    bulkbody = ''

    for i in list_of_tag:

        new_obj = {
            'client': 'COGLTS',
            'area': 'COGEN_LUTOSA_COMPUTED',
            'name': i,
            'value': obj[i],
            'area_name':'COGEN_LUTOSA_COMPUTED'+i,
            'client_area_name': 'COGLTSCOGEN_LUTOSA_COMPUTED'+i,
            '@timestamp': obj['@timestamp']
        }
        
        _id = new_obj['client_area_name'] + '-' + new_obj['@timestamp'].strftime('%Y-%m-%d')

        index = 'opt_sites_computed-' + new_obj['@timestamp'].strftime('%Y-%m')

        action={}
        action["index"]={"_index":index, "_type":"doc", "_id":_id}
        
        
        bulkbody+=json.dumps(action)+"\r\n"
        bulkbody+=json.dumps(new_obj, cls=DateTimeEncoder)+"\r\n"

    diff_list = [
        {
            'name': 'INminusOUT',
            'a': 'in_total_cogen_kWh',
            'b': 'out_total_kWh',
        },
        {
            'name': 'INminusHEAT',
            'a': 'in_total_cogen_kWh',
            'b': 'out_moteur_kWh',
        },
        {
            'name': 'INminusELEC',
            'a': 'in_total_cogen_kWh',
            'b': 'out_elec_kWh',
        },
    ]

    for i in diff_list:

        new_obj = {
            'client': 'COGLTS',
            'area': 'COGEN_LUTOSA_COMPUTED',
            'name': i['name'],
            'value': obj[i['a']] - obj[i['b']],
            'area_name':'COGEN_LUTOSA_COMPUTED'+i['name'],
            'client_area_name': 'COGLTSCOGEN_LUTOSA_COMPUTED'+i['name'],
            '@timestamp': obj['@timestamp']
        }
        
        _id = new_obj['client_area_name'] + '-' + new_obj['@timestamp'].strftime('%Y-%m-%d')

        index = 'opt_sites_computed-' + new_obj['@timestamp'].strftime('%Y-%m')

        action={}
        action["index"]={"_index":index, "_type":"doc", "_id":_id}
        
        
        bulkbody+=json.dumps(action)+"\r\n"
        bulkbody+=json.dumps(new_obj, cls=DateTimeEncoder)+"\r\n"



    bulkres=es.bulk(bulkbody)

    if(not(bulkres["errors"])):
        logger.info("BULK done without errors.")
    else:
        logger.info("BULK ERRORS.")
        logger.info(bulkres)

def getSite(raw):
    site = raw['client']
    if raw['client'] == 'COGEN' or raw['client'] == 'cogenmail':
        splits = raw['area_name'].split('_')
        site = splits[1]

    return site

def removeStr(x):
    response = x
    if isinstance(x, str):
        response = int(x)
        
    return response

        


def doTheWork(start):
    #now = datetime.now()

    start = datetime(start.year, start.month, start.day)

    df = retrieve_raw_data(start)

    try:
        obj_to_es = create_obj(start, df)
        obj_to_es['@timestamp'] = containertimezone.localize(start)
        obj_to_es['site'] = 'LUTOSA'
        es.index(index='daily_cogen_lutosa', doc_type='doc', id=int(start.timestamp()), body = obj_to_es)

        save_tags_to_computed(es, obj_to_es)
    except Exception as er:
        logger.error('Error During creating specifics data')
        logger.error(er)

    try:
        df = retrieve_raw_data(start)
        df['value'] = df['value'].apply(lambda x: removeStr(x))
        df['value_min'] = df['value']
        df['value_min_sec'] = df['value']
        df['value_max'] = df['value']
        df['value_avg'] = df['value']
        df_grouped = df.groupby(['client_area_name', 'area_name', 'client', 'area']).agg({'@timestamp': 'min', 'value_min': 'min', 'value_max': 'max', 'value_avg': 'mean', 'value_min_sec':getCustomMin}).reset_index()
        df_grouped['value_day'] = df_grouped['value_max'] - df_grouped['value_min']
        df_grouped['conso_day'] = df_grouped['value_avg'] * 24
        df_grouped['availability']= df_grouped['value_avg'] * 1440
        df_grouped['availability_perc'] = df_grouped['value_avg'] * 100
        df_grouped['value_day_sec'] = df_grouped['value_max'] - df_grouped['value_min_sec']
        df_grouped['date'] = df_grouped['@timestamp'].apply(lambda x: getDate(x))
        df_grouped['month'] = df_grouped['date'].apply(lambda x: getMonth(x))
        df_grouped['year'] = df_grouped['date'].apply(lambda x: getYear(x))
        df_grouped['_id'] = df_grouped['client_area_name'] +'-'+ df_grouped['date']
        df_grouped['_index'] = df_grouped['date'].apply(lambda x : getIndex(x))
        df_grouped['site'] = df_grouped.apply(lambda raw: getSite(raw), axis=1)
            

        es_helper.dataframe_to_elastic(es, df_grouped)
        print("data inserted for day " + str(start))
            
        print("finished")
    except Exception as er:
        logger.error('Unable to compute data for ' + str(start))
        logger.error(er)


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
                    doTheWork(start-timedelta(1))
                    doTheWork(start)
                except Exception as e2:
                    logger.error("Unable to load sites data.")
                    logger.error(e2,exc_info=True)
            
        except Exception as e:
            logger.error("Unable to send life sign.")
            logger.error(e)
    #app.run(threaded=True,host= '0.0.0.0')