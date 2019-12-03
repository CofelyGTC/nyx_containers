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
* 26 Sep 2019 0.0.17 **VME** Change totally the daily object (daily_cogen_lutosa)  
* 15 Oct 2019 0.0.21 **VME** Add the dynamic target depending on open days in the current year  
* 17 Oct 2019 0.0.22 **VME** Add 'on' field to daily_cogen
* 17 Oct 2019 0.0.23 **VME** Add starts and stops fields to daily_cogen
* 23 Oct 2019 0.0.24 **VME** Get targets from cogen_parameters
* 24 Oct 2019 0.0.25 **VME** Add day_on condition for avail_ratio Thiopaq and COGEN
* 05 Nov 2019 0.0.26 **VME** Adding the drycooler and ht for heat production + modification of condition (thiopaq and cogen)

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

from lib import cogenhelper as ch


containertimezone=pytz.timezone(get_localzone().zone)

MODULE  = "GTC_SITES_COMPUTED"
VERSION = "0.0.26"
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
    df_debit_biogas = df_raw[df_raw['area_name']=='LUTOSA_ExportNyxAWS_COGLTS_BIOLTS_Valeur_Debit_Biogaz_Thiopaq'][['@timestamp', 'value']]
    df_debit_biogas.columns = ['@timestamp', 'value_debit']
    df_pression_thiopaq = df_raw[df_raw['area_name']=='LUTOSA_ExportNyxAWS_COGLTS_BIOLTS_Valeur_Pression_Thiopaq_Entree'][['@timestamp', 'value']]
    df_pression_thiopaq.columns = ['@timestamp', 'value_pression']
    df_fct_thiopaq = df_raw[df_raw['area_name']=='LUTOSA_ExportNyxAWS_LUTOSA_Etat_Thiopaq_Fct'][['@timestamp', 'value']]
    df_fct_thiopaq.columns = ['@timestamp', 'value_fct']

    df_computed = df_debit_biogas.set_index('@timestamp').join(df_pression_thiopaq.set_index('@timestamp').join(df_fct_thiopaq.set_index('@timestamp')))
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

def compute_avail_debit_entry_thiopac(df_raw):
    tag_name = 'LUTOSA_ExportNyxAWS_COGLTS_BIOLTS_Valeur_Debit_Biogaz_Thiopaq'
    df_debit_biogas = df_raw[df_raw['area_name']==tag_name][['@timestamp', 'value']]
    
    df_debit_biogas['bit_minus_120'] = 0
    df_debit_biogas['bit_120_220'] = 0
    df_debit_biogas['bit_220_330'] = 0
    df_debit_biogas['bit_330_600'] = 0
    df_debit_biogas.loc[(df_debit_biogas['value']<120), 'bit_minus_120'] = 1
    df_debit_biogas.loc[(df_debit_biogas['value']>=120) & (df_debit_biogas['value']<220), 'bit_120_220'] = 1
    df_debit_biogas.loc[(df_debit_biogas['value']>=220) & (df_debit_biogas['value']<=330), 'bit_220_330'] = 1
    df_debit_biogas.loc[(df_debit_biogas['value']>330) & (df_debit_biogas['value']<600), 'bit_330_600'] = 1
    
    obj = {
        
        'value': df_debit_biogas['value'].sum()/60,
        'value_minus_120': df_debit_biogas.loc[df_debit_biogas['bit_minus_120'] == 1, 'value'].sum()/60,
        'value_120_220': df_debit_biogas.loc[df_debit_biogas['bit_120_220'] == 1, 'value'].sum()/60,
        'value_220_330': df_debit_biogas.loc[df_debit_biogas['bit_220_330'] == 1, 'value'].sum()/60,
        'value_330_600': df_debit_biogas.loc[df_debit_biogas['bit_330_600'] == 1, 'value'].sum()/60,
    }
    
    if df_debit_biogas.shape[0] != 0:
    
        obj['percent_value_minus_120'] = sum(df_debit_biogas['bit_minus_120']) / df_debit_biogas.shape[0]
        obj['percent_value_120_220']   = sum(df_debit_biogas['bit_120_220']) / df_debit_biogas.shape[0]
        obj['percent_value_220_330']   = sum(df_debit_biogas['bit_220_330']) / df_debit_biogas.shape[0]
        obj['percent_value_330_600']   = sum(df_debit_biogas['bit_330_600']) / df_debit_biogas.shape[0]
    
    return obj



def compute_gasnat_entry(df_raw):
    tag_name = 'LUTOSA_ExportNyxAWS_COGLTS_BIOLTS_Valeur_Debit_GazNat_Thiopaq'
    df_debit_gasnat = df_raw[df_raw['area_name']==tag_name][['@timestamp', 'value']]
    entry_gasnat = df_debit_gasnat['value'].sum()/60
    return entry_gasnat


def compute_entry_biogas_cogen(df_raw):
    tag_name = 'LUTOSA_ExportNyxAWS_COGLTS_BIOLTS_Valeur_Debit_Biogaz_Cogen'
    df_entree_biogas = df_raw[df_raw['area_name']==tag_name][['@timestamp', 'value']]
    
    df_entree_biogas['corrected_value'] = 0
    df_entree_biogas.loc[(df_entree_biogas['value']>0) & (df_entree_biogas['value']<1000), 'corrected_value'] \
                                                                                = df_entree_biogas['value'] 
    entry_biogas_cogen = df_entree_biogas['corrected_value'].sum()/60
    
    return entry_biogas_cogen, df_entree_biogas


def compute_prod_therm_cogen(df_raw):
    tag_name_1 = 'LUTOSA_ExportNyxAWS_LUTOSA_Cpt_Ther_HT'
    tag_name_2 = 'LUTOSA_ExportNyxAWS_LUTOSA_Cpt_Ther_Drycooler'
    
    df_ht = df_raw[df_raw['area_name']==tag_name_1][['@timestamp', 'value']]
    df_ht = df_ht[df_ht['value']!=0]
    
    df_drycooler = df_raw[df_raw['area_name']==tag_name_2][['@timestamp', 'value']]
    df_drycooler = df_drycooler[df_drycooler['value']!=0]
    
    
    if len(df_drycooler) > 0:
        ht = max(df_ht['value']) - min(df_ht['value'])
        drycooler = max(df_drycooler['value']) - min(df_drycooler['value'])
    else:
        ht = 0
        drycooler = 0
    
    return ht, drycooler


def compute_avail_moteur(df_raw):
    df_moteur = df_raw[df_raw['area_name']=='LUTOSA_ExportNyxAWS_LUTOSA_Etat_Mot_Fct']
    
    percent_value = sum(df_moteur['value']) / 1440
    
    return percent_value

def compute_starts_and_stops(df_raw):
    tag_name = 'LUTOSA_ExportNyxAWS_LUTOSA_Etat_Mot_Fct'

    df_fct = df_raw[df_raw['area_name']==tag_name][['@timestamp', 'value']] 

    df_fct['edge']=df_fct.value.diff().fillna(0)

    starts = df_fct.loc[df_fct['edge']==1, 'value'].count()
    stops = df_fct.loc[df_fct['edge']==-1, 'value'].count()
    
    return starts, stops


def compute_prod_elec_cogen(df_raw):
    tag_name_1 = 'LUTOSA_ExportNyxAWS_LUTOSA_Cpt_Elec_IntoMoteur'
    tag_name_2 = 'LUTOSA_ExportNyxAWS_LUTOSA_Cpt_Elec_OutToMoteur'
    
    df_in_motor = df_raw[df_raw['area_name']==tag_name_1][['@timestamp', 'value']]
    df_in_motor = df_in_motor[df_in_motor['value']!=0]
    
    df_out_motor = df_raw[df_raw['area_name']==tag_name_2][['@timestamp', 'value']]
    df_out_motor = df_out_motor[df_out_motor['value']!=0]
    
    if len(df_out_motor) > 0:
        out_motor = max(df_out_motor['value']) - min(df_out_motor['value'])
        in_motor = max(df_in_motor['value']) - min(df_in_motor['value'])
    else:
        out_motor = 0
        in_motor = 0

    return (out_motor - in_motor)



def compute_dispo_thiopaq(df_raw):
    tag_name_1 = 'LUTOSA_ExportNyxAWS_COGLTS_BIOLTS_Valeur_Debit_Biogaz_Thiopaq'
    tag_name_2 = 'LUTOSA_ExportNyxAWS_COGLTS_BIOLTS_Valeur_Pression_Thiopaq_Entree'

    df_debit = df_raw[df_raw['area_name']==tag_name_1][['@timestamp', 'value']]
    df_debit['bit'] = 0
    df_debit.loc[df_debit['value']>120, 'bit'] = 1

    df_pression = df_raw[df_raw['area_name']==tag_name_2][['@timestamp', 'value']]
    df_pression['bit'] = 0
    df_pression.loc[(df_pression['value']>=15)&(df_pression['value']<=36), 'bit'] = 1

    df_merged=df_debit.merge(df_pression, left_on='@timestamp', right_on='@timestamp')
    df_merged['bit'] = 0
    df_merged.loc[(df_merged['bit_x']==1)&(df_merged['bit_y']==1), 'bit'] = 1
    
    tag_name_3 = 'LUTOSA_ExportNyxAWS_LUTOSA_Etat_Thiopaq_Fct'
    
    df_motor = df_raw[df_raw['area_name']==tag_name_3][['@timestamp', 'value']]
    
    obj = {
      'max_theorical_avail_thiopaq_hour': round(df_merged['bit'].sum()/60, 2),  
      'avail_thiopaq_hour': round(df_motor['value'].sum()/60, 2), 
    }
    
    if obj['max_theorical_avail_thiopaq_hour'] == 0:
        obj['avail_thiopaq_ratio'] = 0
    elif obj['avail_thiopaq_hour'] > obj['max_theorical_avail_thiopaq_hour']:
        obj['avail_thiopaq_ratio'] = 1
    else:
        obj['avail_thiopaq_ratio'] = round(obj['avail_thiopaq_hour'] / obj['max_theorical_avail_thiopaq_hour'], 2)
        
    
    return obj


def compute_dispo_cogen(df_raw):
    tag_name_1 = 'LUTOSA_ExportNyxAWS_COGLTS_BIOLTS_Valeur_Debit_Biogaz_Thiopaq'


    df_debit = df_raw[df_raw['area_name']==tag_name_1][['@timestamp', 'value']]
    df_debit['bit'] = 0
    df_debit.loc[df_debit['value']>=220, 'bit'] = 1
    
    tag_name_2 = 'LUTOSA_ExportNyxAWS_LUTOSA_Etat_Mot_Fct'
    
    df_motor = df_raw[df_raw['area_name']==tag_name_2][['@timestamp', 'value']]
    
    obj = {
        'max_theorical_avail_cogen_hour': round(df_debit['bit'].sum()/60, 2),
        'avail_cogen_hour': round(df_motor['value'].sum()/60, 2), 
    }
    
    if obj['max_theorical_avail_cogen_hour'] == 0:
        obj['avail_cogen_ratio'] = 0
    elif obj['avail_cogen_hour'] > obj['max_theorical_avail_cogen_hour']:
        obj['avail_cogen_ratio'] = 1
    else:
        obj['avail_cogen_ratio'] = round(obj['avail_cogen_hour'] / obj['max_theorical_avail_cogen_hour'], 2)
        
    
    return obj




def create_obj(df_raw, start):

    #DISPO DEBIT ENTREE THIOPAQ + ZOOM
    obj = compute_avail_debit_entry_thiopac(df_raw)
    obj_report_cogen = {}

    obj_report_cogen['entry_biogas_thiopaq_Nm3'] = obj['value']
    obj_report_cogen['entry_biogas_thiopaq_kWh'] = round(obj['value']*6.1656, 2)
    obj_report_cogen['entry_biogas_thiopaq_MWh'] = round(obj['value']*6.1656/1000, 2)

    obj_report_cogen['entry_biogas_thiopaq_minus_120_Nm3'] = obj['value_minus_120']
    obj_report_cogen['entry_biogas_thiopaq_minus_120_kWh'] = round(obj['value_minus_120']*6.1656, 2)
    obj_report_cogen['entry_biogas_thiopaq_minus_120_MWh'] = round(obj['value_minus_120']*6.1656/1000, 2)

    obj_report_cogen['entry_biogas_thiopaq_120_220_Nm3'] = obj['value_120_220']
    obj_report_cogen['entry_biogas_thiopaq_120_220_kWh'] = round(obj['value_120_220']*6.1656, 2)
    obj_report_cogen['entry_biogas_thiopaq_120_220_MWh'] = round(obj['value_120_220']*6.1656/1000, 2)

    obj_report_cogen['entry_biogas_thiopaq_220_330_Nm3'] = obj['value_220_330']
    obj_report_cogen['entry_biogas_thiopaq_220_330_kWh'] = round(obj['value_220_330']*6.1656, 2)
    obj_report_cogen['entry_biogas_thiopaq_220_330_MWh'] = round(obj['value_220_330']*6.1656/1000, 2)

    obj_report_cogen['entry_biogas_thiopaq_330_600_Nm3'] = obj['value_330_600']
    obj_report_cogen['entry_biogas_thiopaq_330_600_kWh'] = round(obj['value_330_600']*6.1656, 2)
    obj_report_cogen['entry_biogas_thiopaq_330_600_MWh'] = round(obj['value_330_600']*6.1656/1000, 2)
    
    
    #GASNAT ENRTREE COGEN
    entry_gasnat = compute_gasnat_entry(df_raw)
    obj_report_cogen['entry_gasnat_cogen_Nm3'] = entry_gasnat

    obj_report_cogen['entry_gasnat_cogen_kWh'] = round(entry_gasnat * 10.42, 2)
    obj_report_cogen['entry_gasnat_cogen_MWh'] = round(entry_gasnat * 10.42 / 1000, 2)
    
    
    #ENTREE BIOGASCOGEN ET CHAUDIERE
    entry_biogas_cogen, df_entree_biogas = compute_entry_biogas_cogen(df_raw)


    obj_report_cogen['entry_biogas_cogen_Nm3'] = entry_biogas_cogen

    obj_report_cogen['entry_biogas_cogen_kWh'] = round(entry_biogas_cogen * 6.1656, 2)
    obj_report_cogen['entry_biogas_cogen_MWh'] = round(entry_biogas_cogen * 6.1656 / 1000, 2)

    obj_report_cogen['entry_biogas_boiler_Nm3'] = round(obj_report_cogen['entry_biogas_thiopaq_Nm3'] 
                                                                - entry_biogas_cogen, 2)
    obj_report_cogen['entry_biogas_boiler_kWh'] = round(obj_report_cogen['entry_biogas_thiopaq_kWh'] 
                                                                - obj_report_cogen['entry_biogas_cogen_kWh'], 2)
    obj_report_cogen['entry_biogas_boiler_MWh'] = round(obj_report_cogen['entry_biogas_thiopaq_MWh'] 
                                                                - obj_report_cogen['entry_biogas_cogen_MWh'], 2)

    obj_report_cogen['entry_total_cogen_kWh'] = round(obj_report_cogen['entry_biogas_cogen_kWh']
                                                                + obj_report_cogen['entry_gasnat_cogen_kWh'], 2)
    obj_report_cogen['entry_total_cogen_MWh'] = round(obj_report_cogen['entry_biogas_cogen_MWh']
                                                                + obj_report_cogen['entry_gasnat_cogen_MWh'], 2)
    

    #CALENDAR OPEN DAYS FOR RATIOS
    start = containertimezone.localize(start)
    start_year, end_year = datetime(start.year, 1, 1), datetime(start.year, 12, 31, 23, 59, 59)




    df_calendar = es_helper.elastic_to_dataframe(es, 
                                                    start=start_year, 
                                                    end=end_year, 
                                                    index='nyx_calendar', 
                                                    query='type:LUTOSA',
                                                    datecolumns=['date'],
                                                    timestampfield='date')
    
    open_days = 365
    
    logger.info('size df_calendar: '+str(len(df_calendar)))
    if len(df_calendar) > 0:
        open_days = df_calendar.loc[df_calendar['on'], 'on'].count()
    
    logger.info('opening days: '+str(open_days))

    target_prod_biogaz   = ch.get_targets(es)['biogas']
    target_prod_elec     = ch.get_targets(es)['elec']
    target_prod_heat     = ch.get_targets(es)['heat']
    target_runtime_cogen = ch.get_targets(es)['runtime']
    
    daily_target_prod_biogaz   = target_prod_biogaz   / open_days
    daily_target_prod_elec     = target_prod_elec     / open_days
    daily_target_prod_heat     = target_prod_heat     / open_days
    daily_target_runtime_cogen = target_runtime_cogen / open_days

    day_on = True
    try:
        day_on = bool(df_calendar[df_calendar['date']==start]['on'].iloc[0])
    except:
        pass


    obj_report_cogen['on'] = day_on

    #HEURES DE FCT (DE ROTATION) COGEN
    percent_value = compute_avail_moteur(df_raw)
    obj_report_cogen['daily_avail_motor'] = round(percent_value, 2)
    obj_report_cogen['daily_avail_motor_hour'] = round(percent_value*24, 2)

    if day_on:
        obj_report_cogen['daily_avail_motor_target_hour'] = daily_target_runtime_cogen
        obj_report_cogen['daily_avail_motor_ratio_target'] = round((obj_report_cogen['daily_avail_motor_hour']/daily_target_runtime_cogen), 2)
    else:
        obj_report_cogen['daily_avail_motor_target_hour'] = 0
        



    #PROD ELEC COGEN
    prod_elec_cogen = compute_prod_elec_cogen(df_raw)
    obj_report_cogen['out_elec_cogen_kWh'] = round(prod_elec_cogen, 2)
    obj_report_cogen['out_elec_cogen_MWh'] = round(prod_elec_cogen / 1000, 2)


    if day_on:
        obj_report_cogen['out_elec_cogen_target_MWh'] = daily_target_prod_elec
        obj_report_cogen['out_elec_cogen_ratio_target'] = round(obj_report_cogen['out_elec_cogen_MWh'] 
                                                                / obj_report_cogen['out_elec_cogen_target_MWh'], 2) 
    else:
        obj_report_cogen['out_elec_cogen_target_MWh'] = 0
        
    
    #PROD THERM COGEN
    prod_therm_cogen_ht, prod_therm_cogen_drycooler = compute_prod_therm_cogen(df_raw)
    prod_therm_cogen = prod_therm_cogen_ht + prod_therm_cogen_drycooler
    obj_report_cogen['out_therm_cogen_kWh'] = round(prod_therm_cogen, 2)
    obj_report_cogen['out_therm_cogen_MWh'] = round(prod_therm_cogen / 1000, 2)
    obj_report_cogen['out_therm_cogen_ht_kWh'] = round(prod_therm_cogen_ht, 2)
    obj_report_cogen['out_therm_cogen_ht_MWh'] = round(prod_therm_cogen_ht / 1000, 2)
    obj_report_cogen['out_therm_cogen_drycooler_kWh'] = round(prod_therm_cogen_drycooler, 2)
    obj_report_cogen['out_therm_cogen_drycooler_MWh'] = round(prod_therm_cogen_drycooler / 1000, 2)

    if day_on:
        obj_report_cogen['out_therm_cogen_target_MWh'] = daily_target_prod_heat
        obj_report_cogen['out_therm_cogen_ratio_target'] = round(obj_report_cogen['out_therm_cogen_MWh'] 
                                                                / obj_report_cogen['out_therm_cogen_target_MWh'], 2) 
    else:
        obj_report_cogen['out_therm_cogen_target_MWh'] = 0
        



    obj_report_cogen['out_total_cogen_kWh'] = round(obj_report_cogen['out_therm_cogen_kWh']
                                                                + obj_report_cogen['out_elec_cogen_kWh'], 2)
    obj_report_cogen['out_total_cogen_MWh'] = round(obj_report_cogen['out_therm_cogen_MWh']
                                                                + obj_report_cogen['out_elec_cogen_MWh'], 2)


    if day_on:
        obj_report_cogen['entry_biogas_thiopaq_target_MWh'] = daily_target_prod_biogaz
        obj_report_cogen['entry_biogas_thiopaq_ratio_target'] = round((obj_report_cogen['entry_biogas_thiopaq_MWh'] / 
                                                                obj_report_cogen['entry_biogas_thiopaq_target_MWh']), 2)
    else:
        obj_report_cogen['entry_biogas_thiopaq_target_MWh'] = 0
    

    
    #RENDEMENTS
    if obj_report_cogen['entry_total_cogen_kWh'] == 0: 
        obj_report_cogen['total_efficiency'] = 0
        obj_report_cogen['elec_efficiency'] = 0
        obj_report_cogen['therm_efficiency'] = 0

        obj_report_cogen['gasnat_ratio'] = 0
        obj_report_cogen['biogas_ratio'] = 0

    else:
        obj_report_cogen['total_efficiency'] = round(obj_report_cogen['out_total_cogen_kWh'] / 
                                                     obj_report_cogen['entry_total_cogen_kWh'], 2)
        obj_report_cogen['elec_efficiency'] = round(obj_report_cogen['out_elec_cogen_kWh'] / 
                                                    obj_report_cogen['entry_total_cogen_kWh'], 2)
        obj_report_cogen['therm_efficiency'] = round(obj_report_cogen['out_therm_cogen_kWh'] / 
                                                     obj_report_cogen['entry_total_cogen_kWh'], 2)

        obj_report_cogen['gasnat_ratio'] = round(obj_report_cogen['entry_gasnat_cogen_kWh'] / 
                                                 obj_report_cogen['entry_total_cogen_kWh'], 2)
        obj_report_cogen['biogas_ratio'] = round(obj_report_cogen['entry_biogas_cogen_kWh'] / 
                                                 obj_report_cogen['entry_total_cogen_kWh'], 2)

        obj_report_cogen['total_efficiency_wo_zero'] = obj_report_cogen['total_efficiency']
        obj_report_cogen['therm_efficiency_wo_zero'] = obj_report_cogen['therm_efficiency'] 
        obj_report_cogen['elec_efficiency_wo_zero'] = obj_report_cogen['elec_efficiency']

        obj_report_cogen['gasnat_ratio_wo_zero'] = obj_report_cogen['gasnat_ratio']
        obj_report_cogen['biogas_ratio_wo_zero'] = obj_report_cogen['biogas_ratio']
      
    
    #DISPO THIOPAQ
    obj=compute_dispo_thiopaq(df_raw)

    obj_report_cogen['max_theorical_avail_thiopaq_hour'] = obj['max_theorical_avail_thiopaq_hour']
    obj_report_cogen['avail_thiopaq_hour'] = obj['avail_thiopaq_hour']
    
    if day_on:
        obj_report_cogen['avail_thiopaq_ratio'] = obj['avail_thiopaq_ratio']
        
        
    #DISPO COGEN
    obj=compute_dispo_cogen(df_raw)

    obj_report_cogen['max_theorical_avail_cogen_hour'] = obj['max_theorical_avail_cogen_hour']
    obj_report_cogen['avail_cogen_hour'] = obj['avail_cogen_hour']


    if day_on:
        obj_report_cogen['avail_cogen_ratio'] = obj['avail_cogen_ratio']

    #STARTS AND STOPS
    starts, stops = compute_starts_and_stops(df_raw)
    obj_report_cogen['starts'] = int(starts)
    obj_report_cogen['stops'] = int(stops)
        
    return obj_report_cogen

def retrieve_raw_data(day):
    start_dt = datetime(day.year, day.month, day.day)
    end_dt   = datetime(start_dt.year, start_dt.month, start_dt.day, 23, 59, 59)

    df_raw=es_helper.elastic_to_dataframe(es, index='opt_cleaned_data*', 
                                           query='*', 
                                           start=start_dt, 
                                           end=end_dt,
                                           scrollsize=10000,
                                           size=1000000)

    
    containertimezone=pytz.timezone(get_localzone().zone)
    df_raw['@timestamp'] = pd.to_datetime(df_raw['@timestamp'], \
                                               unit='ms', utc=True).dt.tz_convert(containertimezone)
    df_raw=df_raw.sort_values('@timestamp') 
    
    return df_raw



def save_tags_to_computed(es, obj):
    list_of_tag = ['entry_biogas_cogen_MWh', 'entry_gasnat_cogen_MWh']


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
        # {
        #     'name': 'INminusOUT',
        #     'a': 'in_total_cogen_kWh',
        #     'b': 'out_total_kWh',
        # },
        # {
        #     'name': 'INminusHEAT',
        #     'a': 'in_total_cogen_kWh',
        #     'b': 'out_moteur_kWh',
        # },
        # {
        #     'name': 'INminusELEC',
        #     'a': 'in_total_cogen_kWh',
        #     'b': 'out_elec_kWh',
        # },
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


    try:
        df = retrieve_raw_data(start)
        obj_to_es = create_obj(df, start)
        obj_to_es['@timestamp'] = containertimezone.localize(start)
        obj_to_es['site'] = 'LUTOSA'
        es.index(index='daily_cogen_lutosa', doc_type='doc', id=int(start.timestamp()), body = obj_to_es)

        save_tags_to_computed(es, obj_to_es)
    except Exception as er:
        logger.error('Error During creating specifics data')
        error = traceback.format_exc()
        logger.error(error)

    try:
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
        error = traceback.format_exc()
        logger.error(error)


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