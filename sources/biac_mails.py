"""
BIAC_MAILS
====================================


Listens to:
-------------------------------------



Collections:
-------------------------------------




VERSION HISTORY
===============

* 25 Mar 2020 1.0.0 **AMA** Escape a double backslash
""" 

import re
import json
import time
import uuid
import email
import base64
import imaplib
import ssl
import platform
import threading
import os,logging

import pandas as pd

from logging.handlers import TimedRotatingFileHandler
import amqstomp as amqstompclient
from datetime import datetime
from datetime import timedelta
from functools import wraps
from elasticsearch import Elasticsearch as ES
from logstash_async.handler import AsynchronousLogstashHandler
#from lib import pandastoelastic as pte
import numpy as np
from sqlalchemy import create_engine
from elastic_helper import es_helper 



MODULE  = "BIAC_MAILS"
VERSION = "1.1.2"
QUEUE   = []
MAILS=0
ATTACHMENTS=0

def decode_mime_words(s):
    return u''.join(
        word.decode(encoding or 'utf8') if isinstance(word, bytes) else word
        for word, encoding in email.header.decode_header(s))

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
def messageReceived(destination,message,headers):
    global es
    starttime = time.time()
    logger.info("==> "*10)
    logger.info("Message Received %s" % destination)
    logger.info(headers)



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


def decodePart(part,orgmes,fileHT):
    global ATTACHMENTS
    if part.get_content_maintype()=="application":
        logger.info("FOUND ONE...")
        if part.get_filename() in fileHT:
            logger.info("Already found continuing...")
            return

        fileHT[part.get_filename()]=True
        logger.info(part.get_filename())
        #logger.info(part.get_payload(decode=False))
        payload=part.get_payload(decode=False)
        payload2=base64.b64decode(payload)
        open(part.get_filename(), 'wb').write(payload2)
        subject=orgmes['Subject']
        try:
            logger.info(subject)
            subject=decode_mime_words(subject)
        except Exception as e:
            logger.error("Unable to decode subject")
                                
        ATTACHMENTS+=1

        def clean_string(instr):
            return instr.replace("\\","").replace("\r","_").replace("\n","_")

        #logger.info(orgmes)
        mailfrom=clean_string(orgmes['From'])
        mailto=clean_string(orgmes['To'])
        logger.info(mailfrom)
        logger.info(mailto)


        conn.send_message("/queue/MAIL_LOG",payload,headers={"CamelSplitAttachmentId":part.get_filename(), "file":part.get_filename(),"From":mailfrom,"To":mailto,"Subject":subject})
        conn.send_message("/queue/AVAILABILITIES_IMPORT_2",payload,headers={"CamelSplitAttachmentId":part.get_filename(), "file":part.get_filename(),"From":mailfrom,"To":mailto,"Subject":subject})


def send_attachment(msg):
    logger.info("Attach "*10)
    att_path = "No attachment found."
    fileHT={}

    for part in msg.walk():
        logger.info(" >>"+part.get_content_maintype())
        if part.is_multipart():
            for subpart in part.get_payload():
                logger.info("   >>"+subpart.get_content_maintype())
                decodePart(subpart,msg,fileHT)
                if subpart.is_multipart():
                    for subpart2 in subpart.get_payload():
                        logger.info("     >>"+subpart2.get_content_maintype())
                        decodePart(subpart2,msg,fileHT)

            continue
        if part.get('Content-Disposition') is None:
            continue

        #filename = part.get_filename()
        #att_path = os.path.join(download_folder, filename)

        #if not os.path.isfile(att_path):
        #    fp = open(att_path, 'wb')
        #    fp.write(part.get_payload(decode=True))
        #    fp.close()
    #return att_path

#################################################
def loadMails():
    global MAILS
    logger.info("LOADING MAILS")
    logger.info("=============")

    context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
    context.set_ciphers('DEFAULT@SECLEVEL=1')
    M = imaplib.IMAP4_SSL(os.environ["EMAIL_URL"], os.environ["EMAIL_PORT"], ssl_context=context)
    M.login(os.environ["EMAIL_USER"], os.environ["EMAIL_PASSWORD"])
    M.select()
    typ, data = M.search(None, 'ALL')


    for num in data[0].split():
        logger.info("DECODING MESSAGE")
        logger.info("================")
        MAILS+=1
        rv, data = M.fetch(num, '(RFC822)')
        if rv != 'OK':
            logger.info("ERROR getting message", num)
            continue

        msg = email.message_from_bytes(data[0][1])
        hdr = email.header.make_header(email.header.decode_header(msg['Subject']))
        subject = str(hdr)
        logger.info('Message %s: %s' % (num, subject))
        logger.info('Raw Date: %s' % (msg['Date']))
        # Now convert to local date-time
        date_tuple = email.utils.parsedate_tz(msg['Date'])
        if date_tuple:
            local_date = datetime.fromtimestamp(
                email.utils.mktime_tz(date_tuple))
            print ("Local Date:", \
                local_date.strftime("%a, %d %b %Y %H:%M:%S"))

    #    logger.info(msg)
        send_attachment(msg)
# DELETE MAILS
        M.store(num, '+FLAGS', '\\Deleted')
    logger.info("Closing mail box")
    M.expunge()
    M.close()
    M.logout()

if __name__ == '__main__':
    logger.info("AMQC_URL          :"+os.environ["AMQC_URL"])
    nextload=datetime.now()
    while True:

        SECONDSBETWEENCHECKS=10


        time.sleep(5)
        try:

            variables={"platform":"_/_".join(platform.uname()),"icon":"at","mails":MAILS,"attachments":ATTACHMENTS}

            conn.send_life_sign(variables=variables)

            if (datetime.now() > nextload):
                try:
                    nextload=datetime.now()+timedelta(seconds=SECONDSBETWEENCHECKS)

                    loadMails()
                except Exception as e2:
                    logger.error("Unable to load mails.")
                    logger.error(e2)
        except Exception as e:
            logger.error("Unable to send life sign.")
            logger.error(e)
    #app.run(threaded=True,host= '0.0.0.0')
