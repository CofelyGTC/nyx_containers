import stomp
import logging
import json
import sys
import datetime
import time
import os

logger = logging.getLogger(__name__)
amqclientversion = "1.2.1"


##################################################################################
# AMQ Listener
##################################################################################

class AMQListener(stomp.ConnectionListener):

    def __init__(self, amqconn,  callback):
        self.internal_conn = amqconn
        self.callback = callback
        self.globalerrors = 0
        self.errors = 0
        self.globalmessages = 0
        self.received = {}

    def on_error(self, headers, body):
        logger.error('#=- Received an error "%s"' % body)
        self.errors += 1
        self.internal_conn.general_error()

    def on_heartbeat_timeout(self):
        logger.warn("#=- HEART BEAT TIMEOUT ERROR")
        self.internal_conn.heartbeat_timeout()

    def on_message(self, frame):
        headers = frame.headers
        body = frame.body

        if self.internal_conn.earlyack:
            logger.debug("Early ack")
            self.internal_conn.conn.ack(
                headers["message-id"], headers["subscription"])

        destination = "NA"
        if "destination" in headers:
            destination = headers["destination"]
        logger.debug("#=->>>> Message received (" + destination +
                    ") PAYLOAD=" + str(len(body)))

        if destination not in self.received:
            self.received[destination] = 1
        else:
            self.received[destination] += 1

        self.globalmessages += 1

        try:
            if self.callback is not None:
                self.callback(destination, body, headers)
            else:
                logger.warning("#=- No call back defined")

        except Exception:
            self.globalerrors += 1
            logger.error("ERROR:", exc_info=True)
            err = sys.exc_info()
            errstr = str(err[0]) + str(err[1]) + str(err[2])
            logger.error("ERROR:" + errstr)

        if not self.internal_conn.earlyack:
            self.internal_conn.conn.ack(
                headers["message-id"], headers["subscription"])
        logger.debug("#=-<<<< Message handled")



##################################################################################
# AMQ Listener which gracefully reconnects on disconnect
##################################################################################

class AMQReconnectListener(AMQListener):

    def on_disconnected(self):
        logger.warn("#=- LISTENER DISCONNECTED ERROR")
        self.internal_conn.listener_disconnect()


class AMQClient():

    def __init__(self, server, module, subscription, callback=None,heart_beat_receive_scale=2.0,
                 listener_class=AMQListener):
        
        logger.debug("#=-" * 20)
        logger.debug("#=- Starting AMQ Connection" + amqclientversion)
        logger.debug("#=-" * 20)
        logger.debug("#=- Module       :" + module["name"])
        logger.debug("#=- IP           :" + server["ip"])
        logger.debug("#=- Port         :" + str(server["port"]))
        logger.debug("#=- Login        :" + server["login"])
        logger.debug("#=- Password     :" + ("*" * len(server["password"])))
        logger.debug("#=- Subscription :" + str(subscription))
        logger.debug("#=- Beat         :" + str(heart_beat_receive_scale))
        logger.debug("#=- Listener     :" + str(listener_class))
        
        self.starttime = datetime.datetime.now()
        self.heart_beat_receive_scale=heart_beat_receive_scale
        self.listener_class = listener_class

        self.conn = None
        self.sent = {}
        self.subscription = subscription
        self.callback = callback
        self.server = server
        self.module = module
        self.heartbeaterrors = 0
        self.connections = 0
        self.earlyack=False

        if "earlyack" in server:
            logger.info("Early ack set to true.")
            self.earlyack=server["earlyack"]

        logger.debug("#=- Subscription :" + str(subscription))
        logger.debug("#=- Early Ack    :" + str(self.earlyack))
        logger.debug("#=-" * 20)

        try:
            self.create_connection(self)
        except:
            self.create_connection()

    def disconnect(self):
        logger.info("#=- Disconnecting...")
        self.conn.disconnect()

    def create_connection(self):
        logger.info("#=- Creating connection.")
        heartbeats = (10000, 20000)

        if "heartbeats" in self.server:
            heartbeats = self.server["heartbeats"]

        self.conn = stomp.Connection11(
            [(self.server["ip"], self.server["port"])], heartbeats=heartbeats,heart_beat_receive_scale=self.heart_beat_receive_scale)

        self.listener = self.listener_class(self, self.callback)
        print('self.listener: ', self.listener.__dict__)
        self.conn.set_listener('simplelistener', self.listener)
        logger.debug("#=- Starting connection...")
#        self.conn.start()
        logger.debug("#=- Connection started.")
        self.conn.connect(
            self.server["login"],
            self.server["password"],
            wait=True,
            headers={"body": {"client-id": self.module["name"]}}
        )
        logger.debug("#=- Login passed.")

        self.connections+=1
        curid = 1

        if(len(self.subscription) > 0):
            for sub in self.subscription:
                if len(sub) > 0:
                    logger.debug("#=- Subscribing to:" + sub)
                    self.conn.subscribe(destination=sub, id=curid, ack='client', headers={
                                        "activemq.prefetchSize": 1})
                    curid += 1

    def send_life_sign(self,variables=None):
        if(self.listener != None):
            logger.debug("#=- Send Module Life Sign.")
            if("lifesign" in self.module):
                lifesignstruct = self.generate_life_sign()

                if variables !=None:
                    for key in variables:
                        lifesignstruct[key]=variables[key]

                try:
                    self.send_message(self,self.module["lifesign"], json.dumps(lifesignstruct))
                except:
                    self.send_message(self.module["lifesign"], json.dumps(lifesignstruct))
            else:
                logger.error(
                    "Unable to send life sign. Target queue not defined in module parameters.")
        else:
            logger.error("#=- Unable to send life sign. No listener defined.")

    def generate_life_sign(self):
        return {
            "error": "OK",
            "type": "lifesign",
            "eventtype": "lifesign",
            "module": self.module["name"],
            "version": self.module["version"],
            "alive": 1,
            "errors": self.listener.globalerrors,
            "internalerrors": self.listener.errors,
            "heartbeaterrors": self.heartbeaterrors,
            "messages": self.listener.globalmessages,
            "received": self.listener.received,
            "sent": self.sent,
            "amqclientversion": amqclientversion,
            "starttimets": self.starttime.timestamp(),
            "starttime": str(self.starttime),
            "connections": self.connections
        }

    def send_message(self, destination, message, headers=None):
        logger.debug("#=- Send Message to " + destination +
                     ". LEN=" + str(len(message)))
        
        if destination not in self.sent:
            self.sent[destination] = 1
        else:
            self.sent[destination] += 1

        try:
            self.conn.send(body=message, destination=destination, headers=headers)
        except Exception:            
            
            logger.error("#=- Error raised while sending. ")
            logger.info("Sleeping 5 seconds and disconnects.")
            time.sleep(5)
            err = sys.exc_info()
            errstr = str(err[0]) + str(err[1]) + str(err[2])
            logger.error("ERROR:" + errstr)
            try:
                self.disconnect()
            except Exception:
                logger.error("#=- Unable to disconnect.")
            logger.info("Sleeping 5 seconds and reconnects.")
            time.sleep(5)
            try:
                self.create_connection(self)
            except:
                self.create_connection()

    def heartbeat_timeout(self):
        self.heartbeaterrors += 1
        self.reconnect_and_listen()

    def general_error(self):        
        logger.error("#=- General Error. Exiting")
        time.sleep(5)
        os._exit(1)

    def listener_disconnect(self):
        self.reconnect_and_listen()

    def reconnect_and_listen(self):
        for n in range(1, 31):
            try:
                logger.debug("#=- Reconnecting: Attempt %d" % n)
                time.sleep(5)
                try:
                    self.create_connection(self)
                except:
                    self.create_connection()

                break
            except Exception as e:
                logger.error("#=- Reconnect attempt failed: %s" % e)