"""
*******************************************************************
  Copyright (c) 2013, 2017 IBM Corp.

  All rights reserved. This program and the accompanying materials
  are made available under the terms of the Eclipse Public License v1.0
  and Eclipse Distribution License v1.0 which accompany this distribution.

  The Eclipse Public License is available at
     http://www.eclipse.org/legal/epl-v10.html
  and the Eclipse Distribution License is available at
    http://www.eclipse.org/org/documents/edl-v10.php.

  Contributors:
     Ian Craggs - initial implementation and/or documentation
*******************************************************************
"""

import sys, traceback, socket, logging, getopt, hashlib, base64
import threading, ssl, time

import mqtt.clients.V5
from mqtt.brokers.V5 import MQTTBrokers as MQTTV5Brokers
from mqtt.formats import MQTTV311, MQTTV5

server = None
logger = logging.getLogger('MQTT broker')


class Callbacks(mqtt.clients.V5.Callback):

  def __init__(self, broker):
    self.messages = []
    self.messagedicts = []
    self.publisheds = []
    self.subscribeds = []
    self.unsubscribeds = []
    self.disconnects = []
    self.broker = broker

  def __str__(self):
     return str(self.messages) + str(self.messagedicts) + str(self.publisheds) + \
        str(self.subscribeds) + str(self.unsubscribeds) + str(self.disconnects)

  def clear(self):
    self.__init__()

  def disconnected(self, reasoncode, properties):
    logging.info("disconnected %s %s", str(reasoncode), str(properties))
    self.disconnects.append({"reasonCode" : reasoncode, "properties" : properties})

  def connectionLost(self, cause):
    logging.info("connectionLost %s" % str(cause))

  def publishArrived(self, topicName, payload, qos, retained, msgid, properties=None):
    logging.info("publishArrived %s %s %d %s %d %s", topicName, payload, qos, retained, msgid, str(properties))
    self.messages.append((topicName, payload, qos, retained, msgid, properties))
    self.messagedicts.append({"topicname" : topicName, "payload" : payload,
        "qos" : qos, "retained" : retained, "msgid" : msgid, "properties" : properties})

    # add to local broker
    self.broker.broker.publish(aClientid, topic, message, qos, properties, receivedTime, retained)
    return True

  def published(self, msgid):
    logging.info("published %d", msgid)
    self.publisheds.append(msgid)

  def subscribed(self, msgid, data):
    logging.info("subscribed %d", msgid)
    self.subscribeds.append((msgid, data))

  def unsubscribed(self, msgid):
    logging.info("unsubscribed %d", msgid)
    self.unsubscribeds.append(msgid)


class Bridges:

  def __init__(self, host, port):
    self.host = host
    self.port = port
    self.client = mqtt.clients.V5.Client("local")
    self.callback = Callbacks(broker5)
    self.client.registerCallback(self.callback)
    self.local_connect()

  def local_connect(self):
    # connect locally with V5, so we get noLocal and retainAsPublished
    connect = MQTTV5.Connects()
    connect.ClientIdentifier = "local"
    broker5.connect(self, connect)
    subscribe = MQTTV5.Subscribes()
    options = MQTTV5.SubscribeOptions()
    options.noLocal = options.retainAsPublished = True
    subscribe.data = [('+', options)]
    broker5.subscribe(self, subscribe)

  def connect(self):
    self.client.connect(host=self.host, port=self.port, cleanstart=True)
    # subscribe if necessary
    options = MQTTV5.SubscribeOptions()
    options.noLocal = options.retainAsPublished = True
    self.client.subscribe(["+"], [options])

  def getPacket(self):
    # get packet from remote 
    pass

  def handlePacket(self, packet):
    # response from local broker
    logger.info("from local broker %s", str(packet))
    if packet.fh.PacketType == MQTTV5.PacketTypes.PUBLISH:
      self.client.publish(packet.topicName, packet.data, packet.fh.QoS) #retained=False, properties=None)

  def run(self):
    while True:
      self.connect()
      time.sleep(300)
    self.shutdown()

def setBroker5(aBroker5):
  global broker5
  broker5 = aBroker5

def create(port, host="", TLS=False, 
    cert_reqs=ssl.CERT_REQUIRED,
    ca_certs=None, certfile=None, keyfile=None):

  if host == "":
    host = "localhost"
  logger.info("Starting TCP bridge for address '%s' port %d %s", host, port, "with TLS support" if TLS else "")
  bridge = Bridges(host, port)
  thread = threading.Thread(target=bridge.run)
  thread.start()
  return bridge





