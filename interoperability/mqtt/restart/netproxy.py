"""
*******************************************************************
  Copyright (c) 2013, 2018 IBM Corp.

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

# Trace MQTT traffic

from mqtt.formats import MQTTV311 as MQTTV3
from mqtt.formats import MQTTV5

import paho.mqtt.client as mqtt

import socket, sys, select, socketserver, traceback, datetime, os, threading, time, logging

logging.basicConfig()
logger = logging.getLogger("Restart test proxy")
logger.setLevel(logging.INFO)

break_connections = False

global options, brokers, clients
brokers = clients = None
options = {}
options["control_connection"] = "localhost:7777"
options["control_topic"] = "Eclipse/Paho/restart_test/proxy_control"

class ControlBrokers:

  def __init__(self, options):
    self.messages = []
    self.receiveTopic = options["control_topic"]+"/receive"
    self.sendTopic = options["control_topic"]+"/send"
    self.client = mqtt.Client("proxy_control")
    self.client.on_message = self.on_message
    self.client.on_connect = self.on_connect
    self.client.on_publish = self.on_publish
    self.controlBrokerHost, self.controlBrokerPort = options["control_connection"].split(":")
    self.controlBrokerPort = int(self.controlBrokerPort)
    self.client.connect(self.controlBrokerHost, self.controlBrokerPort, 60)
    self.published = False
    self.client.loop_start()

  def on_connect(self, client, userdata, flags, rc):
      # Subscribing in on_connect() means that if we lose the connection and
    logger.info("Connected to MQTT server, result code "+str(rc))
    # reconnect then subscriptions will be renewed.
    self.client.subscribe(self.receiveTopic, 2)

  def on_message(self, client, userdata, msg):
    global brokers, clients
    logger.info("Received "+str(msg.payload.decode()))
    self.messages.append((client, userdata, msg))
    break_connections = True
    logger.info("Terminating client")
    try:
        brokers.close()
        clients.close()
    except:
        pass

  def on_publish(self, client, userdata, data):
    self.published = True

  def send_control_message(self, msg):
    logger.info("Sending control message: %s", msg)
    self.messages.clear()
    self.published = False
    self.client.publish(self.sendTopic, msg)
    while not self.published:
      time.sleep(.4)

  def stop(self):
    self.client.disconnect()

  def get_next_message(self):
    rc = None
    if len(self.messages) > 0:
      rc = self.messages.pop(0)
    return rc

class MQTTHandler(socketserver.StreamRequestHandler):

  def handle(self):
    global brokers, clients, options, break_connections, control_broker
    if not hasattr(self, "ids"):
      self.ids = {}
    if not hasattr(self, "versions"):
      self.versions = {}
    inbuf = True
    i = o = e = None
    try:
      clients = self.request
      brokers = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      brokers.connect((brokerhost, brokerport))
      while inbuf != None:
        (i, o, e) = select.select([clients, brokers], [], [])
        for s in i:
          if s == clients:
            inbuf = MQTTV3.getPacket(clients) # get one packet
            if inbuf == None:
              break
            try:
              packet = MQTTV3.unpackPacket(inbuf)
              if packet.fh.MessageType == MQTTV3.CONNECT:
                self.ids[id(clients)] = packet.ClientIdentifier
                self.versions[id(clients)] = 3
              logger.debug("C to S "+self.ids[id(clients)]+" "+repr(packet))
              #logger.debug([hex(b) for b in inbuf])
              #logger.debug(inbuf)
            except:
              traceback.print_exc()
            brokers.send(inbuf)       # pass it on
          elif s == brokers:
            inbuf = MQTTV3.getPacket(brokers) # get one packet
            if inbuf == None:
              break
            try:
              logger.debug("S to C "+self.ids[id(clients)]+" "+repr(MQTTV3.unpackPacket(inbuf)))
            except:
              traceback.print_exc()
            clients.send(inbuf)
      if id(clients) in self.ids.keys():
        logger.info("client "+self.ids[id(clients)]+" connection closing")
    except:
      #logger.debug(repr((i, o, e)) + " " + repr(inbuf))
      traceback.print_exc()
    if id(clients) in self.ids.keys():
      del self.ids[id(clients)]
    elif id(clients) in self.versions.keys():
      del self.versions[id(clients)]

class ThreadingTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
  pass

def run():
  global brokerhost, brokerport
  myhost = 'localhost'
  if len(sys.argv) > 1:
    brokerhost = sys.argv[1]
  else:
    brokerhost = 'localhost'

  if len(sys.argv) > 2:
    brokerport = int(sys.argv[2])
  else:
    brokerport = 1883

  if brokerhost == myhost:
    myport = brokerport + 1
  else:
    myport = 1883

  print("MQTT restart netowrk proxy listening on port", str(myport)+", broker on port", brokerport)
  MQTT_proxy = ThreadingTCPServer(("", myport), MQTTHandler)

  control_broker = ControlBrokers(options)
  MQTT_proxy.serve_forever()

  server_thread = threading.Thread(target=MQTT_proxy.serve_forever)
  # Exit the server thread when the main thread terminates
  server_thread.daemon = True
  server_thread.start()
  print("Server loop running in thread:", server_thread.name)

  while True:
    time.sleep(1)

if __name__ == "__main__":
  run()
