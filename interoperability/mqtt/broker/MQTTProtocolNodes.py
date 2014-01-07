"""
*******************************************************************
  Copyright (c) 2013, 2014 IBM Corp.
 
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

import traceback, random, sys, string, copy, threading, logging, socket

from ..formats import MQTTV311 as MQTTV3

from .MQTTBrokerNodes import MQTTBrokerNodes 
 
ordering = "efficient"

def respond(sock, packet):
  logging.info("out: "+repr(packet))
  if hasattr(sock, "handlePacket"):
    sock.handlePacket(packet)
  else:
    sock.send(packet.pack())

class Clients:

  def __init__(self, anId, socket):
    self.id = anId # required
    self.socket = socket
    self.pub = MQTTV3.Publishes()
    self.msgid = 1
    self.publications = []
    self.qos1list = []
    self.qos2list = {}
    self.storedPubs = {} # stored inbound QoS 2 publications

  def reinit(self):
    self.__init__(self.id, self.socket)

  def sendAll(self):
    for p in self.publications:
      respond(self.socket, p)
    self.publications = []

  def publishArrived(self, topic, msg, qos, retained=False):
    "required by broker node class"
    self.pub.topicName = topic
    self.pub.data = msg
    self.pub.fh.QoS = qos[0]
    self.pub.fh.RETAIN = retained
    if self.pub.fh.QoS == 0:
      self.pub.messageIdentifier = 1
    else:
      self.pub.messageIdentifier = self.msgid
      if self.msgid == 65535:
        self.msgid = 1
      else:
        self.msgid += 1
    if qos[0] == 1:
      self.qos1list.append(self.pub.messageIdentifier)
    elif qos[0] == 2:
      self.qos2list[self.pub.messageIdentifier] = "PUBREC"
    if ordering == "efficient":
      # most efficient ordering - no queueing
      respond(self.socket, self.pub)
    else:
      # 'correct' ordering?
      self.publications.append(copy.deepcopy(self.pub))
 
class MQTTProtocolNodes:

  def __init__(self):
    self.broker = MQTTBrokerNodes()
    self.clientids = {}
    self.clients = {}
    self.lock = threading.RLock()

  def processOutput(self):
    for c in self.clients.values():
      c.sendAll()

  def handleRequest(self, sock):
    "this is going to be called from multiple threads, so synchronize"
    self.lock.acquire()
    try:
      packet = MQTTV3.unpackPacket(MQTTV3.getPacket(sock))
      if packet:
        self.handlePacket(packet, sock)
      else:
        raise Exception("handleRequest: badly formed packet")
    finally:
      self.lock.release()

  def handlePacket(self, packet, sock):
    logging.info("in: "+repr(packet))
    if sock not in self.clientids.keys() and \
         MQTTV3.packetNames[packet.fh.MessageType] != "CONNECT":
       self.disconnect(sock, packet)
       logging.info("[MQTT-3.1.0-1] Connect was not first packet on socket")
       raise Exception("Connect was not first packet on socket")
    else:
      getattr(self, MQTTV3.packetNames[packet.fh.MessageType].lower())(sock, packet)
      self.processOutput()

  def connect(self, sock, packet):
    """

    """
    if packet.ProtocolName != "MQTT":
      logging.info("[MQTT-3.1.2-1] Wrong protocol name %s", packet.ProtocolName)
      self.disconnect(sock, None)
      return
    if packet.ProtocolVersion != 4:
      logging.info("[MQTT-3.1.2-2] Wrong protocol version %d", packet.ProtocolVersion)
      resp = MQTTV3.Connacks()
      resp.returnCode = 1
      respond(sock, resp)
      self.disconnect(sock, None)
      return
    if packet.ClientIdentifier in self.clientids.values():
      for s in self.clientids.keys():
        if self.clientids[s] == packet.ClientIdentifier:
          logging.info("[MQTT-3.1.4-2] Disconnecting old client %s", packet.ClientIdentifier)
          self.disconnect(s, None)
          break
    self.clientids[sock] = packet.ClientIdentifier
    me = Clients(packet.ClientIdentifier, sock)
    self.clients[sock] = me
    # queued up publications are delivered after connack
    resp = MQTTV3.Connacks()
    resp.returnCode = 0
    respond(sock, resp)
    self.broker.connect(me, packet.CleanStart)

  def disconnect(self, sock, packet):
    if sock in self.clientids.keys():
      self.broker.disconnect(self.clientids[sock])
      del self.clientids[sock]
    if sock in self.clients.keys():
      del self.clients[sock]
    sock.shutdown(socket.SHUT_RDWR) # must call shutdown to close socket immediately
    sock.close()

  def disconnectAll(self, sock):
    for sock in self.clientids.keys():
      self.disconnect(sock, None)

  def subscribe(self, sock, packet):
    topics = []
    qoss = []
    for p in packet.data:
      topics.append(p[0])
      qoss.append(p[1])
    self.broker.subscribe(self.clientids[sock], topics, qoss)
    resp = MQTTV3.Subacks()
    resp.messageIdentifier = packet.messageIdentifier
    resp.data = qoss
    respond(sock, resp)

  def unsubscribe(self, sock, packet):
    self.broker.unsubscribe(self.clientids[sock], packet.data)
    resp = MQTTV3.Unsubacks()
    resp.messageIdentifier = packet.messageIdentifier
    respond(sock, resp)

  def publish(self, sock, packet):
    if packet.fh.QoS == 0:
      self.broker.publish(self.clientids[sock],
             packet.topicName, packet.data, packet.fh.QoS, packet.fh.RETAIN)
    elif packet.fh.QoS == 1:
      self.broker.publish(self.clientids[sock],
             packet.topicName, packet.data, packet.fh.QoS, packet.fh.RETAIN)
      resp = MQTTV3.Pubacks()
      resp.messageIdentifier = packet.messageIdentifier
      respond(sock, resp)
    elif packet.fh.QoS == 2:
      myclient = self.clients[sock]
      myclient.storedPubs[packet.messageIdentifier] = copy.deepcopy(packet)
      resp = MQTTV3.Pubrecs()
      resp.messageIdentifier = packet.messageIdentifier
      respond(sock, resp)

  def pubrel(self, sock, packet):
    myclient = self.clients[sock]
    key = packet.messageIdentifier
    if key in myclient.storedPubs.keys():
      packet = myclient.storedPubs[key]
      self.broker.publish(myclient.id,
             packet.topicName, packet.data, packet.fh.QoS, packet.fh.RETAIN)
      resp = MQTTV3.Pubcomps()
      resp.messageIdentifier = key
      respond(sock, resp)

  def pingreq(self, sock, packet):
    resp = MQTTV3.Pingresps()
    respond(sock, resp)

  def puback(self, sock, packet):
    "confirmed reception of qos 1"
    myclient = self.clients[sock]
    key = packet.messageIdentifier
    if key in myclient.qos1list:
      myclient.qos1list.remove(key)
    else:
      log.error("Unknown qos 1 message id", key, "\nKnown keys are", myclient.qos1list)

  def pubrec(self, sock, packet):
    "confirmed reception of qos 2"
    myclient = self.clients[sock]
    key = packet.messageIdentifier
    if key in myclient.qos2list.keys():
      #assert self.qos2list[key] == "PUBREC"
      myclient.qos2list[key] = "PUBCOMP"
      resp = MQTTV3.Pubrels()
      resp.messageIdentifier = packet.messageIdentifier
      respond(sock, resp)
    else:
      log.error("Unknown qos 2 message id", key, "\nKnown keys are", myclient.qos2list.keys())

  def pubcomp(self, sock, packet):
    "confirmed reception of qos 2"
    myclient = self.clients[sock]
    key = packet.messageIdentifier
    if key in myclient.qos2list.keys():
      assert myclient.qos2list[key] == "PUBCOMP"
      del myclient.qos2list[key]
    else:
      log.error("Unknown qos 2 message id", key, "\nKnown keys are", myclient.qos2list.keys())
 
