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

import traceback, random, sys, string, copy, threading, logging, socket, time, uuid

from ..formats import MQTTV311 as MQTTV3

from .Brokers import Brokers

logger = logging.getLogger('MQTT broker')

def respond(sock, packet):
  logger.info("out: "+repr(packet))
  if hasattr(sock, "handlePacket"):
    sock.handlePacket(packet)
  else:
    try:
      sock.send(packet.pack()) # Could get socket error on send
    except:
      pass

class MQTTClients:

  def __init__(self, anId, cleansession, keepalive, socket, broker):
    self.id = anId # required
    self.cleansession = cleansession
    self.socket = socket
    self.msgid = 1
    self.outbound = [] # message objects - for ordering 
    self.outmsgs = {} # msgids to message objects
    self.broker = broker
    if broker.publish_on_pubrel:
      self.inbound = {} # stored inbound QoS 2 publications
    else:
      self.inbound = []
    self.connected = False
    self.will = None
    self.keepalive = keepalive
    self.lastPacket = None

  def resend(self):
    for pub in self.outbound:
      logger.debug("resending", pub)
      pub.fh.DUP = 1
      if pub.fh.QoS == 1:
        logger.info("[MQTT-2.1.2-3] Dup when resending QoS 1 publish id %d", pub.messageIdentifier)
        logger.info("[MQTT-2.3.1-4] Message id same as original publish on resend")
        respond(self.socket, pub)
      elif pub.fh.QoS == 2:
        if pub.qos2state == "PUBREC":
          logger.info("[MQTT-2.1.2-3] Dup when resending QoS 2 publish id %d", pub.messageIdentifier)
          pub.fh.DUP = 1
          logger.info("[MQTT-2.3.1-4] Message id same as original publish on resend")
          respond(self.socket, pub)
        else:
          resp = MQTTV3.Pubrels()
          logger.info("[MQTT-2.3.1-4] Message id same as original publish on resend")
          resp.messageIdentifier = pub.messageIdentifier
          respond(self.socket, resp)

  def publishArrived(self, topic, msg, qos, retained=False):
    pub = MQTTV3.Publishes()
    logger.info("[MQTT-3.2.3-3] topic name must match the subscription's topic filter")
    pub.topicName = topic
    pub.data = msg
    pub.fh.QoS = qos
    pub.fh.RETAIN = retained
    if retained:
      logger.info("[MQTT-2.1.2-7] Last retained message on matching topics sent on subscribe")
    if pub.fh.RETAIN:
      logger.info("[MQTT-2.1.2-9] Set retained flag on retained messages")
    if qos == 2:
      pub.qos2state = "PUBREC"
    if qos in [1, 2]:
      pub.messageIdentifier = self.msgid
      logger.debug("client id: %d msgid: %d", self.id, self.msgid)
      if self.msgid == 65535:
        self.msgid = 1
      else:
        self.msgid += 1
      self.outbound.append(pub)
      self.outmsgs[pub.messageIdentifier] = pub
    if qos == 0 and not self.broker.dropQoS0:
      self.outbound.append(pub)
    if self.connected:
      respond(self.socket, pub)
    else:
      if qos in [1, 2]:
        logger.info("[MQTT-3.1.2-5] storing of QoS 1 and 2 messagesfor disconnected client %s", self.id)

  def puback(self, msgid):
    if msgid in self.outmsgs.keys():
      pub = self.outmsgs[msgid]
      if pub.fh.QoS == 1:
        self.outbound.remove(pub)
        del self.outmsgs[msgid]
      else:
        logger.error("%s: Puback received for msgid %d, but QoS is %d", self.id, msgid, pub.fh.QoS)
    else:
      logger.error("%s: Puback received for msgid %d, but no message found", self.id, msgid)

  def pubrec(self, msgid):
    rc = False
    if msgid in self.outmsgs.keys():
      pub = self.outmsgs[msgid]
      if pub.fh.QoS == 2:
        if pub.qos2state == "PUBREC":
          pub.qos2state = "PUBCOMP"
          rc = True
        else:
          logger.error("%s: Pubrec received for msgid %d, but message in wrong state", self.id, msgid)
      else:
        logger.error("%s: Pubrec received for msgid %d, but QoS is %d", self.id, msgid, pub.fh.QoS)
    else:
      logger.error("%s: Pubrec received for msgid %d, but no message found", self.id, msgid)
    return rc

  def pubcomp(self, msgid):
    if msgid in self.outmsgs.keys():
      pub = self.outmsgs[msgid]
      if pub.fh.QoS == 2:
        if pub.qos2state == "PUBCOMP":
          self.outbound.remove(pub)
          del self.outmsgs[msgid]
        else:
          logger.error("Pubcomp received for msgid %d, but message in wrong state", msgid)
      else:
        logger.error("Pubcomp received for msgid %d, but QoS is %d", msgid, pub.fh.QoS)
    else:  
      logger.error("Pubcomp received for msgid %d, but no message found", msgid)

  def pubrel(self, msgid):
    rc = None
    if self.broker.publish_on_pubrel:
        pub = self.inbound[msgid]
        if pub.fh.QoS == 2:
          rc = pub
        else:
          logger.error("Pubrec received for msgid %d, but QoS is %d", msgid, pub.fh.QoS)
    else:
      rc = msgid in self.inbound
    if not rc:
      logger.error("Pubrec received for msgid %d, but no message found", msgid)
    return rc 
  

class MQTTBrokers:

  def __init__(self, publish_on_pubrel=True, overlapping_single=True, dropQoS0=True):

    # optional behaviour
    self.publish_on_pubrel = publish_on_pubrel
    self.dropQoS0 = dropQoS0                    # don't queue QoS 0 messages for disconnected clients

    self.broker = Brokers(overlapping_single)
    self.clients = {}   # socket -> clients
    self.lock = threading.RLock()

    logger.info("MQTT 3.1.1 Paho Test Broker")
    logger.info("Optional behaviour, publish on pubrel: %s", self.publish_on_pubrel)
    logger.info("Optional behaviour, single publish on overlapping topics: %s", self.broker.overlapping_single)
    logger.info("Optional behaviour, drop QoS 0 publications to disconnected clients: %s", self.dropQoS0)


  def reinitialize(self):
    logger.info("Reinitializing broker")
    self.clients = {}   
    self.broker.reinitialize()

  def handleRequest(self, sock):
    "this is going to be called from multiple threads, so synchronize"
    self.lock.acquire()
    terminate = False
    try:
      raw_packet = MQTTV3.getPacket(sock)
      if raw_packet == None:
        # will message
        self.disconnect(sock, None, terminate=True)
        terminate = True
      else:
        packet = MQTTV3.unpackPacket(raw_packet)
        if packet:
          terminate = self.handlePacket(packet, sock)
        else:
          raise MQTTV3.MQTTException("[MQTT-2.0.0-1] handleRequest: badly formed MQTT packet")
    finally:
      self.lock.release()
    return terminate

  def handlePacket(self, packet, sock):
    terminate = False
    logger.info("in: "+repr(packet))
    if sock not in self.clients.keys() and packet.fh.MessageType != MQTTV3.CONNECT:
      self.disconnect(sock, packet)
      raise MQTTV3.MQTTException("[MQTT-3.1.0-1] Connect was not first packet on socket")
    else:
      getattr(self, MQTTV3.packetNames[packet.fh.MessageType].lower())(sock, packet)
      if sock in self.clients.keys():
        self.clients[sock].lastPacket = time.time()
    if packet.fh.MessageType == MQTTV3.DISCONNECT:
      terminate = True
    return terminate

  def connect(self, sock, packet):
    if packet.ProtocolName != "MQTT":
      self.disconnect(sock, None)
      raise MQTTV3.MQTTException("[MQTT-3.1.2-1] Wrong protocol name %s" % packet.ProtocolName)
    if packet.ProtocolVersion != 4:
      logger.error("[MQTT-3.1.2-2] Wrong protocol version %d", packet.ProtocolVersion)
      resp = MQTTV3.Connacks()
      resp.returnCode = 1
      respond(sock, resp)
      self.disconnect(sock, None)
      logger.info("[MQTT-3.1.4-3] When rejecting connect, no more data must be processed")
      return
    if sock in self.clients.keys():    # is socket is already connected?
      self.disconnect(sock, None)
      logger.info("[MQTT-3.1.4-3] When rejecting connect, no more data must be processed")
      raise MQTTV3.MQTTException("[MQTT-3.1.0-2] Second connect packet")
    if len(packet.ClientIdentifier) == 0:
      if packet.CleanSession == False:
        logger.info("[MQTT-3.1.3-8] Reject 0-length clientid with cleansession false")
        logger.info("[MQTT-3.1.3-9] if clientid is rejected, must send connack 2 and close connection")
        resp = MQTTV3.Connacks()
        resp.returnCode = 2
        respond(sock, resp)
        self.disconnect(sock, None)
        logger.info("[MQTT-3.1.4-3] When rejecting connect, no more data must be processed")
        return
      else:
        logger.info("[MQTT-3.1.3-7] 0-length clientid must have cleansession true")
        packet.ClientIdentifier = uuid.uuid4() # give the client a unique clientid
        logger.info("[MQTT-3.1.3-6] 0-length clientid must be assigned a unique id %s", packet.ClientIdentifier)
    logger.info("[MQTT-3.1.3-5] Clientids of 1 to 23 chars and ascii alphanumeric must be allowed")
    if packet.ClientIdentifier in [client.id for client in self.clients.values()]: # is this client already connected on a different socket?
      for s in self.clients.keys():
        if self.clients[s] == packet.ClientIdentifier:
          logger.info("[MQTT-3.1.4-2] Disconnecting old client %s", packet.ClientIdentifier)
          self.disconnect(s, None)
          break
    me = None
    if not packet.CleanSession:
      me = self.broker.getClient(packet.ClientIdentifier) # find existing state, if there is any
      if me:
        logger.info("[MQTT-3.1.3-2] clientid used to retrieve client state")
    if me == None:
      me = MQTTClients(packet.ClientIdentifier, packet.CleanSession, packet.KeepAliveTimer, sock, self)
    else:
      me.socket = sock # set existing client state to new socket
      me.cleansession = packet.CleanSession
      me.keepalive = packet.KeepAliveTimer
    self.clients[sock] = me
    me.will = (packet.WillTopic, packet.WillQoS, packet.WillMessage, packet.WillRETAIN) if packet.WillFlag else None
    self.broker.connect(me)
    resp = MQTTV3.Connacks()
    resp.returnCode = 0
    respond(sock, resp)
    me.resend()

  def disconnect(self, sock, packet, terminate=False):
    if sock in self.clients.keys():
      if terminate:
        self.broker.terminate(self.clients[sock].id)
      else:
        self.broker.disconnect(self.clients[sock].id)
      del self.clients[sock]
    try:   
      sock.shutdown(socket.SHUT_RDWR) # must call shutdown to close socket immediately
    except:
      pass # doesn't matter if the socket has been closed at the other end already
    try:
      sock.close()
    except:
      pass # doesn't matter if the socket has been closed at the other end already

  def disconnectAll(self, sock):
    for sock in self.clients.keys():
      self.disconnect(sock, None)

  def subscribe(self, sock, packet):
    topics = []
    qoss = []
    for p in packet.data:
      topics.append(p[0])
      qoss.append(p[1])
    self.broker.subscribe(self.clients[sock].id, topics, qoss)
    resp = MQTTV3.Subacks()
    logger.info("[MQTT-2.3.1-7][MQTT-3.8.4-2] Suback has same message id as subscribe")
    logger.info("[MQTT-3.8.4-1] Must respond with suback")
    resp.messageIdentifier = packet.messageIdentifier
    logger.info("[MQTT-3.8.4-5] return code must be returned for each topic in subscribe")
    logger.info("[MQTT-3.9.3-1] the order of return codes must match order of topics in subscribe")
    resp.data = qoss
    respond(sock, resp)

  def unsubscribe(self, sock, packet):
    self.broker.unsubscribe(self.clients[sock].id, packet.data)
    resp = MQTTV3.Unsubacks()
    logger.info("[MQTT-2.3.1-7] Unsuback has same message id as unsubscribe")
    resp.messageIdentifier = packet.messageIdentifier
    respond(sock, resp)

  def publish(self, sock, packet):
    if packet.topicName.find("+") != -1 or packet.topicName.find("#") != -1:
      raise MqttException("[MQTT-3.3.2-2][MQTT-4.7.1-1] wildcards not allowed in topic name")
    if packet.fh.QoS == 0:
      self.broker.publish(self.clients[sock].id,
             packet.topicName, packet.data, packet.fh.QoS, packet.fh.RETAIN)
    elif packet.fh.QoS == 1:
      if packet.fh.DUP:
        logger.info("[MQTT-2.1.2-5] Incoming publish DUP 1 ==> outgoing publish with DUP 0")
      self.broker.publish(self.clients[sock].id,
             packet.topicName, packet.data, packet.fh.QoS, packet.fh.RETAIN)
      resp = MQTTV3.Pubacks()
      logger.info("[MQTT-2.3.1-6] puback messge id same as publish")
      resp.messageIdentifier = packet.messageIdentifier
      respond(sock, resp)
    elif packet.fh.QoS == 2:
      myclient = self.clients[sock]
      if self.publish_on_pubrel:
        if packet.messageIdentifier in myclient.inbound.keys():
          if packet.fh.DUP == 0:
            logger.error("[MQTT-2.1.2-2] duplicate QoS 2 message id %d found with DUP 0", packet.messageIdentifier)
          else:
            logger.info("[MQTT-2.1.2-2] DUP flag is 1 on redelivery")
        else:
          myclient.inbound[packet.messageIdentifier] = packet
      else:
        if packet.messageIdentifier in myclient.inbound:
          if packet.fh.DUP == 0:
            logger.error("[MQTT-2.1.2-2] duplicate QoS 2 message id %d found with DUP 0", packet.messageIdentifier)
          else:
            logger.info("[MQTT-2.1.2-2] DUP flag is 1 on redelivery")
        else:
          myclient.inbound.append(packet.messageIdentifier)
          self.broker.publish(myclient, packet.topicName, packet.data, packet.fh.QoS, packet.fh.RETAIN)
      resp = MQTTV3.Pubrecs()
      logger.info("[MQTT-2.3.1-6] pubrec messge id same as publish")
      resp.messageIdentifier = packet.messageIdentifier
      respond(sock, resp)

  def pubrel(self, sock, packet):
    myclient = self.clients[sock]
    pub = myclient.pubrel(packet.messageIdentifier)
    if pub:
      if self.publish_on_pubrel:
        self.broker.publish(myclient.id, pub.topicName, pub.data, pub.fh.QoS, pub.fh.RETAIN)
        del myclient.inbound[packet.messageIdentifier]
      else:
        myclient.inbound.remove(packet.messageIdentifier)
    # must respond with pubcomp regardless of whether a message was found
    if not pub:
      logger.info("[MQTT-3.6.4-1] must respond with a pubcomp packet")
    resp = MQTTV3.Pubcomps()
    logger.info("[MQTT-2.3.1-6] pubcomp messge id same as publish")
    resp.messageIdentifier = packet.messageIdentifier
    respond(sock, resp)

  def pingreq(self, sock, packet):
    resp = MQTTV3.Pingresps()
    logger.info("[MQTT-3.12.4-1] sending pingresp in response to pingreq")
    respond(sock, resp)

  def puback(self, sock, packet):
    "confirmed reception of qos 1"
    self.clients[sock].puback(packet.messageIdentifier)

  def pubrec(self, sock, packet):
    "confirmed reception of qos 2"
    myclient = self.clients[sock]
    if myclient.pubrec(packet.messageIdentifier):
      logger.info("[MQTT-3.5.4-1] must reply with pubrel in response to pubrec")
      resp = MQTTV3.Pubrels()
      resp.messageIdentifier = packet.messageIdentifier
      respond(sock, resp)

  def pubcomp(self, sock, packet):
    "confirmed reception of qos 2"
    self.clients[sock].pubcomp(packet.messageIdentifier)

  def keepalive(self, sock):
    if sock in self.clients.keys():
      client = self.clients[sock]
      if client.keepalive > 0 and time.time() - client.lastPacket > client.keepalive * 1.5:
        # keep alive timeout
        logger.info("[MQTT-3.1.2-22] keepalive timeout for client %s", client.id)
        self.disconnect(sock, None, terminate=True)




#
