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


import socket, time, _thread, logging

from . import internal

from ..formats import MQTTV311 as MQTTV3


logger = logging.getLogger("mqtt-client")
logger.setLevel(logging.ERROR)
logger.propagate = 1

formatter = logging.Formatter(fmt='%(levelname)s %(asctime)s %(name)s %(message)s',  datefmt='%Y%m%d %H%M%S')
ch = logging.StreamHandler()
ch.setFormatter(formatter)
ch.setLevel(logging.INFO)
logger.addHandler(ch)

def sendtosocket(mysocket, data):
  logger.debug("out: %s", str(data))
  sent = 0
  length = len(data)
  while sent < length:
    sent += mysocket.send(data)

class Callback:

  def connectionLost(self, cause):
    logger.debug("default connectionLost %s", str(cause))

  def publishArrived(self, topicName, payload, qos, retained, msgid):
    logger.debug("default publishArrived %s %s %d %d %d", topicName, payload, qos, retained, msgid)
    return True

  def published(self, msgid):
    logger.debug("default published %d", msgid)

  def subscribed(self, msgid):
    logger.debug("default subscribed %d", msgid)

  def unsubscribed(self, msgid):
    logger.debug("default unsubscribed %d", msgid)



class Client:

  def __init__(self, clientid):
    self.clientid = clientid
    self.msgid = 1
    self.callback = None
    self.__receiver = None
    self.cleansession = True


  def __nextMsgid(self):
    def getWrappedMsgid():
      id = self.msgid + 1
      if id == 65535:
        id = 1
      return id

    if len(self.__receiver.outMsgs) >= 65535:
      raise "No slots left!!"
    else:
      self.msgid = getWrappedMsgid()
      while self.msgid in self.__receiver.outMsgs.keys():
        self.msgid = getWrappedMsgid()
    return self.msgid


  def registerCallback(self, callback):
    self.callback = callback


  def connect(self, host="localhost", port=1883, cleansession=True, keepalive=0, newsocket=True, protocolName=None,
              willFlag=False, willTopic=None, willMessage=None, willQoS=2, willRetain=False, username=None, password=None):
    if newsocket:
      self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      self.sock.settimeout(.5)
      self.sock.connect((host, port))

    connect = MQTTV3.Connects()
    connect.ClientIdentifier = self.clientid
    connect.CleanSession = cleansession
    connect.KeepAliveTimer = keepalive
    if protocolName:
      connect.ProtocolName = protocolName

    if willFlag:
      connect.WillFlag = True
      connect.WillTopic = willTopic
      connect.WillMessage = willMessage
      connect.WillQoS = willQoS
      connect.WillRETAIN = willRetain

    if username:
      connect.usernameFlag = True
      connect.username = username

    if password:
      connect.passwordFlag = True
      connect.password = password

    sendtosocket(self.sock, connect.pack())

    response = MQTTV3.unpackPacket(MQTTV3.getPacket(self.sock))
    if not response:
      raise MQTTV3.MQTTException("connect failed - socket closed, no connack")
    assert response.fh.MessageType == MQTTV3.CONNACK

    self.cleansession = cleansession
    assert response.returnCode == 0, "connect was %s" % str(response)
    if self.cleansession or self.__receiver == None:
      self.__receiver = internal.Receivers(self.sock)
    else:
      self.__receiver.socket = self.sock
    if self.callback:
      id = _thread.start_new_thread(self.__receiver, (self.callback,))


  def subscribe(self, topics, qoss):
    subscribe = MQTTV3.Subscribes()
    subscribe.messageIdentifier = self.__nextMsgid()
    count = 0
    for t in topics:
      subscribe.data.append((t, qoss[count]))
      count += 1
    sendtosocket(self.sock, subscribe.pack())
    return subscribe.messageIdentifier


  def unsubscribe(self, topics):
    unsubscribe = MQTTV3.Unsubscribes()
    unsubscribe.messageIdentifier = self.__nextMsgid()
    unsubscribe.data = topics
    sendtosocket(self.sock, unsubscribe.pack())
    return unsubscribe.messageIdentifier


  def publish(self, topic, payload, qos=0, retained=False):
    publish = MQTTV3.Publishes()
    publish.fh.QoS = qos
    publish.fh.RETAIN = retained
    if qos == 0:
      publish.messageIdentifier = 0
    else:
      publish.messageIdentifier = self.__nextMsgid()
      if publish.fh.QoS == 2:
        publish.pubrec_received = False
      self.__receiver.outMsgs[publish.messageIdentifier] = publish
    publish.topicName = topic
    publish.data = payload
    sendtosocket(self.sock, publish.pack())
    return publish.messageIdentifier


  def disconnect(self):
    if self.__receiver:
      self.__receiver.stopping = True
      count = 0
      while (len(self.__receiver.inMsgs) > 0 or len(self.__receiver.outMsgs) > 0) and self.__receiver.paused == False:
        logger.debug("disconnecting %s %s", self.__receiver.inMsgs, self.__receiver.outMsgs)
        time.sleep(.2)
        count += 1
        if count == 20:
          break
      if self.__receiver and self.__receiver.paused == False:
        assert self.__receiver.inMsgs == {}, self.__receiver.inMsgs
        assert self.__receiver.outMsgs == {}, self.__receiver.outMsgs
    disconnect = MQTTV3.Disconnects()
    sendtosocket(self.sock, disconnect.pack())
    time.sleep(0.1)
    if self.cleansession:
      self.__receiver = None
    else:
      self.__receiver.socket = None
    self.sock.close()
    if self.__receiver:
      while self.__receiver.running:
        time.sleep(0.1)
      self.__receiver.stopping = False

  def terminate(self):
    if self.__receiver:
      self.__receiver.stopping = True
    self.sock.shutdown(socket.SHUT_RDWR)
    self.sock.close() # this will stop the receiver too
    self.__receiver = None

  def receive(self):
    return self.__receiver.receive()

  def pause(self):
    self.__receiver.paused = True

  def resume(self):
    self.__receiver.paused = False


if __name__ == "__main__":

  callback = Callback()

  aclient = Client("myclientid")
  aclient.registerCallback(callback)

  aclient.connect(port=1884)
  aclient.disconnect()

  aclient.connect(port=1884)
  aclient.subscribe(["k"], [2])
  aclient.publish("k", "qos 0")
  aclient.publish("k", "qos 1", 1)
  aclient.publish("k", "qos 2", 2)
  time.sleep(1.0)
  aclient.disconnect()

