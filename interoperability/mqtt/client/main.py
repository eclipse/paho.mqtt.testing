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
#logger.propagate = 0

formatter = logging.Formatter(fmt='%(levelname)s %(asctime)s %(name)s %(message)s',  datefmt='%Y%m%d %H%M%S')
ch = logging.StreamHandler()
ch.setFormatter(formatter)
ch.setLevel(logging.INFO)
logger.addHandler(ch)


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

    logger.debug("out: %s", str(connect))
    self.sock.send(connect.pack())

    response = MQTTV3.unpackPacket(MQTTV3.getPacket(self.sock))
    if not response:
      raise MQTTV3.MQTTException("connect failed - socket closed, no connack")
    assert response.fh.MessageType == MQTTV3.CONNACK

    assert response.returnCode == 0
    self.__receiver = internal.Receivers(self.sock)
    if self.callback:
      id = _thread.start_new_thread(self.__receiver, (self.callback,))


  def subscribe(self, topics, qoss):
    subscribe = MQTTV3.Subscribes()
    subscribe.messageIdentifier = self.__nextMsgid()
    count = 0
    for t in topics:
      subscribe.data.append((t, qoss[count]))
      count += 1
    logger.debug("out: %s", str(subscribe))
    self.sock.send(subscribe.pack())
    return subscribe.messageIdentifier


  def unsubscribe(self, topics):
    unsubscribe = MQTTV3.Unsubscribes()
    unsubscribe.messageIdentifier = self.__nextMsgid()
    unsubscribe.data = topics
    logger.debug("out: %s", str(unsubscribe))
    self.sock.send(unsubscribe.pack())
    return unsubscribe.messageIdentifier


  def publish(self, topic, payload, qos=0, retained=False):
    publish = MQTTV3.Publishes()
    publish.fh.QoS = qos
    publish.fh.RETAIN = retained
    if qos == 0:
      publish.messageIdentifier = 0
    else:
      publish.messageIdentifier = self.__nextMsgid()
      self.__receiver.outMsgs[publish.messageIdentifier] = publish
    publish.topicName = topic
    publish.data = payload
    logger.debug("out: %s", str(publish))
    self.sock.send(publish.pack())
    return publish.messageIdentifier


  def disconnect(self):
    if self.__receiver:
      self.__receiver.stopping = True
      while len(self.__receiver.inMsgs) > 0 or len(self.__receiver.outMsgs) > 0:
        logger.debug(self.__receiver.inMsgs, self.__receiver.outMsgs)
        print(self.__receiver.inMsgs, self.__receiver.outMsgs)
        time.sleep(.1)
    if self.__receiver:
      assert self.__receiver.inMsgs == {}
      assert self.__receiver.outMsgs == {}
    disconnect = MQTTV3.Disconnects()
    logger.debug("out: %s", str(disconnect))
    self.sock.send(disconnect.pack())
    time.sleep(0.1)
    self.sock.close() # this will stop the receiver too
    self.__receiver = None

  def terminate(self):
    if self.__receiver:
      self.__receiver.stopping = True
    self.sock.shutdown(socket.SHUT_RDWR)
    self.sock.close() # this will stop the receiver too
    self.__receiver = None

  def receive(self):
    return self.__receiver.receive()


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

