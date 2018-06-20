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


import socket, time, _thread, logging

from . import internal

from mqtt.formats import MQTTV5


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
  try:
    while sent < length:
      sent += mysocket.send(data)
  except:
    pass # could be socket error
  return sent

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

  def disconnected(self, reasoncode, properties):
    logger.debug("default disconnected")



class Client:

  def getReceiver(self):
    return self.__receiver

  def __init__(self, clientid):
    self.clientid = clientid
    self.msgid = 1
    self.callback = None
    self.__receiver = None
    self.cleanstart = True
    self.sessionexpiry = 0


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


  def connect(self, host="localhost", port=1883, cleanstart=True, keepalive=0, newsocket=True, protocolName=None,
              willFlag=False, willTopic=None, willMessage=None, willQoS=2, willRetain=False, username=None, password=None,
              properties=None, willProperties=None):
    if newsocket:
      try:
        self.sock.close()
      except:
        pass
      self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      self.sock.settimeout(.5)
      self.sock.connect((host, port))

    connect = MQTTV5.Connects()
    connect.ClientIdentifier = self.clientid
    connect.CleanStart = cleanstart
    connect.KeepAliveTimer = keepalive
    if protocolName:
      connect.ProtocolName = protocolName

    if willFlag:
      connect.WillFlag = True
      connect.WillTopic = willTopic
      connect.WillMessage = willMessage
      connect.WillQoS = willQoS
      connect.WillRETAIN = willRetain
      if willProperties:
        connect.WillProperties = willProperties

    if username:
      connect.usernameFlag = True
      connect.username = username

    if password:
      connect.passwordFlag = True
      connect.password = password

    if properties:
      connect.properties = properties

    sendtosocket(self.sock, connect.pack())

    response = MQTTV5.unpackPacket(MQTTV5.getPacket(self.sock))
    if not response:
      raise MQTTV5.MQTTException("connect failed - socket closed, no connack")
    assert response.fh.PacketType == MQTTV5.PacketTypes.CONNACK

    self.cleanstart = cleanstart
    assert response.reasonCode.getName() == "Success", "connect was %s" % str(response)
    if self.cleanstart or self.__receiver == None:
      self.__receiver = internal.Receivers(self.sock)
    else:
      self.__receiver.socket = self.sock
    if self.callback:
      id = _thread.start_new_thread(self.__receiver, (self.callback,))
    return response


  def subscribe(self, topics, options, properties=None):
    subscribe = MQTTV5.Subscribes()
    subscribe.packetIdentifier = self.__nextMsgid()
    count = 0
    for t in topics:
      subscribe.data.append((t, options[count]))
      count += 1
    if properties:
      subscribe.properties = properties
    sendtosocket(self.sock, subscribe.pack())
    return subscribe.packetIdentifier


  def unsubscribe(self, topics):
    unsubscribe = MQTTV5.Unsubscribes()
    unsubscribe.packetIdentifier = self.__nextMsgid()
    unsubscribe.topicFilters = topics
    sendtosocket(self.sock, unsubscribe.pack())
    return unsubscribe.packetIdentifier


  def publish(self, topic, payload, qos=0, retained=False, properties=None):
    publish = MQTTV5.Publishes()
    publish.fh.QoS = qos
    publish.fh.RETAIN = retained
    if qos == 0:
      publish.packetIdentifier = 0
    else:
      publish.packetIdentifier = self.__nextMsgid()
      if publish.fh.QoS == 2:
        pass #publish.pubrec_received = False
      self.__receiver.outMsgs[publish.packetIdentifier] = publish
    publish.topicName = topic
    if properties:
      publish.properties = properties
    publish.data = payload if type(payload) == type(b"") else bytes(payload, "utf8")
    sendtosocket(self.sock, publish.pack())
    return publish.packetIdentifier


  def disconnect(self, properties=None):
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
    disconnect = MQTTV5.Disconnects()
    if properties:
      disconnect.properties = properties
    sendtosocket(self.sock, disconnect.pack())
    time.sleep(1.1)
    if self.sessionexpiry == 0:
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
    if self.__receiver:
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
