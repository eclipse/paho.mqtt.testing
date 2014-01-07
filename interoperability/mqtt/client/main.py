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


logging.basicConfig(format='%(levelname)s %(asctime)s %(message)s',  datefmt='%Y%m%d %H%M%S', level=logging.INFO)


class Callback:

  def connectionLost(self, cause):
    logging.info("default connectionLost %s", str(cause))

  def publishArrived(self, topicName, payload, qos, retained, msgid):
    logging.info("default publishArrived %s %s %d %d %d", topicName, payload, qos, retained, msgid)
    return True

  def published(self, msgid):
    logging.info("default published %d", msgid)

  def subscribed(self, msgid):
    logging.info("default subscribed %d", msgid)

  def unsubscribed(self, msgid):
    logging.info("default unsubscribed %d", msgid)



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


  def connect(self, host="localhost", port=1883, cleanstart=True):
    self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #self.sock.settimeout(5.0)

    self.sock.connect((host, port))

    connect = MQTTV3.Connects()
    connect.ClientIdentifier = self.clientid
    connect.CleanStart = cleanstart
    connect.KeepAliveTimer = 0
    self.sock.send(connect.pack())

    response = MQTTV3.unpackPacket(MQTTV3.getPacket(self.sock))
    if not response:
      # connect failed - socket closed, no connack
      return
    assert response.fh.MessageType == MQTTV3.CONNACK

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
    self.sock.send(subscribe.pack())
    return subscribe.messageIdentifier


  def unsubscribe(self, topics):
    unsubscribe = MQTTV3.Unsubscribes()
    unsubscribe.messageIdentifier = self.__nextMsgid()
    unsubscribe.data = topics
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
    self.sock.send(publish.pack())
    return publish.messageIdentifier


  def disconnect(self):
    disconnect = MQTTV3.Disconnects()
    self.sock.send(disconnect.pack())
    time.sleep(0.2)
    if self.__receiver:
      self.__receiver.stopping = True
    self.sock.close() # this will stop the receiver too
    if self.__receiver:
      assert self.__receiver.inMsgs == {}
      assert self.__receiver.outMsgs == {}
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

