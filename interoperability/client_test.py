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

import mqtt.client, time, logging, socket

class Callbacks(mqtt.client.Callback):

  def __init__(self):
    self.messages = []
    self.publisheds = []
    self.subscribeds = []
    self.unsubscribeds = []

  def clear(self):
    self.__init__()

  def connectionLost(self, cause):
    logging.info("connectionLost %s", str(cause))

  def publishArrived(self, topicName, payload, qos, retained, msgid):
    logging.info("publishArrived %s %s %d %d %d", topicName, payload, qos, retained, msgid)
    self.messages.append((topicName, payload, qos, retained, msgid))
    return True

  def published(self, msgid):
    logging.info("published %d", msgid)
    self.publisheds.append(msgid)

  def subscribed(self, msgid):
    logging.info("subscribed %d", msgid)
    self.subscribeds.append(msgid)

  def unsubscribed(self, msgid):
    logging.info("unsubscribed %d", msgid)
    self.unsubscribeds.append(msgid)

if __name__ == "__main__":

  try:
    callback = Callbacks()

    #aclient = mqtt.client.Client(b"\xEF\xBB\xBF" + "myclientid".encode("utf-8"))
    aclient = mqtt.client.Client("myclientid".encode("utf-8"))
    aclient.registerCallback(callback)

    aclient.connect(port=1883)
    aclient.disconnect()

    aclient.connect(port=1883)
    aclient.subscribe(["k"], [2])
    aclient.publish("k", b"qos 0")
    aclient.publish("k", b"qos 1", 1)
    aclient.publish("k", b"qos 2", 2)
    aclient.disconnect()

    aclient.connect(port=1883)
    aclient.connect(port=1883, newsocket=False)

    aclient.connect(port=1883, protocolName="hj")
  except Exception as exc:
    print("Exception", exc)
    

  # message queueing for offline clients
  callback.clear()

  aclient.connect(port=1883, cleansession=False)
  aclient.subscribe(["#"], [2])
  aclient.disconnect()

  callback2 = Callbacks()
  bclient = mqtt.client.Client("myclientid2".encode("utf-8"))
  bclient.registerCallback(callback2)
  bclient.connect(port=1883)
  bclient.publish("fromb qos 0", b"qos 0", 0)
  bclient.publish("fromb qos 1", b"qos 1", 1)
  bclient.publish("fromb qos 2", b"qos 2", 2)
  bclient.disconnect()

  aclient.connect(port=1883, cleansession=False)
  time.sleep(.2)
  aclient.disconnect()

  print(callback.messages)
  assert len(callback.messages) == 2


  # retained messages
  callback.clear()
  aclient.connect(port=1883, cleansession=True)
  aclient.publish("fromb qos 0", b"qos 0", 0, retained=True)
  aclient.publish("fromb qos 1", b"qos 1", 1, retained=True)
  aclient.publish("fromb qos 2", b"qos 2", 2, retained=True)
  time.sleep(.2)
  aclient.subscribe(["#"], [2])
  time.sleep(.2)
  aclient.disconnect()
  
  print(callback.messages)
  assert len(callback.messages) == 3

  # clear retained messages
  callback.clear()
  aclient.connect(port=1883, cleansession=True)
  aclient.publish("fromb qos 0", b"", 0, retained=True)
  aclient.publish("fromb qos 1", b"", 1, retained=True)
  aclient.publish("fromb qos 2", b"", 2, retained=True)
  time.sleep(.2) # wait for QoS 2 exchange to be completed
  aclient.subscribe(["#"], [2])
  time.sleep(.2)
  aclient.disconnect()
  
  print(callback.messages)
  assert len(callback.messages) == 0

  # will messages
  aclient.connect(port=1883, cleansession=True, willFlag=True, willTopic="willTopic", willMessage=b"client not disconnected") 
  bclient.connect(port=1883, cleansession=False)
  bclient.subscribe(["#"], [2])
  time.sleep(.1)
  aclient.terminate()
  time.sleep(.2)
  bclient.disconnect()
  assert len(callback2.messages) == 1 # should have the will message
  print("messages %s", callback2.messages)

  # keepalive
  callback2.clear()
  aclient.connect(port=1883, cleansession=True, keepalive=5, willFlag=True, willTopic="willTopic", willMessage=b"keepalive expiry") 
  bclient.connect(port=1883, cleansession=False, keepalive=0)
  bclient.subscribe(["#"], [2])
  time.sleep(10)
  bclient.disconnect()
  assert len(callback2.messages) == 1 # should have the will message
  print("messages %s", callback2.messages)
  
  # $ topics
  callback2.clear()
  aclient.connect(port=1883) 
  bclient.connect(port=1883, cleansession=True, keepalive=0)
  bclient.subscribe(["$SYS/#"], [2])
  aclient.publish("fromb qos 1", b"", 1, retained=True)
  time.sleep(.2)
  assert len(callback2.messages) == 0 
  aclient.publish("$SYS/fromb qos 1", b"", 1, retained=True)
  time.sleep(.2)
  assert len(callback2.messages) == 1
  bclient.subscribe(["#"], [2])
  aclient.publish("fromb qos 1", b"", 1, retained=True)
  time.sleep(.2)
  print("messages %s", callback2.messages)
  assert len(callback2.messages) == 2
  bclient.disconnect()

  # username & password


  # redelivery on reconnect

 







