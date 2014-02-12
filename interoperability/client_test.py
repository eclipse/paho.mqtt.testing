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

import mqtt.client, time, logging, socket, sys

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

def cleanup():

	# clean all client state
	clientids = ("myclientid", "myclientid2")
	hostname = "localhost" 
	port = 1883 

	for clientid in clientids:
		aclient = mqtt.client.Client("myclientid".encode("utf-8"))
		aclient.connect(host=hostname, port=port, cleansession=True)
		time.sleep(.1)
		aclient.disconnect()
		time.sleep(.1)

	# clean retained messages 
	callback = Callbacks()
	aclient = mqtt.client.Client("clean retained".encode("utf-8"))
	aclient.registerCallback(callback)
	aclient.connect(host=hostname, port=port, cleansession=True)
	aclient.subscribe(["#"], [0])
	time.sleep(2) # wait for all retained messages to arrive
	for message in callback.messages:  
		if message[3]: # retained flag
		  aclient.publish(message[0], b"", 0, retained=True)
	aclient.disconnect()
	time.sleep(.1)

if __name__ == "__main__":

  root = logging.getLogger()
  root.setLevel(logging.INFO)

  if (len(sys.argv) > 1):
    host, port = sys.argv[1].split(":")
    port = int(port)
  else:
    host="localhost"
    port=1883

  cleanup()
  
  try:
    callback = Callbacks()

    #aclient = mqtt.client.Client(b"\xEF\xBB\xBF" + "myclientid".encode("utf-8"))
    aclient = mqtt.client.Client("myclientid".encode("utf-8"))
    aclient.registerCallback(callback)

    aclient.connect(host=host, port=port)
    aclient.disconnect()

    aclient.connect(host=host, port=port)
    aclient.subscribe(["k"], [2])
    aclient.publish("k", b"qos 0")
    aclient.publish("k", b"qos 1", 1)
    aclient.publish("k", b"qos 2", 2)
    aclient.disconnect()

    aclient.connect(host=host, port=port)
    aclient.connect(host=host, port=port, newsocket=False)

    aclient.connect(host=host, port=port, protocolName="hj")
  except Exception as exc:
    print("Exception", exc)
    

  # message queueing for offline clients
  callback.clear()

  aclient.connect(host=host, port=port, cleansession=False)
  aclient.subscribe(["fromb/#"], [2])
  aclient.disconnect()

  callback2 = Callbacks()
  bclient = mqtt.client.Client("myclientid2".encode("utf-8"))
  bclient.registerCallback(callback2)
  bclient.connect(host=host, port=port)
  bclient.publish("fromb/qos 0", b"qos 0", 0)
  bclient.publish("fromb/qos 1", b"qos 1", 1)
  bclient.publish("fromb/qos 2", b"qos 2", 2)
  bclient.disconnect()

  aclient.connect(host=host, port=port, cleansession=False)
  time.sleep(.2)
  aclient.disconnect()

  print(callback.messages)
  assert len(callback.messages) in [2, 3]


  # retained messages
  callback.clear()
  aclient.connect(host=host, port=port, cleansession=True)
  aclient.publish("fromb/qos 0", b"qos 0", 0, retained=True)
  aclient.publish("fromb/qos 1", b"qos 1", 1, retained=True)
  aclient.publish("fromb/qos 2", b"qos 2", 2, retained=True)
  time.sleep(.2)
  aclient.subscribe(["fromb/+"], [2])
  time.sleep(.2)
  aclient.disconnect()
  
  print(callback.messages)
  assert len(callback.messages) == 3
  
  # clear retained messages
  callback.clear()
  aclient.connect(host=host, port=port, cleansession=True)
  aclient.publish("fromb/qos 0", b"", 0, retained=True)
  aclient.publish("fromb/qos 1", b"", 1, retained=True)
  aclient.publish("fromb/qos 2", b"", 2, retained=True)
  time.sleep(.2) # wait for QoS 2 exchange to be completed
  aclient.subscribe(["fromb/#"], [2])
  time.sleep(.2)
  aclient.disconnect()
  
  print(callback.messages)
  assert len(callback.messages) == 0

  # will messages
  aclient.connect(host=host, port=port, cleansession=True, willFlag=True, willTopic="froma/willTopic", willMessage=b"client not disconnected", keepalive=2) 
  bclient.connect(host=host, port=port, cleansession=False)
  bclient.subscribe(["froma/willTopic"], [2])
  time.sleep(.1)
  aclient.terminate()
  time.sleep(5)
  bclient.disconnect()
  print("messages %s", callback2.messages)
  assert len(callback2.messages) == 1 # should have the will message

  
  # $ topics
  callback2.clear()
  aclient.connect(host=host, port=port) 
  bclient.connect(host=host, port=port, cleansession=True, keepalive=0)
  bclient.subscribe(["$SYS/fromb/#"], [2])
  aclient.publish("fromb/qos 1", b"", 1, retained=True)
  time.sleep(.2)
  assert len(callback2.messages) == 0 
  aclient.publish("$SYS/fromb/qos 1", b"", 1, retained=True)
  time.sleep(.2)
  assert len(callback2.messages) in [1, 0]
  bclient.subscribe(["fromb/#"], [2])
  aclient.publish("fromb/qos 1", b"", 1, retained=True)
  time.sleep(.2)
  print("messages", callback2.messages)
  assert len(callback2.messages) in [2, 0]
  bclient.disconnect()

  # overlapping subscriptions
  callback.clear()
  callback2.clear()
  aclient.connect(host=host, port=port)
  aclient.subscribe(["a/#", "a/+"], [2, 1])
  aclient.publish("a/froma qos 2", b"overlapping topic filters", 2)
  time.sleep(1)
  print("messages", callback.messages)
  assert len(callback.messages) in [1, 2]
  aclient.disconnect()

  # username & password (subscribe failure)


  # redelivery on reconnect


  # keepalive
  callback2.clear()
  aclient.connect(host=host, port=port, cleansession=True, keepalive=5, willFlag=True, willTopic="froma/willTopic", willMessage=b"keepalive expiry") 
  bclient.connect(host=host, port=port, cleansession=False, keepalive=0)
  bclient.subscribe(["froma/willTopic"], [2])
  time.sleep(15)
  bclient.disconnect()
  assert len(callback2.messages) == 1, "length should be 1: %s" % callback2.messages # should have the will message
  print("messages", callback2.messages)
  
  # 0 length clientid
  client0 = mqtt.client.Client("")
  fails = False
  try:
    client0.connect(host=host, port=port, cleansession=False) # should be rejected
  except:
    fails = True
  assert fails == True
  fails = False
  try:
    client0.connect(host=host, port=port, cleansession=True) # should work
  except:
    fails = True
  assert fails == False
  client0.disconnect()
 







