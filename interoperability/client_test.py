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

import mqtt.client, time, logging, socket, sys, getopt

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

  def subscribed(self, msgid, data):
    logging.info("subscribed %d", msgid)
    self.subscribeds.append((msgid, data))

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
      print("deleting retained message for topic", message[0])
      aclient.publish(message[0], b"", 0, retained=True)
  aclient.disconnect()
  time.sleep(.1)

def usage():
  print(
"""
 -h: --hostname= hostname or ip address of server to run tests against
 -p: --port= port number of server to run tests against
 -z: --zero_length_clientid run zero length clientid test
 -d: --dollar_topics run $ topics test
 -s: --subscribe_failure run subscribe failure test
 -n: --nosubscribe_topic_filter= topic filter name for which subscriptions aren't allowed
        
""")
  


if __name__ == "__main__":
  try:
    opts, args = getopt.gnu_getopt(sys.argv[1:], "h:p:zdsn:", 
      ["help", "hostname=", "port=", "zero_length_clientid", "dollar_topics", "subscribe_failure", "nosubscribe_topic_filter="])
  except getopt.GetoptError as err:
    print(err) # will print something like "option -a not recognized"
    usage()
    sys.exit(2)

  dollar_topics_test = zero_length_clientid_test = subscribe_failure_test = False
  nosubscribe_topic_filter = "nosubscribe"
  host = "localhost"
  port = 1883
  for o, a in opts:
    if o in ("--help"):
      usage()
      sys.exit()
    elif o in ("-z", "--zero_length_clientid"):
      zero_length_clientid_test = True
    elif o in ("-d", "--dollar_topics"):
      dollar_topics_test = True
    elif o in ("-s", "--subscribe_failure"):
      subscribe_failure_test = True
    elif o in ("-n", "--nosubscribe_topic_filter"):
      nosubscribe_topic_filter = a
    elif o in ("-h", "--hostname"):
      host = a
    elif o in ("-p", "--port"):
      port = int(a)
    else:
      assert False, "unhandled option"

  root = logging.getLogger()
  root.setLevel(logging.INFO)

  print("hostname", host, "port", port)

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

  
  # 0 length clientid
  if zero_length_clientid_test:
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

  # overlapping subscriptions. When there is more than one matching subscription for the same client for a topic,
  # the server may send back one message with the highest QoS of any matching subscription, or one message for
  # each subscription with a matching QoS.
  callback.clear()
  callback2.clear()
  aclient.connect(host=host, port=port)
  aclient.subscribe(["a/#", "a/+"], [2, 1])
  aclient.publish("a/froma qos 2", b"overlapping topic filters", 2)
  time.sleep(1)
  print("messages", callback.messages)
  assert len(callback.messages) in [1, 2]
  if len(callback.messages) == 1:
    print("This server is publishing one message for all matching overlapping subscriptions, not one for each.")
    assert callback.messages[0][2] == 2
  else:
    print("This server is publishing one message per each matching overlapping subscription.")
    assert (callback.messages[0][2] == 2 and callback.messages[1][2] == 1) or \
           (callback.messages[0][2] == 1 and callback.messages[1][2] == 2), callback.messages
  aclient.disconnect()

  # keepalive processing.  We should be kicked off by the server if we don't send or receive any data, and don't send
  # any pings either.
  callback2.clear()
  aclient.connect(host=host, port=port, cleansession=True, keepalive=5, willFlag=True, willTopic="froma/willTopic", willMessage=b"keepalive expiry") 
  bclient.connect(host=host, port=port, cleansession=True, keepalive=0)
  bclient.subscribe(["froma/willTopic"], [2])
  time.sleep(15)
  bclient.disconnect()
  assert len(callback2.messages) == 1, "length should be 1: %s" % callback2.messages # should have the will message
  print("messages", callback2.messages)

  # redelivery on reconnect. When a QoS 1 or 2 exchange has not been completed, the server should retry the 
  # appropriate MQTT packets
  callback.clear()
  callback2.clear()
  bclient.connect(host=host, port=port, cleansession=False)
  bclient.subscribe(["tob/#"], [2])
  bclient.pause() # stops background processing 
  bclient.publish("tob/qos 1", b"", 1, retained=False)
  bclient.publish("tob/qos 2", b"", 2, retained=False)
  time.sleep(1)
  bclient.disconnect()
  assert len(callback2.messages) == 0
  bclient.connect(host=host, port=port, cleansession=False)
  bclient.resume()
  time.sleep(3)
  assert len(callback2.messages) == 2, "length should be 2: %s" % callback2.messages
  bclient.disconnect()

  # Subscribe failure.  A new feature of MQTT 3.1.1 is the ability to send back negative reponses to subscribe
  # requests.  One way of doing this is to subscribe to a topic which is not allowed to be subscribed to.
  if subscribe_failure_test:
    callback.clear()
    aclient.connect(host=host, port=port)
    aclient.subscribe([nosubscribe_topic_filter], [2])
    time.sleep(.2)
    # subscribeds is a list of (msgid, [qos])
    assert callback.subscribeds[0][1][0] == 0x80, "return code should be 0x80 %s" % callback.subscribeds

  
  # $ topics. The specification says that a topic filter which starts with a wildcard does not match topic names that
  # begin with a $.  Publishing to a topic which starts with a $ may not be allowed on some servers (which is entirely valid),
  # so this test will not work and should be omitted in that case.
  if dollar_topics_test:
    callback2.clear()
    bclient.connect(host=host, port=port, cleansession=True, keepalive=0)
    bclient.subscribe(["+/+"], [2])
    time.sleep(1) # wait for all retained messages, hopefully
    callback2.clear() 
    bclient.publish("$fromb/qos 1", b"", 1, retained=False)
    time.sleep(.2)
    assert len(callback2.messages) == 0, callback2.messages
    bclient.disconnect()








 







