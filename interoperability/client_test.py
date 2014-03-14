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

import mqtt.client, time, logging, socket, sys, getopt, traceback

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

  for clientid in clientids:
    aclient = mqtt.client.Client("myclientid".encode("utf-8"))
    aclient.connect(host=host, port=port, cleansession=True)
    time.sleep(.1)
    aclient.disconnect()
    time.sleep(.1)

  # clean retained messages 
  callback = Callbacks()
  aclient = mqtt.client.Client("clean retained".encode("utf-8"))
  aclient.registerCallback(callback)
  aclient.connect(host=host, port=port, cleansession=True)
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

 
def basic_test():
  global aclient
  succeeded = True
  try:
    #aclient = mqtt.client.Client(b"\xEF\xBB\xBF" + "myclientid".encode("utf-8"))
    aclient = mqtt.client.Client("myclientid".encode("utf-8"))
    aclient.registerCallback(callback)

    aclient.connect(host=host, port=port)
    aclient.disconnect()

    aclient.connect(host=host, port=port)
    aclient.subscribe([topics[0]], [2])
    aclient.publish(topics[0], b"qos 0")
    aclient.publish(topics[0], b"qos 1", 1)
    aclient.publish(topics[0], b"qos 2", 2)
    aclient.disconnect()
    assert len(callback.messages) == 3
  except:
    succeeded = False

  try:
    aclient.connect(host=host, port=port)
    aclient.connect(host=host, port=port, newsocket=False) # should fail - second connect on socket
    succeeded = False
  except Exception as exc:
    pass # exception expected

  try:
    aclient.connect(host=host, port=port, protocolName="hj") # should fail - wrong protocol name
    succeeded = False
  except Exception as exc:
    pass # exception expected

  print("Basic test", "succeeded" if succeeded else "failed")
  return succeeded

def retained_message_test(qos0topic="fromb/qos 0", qos1topic="fromb/qos 1", qos2topic="fromb/qos2", 
    wildcardtopic="fromb/+"):
  succeeded = False
  try:
    # retained messages
    callback.clear()
    aclient.connect(host=host, port=port, cleansession=True)
    aclient.publish(topics[1], b"qos 0", 0, retained=True)
    aclient.publish(topics[2], b"qos 1", 1, retained=True)
    aclient.publish(topics[3], b"qos 2", 2, retained=True)
    time.sleep(.2)
    aclient.subscribe([wildtopics[5]], [2])
    time.sleep(.2)
    aclient.disconnect()

    assert len(callback.messages) == 3

    # clear retained messages
    callback.clear()
    aclient.connect(host=host, port=port, cleansession=True)
    aclient.publish(topics[1], b"", 0, retained=True)
    aclient.publish(topics[2], b"", 1, retained=True)
    aclient.publish(topics[3], b"", 2, retained=True)
    time.sleep(.2) # wait for QoS 2 exchange to be completed
    aclient.subscribe([wildtopics[5]], [2])
    time.sleep(.2)
    aclient.disconnect()

    assert len(callback.messages) == 0, "callback messages is %s" % callback.messages
    succeeded = True
  except:
    traceback.print_exc()
  print("Retained message test", "succeeded" if succeeded else "failed")
  return succeeded

def will_message_test():
  # will messages
  succeeded = True
  callback2.clear()
  assert len(callback2.messages) == 0, callback2.messages
  try:
    aclient.connect(host=host, port=port, cleansession=True, willFlag=True, 
      willTopic=topics[2], willMessage=b"client not disconnected", keepalive=2) 
    bclient.connect(host=host, port=port, cleansession=False)
    bclient.subscribe([topics[2]], [2])
    time.sleep(.1)
    aclient.terminate()
    time.sleep(5)
    bclient.disconnect()
    assert len(callback2.messages) == 1, callback2.messages  # should have the will message
  except:
    traceback.print_exc()
    succeeded = False
  print("Will message test", "succeeded" if succeeded else "failed")
  return succeeded

# 0 length clientid
def zero_length_clientid_test():
  succeeded = True
  try:
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
  except:
    traceback.print_exc()
    succeeded = False
  print("Zero length clientid test", "succeeded" if succeeded else "failed")
  return succeeded

def offline_message_queueing_test():
  succeeded = True
  try:
    # message queueing for offline clients
    callback.clear()

    aclient.connect(host=host, port=port, cleansession=False)
    aclient.subscribe([wildtopics[5]], [2])
    aclient.disconnect()

    bclient.connect(host=host, port=port)
    bclient.publish(topics[1], b"qos 0", 0)
    bclient.publish(topics[2], b"qos 1", 1)
    bclient.publish(topics[3], b"qos 2", 2)
    bclient.disconnect()

    aclient.connect(host=host, port=port, cleansession=False)
    time.sleep(.2)
    aclient.disconnect()

    assert len(callback.messages) in [2, 3], callback.messages
    print("This server %s queueing QoS 0 messages for offline clients" % \
        ("is" if len(callback.messages) == 3 else "is not"))
  except:
    succeeded = False
  print("Offline message queueing test", "succeeded" if succeeded else "failed")
  return succeeded

def overlapping_subscriptions_test():
  # overlapping subscriptions. When there is more than one matching subscription for the same client for a topic,
  # the server may send back one message with the highest QoS of any matching subscription, or one message for
  # each subscription with a matching QoS.
  succeeded = True
  try:
    callback.clear()
    callback2.clear()
    aclient.connect(host=host, port=port)
    aclient.subscribe([wildtopics[6], wildtopics[0]], [2, 1])
    aclient.publish(topics[3], b"overlapping topic filters", 2)
    time.sleep(1)
    assert len(callback.messages) in [1, 2]
    if len(callback.messages) == 1:
      print("This server is publishing one message for all matching overlapping subscriptions, not one for each.")
      assert callback.messages[0][2] == 2
    else:
      print("This server is publishing one message per each matching overlapping subscription.")
      assert (callback.messages[0][2] == 2 and callback.messages[1][2] == 1) or \
             (callback.messages[0][2] == 1 and callback.messages[1][2] == 2), callback.messages
    aclient.disconnect()
  except:
    traceback.print_exc()
    succeeded = False
  print("Overlapping subscriptions test", "succeeded" if succeeded else "failed")
  return succeeded

 
def keepalive_test():
  # keepalive processing.  We should be kicked off by the server if we don't send or receive any data, and don't send
  # any pings either.
  succeeded = True
  try:
    callback2.clear()
    aclient.connect(host=host, port=port, cleansession=True, keepalive=5, willFlag=True, 
          willTopic=topics[4], willMessage=b"keepalive expiry") 
    bclient.connect(host=host, port=port, cleansession=True, keepalive=0)
    bclient.subscribe([topics[4]], [2])
    time.sleep(15)
    bclient.disconnect()
    assert len(callback2.messages) == 1, "length should be 1: %s" % callback2.messages # should have the will message
  except:
    traceback.print_exc()
    succeeded = False
  print("Keepalive test", "succeeded" if succeeded else "failed")
  return succeeded


def redelivery_on_reconnect_test():
  # redelivery on reconnect. When a QoS 1 or 2 exchange has not been completed, the server should retry the 
  # appropriate MQTT packets
  succeeded = True
  try:
    callback.clear()
    callback2.clear()
    bclient.connect(host=host, port=port, cleansession=False)
    bclient.subscribe([wildtopics[6]], [2])
    bclient.pause() # stops background processing 
    bclient.publish(topics[1], b"", 1, retained=False)
    bclient.publish(topics[3], b"", 2, retained=False)
    time.sleep(1)
    bclient.disconnect()
    assert len(callback2.messages) == 0
    bclient.connect(host=host, port=port, cleansession=False)
    bclient.resume()
    time.sleep(3)
    assert len(callback2.messages) == 2, "length should be 2: %s" % callback2.messages
    bclient.disconnect()
  except:
    traceback.print_exc()
    succeeded = False
  print("Redelivery on reconnect test", "succeeded" if succeeded else "failed")
  return succeeded

def subscribe_failure_test():
  # Subscribe failure.  A new feature of MQTT 3.1.1 is the ability to send back negative reponses to subscribe
  # requests.  One way of doing this is to subscribe to a topic which is not allowed to be subscribed to.
  succeeded = True
  try:
    callback.clear()
    aclient.connect(host=host, port=port)
    aclient.subscribe([nosubscribe_topics[0]], [2])
    time.sleep(.2)
    # subscribeds is a list of (msgid, [qos])
    assert callback.subscribeds[0][1][0] == 0x80, "return code should be 0x80 %s" % callback.subscribeds
  except:
    traceback.print_exc()
    succeeded = False
  print("Subscribe failure test", "succeeded" if succeeded else "failed")
  return succeeded


def dollar_topics_test():
  # $ topics. The specification says that a topic filter which starts with a wildcard does not match topic names that
  # begin with a $.  Publishing to a topic which starts with a $ may not be allowed on some servers (which is entirely valid),
  # so this test will not work and should be omitted in that case.
  succeeded = True
  try:
    callback2.clear()
    bclient.connect(host=host, port=port, cleansession=True, keepalive=0)
    bclient.subscribe([wildtopics[5]], [2])
    time.sleep(1) # wait for all retained messages, hopefully
    callback2.clear() 
    bclient.publish("$"+topics[1], b"", 1, retained=False)
    time.sleep(.2)
    assert len(callback2.messages) == 0, callback2.messages
    bclient.disconnect()
  except:
    traceback.print_exc()
    succeeded = False
  print("$ topics test", "succeeded" if succeeded else "failed")
  return succeeded


if __name__ == "__main__":
  try:
    opts, args = getopt.gnu_getopt(sys.argv[1:], "h:p:zdsn:", 
      ["help", "hostname=", "port=", "zero_length_clientid", "dollar_topics", 
       "subscribe_failure", "nosubscribe_topic_filter=", "iterations="])
  except getopt.GetoptError as err:
    print(err) # will print something like "option -a not recognized"
    usage()
    sys.exit(2)

  run_dollar_topics_test = run_zero_length_clientid_test = run_subscribe_failure_test = False
  iterations = 1

  global topics, wildtopics, nosubscribe_topics
  topics =  ("TopicA", "TopicA/B", "Topic/C", "TopicA/C", "/TopicA")
  wildtopics = ("TopicA/+", "+/C", "#", "/#", "/+", "+/+", "TopicA/#")
  nosubscribe_topics = ("nosubscribe",)

  host = "localhost"
  port = 1883
  for o, a in opts:
    if o in ("--help"):
      usage()
      sys.exit()
    elif o in ("-z", "--zero_length_clientid"):
      run_zero_length_clientid_test = True
    elif o in ("-d", "--dollar_topics"):
      run_dollar_topics_test = True
    elif o in ("-s", "--subscribe_failure"):
      run_subscribe_failure_test = True
    elif o in ("-n", "--nosubscribe_topic_filter"):
      nosubscribe_topic_filter = a
    elif o in ("-h", "--hostname"):
      host = a
    elif o in ("-p", "--port"):
      port = int(a)
    elif o in ("--iterations"):
      iterations = int(a)
    else:
      assert False, "unhandled option"

  root = logging.getLogger()
  root.setLevel(logging.ERROR)

  print("hostname", host, "port", port)

  cleanup()

  callback = Callbacks()
  callback2 = Callbacks()

  bclient = mqtt.client.Client("myclientid2".encode("utf-8"))
  bclient.registerCallback(callback2)

  tests = [basic_test, retained_message_test, offline_message_queueing_test, will_message_test,
           overlapping_subscriptions_test, keepalive_test, redelivery_on_reconnect_test]

  if run_zero_length_clientid_test:
    tests.append(zero_length_clientid_test)

  if run_subscribe_failure_test:
    tests.append(subscribe_failure_test)

  if dollar_topics_test:
    tests.append(dollar_topics_test)

  for i in range(iterations):
    print("test suite", "succeeded" if False not in [test() for test in tests] else "failed")















 







