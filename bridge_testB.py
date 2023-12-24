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
    opts, args = getopt.gnu_getopt(sys.argv[1:], "h:p:", 
      ["help", "hostname=", "port="])
  except getopt.GetoptError as err:
    print(err) # will print something like "option -a not recognized"
    usage()
    sys.exit(2)

  host = "localhost"
  port = 1883
  for o, a in opts:
    if o in ("--help"):
      usage()
      sys.exit()
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
  
  callback = Callbacks()

  bclient = mqtt.client.Client("Bridge_test_B".encode("utf-8"))
  bclient.registerCallback(callback)

  bclient.connect(host=host, port=port)

  bclient.subscribe(["bridged/k"], [2])

  count = 0
  max_count = 30
  while len(callback.messages) == 0:
    time.sleep(1)
    count += 1 
  time.sleep(2)

  assert len(callback.messages) == 5, callback.messages
  # check topic
  assert [m[0] for m in callback.messages] == ["bridged/k"]*5, [m[0] for m in callback.messages]
  # check payloads
  assert set([m[1] for m in callback.messages]) == set([b"1 test start", b"qos 0", b"qos 1", b"qos 2", b"1 test end"]), [m[1] for m in callback.messages]
  # check QoS
  assert set([m[2] for m in callback.messages]) == set([0, 0, 1, 2, 2]), [m[2] for m in callback.messages]

  bclient.disconnect()


  
 








 







