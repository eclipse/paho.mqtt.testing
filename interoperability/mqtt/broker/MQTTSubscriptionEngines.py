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

import types, time, re

from . import topics
from .Subscriptions import Subscriptions
from .SubscriptionEngines import SubscriptionEngines


class MQTTSubscriptions(Subscriptions):

  def __init__(self, aClientid, aTopic):
    Subscriptions.__init__(self, aClientid, aTopic)
    self.__qos = None

  def setQoS(self, qos):
    assert qos in [0,1,2]
    self.__qos = qos

  def getQoS(self):
    return self.__qos

  def __repr__(self):
    return repr((self.getClientid(), self.getTopic(), self.getTimestamp(), self.__qos))

  def get(self):
    return self.getClientid(), self.getTopic(), self.getTimestamp(), self.__qos

  def __getitem__(self, index):
    return [self.getClientid(), self.getTopic(), self.getTimestamp(), self.__qos][index]
 
class MQTTSubscriptionEngines(SubscriptionEngines):

   def __init__(self):
     SubscriptionEngines.__init__(self)
     self.__retained = {}          # map of topics to retained msg+qos

   def subscribe(self, aClientid, topic, qos):
     "set the qos on each subscription too"
     subs = SubscriptionEngines.subscribe(self, aClientid, topic, MQTTSubscriptions)
     rc = []
     if type(qos) == type([]):
       assert len(topic) == len(qos)
       i = 0
       for s in subs:
         s.setQoS(qos[i])
         rc.append(qos[i])
         i += 1
     else:
       subs.setQoS(qos)
       rc.append(qos)
     return rc

   def setRetained(self, aTopic, aMessage, aQoS):
     "set a retained message on a non-wildcard topic"
     self.__retained[aTopic] = (aMessage, aQoS)

   def retained(self, topic):
     "returns (msg, QoS) for a topic"
     if topic in self.__retained.keys():
       result = self.__retained[topic]
     else:
       result = None
     return result

   def retainedTopics(self):
     "returns a list of topics for which retained publications exist"
     return self.__retained.keys()
 
   def qosOf(self, clientid, topic):
     # if there are overlapping subscriptions, choose maximum QoS
     chosen = None
     for sub in SubscriptionEngines.subscriptions(self, clientid):
       if topics.topicMatches(sub.getTopic(), topic):
         if chosen == None:
           chosen = sub.getQoS()
         else:
           logging.info("[MQTT-3.3.5-1] Overlapping subscriptions max QoS")
           if sub.getQoS() > chosen:
             chosen = sub.getQoS()
         # Omit the following optimization because we want to check for condition [MQTT-3.3.5-1]
         #if chosen == 2:
         #  break
     return chosen
 
if __name__ == "__main__":
  se = MQTTSubscriptionEngines()
  se.subscribe("Client1", ["topic1", "topic2"], [1, 2])
  assert se.subscribers("topic1") == ["Client1"]
  assert se.qosOf("Client1", "topic1") == [1]
  assert se.qosOf("Client1", "topic2") == [2]
  se.subscribe("Client1", ["#"], [0])
  if rule.properties["OVERLAPPING_QOS"] == "MOST_SPECIFIC":
    assert se.qosOf("Client1", "topic1") == [1]
    assert se.qosOf("Client1", "topic2") == [2]
  else:
    assert se.qosOf("Client1", "topic1") == [0]
    assert se.qosOf("Client1", "topic2") == [0]
  logging.info("All tests passed", se.subscriptions())
 
