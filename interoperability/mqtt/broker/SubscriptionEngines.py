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

import types, logging

from . import Topics, Subscriptions

from .Subscriptions import *
 
class SubscriptionEngines:

   def __init__(self):
     self.__subscriptions = [] # list of subscriptions
     self.__retained = {}      # map of topics to retained msg+qos

   def subscribe(self, aClientid, topic, qos):
     if type(topic) == type([]):
       rc = []
       count = 0
       for aTopic in topic:
         rc.append(self.__subscribe(aClientid, aTopic, qos[count]))
         count += 1
     else:
       rc = self.__subscribe(aClientid, topic, qos)
     return rc

   def __subscribe(self, aClientid, aTopic, aQos):
     "subscribe to one topic"
     rc = None
     assert Topics.isValidTopicName(aTopic)
     resubscribed = False
     for s in self.__subscriptions:
       if s.getClientid() == aClientid and s.getTopic() == aTopic:
         s.resubscribe(aQos)
         return s
     rc = Subscriptions(aClientid, aTopic, aQos)
     self.__subscriptions.append(rc)
     return rc

   def unsubscribe(self, aClientid, aTopic):
     if type(aTopic) == type([]):
       for t in aTopic:
         self.__unsubscribe(aClientid, t)
     else:
       self.__unsubscribe(aClientid, aTopic)

   def __unsubscribe(self, aClientid, aTopic):
     "unsubscribe to one topic"
     for s in self.__subscriptions:
       if s.getClientid() == aClientid and s.getTopic() == aTopic:
         self.__subscriptions.remove(s)
         break # once we've hit one, that's us done

   def clearSubscriptions(self, aClientid):
     for s in self.__subscriptions[:]:
       if s.getClientid() == aClientid:
         self.__subscriptions.remove(s)

   def subscriptions(self, aClientid=None):
     "return a list of subscriptions for this client"
     if aClientid == None:
       rc = self.__subscriptions
     else:
       rc = [s for s in self.__subscriptions if s.getClientid() == aClientid]
     return rc

   def qosOf(self, clientid, topic):
     # if there are overlapping subscriptions, choose maximum QoS
     chosen = None
     for sub in self.subscriptions(clientid):
       if Topics.topicMatches(sub.getTopic(), topic):
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

   def subscribers(self, aTopic):
     "list all clients subscribed to this (non-wildcard) topic"
     result = []
     for s in self.__subscriptions:
       if Topics.topicMatches(s.getTopic(), aTopic):
         if s.getClientid() not in result: # don't add a client id twice
             result.append(s.getClientid())
     return result

   def setRetained(self, aTopic, aMessage, aQoS):
     "set a retained message on a non-wildcard topic"
     if len(aMessage) == 0:
       if aTopic in self.__retained.keys():
         logging.info("[MQTT-2.1.1-11] Deleting retained message")
         del self.__retained[aTopic]
     else:
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


if __name__ == "__main__":
  se = SubscriptionEngines()
  se.subscribe("Client1", ["topic1", "topic2"])
  assert se.subscribers("topic1") == ["Client1"]
  se.subscribe("Client2", ["topic2", "topic3"])
  assert se.subscribers("topic1") == ["Client1"]
  assert se.subscribers("topic2") == ["Client1", "Client2"]
  se.subscribe("Client2", ["#"])
  assert se.subscribers("topic1") == ["Client1", "Client2"]
  assert se.subscribers("topic2") == ["Client1", "Client2"]
  assert se.subscribers("topic3") == ["Client2"]
  assert setEquals(map(lambda s:s.getTopic(), se.subscriptions("Client2")),
                       ["#", "topic2", "topic3"])
  logging.info("Before clear:", se.subscriptions("Client2"))
  se.clearSubscriptions("Client2")
  assert se.subscriptions("Client2") == []
  assert se.subscriptions("Client1") != []
  logging.info("After clear, client1:", se.subscriptions("Client1"))
  logging.info("After clear, client2:", se.subscriptions("Client2"))
 
