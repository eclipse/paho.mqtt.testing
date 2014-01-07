# SubscriptionEngines model - produced from XML source

import types

from . import topics, Subscriptions

from .Subscriptions import *
 
class SubscriptionEngines:

   def __init__(self):
     self.__subscriptions = [] # list of subscriptions

   def subscribe(self, aClientid, topic, subsClass=Subscriptions):
     if type(topic) == type([]):
       rc = []
       for t in topic:
         rc.append(self.__subscribe(aClientid, t, subsClass))
     else:
       rc = self.__subscribe(aClientid, topic, subsClass)
     return rc

   def __subscribe(self, aClientid, aTopic, subsClass):
     "subscribe to one topic"
     rc = None
     assert topics.isValidTopicName(aTopic)
     resubscribed = False
     for s in self.__subscriptions:
       if s.getClientid() == aClientid and s.getTopic() == aTopic:
         s.resubscribe()
         return s
     rc = subsClass(aClientid, aTopic)
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
     if aTopic == '#':
       self.clearSubscriptions(aClientid)
     else:
       for s in self.__subscriptions:
         if s.getClientid() == aClientid and s.getTopic() == aTopic:
           self.__subscriptions.remove(s)
           return

   def clearSubscriptions(self, aClientid):
     for s in self.__subscriptions[:]:
       if s.getClientid() == aClientid:
         self.__subscriptions.remove(s)

   def subscriptions(self, aClientid=None):
     "return a list of subscriptions for this client"
     if aClientid == None:
       rc = self.__subscriptions
     else:
       rc = filter(lambda s: s.getClientid() == aClientid, \
            self.__subscriptions)
     return rc

   def subscribers(self, aTopic):
     "list all clients subscribed to this (non-wildcard) topic"
     result = []
     for s in self.__subscriptions:
       if topics.topicMatches(s.getTopic(), aTopic):
         if s.getClientid() not in result: # don't add a client id twice
             result.append(s.getClientid())
     return result
 
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
 
