# MQTTSubscriptionEngines - produced from XML source


#if not rule.properties.has_key("OVERLAPPING_QOS"):
#  rule.properties["OVERLAPPING_QOS"] = "LATEST"

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
 
   def choose(self, new, old):
     rc = False
     if old == None:
       rc = True
     elif rule.properties["OVERLAPPING_QOS"] in ["ANY", "MULTIPLE"]:
       rc = "DON'T_KNOW"
     elif rule.properties["OVERLAPPING_QOS"] == "MOST_SPECIFIC":
       new_start = len(re.split("[+|#]", new.getTopic())[0])
       old_start = len(re.split("[+|#]", old.getTopic())[0])
       if new_start == old_start:
         if new.getTopic()[new_start] != old.getTopic()[old_start]:
           rc = (new_start == '+')
         else:
           rc = "DON'T_KNOW"
       else:
         rc = new_start > old_start
     else:
       # MicroBroker V1 uses the latest matching subscription.
       rc = new.getTimestamp() > old.getTimestamp()
     return rc

   def qosOf(self, clientid, topic):
     """ if there are overlapping subscriptions, which one do we choose?
     """
     rc = []
     chosen = None
     subs = SubscriptionEngines.subscriptions(self, clientid)
     for s in subs:
       if topics.topicMatches(s.getTopic(), topic):
         ch = self.choose(s, chosen)
         if ch == True:
           rc = [s.getQoS()]
           chosen = s
         elif ch == "DON'T_KNOW":
           rc.append(s.getQoS())
           chosen = s
     rc.sort()
     return rc
 
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
 
