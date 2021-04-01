"""
*******************************************************************
  Copyright (c) 2013, 2018 IBM Corp.

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

import types, time, logging, random

from . import Topics
from .SubscriptionEngines import SubscriptionEngines
from mqtt.formats.MQTTV5 import ProtocolError

logger = logging.getLogger('MQTT broker')

class Brokers:

  def __init__(self, overlapping_single=True, topicAliasMaximum=0, sharedData={}):
    self.sharedData = sharedData
    self.se = SubscriptionEngines(self.sharedData)
    self.__clients = {} # clientid -> client
    self.overlapping_single = overlapping_single
    self.topicAliasMaximum = topicAliasMaximum
    self.__broker3 = None
    self.willMessageClients = set() # set of clients for which will delay calculations are needed

  def setBroker3(self, broker3):
    self.__broker3 = broker3

  def reinitialize(self):
    self.__clients = {}
    self.se.reinitialize()

  def getClients(self):
    return self.__clients
  
  def getClient(self, clientid):
    return self.__clients[clientid] if (clientid in self.__clients.keys()) else None

  def cleanSession(self, aClientid):
    "clear any outstanding subscriptions and publications"
    if len(self.se.getRetainedTopics("#")) > 0:
      logger.info("[MQTT-3.1.2-7] retained messages not cleaned up as part of session state for client %s", aClientid)
    self.se.clearSubscriptions(aClientid)

  def connect(self, aClient, clean=False):
    aClient.connected = True
    try:
      aClient.timestamp = time.clock() # time.clock is deprecated
    except:
      aClient.timestamp = time.process_time()
    self.__clients[aClient.id] = aClient
    if clean:
      self.cleanSession(aClient.id)

  def sendWillMessage(self, aClientid):
    "Sends the will message, if any, for a client"
    self.__clients[aClientid].delayedWillTime = None
    if aClientid in self.willMessageClients:
      self.willMessageClients.remove(aClientid)
    logger.info("[MQTT5-3.1.2-8] sending will message for client %s", aClientid)
    willtopic, willQoS, willmsg, willRetain, willProperties = self.__clients[aClientid].will
    if willRetain:
      logger.info("[MQTT5-3.1.2-15] sending will message retained for client %s", aClientid)
    else:
      logger.info("[MQTT5-3.1.2-14] sending will message non-retained for client %s", aClientid)
    self.publish(aClientid, willtopic, willmsg, willQoS, willRetain, willProperties, time.monotonic())
    logger.info("[MQTT5-3.1.2-10] will message is deleted after use or disconnect, for client %s", aClientid)
    logger.info("[MQTT-3.14.4-3] on receipt of disconnect, will message is deleted")
    self.__clients[aClientid].will = None

  def setupWillMessage(self, aClientid):
    "Sends the will message, if any, for a client"
    if aClientid in self.__clients.keys() and self.__clients[aClientid].connected:
      if self.__clients[aClientid].will != None:
        if self.__clients[aClientid].willDelayInterval > 0:
          self.__clients[aClientid].delayedWillTime = time.monotonic() + self.__clients[aClientid].willDelayInterval
          self.__clients[aClientid].willDelayInterval = 0 # will be changed on next connect
          self.willMessageClients.add(aClientid)
        else:
          self.sendWillMessage(aClientid)

  def disconnect(self, aClientid, willMessage=False, sessionExpiryInterval=-1):
    if willMessage:
      self.setupWillMessage(aClientid)
    if aClientid in self.__clients.keys():
      self.__clients[aClientid].connected = False
      if sessionExpiryInterval == 0:
        self.cleanSession(aClientid)
        del self.__clients[aClientid]
      else:
        logger.info("[MQTT5-3.1.2-23] broker must store the session data for client %s", aClientid)
        self.__clients[aClientid].sessionEndedTime = time.monotonic()
        self.__clients[aClientid].connected = False

  def disconnectAll(self):
    for c in self.__clients.keys()[:]: # copy the array because disconnect will remove an element
      self.disconnect(c)

  def getAliasTopic(self, aClientid, topicAlias):
    mytopic = None
    if topicAlias > 0 and topicAlias in self.__clients[aClientid].topicAliasToNames.keys():
      mytopic = self.__clients[aClientid].topicAliasToNames[topicAlias]  
    else:
      raise ProtocolError("Topic alias invalid", topicAlias)
    return mytopic

  def publish(self, aClientid, topic, message, qos, retained, properties, receivedTime):
    """publish to all subscribed connected clients
       also to any disconnected non-cleanstart clients with qos in [1,2]
    """

    def publishAction(options, subsprops, subsids=None):
      if hasattr(properties, "SubscriptionIdentifier"):
        delattr(properties, "SubscriptionIdentifier")
      if subsids or hasattr(subsprops, "SubscriptionIdentifier"):
        if subsids:
          for subsid in subsids:
            properties.SubscriptionIdentifier = subsid
        else:
          properties.SubscriptionIdentifier = subsprops.SubscriptionIdentifier[0]
      out_qos = min(options.QoS, qos)
      outretain = retained if options.retainAsPublished else False
      self.__clients[subscriber].publishArrived(topic, message, out_qos, properties, receivedTime, outretain)

    # topic alias
    if hasattr(properties, "TopicAlias"):
      if properties.TopicAlias == 0:
        raise ProtocolError("Topic alias invalid", properties.TopicAlias)
      if len(topic) == 0:
        topic = self.getAliasTopic(aClientid, properties.TopicAlias)
      else: # set incoming topic alias
        if properties.TopicAlias in self.__clients[aClientid].topicAliasToNames.keys() or \
            properties.TopicAlias <= self.topicAliasMaximum:
          self.__clients[aClientid].topicAliasToNames[properties.TopicAlias] = topic
        else:
          raise ProtocolError("Topic alias invalid", self.__clients[aClientid].topicAliasMaximum)
    assert len(topic) > 0

    if retained:
      logger.info("[MQTT-2.1.2-6] store retained message and QoS")
      self.se.setRetained(topic, message, qos, receivedTime, properties)
    else:
      logger.info("[MQTT-2.1.2-12] non-retained message - do not store")

    subscriptions = self.se.subscriptions(topic)
    # For shared subscriptions, there is only one recipient
    nonshared = [s for s in list(subscriptions) if not s.getTopic().startswith('$share/')]
    shared = [s for s in list(subscriptions) if s.getTopic().startswith('$share/')]
    subscriptions = []
    clientids = set()
    for n in nonshared:
      if n.getClientid() not in clientids:
        clientids.add(n.getClientid())
        subscriptions.append(n)
    sharenames = set()  # set of shared topic names
    for s in shared:
      sharenames.add(s.getTopic())
    for sname in list(sharenames):
      subscriptions.append(random.choice([s for s in shared if s.getTopic() == sname]))
    
    subscribed_clients = [s.getClientid() for s in subscriptions]
    for subscriber in subscribed_clients:  # all subscribed clients
      # qos is lower of publication and subscription
      overlapping = False
      subscriptions = self.se.getSubscriptions(topic, subscriber)
      if len(subscriptions) > 1:
        logger.info("[MQTT-3.3.5-1] overlapping subscriptions")
        overlapping = True
      if retained:
        logger.info("[MQTT-2.1.2-10] outgoing publish does not have retained flag set")
      if self.overlapping_single:
        if subscriber in self.__clients.keys():
          options, subsprops = self.se.optionsOf(subscriber, topic)
          # any other subscription ids?
          subsids = []
          nolocalfilter = []
          if overlapping:
            for subscription in subscriptions:
              subopts, subprops = subscription.getOptions()
              if not subopts.noLocal or subscriber != aClientid: # noLocal
                nolocalfilter.append(subscription)
                if hasattr(subprops, "SubscriptionIdentifier"):
                  subsids += subprops.SubscriptionIdentifier
          else:
            if not options.noLocal or subscriber != aClientid: # noLocal
              nolocalfilter = subscriptions
          if len(nolocalfilter) > 0:
            publishAction(options, subsprops, subsids=subsids)
        else:
          # MQTT V3 subscription
          out_qos = min(self.__broker3.se.qosOf(subscriber, topic), qos)
          self.__broker3.getClient(subscriber).publishArrived(topic, message, out_qos)
      else:
        for subscription in subscriptions:
          if subscriber in self.__clients.keys():
            options, subsprops = subscription.getOptions()
            if not options.noLocal or subscriber != aClientid: # noLocal
              publishAction(options, subsprops)
          else:
            # MQTT V3 subscription
            out_qos = min(self.__broker3.se.qosOf(subscriber, topic), qos)
            self.__broker3.getClient(subscriber).publishArrived(topic, message, out_qos)
    return subscribed_clients if len(subscribed_clients) > 0 else None

  def __doRetained__(self, aClientid, topic, subsoptions, resubscribeds):
    # topic can be single, or a list
    if type(topic) != type([]):
      topic = [topic]
      suboptions = [subsoptions]
    i = 0
    for t in topic: # t is a wildcard subscription topic
      if subsoptions[i].retainHandling == 2 or \
        (subsoptions[i].retainHandling == 1 and resubscribeds[i]):
        i += 1
        continue
      topicsUsed = []
      for s in self.se.getRetainedTopics(t): # s is a non-wildcard retained topic
        if s not in topicsUsed and Topics.topicMatches(t, s):
          # topic has retained publication
          topicsUsed.append(s)
          retained_message = self.se.getRetained(s)
          if len(retained_message) == 3:
            (ret_msg, ret_qos, receivedTime) = retained_message
            properties = None
          else:
            (ret_msg, ret_qos, receivedTime, properties) = retained_message
          thisqos = min(ret_qos, subsoptions[i].QoS)
          self.__clients[aClientid].publishArrived(s, ret_msg, thisqos, properties, receivedTime, True)
      i += 1

  def subscribe(self, aClientid, topic, optionsprops):
    rc = self.se.subscribe(aClientid, topic, optionsprops)
    resubscribeds = [resubscribed for (x, resubscribed) in rc]
    self.__doRetained__(aClientid, topic, [options for (options, props) in optionsprops], resubscribeds)
    return rc

  def unsubscribe(self, aClientid, topic):
    return self.se.unsubscribe(aClientid, topic)

  def getSubscriptions(self, aClientid=None):
    return self.se.getSubscriptions(aClientid)

def unit_tests():
  bn = Brokers()

  class Clients:

    def __init__(self, anId):
     self.id = anId # required
     self.msgqueue = []
     self.cleanstart = True

    def publishArrived(self, topic, msg, qos, retained=False):
      "required by broker node class"
      logger.debug(self.id, "publishArrived", repr((topic, msg, qos, retained)))
      self.msgqueue.append((topic, msg, qos))

  Client1 = Clients("Client1")
  Client1.cleanstart = False

  bn.connect(Client1)
  bn.subscribe(Client1.id, "topic1", 1)
  bn.publish(Client1.id, "topic1", "message 1", 1)

  assert Client1.msgqueue.pop(0) == ("topic1", "message 1", 1)

  bn.publish(Client1.id, "topic2", "message 2", 1, retained=True)
  bn.subscribe(Client1.id, "topic2", 2)

  assert Client1.msgqueue.pop(0) == ("topic2", "message 2", 1)

  bn.subscribe(Client1.id, "#", 2)
  assert Client1.msgqueue.pop(0) == ("topic2", "message 2", 1)

  bn.subscribe(Client1.id, "#", 0)
  assert Client1.msgqueue.pop(0) == ("topic2", "message 2", 0)
  bn.unsubscribe(Client1.id, "#")

  bn.publish(Client1.id, "topic2/next", "message 3", 2, retained=True)
  bn.publish(Client1.id, "topic2/blah", "message 4", 1, retained=True)
  bn.subscribe(Client1.id, "topic2/+", 2)
  logger.debug(Client1.msgqueue)
  msg1 = Client1.msgqueue.pop(0)
  msg2 = Client1.msgqueue.pop(0)

  assert (msg1 == ("topic2/next", "message 3", 2) and \
          msg2 == ("topic2/blah", "message 4", 1)) or \
         (msg2 == ("topic2/next", "message 3", 2) and \
          msg1 == ("topic2/blah", "message 4", 1))

  assert Client1.msgqueue == []

  bn.disconnect(Client1.id)

  Client2 = Clients("Client2")
  Client2.cleanstart = False
  bn.connect(Client2)
  bn.publish(Client2.id, "topic2/a", "queued message 0", 0)
  bn.publish(Client2.id, "topic2/a", "queued message 1", 1)
  bn.publish(Client2.id, "topic2/a", "queued message 2", 2)

  bn.connect(Client1)
  Client1.cleanstart = False
  print(Client1.msgqueue)
  assert Client1.msgqueue.pop(0) == ("topic2/a", "queued message 0", 0)
  assert Client1.msgqueue.pop(0) == ("topic2/a", "queued message 1", 1)
  assert Client1.msgqueue.pop(0) == ("topic2/a", "queued message 2", 2)

  bn.disconnect(Client1.id)
  bn.disconnect(Client2.id)
