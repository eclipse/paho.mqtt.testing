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

import time, logging

logger = logging.getLogger('MQTT broker')
 
class Subscriptions:

  def __init__(self, aClientid, aTopic, aQos):
    self.__clientid = aClientid
    self.__topic = aTopic
    self.__qos = aQos

  def getClientid(self):
    return self.__clientid

  def getTopic(self):
    return self.__topic

  def getQoS(self):
    return self.__qos

  def resubscribe(self, qos):
    logger.info("[MQTT-1.1.0-1] resubscription for client %s on topic %s", self.__clientid, self.__topic)
    logger.info("[MQTT-3.8.4-3] resubscription for client %s on topic %s", self.__clientid, self.__topic)
    self.__qos = qos

  def __repr__(self):
    return repr({"clientid":self.__clientid, "topic":self.__topic, "qos":self.__qos})


