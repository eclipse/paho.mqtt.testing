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
 
class Subscriptions:

  def __init__(self, aClientid, aTopic):
    self.__clientid = aClientid
    self.__topic = aTopic
    self.__timestamp = time.clock()

  def getClientid(self):
    return self.__clientid

  def getTopic(self):
    return self.__topic

  def resubscribe(self):
    logging.info("[MQTT-1.1.0-1] resubscription for client %s on topic %s", self.__clientid, self.__topic)
    self.__timestamp = time.clock()

  def getTimestamp(self):
    return self.__timestamp

  def __repr__(self):
    return repr((self.__clientid, self.__topic, self.__timestamp))
 
