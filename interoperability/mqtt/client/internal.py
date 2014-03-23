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


import time, sys, socket, traceback, logging

from ..formats import MQTTV311 as MQTTV3

class Receivers:

  def __init__(self, socket):
    logging.debug("initializing receiver")
    self.socket = socket
    self.stopping = False
    self.paused = False

    self.inMsgs = {}
    self.outMsgs = {}

    self.puback = MQTTV3.Pubacks()
    self.pubrec = MQTTV3.Pubrecs()
    self.pubrel = MQTTV3.Pubrels()
    self.pubcomp = MQTTV3.Pubcomps()
    self.running = False

  def receive(self, callback=None):
    packet = None
    try:
      packet = MQTTV3.unpackPacket(MQTTV3.getPacket(self.socket))
    except:
      if not self.stopping and sys.exc_info()[0] != socket.timeout:
        logging.error("receive: unexpected exception %s", str(sys.exc_info()))
        traceback.print_exc()
        raise 
    if packet == None:
      time.sleep(0.1)
      return
    logging.debug("in :%s", str(packet))

    if packet.fh.MessageType == MQTTV3.SUBACK:
      if hasattr(callback, "subscribed"):
        callback.subscribed(packet.messageIdentifier, packet.data)

    elif packet.fh.MessageType == MQTTV3.UNSUBACK:
      if hasattr(callback, "unsubscribed"):
        callback.unsubscribed(packet.messageIdentifier)

    elif packet.fh.MessageType == MQTTV3.PUBACK:
      "check if we are expecting a puback"
      if packet.messageIdentifier in self.outMsgs.keys() and \
        self.outMsgs[packet.messageIdentifier].fh.QoS == 1:
        del self.outMsgs[packet.messageIdentifier]
        if hasattr(callback, "published"):
          callback.published(packet.messageIdentifier)
      else:
        raise Exception("No QoS 1 with that message id sent")

    elif packet.fh.MessageType == MQTTV3.PUBREC:
      if packet.messageIdentifier in self.outMsgs.keys():
        self.outMsgs[packet.messageIdentifier].pubrec_received == True
        self.pubrel.messageIdentifier = packet.messageIdentifier
        logging.debug("out: %s", str(self.pubrel))
        self.socket.send(self.pubrel.pack())
      else:
        raise Exception("PUBREC received for unknown msg id "+ \
                    str(packet.messageIdentifier))

    elif packet.fh.MessageType == MQTTV3.PUBREL:
      "release QOS 2 publication to client, & send PUBCOMP"
      msgid = packet.messageIdentifier
      if packet.messageIdentifier not in self.inMsgs.keys():
        pass # what should we do here?
      else:
        pub = self.inMsgs[packet.messageIdentifier]
        if callback == None or \
           callback.publishArrived(pub.topicName, pub.data, 2,
                           pub.fh.RETAIN, pub.messageIdentifier):
          del self.inMsgs[packet.messageIdentifier]
          self.pubcomp.messageIdentifier = packet.messageIdentifier
          logging.debug("out: %s", str(self.pubcomp))
          self.socket.send(self.pubcomp.pack())
        if callback == None:
          return (pub.topicName, pub.data, 2,
                           pub.fh.RETAIN, pub.messageIdentifier)

    elif packet.fh.MessageType == MQTTV3.PUBCOMP:
      "finished with this message id"
      if packet.messageIdentifier in self.outMsgs.keys():
        del self.outMsgs[packet.messageIdentifier]
        if hasattr(callback, "published"):
          callback.published(packet.messageIdentifier)
      else:
        raise Exception("PUBCOMP received for unknown msg id "+ \
                    str(packet.messageIdentifier))

    elif packet.fh.MessageType == MQTTV3.PUBLISH:
      if self.paused:
        return
      if packet.fh.QoS == 0:
        if callback == None:
          return (packet.topicName, packet.data, 0,
                           packet.fh.RETAIN, packet.messageIdentifier)
        else:
          callback.publishArrived(packet.topicName, packet.data, 0,
                        packet.fh.RETAIN, packet.messageIdentifier)
      elif packet.fh.QoS == 1:
        if callback == None:
          return (packet.topicName, packet.data, 1,
                           packet.fh.RETAIN, packet.messageIdentifier)
        else:
          if callback.publishArrived(packet.topicName, packet.data, 1,
                           packet.fh.RETAIN, packet.messageIdentifier):
            self.puback.messageIdentifier = packet.messageIdentifier
            logging.debug("out: %s", str(self.puback))
            self.socket.send(self.puback.pack())
      elif packet.fh.QoS == 2:
        self.inMsgs[packet.messageIdentifier] = packet
        self.pubrec.messageIdentifier = packet.messageIdentifier
        logging.debug("out: %s", str(self.pubrec))
        self.socket.send(self.pubrec.pack())

    else:
      raise Exception("Unexpected packet"+str(packet))

  def resend(self):
    for packetid in self.outMsgs.keys():
      packet = self.outMsgs[packetid]
      if packet.fh.QoS == 2 and hasattr(packet, "pubrec_received"):
        self.pubrel.messageIdentifier = packet.messageIdentifier
        packet = self.pubrel
      else:
        packet.fh.DUP = True
      logging.debug("out: %s", str(packet))
      rc = self.socket.send(packet.pack())

  def __call__(self, callback):
    self.running = True
    try:
      while not self.stopping:
        self.receive(callback)
    except:
      if not self.stopping and sys.exc_info()[0] != socket.error:
        logging.error("call: unexpected exception %s", str(sys.exc_info()))
        traceback.print_exc()
    self.running = False

