"""
*******************************************************************
  Copyright (c) 2013, 2017 IBM Corp.

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

from mqtt.formats import MQTTV5

logger = logging.getLogger("mqtt-client")
logger.propagate = 1

class Receivers:

  def __init__(self, socket):
    logger.debug("initializing receiver")
    self.socket = socket
    self.stopping = False
    self.paused = False

    self.inMsgs = {}
    self.outMsgs = {}

    self.puback = MQTTV5.Pubacks()
    self.pubrec = MQTTV5.Pubrecs()
    self.pubrel = MQTTV5.Pubrels()
    self.pubcomp = MQTTV5.Pubcomps()
    self.running = False

  def receive(self, callback=None):
    packet = None
    try:
      packet = MQTTV5.unpackPacket(MQTTV5.getPacket(self.socket))
    except:
      if not self.stopping and sys.exc_info()[0] != socket.timeout:
        logger.info("receive: unexpected exception %s", str(sys.exc_info()))
        #traceback.print_exc()
        raise
    if packet == None:
      time.sleep(0.1)
      return
    logger.debug("in :%s", str(packet))

    if packet.fh.PacketType == MQTTV5.PacketTypes.SUBACK:
      if hasattr(callback, "subscribed"):
        callback.subscribed(packet.packetIdentifier, packet.reasonCodes)

    elif packet.fh.PacketType == MQTTV5.PacketTypes.DISCONNECT:
      if hasattr(callback, "disconnected"):
        callback.disconnected(packet.reasonCode, packet.properties)

    elif packet.fh.PacketType == MQTTV5.PacketTypes.UNSUBACK:
      if hasattr(callback, "unsubscribed"):
        callback.unsubscribed(packet.packetIdentifier)

    elif packet.fh.PacketType == MQTTV5.PacketTypes.PUBACK:
      "check if we are expecting a puback"
      if packet.packetIdentifier in self.outMsgs.keys() and \
        self.outMsgs[packet.packetIdentifier].fh.QoS == 1:
        del self.outMsgs[packet.packetIdentifier]
        if hasattr(callback, "published"):
          callback.published(packet.packetIdentifier)
      else:
        raise Exception("No QoS 1 with that message id sent")

    elif packet.fh.PacketType == MQTTV5.PacketTypes.PUBREC:
      if packet.packetIdentifier in self.outMsgs.keys():
        #self.outMsgs[packet.packetIdentifier].pubrec_received = True
        self.pubrel.packetIdentifier = packet.packetIdentifier
        logger.debug("out: %s", str(self.pubrel))
        self.socket.send(self.pubrel.pack())
      else:
        raise Exception("PUBREC received for unknown msg id "+ \
                    str(packet.packetIdentifier))

    elif packet.fh.PacketType == MQTTV5.PacketTypes.PUBREL:
      "release QOS 2 publication to client, & send PUBCOMP"
      msgid = packet.packetIdentifier
      if packet.packetIdentifier not in self.inMsgs.keys():
        pass # what should we do here?
      else:
        pub = self.inMsgs[packet.packetIdentifier]
        if callback == None or \
           callback.publishArrived(pub.topicName, pub.data, 2,
                           pub.fh.RETAIN, pub.packetIdentifier, pub.properties):
          del self.inMsgs[packet.packetIdentifier]
          self.pubcomp.packetIdentifier = packet.packetIdentifier
          logger.debug("out: %s", str(self.pubcomp))
          self.socket.send(self.pubcomp.pack())
        if callback == None:
          return (pub.topicName, pub.data, 2,
                           pub.fh.RETAIN, pub.packetIdentifier)

    elif packet.fh.PacketType == MQTTV5.PacketTypes.PUBCOMP:
      "finished with this message id"
      if packet.packetIdentifier in self.outMsgs.keys():
        del self.outMsgs[packet.packetIdentifier]
        if hasattr(callback, "published"):
          callback.published(packet.packetIdentifier)
      else:
        raise Exception("PUBCOMP received for unknown msg id "+ \
                    str(packet.packetIdentifier))

    elif packet.fh.PacketType == MQTTV5.PacketTypes.PUBLISH:
      if self.paused:
        return
      if packet.fh.QoS == 0:
        if callback == None:
          return (packet.topicName, packet.data, 0,
                           packet.fh.RETAIN, packet.packetIdentifier)
        else:
          callback.publishArrived(packet.topicName, packet.data, 0,
                        packet.fh.RETAIN, packet.packetIdentifier, packet.properties)
      elif packet.fh.QoS == 1:
        if callback == None:
          return (packet.topicName, packet.data, 1,
                           packet.fh.RETAIN, packet.packetIdentifier)
        else:
          if callback.publishArrived(packet.topicName, packet.data, 1,
                           packet.fh.RETAIN, packet.packetIdentifier, packet.properties):
            self.puback.packetIdentifier = packet.packetIdentifier
            logger.debug("out: %s", str(self.puback))
            self.socket.send(self.puback.pack())
      elif packet.fh.QoS == 2:
        self.inMsgs[packet.packetIdentifier] = packet
        self.pubrec.packetIdentifier = packet.packetIdentifier
        logger.debug("out: %s", str(self.pubrec))
        self.socket.send(self.pubrec.pack())

    else:
      raise Exception("Unexpected packet"+str(packet))

  def resend(self):
    for packetid in self.outMsgs.keys():
      packet = self.outMsgs[packetid]
      if packet.fh.QoS == 2 and hasattr(packet, "pubrec_received"):
        self.pubrel.packetIdentifier = packet.packetIdentifier
        packet = self.pubrel
      else:
        packet.fh.DUP = True
      logger.debug("out: %s", str(packet))
      rc = self.socket.send(packet.pack())

  def __call__(self, callback):
    self.running = True
    try:
      while not self.stopping:
        self.receive(callback)
    except:
      if not self.stopping and sys.exc_info()[0] != socket.error:
        logger.error("call: unexpected exception %s", str(sys.exc_info()))
        traceback.print_exc()
    self.running = False
