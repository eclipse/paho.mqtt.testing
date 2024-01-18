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

"""

Assertions are used to validate incoming data, but are omitted from outgoing packets.  This is
so that the tests that use this package can send invalid data for error testing.

"""

import logging

logger = logging.getLogger('MQTT-SN')

# Low-level protocol interface

class MQTTSNException(Exception):
  pass

MAX_PACKET_SIZE = 2**16-1
MAX_PACKETID = 2**16-1

class MessageTypes:

  indexes = [x for x in range(0, 30)] + [0XFE]

  # Packet types
  ADVERTISE, SEARCHGW, GWINFO, RESERVED3, CONNECT, CONNACK, \
  WILLTOPICREQ, WILLTOPIC, WILLMSGREQ, WILLMSG, \
  REGISTER, REGACK, PUBLISH, PUBACK, PUBCOMP, PUBREC, PUBREL, \
  RESERVED11, SUBSCRIBE, SUBACK, UNSUBSCRIBE, UNSUBACK, \
  PINGREQ, PINGRESP, DISCONNECT, RESERVED19, \
  WILLTOPICUPD, WILLTOPICRESP, \
  WILLMSGUPD, WILLMSGRESP, ENCAPSULATED_MESSAGE = indexes


def MessageType(buffer):
  index = 1
  if buffer[0] == 1:
    index = 3
  return buffer[index]


class Messages(object):

  Names = ["Advertise", "SearchGW", "GWInfo", "reserved", \
    "Connect", "Connack", \
    "WillTopicReq", "WillTopic", "WillMsgReq", "WillMsg", \
    "Register", "Regack", \
    "Publish", "Puback", "Pubcomp", "Pubrec", "Pubrel", \
    "reserved", "Subscribe", "Suback", "Unsubscribe", "Unsuback", \
    "Pingreq", "Pingresp", "Disconnect", "reserved",
    "WillTopicUpd", "WillTopicResp", "WillMsgUpd", "WillMsgResp"]

  classNames = [name+'es' if name == "Publish" else
                name+'s' if name != "reserved" else name for name in Names]

  def pack(self):
    buffer = self.fh.pack(0)
    return buffer

  def __str__(self):
    return str(self.fh)

  def __eq__(self, packet):
    return self.fh == packet.fh if packet else False

  def __setattr__(self, name, value):
    if name not in self.names:
      raise MQTTSNException(name + " Attribute name must be one of "+str(self.names))
    object.__setattr__(self, name, value)


def getPacket(aSocket):
  "receive the next packet"
  buf = aSocket.recv(1) # get the first byte fixed header
  if buf == b"":
    return None
  if str(aSocket).find("[closed]") != -1:
    closed = True
  else:
    closed = False
  if closed:
    return None
  # now get the remaining length - the length field is the length of the
  # entire packet including the length field itself
  if buf[0] == 1: # indicates following two bytes are the length
    buf += aSocket.recv(2)
    remlength = buf[1]*256 + buf[2] - 3
  else:
    remlength = buf[0] - 1
  # receive the remaining length if there is any
  rest = bytes([])
  if remlength > 0:
    while len(rest) < remlength:
      rest += aSocket.recv(remlength-len(rest))
  assert len(rest) == remlength
  return buf + rest

def writeInt16(length):
  return bytes([length // 256, length % 256])

def readInt16(buf):
  return buf[0]*256 + buf[1]

def writeData(data):
  # data could be a string, or bytes.  If string, encode into bytes with utf-8
  return data if type(data) == type(b"") else bytes(data, "utf-8")

class MessageLens:

  @staticmethod
  def encode(x):
    assert 2 <= x <= 65535
    buffer = b''
    if x < 256:
      buffer = bytes([x])
    else:
      buffer = bytes([1]) + writeInt16(x)
    return buffer

  @staticmethod
  def decode(buffer):
    if buffer[0] == 1:
      bytes = 3
      value = readInt16(buffer[1:])
    else:
      bytes = 1
      value = buffer[0]
    return (value, bytes)

class Flags:

  def __init__(self):
    self.DUP = False # 1 bit
    self.QoS = 0 # 2 bits
    self.RETAIN = False # 1 bit
    self.Will = False # 1 bit
    self.CleanSession = False # 1 bit
    self.TopicIdType = 0 # 2 bits

  def __eq__(self, flags):
    return self.DUP == flags.DUP and \
           self.QoS == flags.QoS and \
           self.RETAIN == flags.RETAIN and \
           self.Will == flags.Will and \
           self.CleanSession == flags.CleanSession and \
           self.TopicIdType == flags.TopicIdType

  def __setattr__(self, name, value):
    names = ["DUP", "QoS", "RETAIN", "Will", "CleanSession", "TopicIdType"]
    if name not in names:
      raise MQTTException(name + " Attribute name must be one of "+str(names))
    object.__setattr__(self, name, value)

  def __str__(self):
    return 'Flags(DUP='+str(self.DUP) + \
           ", QoS="+str(self.QoS) + \
           ", Retain="+str(self.RETAIN) + \
           ", Will="+str(self.Will) + \
           ", CleanSession="+str(self.CleanSession) + \
           ", TopicIdType="+str(self.TopicIdType) +")"

  def pack(self):
    "pack data into string buffer ready for transmission down socket"
    QoS = 3 if self.QoS == -1 else self.QoS
    buffer = bytes([(self.DUP << 7) | (QoS << 5) |
      (self.RETAIN << 4) | (self.Will << 3) | \
      (self.CleanSession << 2) | self.TopicIdType])
    return buffer

  def unpack(self, b0):
    "unpack data from string buffer into separate fields"
    self.DUP = ((b0 >> 7) & 0x01) == 1
    QoS = (b0 >> 5) & 0x03
    self.QoS = -1 if QoS == 3 else QoS
    self.RETAIN = ((b0 >> 4) & 0x01) == 1
    self.Will = ((b0 >> 3) & 0x01) == 1
    self.CleanSession = ((b0 >> 2) & 0x01) == 1
    self.TopicIdType = (b0 & 0x03)
    return 1 # length of flags


class Connects(Messages):

  def __init__(self, buffer=None):
    object.__setattr__(self, "names",
         ["messageType", "Flags", "ProtocolId", "Duration", "ClientId"])
    self.messageType = MessageTypes.CONNECT
    self.Flags = Flags()
    self.ProtocolId = 1 
    self.Duration = 0
    self.ClientId = ""
    if buffer != None:
      self.unpack(buffer)

  def pack(self):
    msglen = 6 + len(self.ClientId)
    buffer = bytes([msglen, MessageTypes.CONNECT]) + self.Flags.pack() +\
      bytes([self.ProtocolId]) + writeInt16(self.Duration) + writeData(self.ClientId)
    return buffer

  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert MessageType(buffer) == MessageTypes.CONNECT
    try:
      messagelen, lenlen = MessageLens.decode(buffer)
      self.Flags.unpack(buffer[lenlen + 1])
      self.ProtocolId = buffer[lenlen + 2]
      self.Duration = readInt16(buffer[lenlen + 3:])
      self.ClientId = buffer[lenlen + 5:].decode()
    except:
      logger.exception("Validating connect packet")
      raise

  def __str__(self):
    return "Connect (" + str(self.Flags)+", ProtocolId="+str(self.ProtocolId)+", Duration=" +\
          str(self.Duration)+", ClientId="+str(self.ClientId) + ")"

  def __eq__(self, packet):
    rc = self.Flags == packet.Flags and \
      self.ProtocolId == packet.ProtocolId and \
      self.Duration == packet.Duration and \
      self.ClientId == packet.ClientId
    return rc

class Connacks(Messages):

  def __init__(self, buffer=None):
    object.__setattr__(self, "names", ["messageType", "ReturnCode"])
    self.messageType = MessageTypes.CONNACK
    self.ReturnCode = 0
    if buffer != None:
      self.unpack(buffer)

  def pack(self):
    msglen = 3
    buffer = bytes([msglen, MessageTypes.CONNACK, self.ReturnCode])
    return buffer

  def unpack(self, buffer):
    assert len(buffer) >= 3
    assert MessageType(buffer) == MessageTypes.CONNACK
    try:
      messagelen, lenlen = MessageLens.decode(buffer)
      assert lenlen == 1
      assert messagelen == 3
      self.ReturnCode = buffer[2]
    except:
      logger.exception("Validating connack packet")
      raise

  def __str__(self):
    return "Connack (" + str(self.ReturnCode) + ")"

  def __eq__(self, packet):
    rc = self.ReturnCode == packet.ReturnCode
    return rc

class Publishes(Messages):

  def __init__(self, buffer=None):
    object.__setattr__(self, "names",
         ["Flags", "TopicId", "MsgId", "Data"])
    object.__setattr__(self, "messageType", MessageTypes.PUBLISH)
    self.Flags = Flags()
    self.TopicId = 1 
    self.MsgId = 0
    self.Data = b""
    if buffer != None:
      self.unpack(buffer)

  def pack(self):
    msglen = 6 + len(self.Data)
    buffer = bytes([msglen, self.messageType]) + self.Flags.pack() +\
      writeInt16(self.TopicId) + writeInt16(self.MsgId) + writeData(self.Data)
    return buffer

  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert MessageType(buffer) == MessageTypes.PUBLISH
    try:
      messagelen, lenlen = MessageLens.decode(buffer)
      self.Flags.unpack(buffer[lenlen + 1])
      self.TopicId = readInt16(buffer[lenlen + 2:])
      self.MsgId = readInt16(buffer[lenlen + 4:])
      self.Data = buffer[lenlen + 6:]
    except:
      logger.exception("Validating publish packet")
      raise

  def __str__(self):
    return "Publish (" + str(self.Flags)+", TopicId="+str(self.TopicId)+", MsgId=" +\
          str(self.MsgId)+", Data="+str(self.Data) + ")"

  def __eq__(self, packet):
    rc = self.Flags == packet.Flags and \
      self.TopicId == packet.TopicId and \
      self.MsgId == packet.MsgId and \
      self.Data == packet.Data
    return rc

WillTopicReqs = WillTopics = WillMsgReqs = WillMsgs = Registers = Regacks = Pubacks = None

classes = [None, None, None, None, Connects, Connacks, 
           WillTopicReqs, WillTopics, WillMsgReqs, WillMsgs,
           Registers, Regacks, Publishes, Pubacks]

def unpackPacket(buffer, maximumPacketSize=MAX_PACKET_SIZE):
  if MessageType(buffer) != None:
    packet = classes[MessageType(buffer)]()
    packet.unpack(buffer) #, maximumPacketSize=maximumPacketSize)
  else:
    packet = None
  return packet
