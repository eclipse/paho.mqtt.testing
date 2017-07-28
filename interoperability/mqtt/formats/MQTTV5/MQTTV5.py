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
     Ian Craggs - take MQTT 3.1.1 and create MQTT 5.0 version
*******************************************************************
"""

"""

Assertions are used to validate incoming data, but are omitted from outgoing packets.  This is
so that the tests that use this package can send invalid data for error testing.

"""

import logging, struct

logger = logging.getLogger('MQTTV5')

# Low-level protocol interface

class MQTTException(Exception):
  pass


class PacketTypes:

  indexes = range(1, 16)

  # Packet types
  CONNECT, CONNACK, PUBLISH, PUBACK, PUBREC, PUBREL, \
  PUBCOMP, SUBSCRIBE, SUBACK, UNSUBSCRIBE, UNSUBACK, \
  PINGREQ, PINGRESP, DISCONNECT, AUTH = indexes


class Packets:

  Names = [ "reserved", \
    "Connect", "Connack", "Publish", "Puback", "Pubrec", "Pubrel", \
    "Pubcomp", "Subscribe", "Suback", "Unsubscribe", "Unsuback", \
    "Pingreq", "Pingresp", "Disconnect", "Auth"]

  classNames = [name+'es' if name == "Publish" else
                name+'s' if name != "reserved" else name for name in Names]

  def pack(self):
    buffer = self.fh.pack(0)
    return buffer

  def __repr__(self):
    return repr(self.fh)

  def __eq__(self, packet):
    return self.fh == packet.fh if packet else False


def PacketType(byte):
  """
    Retrieve the message type from the first byte of the fixed header.
  """
  if byte != None:
    rc = byte[0] >> 4
  else:
    rc = None
  return rc

class reasonCodes:
  """
    The reason code used in MQTT V5.0

  """

  def getName(self, identifier, packetType):
    """
    used when displaying the reason code
    """
    assert identifier in self.names.keys()
    names = self.names[identifier]
    namelist = [name for name in names.keys() if packetType in names[name]]
    assert len(namelist) == 1
    return namelist[0]

  def getId(self, name):
    """
    used when setting the reason code for a packetType
    check that only valid codes for the packet are set
    """
    identifier = None
    for code in self.names.keys():
      if name in self.names[code]:
        identifier = code
        break
    assert identifier != None
    return identifier

  def __init__(self):
    self.names = {
    0 : { "Success" : [PacketTypes.CONNECT, PacketTypes.PUBACK,
        PacketTypes.PUBREC, PacketTypes.PUBREL, PacketTypes.PUBCOMP,
        PacketTypes.UNSUBACK, PacketTypes.AUTH],
          "Normal disconnection" : [PacketTypes.DISCONNECT],
          "Granted QoS 0" : [PacketTypes.SUBACK] },
    1 : { "Granted QoS 1" : [PacketTypes.SUBACK] },
    2 : { "Granted QoS 2" : [PacketTypes.SUBACK] },
    4 : { "Disconnect with will message" : [PacketTypes.DISCONNECT] },
    16 : { "No matching subscribers" :
      [PacketTypes.PUBACK, PacketTypes.PUBREC] },
    17 : { "No subscription existed" : [PacketTypes.UNSUBACK] },
    24 : { "Continue authentication" : [PacketTypes.AUTH] },
    25 : { "Re-authenticate" : [PacketTypes.AUTH] },
    128 : { "Unspecified error" : [PacketTypes.CONNACK, PacketTypes.PUBACK,
      PacketTypes.PUBREC, PacketTypes.SUBACK, PacketTypes.UNSUBACK,
      PacketTypes.DISCONNECT], },
    129 : { "Malformed packet" :
          [PacketTypes.CONNACK, PacketTypes.DISCONNECT] },
    130 : { "Protocol error" :
          [PacketTypes.CONNACK, PacketTypes.DISCONNECT] },
    131 : { "Implementation specific error": [PacketTypes.CONNACK,
          PacketTypes.PUBACK, PacketTypes.PUBREC, PacketTypes.SUBACK,
          PacketTypes.UNSUBACK, PacketTypes.DISCONNECT], },
    132 : { "Unsupported protocol version" : [PacketTypes.CONNACK] },
    133 : { "Client identifier not valid" : [PacketTypes.CONNACK] },
    134 : { "Bad user name or password" : [PacketTypes.CONNACK] },
    135 : { "Not authorized" : [PacketTypes.CONNACK, PacketTypes.PUBACK,
              PacketTypes.PUBREC, PacketTypes.SUBACK, PacketTypes.UNSUBACK,
              PacketTypes.DISCONNECT], },
    136 : { "Server unavailable" : [PacketTypes.CONNACK] },
    137 : { "Server busy" : [PacketTypes.CONNACK, PacketTypes.DISCONNECT] },
    138 : { "Banned" : [PacketTypes.CONNACK] },
    139 : { "Server shutting down" : [PacketTypes.DISCONNECT] },
    140 : { "Bad authentication method" :
            [PacketTypes.CONNACK, PacketTypes.DISCONNECT] },
    141 : { "Keep alive timeout" : [PacketTypes.DISCONNECT] },
    142 : { "Session taken over" : [PacketTypes.DISCONNECT] },
    143 : { "Topic filter invalid" :
            [PacketTypes.SUBACK, PacketTypes.UNSUBACK, PacketTypes.DISCONNECT]},
    144 : { "Topic name invalid" :
            [PacketTypes.CONNACK, PacketTypes.PUBACK,
            PacketTypes.PUBREC, PacketTypes.DISCONNECT]},
    145 : { "Packet identifier in use" :
            [PacketTypes.PUBACK, PacketTypes.PUBREC,
             PacketTypes.SUBACK, PacketTypes.UNSUBACK]},
    146 : { "Packet identifier not found" :
            [PacketTypes.PUBREL, PacketTypes.PUBCOMP] },
    147 : { "Receive maximum exceeded": [PacketTypes.DISCONNECT] },
    148 : { "Topic aliad invalid": [PacketTypes.DISCONNECT] },
    149 : { "Packet too large": [PacketTypes.CONNACK, PacketTypes.DISCONNECT] },
    150 : { "Message rate too high": [PacketTypes.DISCONNECT] },
    151 : { "Quota exceeded": [PacketTypes.CONNACK, PacketTypes.PUBACK,
          PacketTypes.PUBREC, PacketTypes.SUBACK, PacketTypes.DISCONNECT], },
    152 : { "Administrative action" : [PacketTypes.DISCONNECT] },
    153 : { "Payload format invalid" :
            [PacketTypes.PUBACK, PacketTypes.PUBREC, PacketTypes.DISCONNECT]},
    154 : { "Retain not supported" :
            [PacketTypes.CONNACK, PacketTypes.DISCONNECT] },
    155 : { "QoS not supported" :
            [PacketTypes.CONNACK, PacketTypes.DISCONNECT] },
    156 : { "Use another server" :
            [PacketTypes.CONNACK, PacketTypes.DISCONNECT] },
    157 : { "Server moved" :
            [PacketTypes.CONNACK, PacketTypes.DISCONNECT] },
    158 : { "Shared subscription not supported" :
            [PacketTypes.SUBACK, PacketTypes.DISCONNECT] },
    159 : { "Connection rate exceeded" :
            [PacketTypes.CONNACK, PacketTypes.DISCONNECT] },
    160 : { "Maximum connect time" :
            [PacketTypes.DISCONNECT] },
    161 : { "Subscription identifiers not supported" :
            [PacketTypes.SUBACK, PacketTypes.DISCONNECT] },
    162 : { "Wildcard subscription not supported" :
            [PacketTypes.SUBACK, PacketTypes.DISCONNECT] },
    }


class MBIs:

  @staticmethod
  def encode(x):
    """
      Convert an integer 0 <= x <= 268435455 into multi-byte format.
      Returns the buffer convered from the integer.
    """
    assert 0 <= x <= 268435455
    buffer = b''
    while 1:
      digit = x % 128
      x //= 128
      if x > 0:
        digit |= 0x80
      buffer += bytes([digit])
      if x == 0:
        break
    return buffer

  @staticmethod
  def decode(buffer):
    """
      Get the value of a multi-byte integer from a buffer
      Return the value, and the number of bytes used.
    """
    multiplier = 1
    value = 0
    bytes = 0
    while 1:
      bytes += 1
      digit = buffer[0]
      buffer = buffer[1:]
      value += (digit & 127) * multiplier
      if digit & 128 == 0:
        break
      multiplier *= 128
    return (value, bytes)

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
  # now get the remaining length
  multiplier = 1
  remlength = 0
  while 1:
    next = aSocket.recv(1)
    while len(next) == 0:
      next = aSocket.recv(1)
    buf += next
    digit = buf[-1]
    remlength += (digit & 127) * multiplier
    if digit & 128 == 0:
      break
    multiplier *= 128
  # receive the remaining length if there is any
  rest = bytes([])
  if remlength > 0:
    while len(rest) < remlength:
      rest += aSocket.recv(remlength-len(rest))
  assert len(rest) == remlength
  return buf + rest


class FixedHeaders(object):

  def __init__(self, aPacketType):
    self.PacketType = aPacketType
    self.DUP = False
    self.QoS = 0
    self.RETAIN = False
    self.remainingLength = 0

  def __eq__(self, fh):
    return self.PacketType == fh.PacketType and \
           self.DUP == fh.DUP and \
           self.QoS == fh.QoS and \
           self.RETAIN == fh.RETAIN # and \
           # self.remainingLength == fh.remainingLength

  def __setattr__(self, name, value):
    names = ["PacketType", "DUP", "QoS", "RETAIN", "remainingLength"]
    if name not in names:
      raise MQTTException(name + " Attribute name must be one of "+str(names))
    object.__setattr__(self, name, value)

  def __repr__(self):
    "return printable representation of our data"
    return Packets.classNames[self.PacketType]+'(fh.DUP='+repr(self.DUP)+ \
           ", fh.QoS="+repr(self.QoS)+", fh.RETAIN="+repr(self.RETAIN)

  def pack(self, length):
    "pack data into string buffer ready for transmission down socket"
    buffer = bytes([(self.PacketType << 4) | (self.DUP << 3) |\
                         (self.QoS << 1) | self.RETAIN])
    self.remainingLength = length
    buffer += MBIs.encode(length)
    return buffer

  def unpack(self, buffer):
    "unpack data from string buffer into separate fields"
    b0 = buffer[0]
    self.PacketType = b0 >> 4
    self.DUP = ((b0 >> 3) & 0x01) == 1
    self.QoS = (b0 >> 1) & 0x03
    self.RETAIN = (b0 & 0x01) == 1
    (self.remainingLength, bytes) = MBIs.decode(buffer[1:])
    return bytes + 1 # length of fixed header


def writeInt16(length):
  return bytes([length // 256, length % 256])

def readInt16(buf):
  return buf[0]*256 + buf[1]

def writeUTF(data):
  # data could be a string, or bytes.  If string, encode into bytes with utf-8
  return writeInt16(len(data)) + (data if type(data) == type(b"") else bytes(data, "utf-8"))

def readUTF(buffer, maxlen):
  if maxlen >= 2:
    length = readInt16(buffer)
  else:
    raise MQTTException("Not enough data to read string length")
  maxlen -= 2
  if length > maxlen:
    raise MQTTException("Length delimited string too long")
  buf = buffer[2:2+length].decode("utf-8")
  logger.info("[MQTT-4.7.3-2] topic names and filters not include null")
  zz = buf.find("\x00") # look for null in the UTF string
  if zz != -1:
    raise MQTTException("[MQTT-1.5.3-2] Null found in UTF data "+buf)
  for c in range (0xD800, 0xDFFF):
    zz = buf.find(chr(c)) # look for D800-DFFF in the UTF string
    if zz != -1:
      raise MQTTException("[MQTT-1.5.3-1] D800-DFFF found in UTF data "+buf)
  if buf.find("\uFEFF") != -1:
    logger.info("[MQTT-1.5.3-3] U+FEFF in UTF string")
  return buf, length+2

def writeBytes(buffer):
  return writeInt16(len(buffer)) + buffer

def readBytes(buffer):
  length = readInt16(buffer)
  return buffer[2:2+length], length+2


class Properties(object):

  def __init__(self, packetType):
    self.packetType = packetType
    self.types = ["Byte", "Two Byte Integer", "Four Byte Integer", "Variable Byte Integer",
       "Binary Data", "UTF-8 Encoded String", "UTF-8 String Pair"]

    self.names = {
      "Payload Format Indicator" : 1,
      "Publication Expiry Interval" : 2,
      "Content Type" : 3,
      "Response Topic" : 8,
      "Correlation Data" : 9,
      "Subscription Identifier" : 11,
      "Session Expiry Interval" : 17,
      "Assigned Client Identifier" : 18,
      "Server Keep Alive" : 19,
      "Authentication Method" : 21,
      "Authentication Data" : 22,
      "Request Problem Information" : 23,
      "Will Delay Interval" : 24,
      "Request Response Information" : 25,
      "Response Information" : 26,
      "Server Reference" : 28,
      "Reason String" : 31,
      "Receive Maximum" : 33,
      "Topic Alias Maximum" : 34,
      "Topic Alias" : 35,
      "Maximum QoS" : 36,
      "Retain Available" : 37,
      "User Property" : 38,
      "Maximum Packet Size" : 39,
      "Wildcard Subscription Available" : 40,
      "Subscription Identifier Available" : 41,
      "Shared Subscription Available" : 42
    }

    self.properties = {
    # id:  type, packets
      1  : (self.types.index("Byte"), [PacketTypes.PUBLISH]), # payload format indicator
      2  : (self.types.index("Four Byte Integer"), [PacketTypes.PUBLISH]),
      3  : (self.types.index("UTF-8 Encoded String"), [PacketTypes.PUBLISH]),
      8  : (self.types.index("UTF-8 Encoded String"), [PacketTypes.PUBLISH]),
      9  : (self.types.index("Binary Data"), [PacketTypes.PUBLISH]),
      11 : (self.types.index("Variable Byte Integer"),
           [PacketTypes.PUBLISH, PacketTypes.SUBSCRIBE]),
      17 : (self.types.index("Four Byte Integer"),
           [PacketTypes.CONNECT, PacketTypes.DISCONNECT]),
      18 : (self.types.index("UTF-8 Encoded String"), [PacketTypes.CONNACK]),
      19 : (self.types.index("Two Byte Integer"), [PacketTypes.CONNACK]),
      21 : (self.types.index("UTF-8 Encoded String"),
           [PacketTypes.CONNECT, PacketTypes.CONNACK, PacketTypes.AUTH]),
      22 : (self.types.index("Binary Data"),
           [PacketTypes.CONNECT, PacketTypes.CONNACK, PacketTypes.AUTH]),
      23 : (self.types.index("Byte"),
           [PacketTypes.CONNECT]),
      24 : (self.types.index("Four Byte Integer"), [PacketTypes.CONNECT]),
      25 : (self.types.index("Byte"), [PacketTypes.CONNECT]),
      26 : (self.types.index("UTF-8 Encoded String"), [PacketTypes.CONNACK]),
      28 : (self.types.index("UTF-8 Encoded String"),
           [PacketTypes.CONNACK, PacketTypes.DISCONNECT]),
      31 : (self.types.index("UTF-8 Encoded String"),
           [PacketTypes.CONNACK, PacketTypes.PUBACK, PacketTypes.PUBREC,
            PacketTypes.PUBREL, PacketTypes.PUBCOMP, PacketTypes.SUBACK,
            PacketTypes.UNSUBACK, PacketTypes.DISCONNECT, PacketTypes.AUTH]),
      33 : (self.types.index("Two Byte Integer"),
           [PacketTypes.CONNECT, PacketTypes.CONNACK]),
      34 : (self.types.index("Two Byte Integer"),
           [PacketTypes.CONNECT, PacketTypes.CONNACK]),
      35 : (self.types.index("Two Byte Integer"), [PacketTypes.PUBLISH]),
      36 : (self.types.index("Byte"), [PacketTypes.CONNACK]),
      37 : (self.types.index("Byte"), [PacketTypes.CONNACK]),
      38 : (self.types.index("UTF-8 String Pair"),
           [PacketTypes.CONNECT, PacketTypes.CONNACK, PacketTypes.PUBLISH,
           PacketTypes.PUBACK, PacketTypes.PUBREC, PacketTypes.PUBREL,
           PacketTypes.PUBCOMP, PacketTypes.SUBACK, PacketTypes.UNSUBACK,
           PacketTypes.DISCONNECT, PacketTypes.AUTH]),
      39 : (self.types.index("Four Byte Integer"),
           [PacketTypes.CONNECT, PacketTypes.CONNACK]),
      40 : (self.types.index("Byte"), [PacketTypes.CONNACK]),
      41 : (self.types.index("Byte"), [PacketTypes.CONNACK]),
      42 : (self.types.index("Byte"), [PacketTypes.CONNACK]),
    }

  def getIdentFromName(self, compressedName):
    result = -1
    for name in self.names.keys():
      if compressedName == name.replace(' ', ''):
         result = self.names[name]
         break
    return result

  def __setattr__(self, name, value):
    name = name.replace(' ', '')
    privateVars = ["packetType", "types", "names", "properties"]
    if name in privateVars:
      object.__setattr__(self, name, value)
    else:
      # the name could have spaces in, or not.  Remove spaces before assignment
      if name not in [name.replace(' ', '') for name in self.names.keys()]:
        raise MQTTException("Attribute name must be one of "+str(names.keys()))
      # check that this attribute applies to the packet type
      if self.packetType not in self.properties[self.getIdentFromName(name)][1]:
        raise MQTTException("Attribute %s does not apply to packet type %s"
            % (name, Packets.Names[self.packetType]) )
      object.__setattr__(self, name, value)

  def writeProperty(self, identifier, type, value):
    buffer = b""
    buffer += MBIs.encode(identifier) # identifier
    if type == self.types.index("Byte"): # value
      buffer += bytes([value])
    elif type == self.types.index("Two Byte Integer"):
      buffer += writeInt16(value)
    elif type == self.types.index("Four Byte Integer"):
      buffer += writeInt32(value)
    elif type == self.types.index("Variable Byte Integer"):
      buffer += MBIs.encode(value)
    elif type == self.types.index("Binary Data"):
      buffer += writeBytes(value)
    elif type == self.types.index("UTF-8 Encoded String"):
      buffer += writeUTF(value)
    elif type == self.types.index("UTF-8 String Pair"):
      buffer += writeUTF(value[0]) + writeUTF(value[1])
    return buffer

  def pack(self):
    # serialize properties into buffer for sending over network
    buffer = b""
    for name in self.names.keys():
      compressedName = name.replace(' ', '')
      if hasattr(self, compressedName):
        identifier = self.getIdentFromName(compressedName)
        attr_type = self.properties[identifier][0]
        buffer += self.writeProperty(identifier, attr_type,
                           getattr(self, compressedName))
    return MBIs.encode(len(buffer)) + buffer

  def readProperty(self, buffer, type, propslen):
    if type == self.types.index("Byte"):
      value = buffer[0]
      valuelen = 1
    elif type == self.types.index("Two Byte Integer"):
      value = readInt16(buffer)
      valuelen = 2
    elif type == self.types.index("Four Byte Integer"):
      value = readInt32(buffer)
      valuelen = 4
    elif type == self.types.index("Variable Byte Integer"):
      value, valuelen = MBIs.decode(buffer)
    elif type == self.types.index("Binary Data"):
      value, valuelen = readBytes(buffer)
    elif type == self.types.index("UTF-8 Encoded String"):
      value, valuelen = readUTF(buffer, propslen)
    elif type == self.types.index("UTF-8 String Pair"):
      value, valuelen = readUTF(buffer, propslen)
      buffer = buffer[valuelen:] # strip the bytes used by the value
      value1, valuelen1 = readUTF(buffer, propslen - valuelen)
      value += value1
      valuelen += valuelen1
    return value, valuelen

  def unpack(self, buffer):
    # deserialize properties into attributes from buffer received from network
    propslen, MBIlen = MBIs.decode(buffer)
    buffer = buffer[MBIlen:] # strip the bytes used by the MBI
    while propslen > 0: # properties length is 0 if there are none
      identifier, MBIlen = MBIs.decode(buffer) # property identifier
      buffer = buffer[MBIlen:] # strip the bytes used by the MBI
      propslen -= MBIlen
      attr_type = self.properties[identifier][0]
      value, valuelen = self.readProperty(buffer, attr_type, propslen)
      buffer = buffer[valuelen:] # strip the bytes used by the value
      propslen -= valuelen
    return self, propslen + MBIlen


class ConnectFlags:

  def __init__(self):
    pass


class Connects(Packets):

  def __init__(self, buffer = None):
    self.fh = FixedHeaders(PacketTypes.CONNECT)

    # variable header
    self.ProtocolName = "MQTT"
    self.ProtocolVersion = 5
    self.CleanSession = True
    self.WillFlag = False
    self.WillQoS = 0
    self.WillRETAIN = 0
    self.KeepAliveTimer = 30
    self.usernameFlag = False
    self.passwordFlag = False

    self.properties = Properties(PacketTypes.CONNECT)

    # Payload
    self.ClientIdentifier = ""   # UTF-8
    self.WillTopic = None        # UTF-8
    self.WillMessage = None      # binary
    self.username = None         # UTF-8
    self.password = None         # binary

    #self.properties = Properties()
    if buffer != None:
      self.unpack(buffer)

  def __setattr__(self, name, value):
    names = ["fh", "properties", "ProtocolName", "ProtocolVersion",
                  "ClientIdentifier", "CleanSession", "KeepAliveTimer",
                  "WillFlag", "WillQoS", "WillRETAIN", "WillTopic", "WillMessage",
                  "usernameFlag", "passwordFlag", "username", "password"]
    if name not in names:
      raise MQTTException(name + " Attribute name must be one of "+str(names))
    Packets.__setattr__(self, name, value)

  def pack(self):
    connectFlags = bytes([(self.CleanSession << 1) | (self.WillFlag << 2) | \
                       (self.WillQoS << 3) | (self.WillRETAIN << 5) | \
                       (self.usernameFlag << 6) | (self.passwordFlag << 7)])
    buffer = writeUTF(self.ProtocolName) + bytes([self.ProtocolVersion]) + \
              connectFlags + writeInt16(self.KeepAliveTimer)
    tempbuffer = self.properties.pack()
    buffer += tempbuffer
    buffer += writeUTF(self.ClientIdentifier)
    if self.WillFlag:
      buffer += writeUTF(self.WillTopic)
      buffer += writeBytes(self.WillMessage)
    if self.usernameFlag:
      buffer += writeUTF(self.username)
    if self.passwordFlag:
      buffer += writeBytes(self.password)
    buffer = self.fh.pack(len(buffer)) + buffer
    return buffer

  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert PacketType(buffer) == PacketTypes.CONNECT

    try:
      fhlen = self.fh.unpack(buffer)
      packlen = fhlen + self.fh.remainingLength
      assert len(buffer) >= packlen, "buffer length %d packet length %d" % (len(buffer), packlen)
      curlen = fhlen # points to after header + remaining length
      assert self.fh.DUP == False, "[MQTT-2.1.2-1]"
      assert self.fh.QoS == 0, "[MQTT-2.1.2-1] QoS was not 0, was %d" % self.fh.QoS
      assert self.fh.RETAIN == False, "[MQTT-2.1.2-1]"

      self.ProtocolName, valuelen = readUTF(buffer[curlen:], packlen - curlen)
      curlen += valuelen
      assert self.ProtocolName == "MQTT", "Wrong protocol name %s" % self.ProtocolName

      self.ProtocolVersion = buffer[curlen]
      curlen += 1

      connectFlags = buffer[curlen]
      assert (connectFlags & 0x01) == 0, "[MQTT-3.1.2-3] reserved connect flag must be 0"
      self.CleanSession = ((connectFlags >> 1) & 0x01) == 1
      self.WillFlag = ((connectFlags >> 2) & 0x01) == 1
      self.WillQoS = (connectFlags >> 3) & 0x03
      self.WillRETAIN = (connectFlags >> 5) & 0x01
      self.passwordFlag = ((connectFlags >> 6) & 0x01) == 1
      self.usernameFlag = ((connectFlags >> 7) & 0x01) == 1
      curlen +=1

      if self.WillFlag:
        assert self.WillQoS in [0, 1, 2], "[MQTT-3.1.2-12] will qos must not be 3"
      else:
        assert self.WillQoS == 0, "[MQTT-3.1.2-11] will qos must be 0, if will flag is false"
        assert self.WillRETAIN == False, "[MQTT-3.1.2-13] will retain must be false, if will flag is false"

      self.KeepAliveTimer = readInt16(buffer[curlen:])
      curlen += 2

      curlen += self.properties.unpack(buffer[curlen:])[1]

      logger.info("[MQTT-3.1.3-3] Clientid must be present, and first field")
      logger.info("[MQTT-3.1.3-4] Clientid must be Unicode, and between 0 and 65535 bytes long")
      self.ClientIdentifier, valuelen = readUTF(buffer[curlen:], packlen - curlen)
      curlen += valuelen

      if self.WillFlag:
        self.WillTopic, valuelen = readUTF(buffer[curlen:], packlen - curlen)
        curlen += valuelen
        self.WillMessage, valuelen = readBytes(buffer[curlen:])
        curlen += valuelen
        logger.info("[[MQTT-3.1.2-9] will topic and will message fields must be present")
      else:
        self.WillTopic = self.WillMessage = None

      if self.usernameFlag:
        assert len(buffer) > curlen+2, "Buffer too short to read username length"
        self.username, valuelen = readUTF(buffer[curlen:], packlen - curlen)
        curlen += valuelen
        logger.info("[MQTT-3.1.2-19] username must be in payload if user name flag is 1")
      else:
        logger.info("[MQTT-3.1.2-18] username must not be in payload if user name flag is 0")
        assert self.passwordFlag == False, "[MQTT-3.1.2-22] password flag must be 0 if username flag is 0"

      if self.passwordFlag:
        assert len(buffer) > curlen+2, "Buffer too short to read password length"
        self.password, valuelen = readBytes(buffer[curlen:])
        curlen += valuelen
        logger.info("[MQTT-3.1.2-21] password must be in payload if password flag is 0")
      else:
        logger.info("[MQTT-3.1.2-20] password must not be in payload if password flag is 0")

      if self.WillFlag and self.usernameFlag and self.passwordFlag:
        logger.info("[MQTT-3.1.3-1] clientid, will topic, will message, username and password all present")

      assert curlen == packlen, "Packet is wrong length curlen %d != packlen %d"
    except:
      logger.exception("[MQTT-3.1.4-1] server must validate connect packet and close connection without connack if it does not conform")
      raise

  def __str__(self):
    buf = repr(self.fh)+", ProtocolName="+str(self.ProtocolName)+", ProtocolVersion=" +\
          repr(self.ProtocolVersion)+", CleanSession="+repr(self.CleanSession) +\
          ", WillFlag="+repr(self.WillFlag)+", KeepAliveTimer=" +\
          repr(self.KeepAliveTimer)+", ClientId="+str(self.ClientIdentifier) +\
          ", usernameFlag="+repr(self.usernameFlag)+", passwordFlag="+repr(self.passwordFlag)
    if self.WillFlag:
      buf += ", WillQoS=" + repr(self.WillQoS) +\
             ", WillRETAIN=" + repr(self.WillRETAIN) +\
             ", WillTopic='"+ self.WillTopic +\
             "', WillMessage='"+str(self.WillMessage)+"'"
    if self.username:
      buf += ", username="+self.username
    if self.password:
      buf += ", password="+str(self.password)
    return buf+")"

  def __eq__(self, packet):
    rc = Packets.__eq__(self, packet) and \
           self.ProtocolName == packet.ProtocolName and \
           self.ProtocolVersion == packet.ProtocolVersion and \
           self.CleanSession == packet.CleanSession and \
           self.WillFlag == packet.WillFlag and \
           self.KeepAliveTimer == packet.KeepAliveTimer and \
           self.ClientIdentifier == packet.ClientIdentifier and \
           self.WillFlag == packet.WillFlag
    if rc and self.WillFlag:
      rc = self.WillQoS == packet.WillQoS and \
           self.WillRETAIN == packet.WillRETAIN and \
           self.WillTopic == packet.WillTopic and \
           self.WillMessage == packet.WillMessage
    return rc


class Connacks(Packets):

  def __init__(self, buffer=None, DUP=False, QoS=0, RETAIN=False, ReasonCode=0):
    self.fh = FixedHeaders(PacketTypes.CONNACK)
    self.fh.DUP = DUP
    self.fh.QoS = QoS
    self.fh.RETAIN = RETAIN
    self.flags = 0
    self.reasonCode = ReasonCode
    if buffer != None:
      self.unpack(buffer)

  def pack(self):
    buffer = bytes([self.flags, self.reasonCode])
    buffer = self.fh.pack(len(buffer)) + buffer
    return buffer

  def unpack(self, buffer):
    assert len(buffer) >= 4
    assert PacketType(buffer) == PacketTypes.CONNACK
    self.fh.unpack(buffer)
    assert self.fh.remainingLength == 2, "Connack packet is wrong length %d" % self.fh.remainingLength
    assert buffer[2] in  [0, 1], "Connect Acknowledge Flags"
    self.reasonCode = buffer[3]
    assert self.fh.DUP == False, "[MQTT-2.1.2-1]"
    assert self.fh.QoS == 0, "[MQTT-2.1.2-1]"
    assert self.fh.RETAIN == False, "[MQTT-2.1.2-1]"

  def __repr__(self):
    return repr(self.fh)+", Session present="+str((self.flags & 0x01) == 1)+", ReturnCode="+repr(self.reasonCode)+")"

  def __eq__(self, packet):
    return Packets.__eq__(self, packet) and \
           self.reasonCode == packet.reasonCode


class Disconnects(Packets):

  def __init__(self, buffer=None, DUP=False, QoS=0, RETAIN=False):
    self.fh = FixedHeaders(PacketTypes.DISCONNECT)
    self.fh.DUP = DUP
    self.fh.QoS = QoS
    self.fh.RETAIN = RETAIN
    if buffer != None:
      self.unpack(buffer)

  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert PacketType(buffer) == PacketTypes.DISCONNECT
    self.fh.unpack(buffer)
    assert self.fh.remainingLength == 0, "Disconnect packet is wrong length %d" % self.fh.remainingLength
    logger.info("[MQTT-3.14.1-1] disconnect reserved bits must be 0")
    assert self.fh.DUP == False, "[MQTT-2.1.2-1]"
    assert self.fh.QoS == 0, "[MQTT-2.1.2-1]"
    assert self.fh.RETAIN == False, "[MQTT-2.1.2-1]"

  def __repr__(self):
    return repr(self.fh)+")"


class Publishes(Packets):

  def __init__(self, buffer=None, DUP=False, QoS=0, RETAIN=False, MsgId=1, TopicName="", Payload=b""):
    self.fh = FixedHeaders(PacketTypes.PUBLISH)
    self.fh.DUP = DUP
    self.fh.QoS = QoS
    self.fh.RETAIN = RETAIN
    # variable header
    self.topicName = TopicName
    self.messageIdentifier = MsgId
    # payload
    self.data = Payload
    if buffer != None:
      self.unpack(buffer)

  def pack(self):
    buffer = writeUTF(self.topicName)
    if self.fh.QoS != 0:
      buffer +=  writeInt16(self.messageIdentifier)
    buffer += self.data
    buffer = self.fh.pack(len(buffer)) + buffer
    return buffer

  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert PacketType(buffer) == PacketTypes.PUBLISH
    fhlen = self.fh.unpack(buffer)
    assert self.fh.QoS in [0, 1, 2], "QoS in Publish must be 0, 1, or 2"
    packlen = fhlen + self.fh.remainingLength
    assert len(buffer) >= packlen
    curlen = fhlen
    try:
      self.topicName, valuelen = readUTF(buffer[fhlen:], packlen - curlen)
    except UnicodeDecodeError:
      logger.info("[MQTT-3.3.2-1] topic name in publish must be utf-8")
      raise
    curlen += valuelen
    if self.fh.QoS != 0:
      self.messageIdentifier = readInt16(buffer[curlen:])
      logger.info("[MQTT-2.3.1-1] packet indentifier must be in publish if QoS is 1 or 2")
      curlen += 2
      assert self.messageIdentifier > 0, "[MQTT-2.3.1-1] packet indentifier must be > 0"
    else:
      logger.info("[MQTT-2.3.1-5] no packet indentifier in publish if QoS is 0")
      self.messageIdentifier = 0
    self.data = buffer[curlen:fhlen + self.fh.remainingLength]
    if self.fh.QoS == 0:
      assert self.fh.DUP == False, "[MQTT-2.1.2-4]"
    return fhlen + self.fh.remainingLength

  def __repr__(self):
    rc = repr(self.fh)
    if self.fh.QoS != 0:
      rc += ", MsgId="+repr(self.messageIdentifier)
    rc += ", TopicName="+repr(self.topicName)+", Payload="+repr(self.data)+")"
    return rc

  def __eq__(self, packet):
    rc = Packets.__eq__(self, packet) and \
         self.topicName == packet.topicName and \
         self.data == packet.data
    if rc and self.fh.QoS != 0:
      rc = self.messageIdentifier == packet.messageIdentifier
    return rc


class Pubacks(Packets):

  def __init__(self, buffer=None, DUP=False, QoS=0, RETAIN=False, MsgId=1):
    self.fh = FixedHeaders(PacketTypes.PUBACK)
    self.fh.DUP = DUP
    self.fh.QoS = QoS
    self.fh.RETAIN = RETAIN
    # variable header
    self.messageIdentifier = MsgId
    if buffer != None:
      self.unpack(buffer)

  def pack(self):
    buffer = writeInt16(self.messageIdentifier)
    buffer = self.fh.pack(len(buffer)) + buffer
    return buffer

  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert PacketType(buffer) == PacketTypes.PUBACK
    fhlen = self.fh.unpack(buffer)
    assert self.fh.remainingLength == 2, "Puback packet is wrong length %d" % self.fh.remainingLength
    assert len(buffer) >= fhlen + self.fh.remainingLength
    self.messageIdentifier = readInt16(buffer[fhlen:])
    assert self.fh.DUP == False, "[MQTT-2.1.2-1] Puback reserved bits must be 0"
    assert self.fh.QoS == 0, "[MQTT-2.1.2-1] Puback reserved bits must be 0"
    assert self.fh.RETAIN == False, "[MQTT-2.1.2-1] Puback reserved bits must be 0"
    return fhlen + 2

  def __repr__(self):
    return repr(self.fh)+", MsgId "+repr(self.messageIdentifier)

  def __eq__(self, packet):
    return Packets.__eq__(self, packet) and \
           self.messageIdentifier == packet.messageIdentifier


class Pubrecs(Packets):

  def __init__(self, buffer=None, DUP=False, QoS=0, RETAIN=False, MsgId=1):
    self.fh = FixedHeaders(PacketTypes.PUBREC)
    self.fh.DUP = DUP
    self.fh.QoS = QoS
    self.fh.RETAIN = RETAIN
    # variable header
    self.messageIdentifier = MsgId
    if buffer != None:
      self.unpack(buffer)

  def pack(self):
    buffer = writeInt16(self.messageIdentifier)
    buffer = self.fh.pack(len(buffer)) + buffer
    return buffer

  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert PacketType(buffer) == PacketTypes.PUBREC
    fhlen = self.fh.unpack(buffer)
    assert self.fh.remainingLength == 2, "Pubrec packet is wrong length %d" % self.fh.remainingLength
    assert len(buffer) >= fhlen + self.fh.remainingLength
    self.messageIdentifier = readInt16(buffer[fhlen:])
    assert self.fh.DUP == False, "[MQTT-2.1.2-1] Pubrec reserved bits must be 0"
    assert self.fh.QoS == 0, "[MQTT-2.1.2-1] Pubrec reserved bits must be 0"
    assert self.fh.RETAIN == False, "[MQTT-2.1.2-1] Pubrec reserved bits must be 0"
    return fhlen + 2

  def __repr__(self):
    return repr(self.fh)+", MsgId="+repr(self.messageIdentifier)+")"

  def __eq__(self, packet):
    return Packets.__eq__(self, packet) and \
           self.messageIdentifier == packet.messageIdentifier


class Pubrels(Packets):

  def __init__(self, buffer=None, DUP=False, QoS=1, RETAIN=False, MsgId=1):
    self.fh = FixedHeaders(PacketTypes.PUBREL)
    self.fh.DUP = DUP
    self.fh.QoS = QoS
    self.fh.RETAIN = RETAIN
    # variable header
    self.messageIdentifier = MsgId
    if buffer != None:
      self.unpack(buffer)

  def pack(self):
    buffer = writeInt16(self.messageIdentifier)
    buffer = self.fh.pack(len(buffer)) + buffer
    return buffer

  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert PacketType(buffer) == PacketTypes.PUBREL
    fhlen = self.fh.unpack(buffer)
    assert self.fh.remainingLength == 2, "Pubrel packet is wrong length %d" % self.fh.remainingLength
    assert len(buffer) >= fhlen + self.fh.remainingLength
    self.messageIdentifier = readInt16(buffer[fhlen:])
    assert self.fh.DUP == False, "[MQTT-2.1.2-1] DUP should be False in PUBREL"
    assert self.fh.QoS == 1, "[MQTT-2.1.2-1] QoS should be 1 in PUBREL"
    assert self.fh.RETAIN == False, "[MQTT-2.1.2-1] RETAIN should be False in PUBREL"
    logger.info("[MQTT-3.6.1-1] bits in fixed header for pubrel are ok")
    return fhlen + 2

  def __repr__(self):
    return repr(self.fh)+", MsgId="+repr(self.messageIdentifier)+")"

  def __eq__(self, packet):
    return Packets.__eq__(self, packet) and \
           self.messageIdentifier == packet.messageIdentifier


class Pubcomps(Packets):

  def __init__(self, buffer=None, DUP=False, QoS=0, RETAIN=False, MsgId=1):
    self.fh = FixedHeaders(PacketTypes.PUBCOMP)
    self.fh.DUP = DUP
    self.fh.QoS = QoS
    self.fh.RETAIN = RETAIN
    # variable header
    self.messageIdentifier = MsgId
    if buffer != None:
      self.unpack(buffer)

  def pack(self):
    buffer = writeInt16(self.messageIdentifier)
    buffer = self.fh.pack(len(buffer)) + buffer
    return buffer

  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert PacketType(buffer) == PacketTypes.PUBCOMP
    fhlen = self.fh.unpack(buffer)
    assert len(buffer) >= fhlen + self.fh.remainingLength
    assert self.fh.remainingLength == 2, "Pubcomp packet is wrong length %d" % self.fh.remainingLength
    self.messageIdentifier = readInt16(buffer[fhlen:])
    assert self.fh.DUP == False, "[MQTT-2.1.2-1] DUP should be False in Pubcomp"
    assert self.fh.QoS == 0, "[MQTT-2.1.2-1] QoS should be 0 in Pubcomp"
    assert self.fh.RETAIN == False, "[MQTT-2.1.2-1] Retain should be false in Pubcomp"
    return fhlen + 2

  def __repr__(self):
    return repr(self.fh)+", MsgId="+repr(self.messageIdentifier)+")"

  def __eq__(self, packet):
    return Packets.__eq__(self, packet) and \
           self.messageIdentifier == packet.messageIdentifier


class Subscribes(Packets):

  def __init__(self, buffer=None, DUP=False, QoS=1, RETAIN=False, MsgId=1, Data=[]):
    self.fh = FixedHeaders(PacketTypes.SUBSCRIBE)
    self.fh.DUP = DUP
    self.fh.QoS = QoS
    self.fh.RETAIN = RETAIN
    # variable header
    self.messageIdentifier = MsgId
    # payload - list of topic, qos pairs
    self.data = Data[:]
    if buffer != None:
      self.unpack(buffer)

  def pack(self):
    buffer = writeInt16(self.messageIdentifier)
    for d in self.data:
      buffer += writeUTF(d[0]) + bytes([d[1]])
    buffer = self.fh.pack(len(buffer)) + buffer
    return buffer

  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert PacketType(buffer) == PacketTypes.SUBSCRIBE
    fhlen = self.fh.unpack(buffer)
    assert len(buffer) >= fhlen + self.fh.remainingLength
    logger.info("[MQTT-2.3.1-1] packet indentifier must be in subscribe")
    self.messageIdentifier = readInt16(buffer[fhlen:])
    assert self.messageIdentifier > 0, "[MQTT-2.3.1-1] packet indentifier must be > 0"
    leftlen = self.fh.remainingLength - 2
    self.data = []
    while leftlen > 0:
      topic, topiclen = readUTF(buffer[-leftlen:], leftlen)
      leftlen -= topiclen
      qos = buffer[-leftlen]
      assert qos in [0, 1, 2], "[MQTT-3-8.3-2] reserved bits must be zero"
      leftlen -= 1
      self.data.append((topic, qos))
    assert len(self.data) > 0, "[MQTT-3.8.3-1] at least one topic, qos pair must be in subscribe"
    assert leftlen == 0
    assert self.fh.DUP == False, "[MQTT-2.1.2-1] DUP must be false in subscribe"
    assert self.fh.QoS == 1, "[MQTT-2.1.2-1] QoS must be 1 in subscribe"
    assert self.fh.RETAIN == False, "[MQTT-2.1.2-1] RETAIN must be false in subscribe"
    return fhlen + self.fh.remainingLength

  def __repr__(self):
    return repr(self.fh)+", MsgId="+repr(self.messageIdentifier)+\
           ", Data="+repr(self.data)+")"

  def __eq__(self, packet):
    return Packets.__eq__(self, packet) and \
           self.messageIdentifier == packet.messageIdentifier and \
           self.data == packet.data


class Subacks(Packets):

  def __init__(self, buffer=None, DUP=False, QoS=0, RETAIN=False, MsgId=1, Data=[]):
    self.fh = FixedHeaders(PacketTypes.SUBACK)
    self.fh.DUP = DUP
    self.fh.QoS = QoS
    self.fh.RETAIN = RETAIN
    # variable header
    self.messageIdentifier = MsgId
    # payload - list of qos
    self.data = Data[:]
    if buffer != None:
      self.unpack(buffer)

  def pack(self):
    buffer = writeInt16(self.messageIdentifier)
    for d in self.data:
      buffer += bytes([d])
    buffer = self.fh.pack(len(buffer)) + buffer
    return buffer

  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert PacketType(buffer) == PacketTypes.SUBACK
    fhlen = self.fh.unpack(buffer)
    assert len(buffer) >= fhlen + self.fh.remainingLength
    self.messageIdentifier = readInt16(buffer[fhlen:])
    leftlen = self.fh.remainingLength - 2
    self.data = []
    while leftlen > 0:
      qos = buffer[-leftlen]
      assert qos in [0, 1, 2, 0x80], "[MQTT-3.9.3-2] return code in QoS must be 0, 1, 2 or 0x80"
      leftlen -= 1
      self.data.append(qos)
    assert leftlen == 0
    assert self.fh.DUP == False, "[MQTT-2.1.2-1] DUP should be false in suback"
    assert self.fh.QoS == 0, "[MQTT-2.1.2-1] QoS should be 0 in suback"
    assert self.fh.RETAIN == False, "[MQTT-2.1.2-1] Retain should be false in suback"
    return fhlen + self.fh.remainingLength

  def __repr__(self):
    return repr(self.fh)+", MsgId="+repr(self.messageIdentifier)+\
           ", Data="+repr(self.data)+")"

  def __eq__(self, packet):
    return Packets.__eq__(self, packet) and \
           self.messageIdentifier == packet.messageIdentifier and \
           self.data == packet.data


class Unsubscribes(Packets):

  def __init__(self, buffer=None, DUP=False, QoS=1, RETAIN=False, MsgId=1, Data=[]):
    self.fh = FixedHeaders(PacketTypes.UNSUBSCRIBE)
    self.fh.DUP = DUP
    self.fh.QoS = QoS
    self.fh.RETAIN = RETAIN
    # variable header
    self.messageIdentifier = MsgId
    # payload - list of topics
    self.data = Data[:]
    if buffer != None:
      self.unpack(buffer)

  def pack(self):
    buffer = writeInt16(self.messageIdentifier)
    for d in self.data:
      buffer += writeUTF(d)
    buffer = self.fh.pack(len(buffer)) + buffer
    return buffer

  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert PacketType(buffer) == PacketTypes.UNSUBSCRIBE
    fhlen = self.fh.unpack(buffer)
    assert len(buffer) >= fhlen + self.fh.remainingLength
    logger.info("[MQTT-2.3.1-1] packet indentifier must be in unsubscribe")
    self.messageIdentifier = readInt16(buffer[fhlen:])
    assert self.messageIdentifier > 0, "[MQTT-2.3.1-1] packet indentifier must be > 0"
    leftlen = self.fh.remainingLength - 2
    self.data = []
    while leftlen > 0:
      topic = readUTF(buffer[-leftlen:], leftlen)
      leftlen -= len(topic) + 2
      self.data.append(topic)
    assert leftlen == 0
    assert self.fh.DUP == False, "[MQTT-2.1.2-1]"
    assert self.fh.QoS == 1, "[MQTT-2.1.2-1]"
    assert self.fh.RETAIN == False, "[MQTT-2.1.2-1]"
    logger.info("[MQTT-3-10.1-1] fixed header bits are 0,0,1,0")
    return fhlen + self.fh.remainingLength

  def __repr__(self):
    return repr(self.fh)+", MsgId="+repr(self.messageIdentifier)+\
           ", Data="+repr(self.data)+")"

  def __eq__(self, packet):
    return Packets.__eq__(self, packet) and \
           self.messageIdentifier == packet.messageIdentifier and \
           self.data == packet.data


class Unsubacks(Packets):

  def __init__(self, buffer=None, DUP=False, QoS=0, RETAIN=False, MsgId=1):
    self.fh = FixedHeaders(PacketTypes.UNSUBACK)
    self.fh.DUP = DUP
    self.fh.QoS = QoS
    self.fh.RETAIN = RETAIN
    # variable header
    self.messageIdentifier = MsgId
    if buffer != None:
      self.unpack(buffer)

  def pack(self):
    buffer = writeInt16(self.messageIdentifier)
    buffer = self.fh.pack(len(buffer)) + buffer
    return buffer

  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert PacketType(buffer) == PacketTypes.UNSUBACK
    fhlen = self.fh.unpack(buffer)
    assert len(buffer) >= fhlen + self.fh.remainingLength
    self.messageIdentifier = readInt16(buffer[fhlen:])
    assert self.messageIdentifier > 0, "[MQTT-2.3.1-1] packet indentifier must be > 0"
    self.messageIdentifier = readInt16(buffer[fhlen:])
    assert self.fh.DUP == False, "[MQTT-2.1.2-1]"
    assert self.fh.QoS == 0, "[MQTT-2.1.2-1]"
    assert self.fh.RETAIN == False, "[MQTT-2.1.2-1]"
    return fhlen + self.fh.remainingLength

  def __repr__(self):
    return repr(self.fh)+", MsgId="+repr(self.messageIdentifier)+")"

  def __eq__(self, packet):
    return Packets.__eq__(self, packet) and \
           self.messageIdentifier == packet.messageIdentifier


class Pingreqs(Packets):

  def __init__(self, buffer=None, DUP=False, QoS=0, RETAIN=False):
    self.fh = FixedHeaders(PacketTypes.PINGREQ)
    self.fh.DUP = DUP
    self.fh.QoS = QoS
    self.fh.RETAIN = RETAIN
    if buffer != None:
      self.unpack(buffer)

  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert PacketType(buffer) == PacketTypes.PINGREQ
    fhlen = self.fh.unpack(buffer)
    assert self.fh.remainingLength == 0
    assert self.fh.DUP == False, "[MQTT-2.1.2-1]"
    assert self.fh.QoS == 0, "[MQTT-2.1.2-1]"
    assert self.fh.RETAIN == False, "[MQTT-2.1.2-1]"
    return fhlen

  def __repr__(self):
    return repr(self.fh)+")"


class Pingresps(Packets):

  def __init__(self, buffer=None, DUP=False, QoS=0, RETAIN=False):
    self.fh = FixedHeaders(PacketTypes.PINGRESP)
    self.fh.DUP = DUP
    self.fh.QoS = QoS
    self.fh.RETAIN = RETAIN
    if buffer != None:
      self.unpack(buffer)

  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert PacketType(buffer) == PacketTypes.PINGRESP
    fhlen = self.fh.unpack(buffer)
    assert self.fh.remainingLength == 0
    assert self.fh.DUP == False, "[MQTT-2.1.2-1]"
    assert self.fh.QoS == 0, "[MQTT-2.1.2-1]"
    assert self.fh.RETAIN == False, "[MQTT-2.1.2-1]"
    return fhlen

  def __repr__(self):
    return repr(self.fh)+")"

classes = [Connects, Connacks, Publishes, Pubacks, Pubrecs,
           Pubrels, Pubcomps, Subscribes, Subacks, Unsubscribes,
           Unsubacks, Pingreqs, Pingresps, Disconnects]

def unpackPacket(buffer):
  if PacketType(buffer) != None:
    packet = classes[PacketType(buffer)-1]()
    packet.unpack(buffer)
  else:
    packet = None
  return packet

if __name__ == "__main__":
  fh = FixedHeaders(PacketTypes.CONNECT)
  tests = [0, 56, 127, 128, 8888, 16383, 16384, 65535, 2097151, 2097152,
           20555666, 268435454, 268435455]
  for x in tests:
    try:
      assert x == MBIs.decode(MBIs.encode(x))[0]
    except AssertionError:
      print("Test failed for x =", x, MBIs.decode(MBIs.encode(x)))
  try:
    MBIs.decode(MBIs.encode(268435456))
    print("Error")
  except AssertionError:
    pass

  for packet in classes[1:]:
    before = str(packet())
    after = str(unpackPacket(packet().pack()))
    print("before:", before, "\nafter:", after)
    try:
      assert before == after
    except:
      print("before:", before, "\nafter:", after)
  print("End")
