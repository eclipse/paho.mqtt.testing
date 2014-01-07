# Low-level protocol interface

# Message types
CONNECT, CONNACK, PUBLISH, PUBACK, PUBREC, PUBREL, \
PUBCOMP, SUBSCRIBE, SUBACK, UNSUBSCRIBE, UNSUBACK, \
PINGREQ, PINGRESP, DISCONNECT = range(1, 15)

packetNames = [ "reserved", \
"CONNECT", "CONNACK", "PUBLISH", "PUBACK", "PUBREC", "PUBREL", \
"PUBCOMP", "SUBSCRIBE", "SUBACK", "UNSUBSCRIBE", "UNSUBACK", \
"PINGREQ", "PINGRESP", "DISCONNECT"]

def MessageType(byte):
  if byte != None:
    rc = byte[0] >> 4
  else:
    rc = None
  return rc


def getPacket(aSocket):
  "receive the next packet"
  buf = aSocket.recv(1) # get the first byte fixed header
  if buf == b"":
    return None
  # now get the remaining length
  multiplier = 1
  remlength = 0
  while 1:
    buf += aSocket.recv(1)
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
  return buf + rest


class FixedHeaders:

  def __init__(self, aMessageType):
    self.MessageType = aMessageType
    self.DUP = False
    self.QoS = 0
    self.RETAIN = False
    self.remainingLength = 0

  def __eq__(self, fh):
    return self.MessageType == fh.MessageType and \
           self.DUP == fh.DUP and \
           self.QoS == fh.QoS and \
           self.RETAIN == fh.RETAIN # and \
           # self.remainingLength == fh.remainingLength

  def __repr__(self):
    "return printable representation of our data"
    return packetNames[self.MessageType]+': DUP '+repr(self.DUP)+ \
           ", QoS "+repr(self.QoS)+", Retain "+repr(self.RETAIN)

  def pack(self, length):
    "pack data into string buffer ready for transmission down socket"
    buffer = bytes([(self.MessageType << 4) | (self.DUP << 3) |\
                         (self.QoS << 1) | self.RETAIN])
    self.remainingLength = length
    buffer += self.encode(length)
    return buffer

  def encode(self, x):
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

  def unpack(self, buffer):
    "unpack data from string buffer into separate fields"
    b0 = buffer[0]
    self.MessageType = b0 >> 4
    self.DUP = ((b0 >> 3) & 0x01) == 1
    self.QoS = (b0 >> 1) & 0x03
    self.RETAIN = (b0 & 0x01) == 1
    (self.remainingLength, bytes) = self.decode(buffer[1:])
    return bytes + 1 # length of fixed header

  def decode(self, buffer):
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


def writeInt16(length):
  return bytes([length // 256, length % 256])

def readInt16(buf):
  return buf[0]*256 + buf[1]

def writeUTF(aString):
  return writeInt16(len(aString)) + bytes(aString, "utf8")

def readUTF(buffer):
  length = readInt16(buffer)
  return buffer[2:2+length].decode("utf-8")


class Packets:

  def pack(self):
    buffer = self.fh.pack(0)
    return buffer

  def __repr__(self):
    return repr(self.fh)

  def __eq__(self, packet):
    return self.fh == packet.fh if packet else False


class Connects(Packets):

  def __init__(self, buffer = None):
    self.fh = FixedHeaders(CONNECT)

    # variable header
    self.ProtocolName = 'MQTT'
    self.ProtocolVersion = 4
    self.CleanStart = False
    self.WillFlag = False
    self.WillQoS = 0
    self.WillRETAIN = 0
    self.KeepAliveTimer = 30
    self.usernameFlag = False
    self.passwordFlag = False

    # Payload
    self.ClientIdentifier = None
    self.WillTopic = None
    self.WillMessage = None
    self.username = None
    self.password = None

    if buffer != None:
      self.unpack(buffer)

  def pack(self):
    connectFlags = bytes([(self.CleanStart << 1) | (self.WillFlag << 2) | \
                       (self.WillQoS << 3) | (self.WillRETAIN << 5) | \
                       (self.usernameFlag << 6) | (self.passwordFlag << 7)])
    buffer = writeUTF(self.ProtocolName) + bytes([self.ProtocolVersion]) + \
              connectFlags + writeInt16(self.KeepAliveTimer)
    buffer += writeUTF(self.ClientIdentifier)
    if self.WillFlag:
      buffer += writeUTF(self.WillTopic)
      buffer += writeUTF(self.WillMessage)
    if self.usernameFlag:
      buffer += writeUTF(self.username)
    if self.passwordFlag:
      buffer += writeUTF(self.password)
    buffer = self.fh.pack(len(buffer)) + buffer
    return buffer

  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert MessageType(buffer) == CONNECT
    fhlen = self.fh.unpack(buffer)
    assert len(buffer) >= fhlen + self.fh.remainingLength
    curlen = fhlen

    self.ProtocolName = readUTF(buffer[curlen:])
    curlen += len(self.ProtocolName) + 2
    self.ProtocolVersion = buffer[curlen]
    assert self.ProtocolVersion in [3, 131, 4]
    curlen += 1

    connectFlags = buffer[curlen]
    #print "connectFlags", connectFlags
    self.CleanStart = (connectFlags >> 1) & 0x01
    self.WillFlag = (connectFlags >> 2) & 0x01
    self.WillQoS = (connectFlags >> 3) & 0x03
    self.WillRETAIN = (connectFlags >> 5) & 0x01
    self.usernameFlag = (connectFlags >> 6) & 0x01
    self.passwordFlag = (connectFlags >> 7) & 0x01
    curlen +=1

    self.KeepAliveTimer = readInt16(buffer[curlen:])
    curlen += 2
    self.ClientIdentifier = readUTF(buffer[curlen:])
    curlen += len(self.ClientIdentifier) + 2

    if self.WillFlag:
      self.WillTopic = readUTF(buffer[curlen:])
      curlen += len(self.WillTopic) + 2
      self.WillMessage = readUTF(buffer[curlen:])
      curlen += len(self.WillMessage) + 2
    else:
      self.WillTopic = self.WillMessage = None

    #print "usernameFlag", self.usernameFlag, len(buffer), curlen+2
    if self.usernameFlag and len(buffer) > curlen+2:
      self.username = readUTF(buffer[curlen:])
      curlen += len(self.username) + 2

    if self.passwordFlag and len(buffer) > curlen+2:
      self.password = readUTF(buffer[curlen:])
      curlen += len(self.password) + 2

  def __repr__(self):
    buf = repr(self.fh)+", ProtocolName "+str(self.ProtocolName)+", ProtocolVersion " +\
          repr(self.ProtocolVersion)+", CleanStart "+repr(self.CleanStart) +\
          ", WillFlag "+repr(self.WillFlag)+", KeepAliveTimer " +\
          repr(self.KeepAliveTimer)+", ClientId "+str(self.ClientIdentifier) +\
          ", usernameFlag "+repr(self.usernameFlag)+", passwordFlag "+repr(self.passwordFlag)
    if self.WillFlag:
      buf += ", WillQoS " + repr(self.WillQoS) +\
             ", WillRETAIN " + repr(self.WillRETAIN) +\
             ", WillTopic '"+ self.WillTopic +\
             "', WillMessage '"+self.WillMessage+"'"
    if self.username:
      buf += ", username "+self.username
    if self.password:
      buf += ", password "+self.password
    return buf

  def __eq__(self, packet):
    rc = Packets.__eq__(self, packet) and \
           self.ProtocolName == packet.ProtocolName and \
           self.ProtocolVersion == packet.ProtocolVersion and \
           self.CleanStart == packet.CleanStart and \
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

  def __init__(self, buffer = None):
    self.fh = FixedHeaders(CONNACK)
    self.returnCode = 0
    if buffer != None:
      self.unpack(buffer)

  def pack(self):
    buffer = bytes([0, self.returnCode])
    buffer = self.fh.pack(len(buffer)) + buffer
    return buffer

  def unpack(self, buffer):
    assert len(buffer) >= 4
    assert MessageType(buffer) == CONNACK
    self.fh.unpack(buffer)
    self.returnCode = buffer[3]

  def __repr__(self):
    return repr(self.fh)+", ReturnCode "+repr(self.returnCode)

  def __eq__(self, packet):
    return Packets.__eq__(self, packet) and \
           self.returnCode == packet.returnCode


class Disconnects(Packets):

  def __init__(self, buffer = None):
    self.fh = FixedHeaders(DISCONNECT)
    if buffer != None:
      self.unpack(buffer)

  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert MessageType(buffer) == DISCONNECT
    self.fh.unpack(buffer)


class Publishes(Packets):

  def __init__(self, buffer = None):
    self.fh = FixedHeaders(PUBLISH)
    # variable header
    self.topicName = ""
    self.messageIdentifier = 0
    # payload
    self.data = b""
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
    assert MessageType(buffer) == PUBLISH
    fhlen = self.fh.unpack(buffer)
    assert len(buffer) >= fhlen + self.fh.remainingLength
    curlen = fhlen
    self.topicName = readUTF(buffer[fhlen:])
    curlen += len(self.topicName) + 2
    if self.fh.QoS != 0:
      self.messageIdentifier = readInt16(buffer[curlen:])
      curlen += 2
    else:
      self.messageIdentifier = 0
    self.data = buffer[curlen:fhlen + self.fh.remainingLength]
    return fhlen + self.fh.remainingLength

  def __repr__(self):
    rc = repr(self.fh)
    if self.fh.QoS != 0:
      rc += ", MsgId "+repr(self.messageIdentifier)
    rc += ", TopicName "+repr(self.topicName)+", "+repr(self.data)
    return rc

  def __eq__(self, packet):
    rc = Packets.__eq__(self, packet) and \
         self.topicName == packet.topicName and \
         self.data == packet.data
    if rc and self.fh.QoS != 0:
      rc = self.messageIdentifier == packet.messageIdentifier
    return rc


class Pubacks(Packets):

  def __init__(self, buffer = None):
    self.fh = FixedHeaders(PUBACK)
    # variable header
    self.messageIdentifier = 0
    if buffer != None:
      self.unpack(buffer)

  def pack(self):
    buffer = writeInt16(self.messageIdentifier)
    buffer = self.fh.pack(len(buffer)) + buffer
    return buffer

  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert MessageType(buffer) == PUBACK
    fhlen = self.fh.unpack(buffer)
    assert len(buffer) >= fhlen + self.fh.remainingLength
    self.messageIdentifier = readInt16(buffer[fhlen:])
    return fhlen + 2

  def __repr__(self):
    return repr(self.fh)+", MsgId "+repr(self.messageIdentifier)

  def __eq__(self, packet):
    return Packets.__eq__(self, packet) and \
           self.messageIdentifier == packet.messageIdentifier


class Pubrecs(Packets):

  def __init__(self, buffer = None):
    self.fh = FixedHeaders(PUBREC)
    # variable header
    self.messageIdentifier = 0
    if buffer != None:
      self.unpack(buffer)

  def pack(self):
    buffer = writeInt16(self.messageIdentifier)
    buffer = self.fh.pack(len(buffer)) + buffer
    return buffer

  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert MessageType(buffer) == PUBREC
    fhlen = self.fh.unpack(buffer)
    assert len(buffer) >= fhlen + self.fh.remainingLength
    self.messageIdentifier = readInt16(buffer[fhlen:])
    return fhlen + 2

  def __repr__(self):
    return repr(self.fh)+", MsgId "+repr(self.messageIdentifier)

  def __eq__(self, packet):
    return Packets.__eq__(self, packet) and \
           self.messageIdentifier == packet.messageIdentifier


class Pubrels(Packets):

  def __init__(self, buffer = None):
    self.fh = FixedHeaders(PUBREL)
    # variable header
    self.messageIdentifier = 0
    if buffer != None:
      self.unpack(buffer)

  def pack(self):
    buffer = writeInt16(self.messageIdentifier)
    buffer = self.fh.pack(len(buffer)) + buffer
    return buffer

  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert MessageType(buffer) == PUBREL
    fhlen = self.fh.unpack(buffer)
    assert len(buffer) >= fhlen + self.fh.remainingLength
    self.messageIdentifier = readInt16(buffer[fhlen:])
    return fhlen + 2

  def __repr__(self):
    return repr(self.fh)+", MsgId "+repr(self.messageIdentifier)

  def __eq__(self, packet):
    return Packets.__eq__(self, packet) and \
           self.messageIdentifier == packet.messageIdentifier


class Pubcomps(Packets):

  def __init__(self, buffer = None):
    self.fh = FixedHeaders(PUBCOMP)
    # variable header
    self.messageIdentifier = 0
    if buffer != None:
      self.unpack(buffer)

  def pack(self):
    buffer = writeInt16(self.messageIdentifier)
    buffer = self.fh.pack(len(buffer)) + buffer
    return buffer

  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert MessageType(buffer) == PUBCOMP
    fhlen = self.fh.unpack(buffer)
    assert len(buffer) >= fhlen + self.fh.remainingLength
    self.messageIdentifier = readInt16(buffer[fhlen:])
    return fhlen + 2

  def __repr__(self):
    return repr(self.fh)+", MsgId "+repr(self.messageIdentifier)

  def __eq__(self, packet):
    return Packets.__eq__(self, packet) and \
           self.messageIdentifier == packet.messageIdentifier


class Subscribes(Packets):

  def __init__(self, buffer = None):
    self.fh = FixedHeaders(SUBSCRIBE)
    # variable header
    self.messageIdentifier = 0
    # payload - list of topic, qos pairs
    self.data = []
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
    assert MessageType(buffer) == SUBSCRIBE
    fhlen = self.fh.unpack(buffer)
    assert len(buffer) >= fhlen + self.fh.remainingLength
    self.messageIdentifier = readInt16(buffer[fhlen:])
    leftlen = self.fh.remainingLength - 2
    self.data = []
    while leftlen > 0:
      topic = readUTF(buffer[-leftlen:])
      leftlen -= len(topic) + 2
      qos = buffer[-leftlen]
      leftlen -= 1
      self.data.append((topic, qos))
    assert leftlen == 0
    return fhlen + self.fh.remainingLength

  def __repr__(self):
    return repr(self.fh)+", MsgId "+repr(self.messageIdentifier)+\
           ", "+repr(self.data)

  def __eq__(self, packet):
    return Packets.__eq__(self, packet) and \
           self.messageIdentifier == packet.messageIdentifier and \
           self.data == packet.data


class Subacks(Packets):

  def __init__(self, buffer = None):
    self.fh = FixedHeaders(SUBACK)
    self.fh.QoS = 1
    # variable header
    self.messageIdentifier = 0
    # payload - list of qos
    self.data = []
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
    assert MessageType(buffer) == SUBACK
    fhlen = self.fh.unpack(buffer)
    assert len(buffer) >= fhlen + self.fh.remainingLength
    self.messageIdentifier = readInt16(buffer[fhlen:])
    leftlen = self.fh.remainingLength - 2
    self.data = []
    while leftlen > 0:
      qos = buffer[-leftlen]
      leftlen -= 1
      self.data.append(qos)
    assert leftlen == 0
    return fhlen + self.fh.remainingLength

  def __repr__(self):
    return repr(self.fh)+", MsgId "+repr(self.messageIdentifier)+\
           ", "+repr(self.data)

  def __eq__(self, packet):
    return Packets.__eq__(self, packet) and \
           self.messageIdentifier == packet.messageIdentifier and \
           self.data == packet.data


class Unsubscribes(Packets):

  def __init__(self, buffer = None):
    self.fh = FixedHeaders(UNSUBSCRIBE)
    # variable header
    self.messageIdentifier = 0
    # payload - list of topics
    self.data = []
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
    assert MessageType(buffer) == UNSUBSCRIBE
    fhlen = self.fh.unpack(buffer)
    assert len(buffer) >= fhlen + self.fh.remainingLength
    self.messageIdentifier = readInt16(buffer[fhlen:])
    leftlen = self.fh.remainingLength - 2
    self.data = []
    while leftlen > 0:
      topic = readUTF(buffer[-leftlen:])
      leftlen -= len(topic) + 2
      self.data.append(topic)
    assert leftlen == 0
    return fhlen + self.fh.remainingLength

  def __repr__(self):
    return repr(self.fh)+", MsgId "+repr(self.messageIdentifier)+\
           ", "+repr(self.data)

  def __eq__(self, packet):
    return Packets.__eq__(self, packet) and \
           self.messageIdentifier == packet.messageIdentifier and \
           self.data == packet.data


class Unsubacks(Packets):

  def __init__(self, buffer = None):
    self.fh = FixedHeaders(UNSUBACK)
    self.fh.QoS = 1
    # variable header
    self.messageIdentifier = 0
    if buffer != None:
      self.unpack(buffer)

  def pack(self):
    buffer = writeInt16(self.messageIdentifier)
    buffer = self.fh.pack(len(buffer)) + buffer
    return buffer

  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert MessageType(buffer) == UNSUBACK
    fhlen = self.fh.unpack(buffer)
    assert len(buffer) >= fhlen + self.fh.remainingLength
    self.messageIdentifier = readInt16(buffer[fhlen:])
    return fhlen + self.fh.remainingLength

  def __repr__(self):
    return repr(self.fh)+", MsgId "+repr(self.messageIdentifier)

  def __eq__(self, packet):
    return Packets.__eq__(self, packet) and \
           self.messageIdentifier == packet.messageIdentifier


class Pingreqs(Packets):

  def __init__(self, buffer = None):
    self.fh = FixedHeaders(PINGREQ)
    if buffer != None:
      self.unpack(buffer)

  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert MessageType(buffer) == PINGREQ
    fhlen = self.fh.unpack(buffer)
    assert self.fh.remainingLength == 0
    return fhlen


class Pingresps(Packets):

  def __init__(self, buffer = None):
    self.fh = FixedHeaders(PINGRESP)
    if buffer != None:
      self.unpack(buffer)

  def unpack(self, buffer):
    assert len(buffer) >= 2
    assert MessageType(buffer) == PINGRESP
    fhlen = self.fh.unpack(buffer)
    assert self.fh.remainingLength == 0
    return fhlen

objects = [None, Connects, Connacks, Publishes, Pubacks, Pubrecs,
           Pubrels, Pubcomps, Subscribes, Subacks, Unsubscribes,
           Unsubacks, Pingreqs, Pingresps, Disconnects]

def unpackPacket(buffer):
  if MessageType(buffer) != None:
    packet = objects[MessageType(buffer)]()
    packet.unpack(buffer)
  else:
    packet = None
  return packet

if __name__ == "__main__":
  fh = FixedHeaders(CONNECT)
  tests = [0, 56, 127, 128, 8888, 16383, 16384, 65535, 2097151, 2097152,
           20555666, 268435454, 268435455]
  for x in tests:
    try:
      assert x == fh.decode(fh.encode(x))[0]
    except AssertionError:
      print("Test failed for x =", x, fh.decode(fh.encode(x)))
  try:
    fh.decode(fh.encode(268435456))
    print("Error")
  except AssertionError:
    pass

