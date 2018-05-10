import unittest

import MQTTV5

class Test(unittest.TestCase):

    def testVBIs(self):
      fh = MQTTV5.FixedHeaders(MQTTV5.PacketTypes.CONNECT)
      tests = [0, 56, 127, 128, 8888, 16383, 16384, 65535, 2097151, 2097152,
               20555666, 268435454, 268435455]
      for x in tests:
        self.assertEqual(x, MQTTV5.VBIs.decode(MQTTV5.VBIs.encode(x))[0])

      with self.assertRaises(AssertionError):
        MQTTV5.VBIs.decode(MQTTV5.VBIs.encode(268435456)) # out of VBI range
        MQTTV5.VBIs.decode(MQTTV5.VBIs.encode(-1)) # out of VBI range

    def testProperties(self):
      for packetType in MQTTV5.PacketTypes.indexes:
        #print("Properties", MQTTV5.Packets.Names[packetType])
        p = MQTTV5.Properties(packetType)
        q = MQTTV5.Properties(packetType)
        if packetType not in [MQTTV5.PacketTypes.PINGREQ,
                              MQTTV5.PacketTypes.PINGRESP]:
           p.UserPropertyList = [("jk", "jk"), ("kk", "kl")]
        else:
           with self.assertRaises(MQTTV5.MQTTException):
             p.UserPropertyList = [("jk", "jk")]
        if packetType in [MQTTV5.PacketTypes.PUBLISH]:
            p.PayloadFormatIndicator = 3
        if packetType in [MQTTV5.PacketTypes.CONNECT,
           MQTTV5.PacketTypes.DISCONNECT]:
            p.SessionExpiryInterval = 120
        #print(packetType, str(p.pack())+ " " + str(p.unpack(p.pack())[0].pack()))
        before = str(p)
        after = str(q.unpack(p.pack())[0])
        self.assertTrue(before == after,
            "For packet type %s" % (MQTTV5.Packets.Names[packetType]))


    def testPropertyTypes(self):
        packetType = MQTTV5.PacketTypes.PUBLISH
        p = MQTTV5.Properties(packetType)
        q = MQTTV5.Properties(packetType)
        # Byte
        p.PayloadFormatIndicator = 56
        # Two Byte Integer
        p.TopicAlias = 378
        # Four Byte Integer
        p.MessageExpiryInterval = 108927
        # Variable Byte Integer
        p.SubscriptionIdentifier = 45678988
        # Binary Data
        p.CorrelationData = bytes([4,6,6])
        # UTF-8 Encoded String
        p.ContentType = "Content type test"
        # UTF-8 Encoded String Pair
        p.UserPropertyList = [("a property name", "a property value")]
        before = str(p)
        after = str(q.unpack(p.pack())[0])
        #print(p, after)
        self.assertTrue(before == after,
            "For packet type %s" % (MQTTV5.Packets.Names[packetType]))

    def testBasicPackets(self):
      for packet in MQTTV5.classes:
        #print("BasicPacket", packet.__name__)
        p = packet()
        if packet == MQTTV5.Subscribes:
          options = MQTTV5.SubscribeOptions(2)
          options.retainHandling = 2
          options.noLocal = True
          p.data = [("#", options)]
        elif packet == MQTTV5.Unsubscribes:
          p.topicFilters = [("#")]
        elif packet == MQTTV5.Subacks:
          p.reasonCodes = [MQTTV5.ReasonCodes(MQTTV5.PacketTypes.SUBACK, "Unspecified error")]
        elif packet == MQTTV5.Connacks:
          p.properties.AssignedClientIdentifier = "klklkl"
        before = str(p)
        after = str(MQTTV5.unpackPacket(p.pack()))
        self.assertEqual(before, after)

    def testReasonCodes(self):
      r = MQTTV5.ReasonCodes(MQTTV5.PacketTypes.DISCONNECT, "Normal disconnection")
      self.assertEqual(r.__getName__(MQTTV5.PacketTypes.DISCONNECT, 0),
        "Normal disconnection")
      self.assertEqual(r.getId("Normal disconnection"), 0)
      self.assertEqual(r.__getName__(MQTTV5.PacketTypes.PUBREL, 0),
              "Success")
      r = MQTTV5.ReasonCodes(MQTTV5.PacketTypes.CONNACK)
      self.assertEqual(r.getId("Success"), 0)
      self.assertEqual(r.__getName__(MQTTV5.PacketTypes.DISCONNECT, 162),
        "Wildcard subscription not supported")
      self.assertEqual(r.getId("Packet too large"), 149)
      with self.assertRaises(AssertionError):
          r.__getName__(201, MQTTV5.PacketTypes.PUBREL)
          r.__getName__(146, MQTTV5.PacketTypes.CONNACK)
          r.getId("rubbish")


if __name__ == "__main__":
    import sys
    if sys.version_info[0] < 3:
        print("This program requires Python 3")
        sys.exit()
    unittest.main()
