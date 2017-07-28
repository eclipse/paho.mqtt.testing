import unittest

import MQTTV5

class Test(unittest.TestCase):

    def testMBIs(self):
      fh = MQTTV5.FixedHeaders(MQTTV5.PacketTypes.CONNECT)
      tests = [0, 56, 127, 128, 8888, 16383, 16384, 65535, 2097151, 2097152,
               20555666, 268435454, 268435455]
      for x in tests:
        self.assertEqual(x, MQTTV5.MBIs.decode(MQTTV5.MBIs.encode(x))[0])

      with self.assertRaises(AssertionError):
        MQTTV5.MBIs.decode(MQTTV5.MBIs.encode(268435456)) # out of MBI range
        MQTTV5.MBIs.decode(MQTTV5.MBIs.encode(-1)) # out of MBI range

    def testProperties(self):
      for packetType in MQTTV5.PacketTypes.indexes:
        p = MQTTV5.Properties(packetType)
        if packetType not in [MQTTV5.PacketTypes.SUBSCRIBE,
                              MQTTV5.PacketTypes.UNSUBSCRIBE,
                              MQTTV5.PacketTypes.PINGREQ,
                              MQTTV5.PacketTypes.PINGRESP]:
           p.UserProperty = ("jk", "jk")
        else:
           with self.assertRaises(MQTTV5.MQTTException):
             p.UserProperty = ("jk", "jk")
        if packetType in [MQTTV5.PacketTypes.PUBLISH]:
            p.PayloadFormatIndicator = '3'
      self.assertTrue(p.pack() == p.unpack(p.pack())[0].pack(), "For packet type %s" % (MQTTV5.Packets.Names[packetType]))

    def testBasicPackets(self):
      for packet in MQTTV5.classes:
        if packet == MQTTV5.Subscribes:
          p = packet()
          p.data = [("#", 2)]
          before = str(p)
          after = str(MQTTV5.unpackPacket(p.pack()))
        else:
          before = str(packet())
          after = str(MQTTV5.unpackPacket(packet().pack()))
        self.assertEqual(before, after)

if __name__ == "__main__":
    unittest.main()
