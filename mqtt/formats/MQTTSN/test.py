import unittest

import MQTTSN

class Test(unittest.TestCase):

    def testFlags(self):
      flags = MQTTSN.Flags()
      outflags = MQTTSN.Flags()
      buf = flags.pack()
      outflags.unpack(buf[0])
      assert flags == outflags

    def testPackets(self):
      for packet in [x for x in MQTTSN.classes if x != None]:
        inpacket = packet()
        print(MQTTSN.Messages.Names[inpacket.messageType])
        outpacket = packet()
        buf = inpacket.pack()
        outpacket.unpack(buf)
        #print("in", str(inpacket))
        #print("out", str(outpacket))
        assert inpacket == outpacket



if __name__ == "__main__":
    import sys
    if sys.version_info[0] < 3:
        print("This program requires Python 3")
        sys.exit()
    unittest.main()
