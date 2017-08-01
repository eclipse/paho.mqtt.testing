import unittest, mqtt, time

class Test(unittest.TestCase):

  def testBasicClient(self):
    callback = mqtt.clients.V5.Callback()

    aclient = mqtt.clients.V5.Client("myclientid")
    aclient.registerCallback(callback)

    self.assertEqual(aclient.connect(port=1883).reasonCode.getName(), "Success")
    time.sleep(1.0)
    aclient.disconnect()

    aclient.connect(port=1883)
    aclient.subscribe(["k"], [2])
    aclient.publish("k", "qos 0")
    aclient.publish("k", "qos 1", 1)
    aclient.publish("k", "qos 2", 2)
    time.sleep(1.0)
    aclient.disconnect()


if __name__ == "__main__":
    unittest.main()
