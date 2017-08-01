import unittest, mqtt, time

class callbacks(mqtt.clients.V5.Callback):

  def __init__(self, test):
    self.count = 0
    self.test = test

  def publishArrived(self, topicName, payload, qos, retained, msgid):
    self.test.assertEqual(topicName, "k")
    self.test.assertEqual(qos, self.count)
    self.test.assertEqual(retained, False)
    self.test.assertEqual(payload, bytes("qos "+str(self.count), "utf8"))
    self.count += 1
    return True

class Test(unittest.TestCase):

  def testBasicClient(self):
    callback = callbacks(self)

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
    self.assertEqual(callback.count, 3)


if __name__ == "__main__":
    unittest.main()
