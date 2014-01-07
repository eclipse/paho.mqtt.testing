import mqtt.client, time

if __name__ == "__main__":

  callback = mqtt.client.Callback()

  aclient = mqtt.client.Client("myclientid")
  aclient.registerCallback(callback)

  aclient.connect(port=1883)
  aclient.disconnect()

  aclient.connect(port=1883)
  aclient.subscribe(["k"], [2])
  aclient.publish("k", b"qos 0")
  aclient.publish("k", b"qos 1", 1)
  aclient.publish("k", b"qos 2", 2)
  time.sleep(1.0)
  aclient.disconnect()
