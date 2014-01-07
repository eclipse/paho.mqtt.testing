"""
*******************************************************************
  Copyright (c) 2013, 2014 IBM Corp.
 
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
