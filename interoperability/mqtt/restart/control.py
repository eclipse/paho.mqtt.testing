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
*******************************************************************
"""

import sys, time, logging, getopt, os, random

import paho.mqtt.client as mqtt

logging.basicConfig()
logger = logging.getLogger("Restart test")
logger.setLevel(logging.INFO)


class ControlBrokers:

  def __init__(self, options):
    self.messages = []
    self.receiveTopic = options["control_topic"]+"/receive"
    self.sendTopic = options["control_topic"]+"/send"
    self.sendProxyTopic = options["proxy_control_topic"]+"/receive"
    self.client = mqtt.Client("control_connection")
    self.client.on_message = self.on_message
    self.client.on_connect = self.on_connect
    self.client.on_publish = self.on_publish
    self.controlBrokerHost, self.controlBrokerPort = options["control_connection"].split(":")
    self.controlBrokerPort = int(self.controlBrokerPort)
    self.ready = False
    self.client.connect(self.controlBrokerHost, self.controlBrokerPort, 60)
    self.published = False
    self.client.loop_start()
    while not self.ready:
      time.sleep(.4)

  def on_connect(self, client, userdata, flags, rc):
    logger.info("Connected to %s:%d, result code "+str(rc),
       self.controlBrokerHost, self.controlBrokerPort)
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    logger.info("Subscribing to %s", self.receiveTopic)
    self.client.subscribe(self.receiveTopic, 2)
    self.ready = True

  def on_message(self, client, userdata, msg):
    logger.info("Received "+str(msg.payload.decode()))
    self.messages.append((client, userdata, msg))

  def on_publish(self, client, userdata, data):
    self.published = True

  def send_control_message(self, msg):
    logger.info("Sending control message: '%s' to topic %s", msg, self.sendTopic)
    self.messages.clear()
    self.published = False
    self.client.publish(self.sendTopic, msg)
    while not self.published:
      time.sleep(.4)

  def send_proxy_message(self, msg):
    logger.info("Sending proxy message: %s", msg)
    self.messages.clear()
    self.published = False
    self.client.publish(self.sendProxyTopic, msg)
    while not self.published:
      time.sleep(.4)

  def stop(self):
    self.client.disconnect()

  def get_next_message(self):
    rc = None
    if len(self.messages) > 0:
      rc = self.messages.pop(0)
    return rc

def elapsed(start_time):
  return time.time() - start_time

class TestControllers:

  def __init__(self, broker, options):
    self.broker = broker
    self.options = options
    self.test_count = 0

  def wait_for_client_test_programs(self):
    start_time = time.time()
    self.clients = []
    timed_out = False

    logger.info("Waiting for %d client test program%s" %
      (self.options["no_clients"], "" if self.options["no_clients"] == 1 else "s"))
    self.broker.send_control_message("who is ready?")
    while len(self.clients) < self.options["no_clients"]:
      message = self.broker.get_next_message()

      if message == None:
        time.sleep(0.3)
      else:
        clientid, content = message[2].payload.decode().split(":", 1)
        if content.strip() == "Ready":
          logger.info("Adding client id "+clientid)
          self.clients.append(clientid)
        elif content.strip() == "waiting for: who is ready?":
          self.broker.send_control_message("who is ready?")

      if len(self.clients) < self.options["no_clients"]:
        if int(elapsed(start_time)) % 10 == 0:
          self.broker.send_control_message("who is ready?")
          if elapsed(start_time) > 120:
            timed_out = True
            break

    if timed_out:
      self.broker.send_control_message("restart")
    else:
      self.broker.send_control_message("continue")

  def wait_for(self, message):
    logger.info("Waiting for message '%s' from %d test clients" % (message, len(self.clients)))
    if len(self.clients) == 0:
      return True
    # wait for all test programs to send the specified message
    start_time = time.time()
    self.broker.messages.clear()
    receiveds = {}
    for client in self.clients:
      receiveds[client] = False
    while not all(receiveds.values()):
      msg = self.broker.get_next_message()
      while msg:
        clientid, content = msg[2].payload.decode().split(":", 1)
        if content.strip() == message:
          receiveds[clientid.strip()] = True
        msg = self.broker.get_next_message()
      if elapsed(start_time) > 60:
         break
      time.sleep(.3)
    rc = all(receiveds.values())
    logger.info(("Waited for message '%s' from %d test clients: "+str(rc)) % (message, len(self.clients)))
    return rc

  def one_iteration(self):
    # choices range from options stoptypemin to stoptypemax
    stop_type = random.randint(options["stoptypemin"], options["stoptypemax"])

    self.test_count += 1
    self.broker.messages.clear()  # make sure no messages are left over from any previous test
    logger.info("===================================================")
    logger.info("Starting test number %d with stop type %d" % (self.test_count, stop_type))
    logger.info("===================================================")

    self.wait_for("waiting for: start_measuring")  # wait for all clients to be ready
    self.broker.send_control_message("start_measuring")

    interval = random.randint(1, 4)
    logger.info("Interval until restart is %d seconds" % interval)

    self.wait_for("waiting for: start_test")
    self.broker.send_control_message("start_test")

    time.sleep(interval)
    self.restart_operation(stop_type)

    #new RestartThread(interval, stop_type).start();

    self.wait_for("waiting for: test finished")
    self.broker.send_control_message("test finished")

  def stop(self):
    self.broker.send_control_message("stop")

  def restart_operation(self, stop_type):
    if stop_type == 0:
      logger.info("no auto stopping... your own if you want to")
    elif stop_type == 1:
      logger.info("stopping... sending break to proxy")
      self.broker.send_proxy_message("break")

def run(options):
  logger.info("Running")
  control_broker = ControlBrokers(options)
  controller = TestControllers(control_broker, options)
  controller.wait_for_client_test_programs()
  start_time = time.time()
  iterations = 0
  while True:
    controller.one_iteration()
    iterations += 1
    if options["iterations"] > 0 and iterations >= options["iterations"]:
      logger.info("Test ending at %d iterations", iterations)
      break
    if options["duration"] > 0 and elapsed(start_time) > options["duration"]:
      logger.info("Test ending after %d seconds", options["duration"])
      break

  controller.stop()
  control_broker.stop()
  logger.info("Stopping")

def usage(options):
  print("Options with current values:")
  for option in options.keys():
    print("  --%s: %s" % (option, options[option]))

if __name__ == "__main__":

  options = {}
  # connection to MQTT broker used for control messages
  options["control_connection"] = "localhost:7777"
  options["control_topic"] = "Eclipse/Paho/restart_test/control"
  options["proxy_control_topic"] = "Eclipse/Paho/restart_test/proxy_control"

  options["proxy_connection"] ="localhost:1884"
  options["broker_connection"] = "localhost:1883"

  options["clientid"] = "Paho restart test control"

  options["no_clients"] = 1
  options["verbose"] = False
  options["duration"] = 3600
  options["iterations"] = 1
  options["stoptypemin"] = 1
  options["stoptypemax"] = 1

  intoptions = ["no_clients", "duration", "iterations"]
  try:
    opts, args = getopt.gnu_getopt(sys.argv[1:], "h",
         ["control_connection=", "control_topic=", "proxy_connection=", "broker_connection=",
          "clientid=", "no_clients=", "verbose", "duration=", "iterations=",
          "stoptypemin=", "stoptypemax=", "help"])
  except getopt.GetoptError as err:
    print(err) # will print something like "option -a not recognized"
    usage(options)
    sys.exit(2)

  for o, a in opts:
    if o in ("-h", "--help"):
      usage(options)
      sys.exit()
    elif o == "--verbose":
      options["verbose"] = True
    else:
      o = o[2:] # get rid of the --
      logger.info("Setting option %s to %s", o, a)
      options[o] = int(a) if o in intoptions else a

  run(options)
