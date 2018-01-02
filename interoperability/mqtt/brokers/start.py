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
     Ian Craggs - add websockets support
     Ian Craggs - add TLS support
*******************************************************************
"""

import socketserver, select, sys, traceback, socket, logging, getopt, hashlib, base64
import threading, ssl

from .V311 import MQTTBrokers as MQTTV3Brokers
from .V5 import MQTTBrokers as MQTTV5Brokers
from .SN import MQTTSNBrokers
from .coverage import filter, measure
from mqtt.formats.MQTTV311 import MQTTException as MQTTV3Exception
from mqtt.formats.MQTTV5 import MQTTException as MQTTV5Exception
from mqtt.formats.MQTTSN import MQTTSNException
from mqtt.brokers import TCPListeners, UDPListeners


logger = None

def setup_persistence(persistence_filename):
  import ZODB, ZODB.FileStorage, BTrees.OOBTree, transaction, persistent
  storage = ZODB.FileStorage.FileStorage(persistence_filename)
  db = ZODB.DB(storage)
  connection = db.open()
  root = connection.root

  if not hasattr(root, 'mqtt'):
    root.mqtt = BTrees.OOBTree.BTree()
    transaction.commit()

  if not root.mqtt.has_key("sharedData"):
    root.mqtt["sharedData"] = persistent.mapping.PersistentMapping()
    transaction.commit()

  sharedData = root.mqtt["sharedData"]
  return connection, sharedData

def run(port=1883, config=None,
        publish_on_pubrel=True,
        overlapping_single=True,
        dropQoS0=True,
        zero_length_clientids=True,
        topicAliasMaximum=2,
        maximumPacketSize=1000,
        receiveMaximum=2,
        serverKeepAlive=60):
  global logger, broker3, broker5, brokerSN, server
  logger = logging.getLogger('MQTT broker')
  logger.setLevel(logging.INFO)
  logger.addFilter(filter)

  lock = threading.RLock() # shared lock
  persistence = False
  if persistence:
    connection, sharedData = setup_persistence("sharedData") # location for data shared between brokers - subscriptions for example
  else:
    sharedData = {}

  broker3 = MQTTV3Brokers(publish_on_pubrel=publish_on_pubrel,
      overlapping_single=overlapping_single,
      dropQoS0=dropQoS0,
      zero_length_clientids=zero_length_clientids,
      lock=lock,
      sharedData=sharedData)

  broker5 = MQTTV5Brokers(publish_on_pubrel=publish_on_pubrel,
      overlapping_single=overlapping_single,
      dropQoS0=dropQoS0,
      zero_length_clientids=zero_length_clientids,
      topicAliasMaximum=topicAliasMaximum,
      maximumPacketSize=maximumPacketSize,
      receiveMaximum=receiveMaximum,
      serverKeepAlive=serverKeepAlive,
      lock=lock,
      sharedData=sharedData)

  brokerSN = MQTTSNBrokers(lock=lock,
      sharedData=sharedData)

  broker3.setBroker5(broker5)
  broker5.setBroker3(broker3)

  brokerSN.setBroker3(broker3)
  brokerSN.setBroker5(broker5)

  servers = []

  try:
    if config == None:
      servers.append(TCPListeners.create(1883, serve_forever=True))
    else:
      servers_to_create = []
      UDPListeners.setBroker(brokerSN)
      TCPListeners.setBrokers(broker3, broker5)
      lineno = 0
      while lineno < len(config):
        curline = config[lineno].strip()
        lineno += 1
        if curline.startswith('#') or len(curline) == 0:
          continue
        words = curline.split()
        if words[0] == "listener":
          ca_certs = certfile = keyfile = None
          cert_reqs=ssl.CERT_REQUIRED
          port = 1883; TLS=False
          if len(words) > 1:
            port = int(words[1])
          protocol = "mqtt"
          if len(words) >= 4 and words[3] == "mqttsn":
            bind_address = words[2]
            protocol = "mqttsn"
          while lineno < len(config) and not config[lineno].strip().startswith("listener"):
            curline = config[lineno].strip()
            lineno += 1
            if curline.startswith('#') or len(curline) == 0:
              continue
            words = curline.split()
            if words[0] == "require_certificate":
              if words[1] == "false":
                cert_reqs=ssl.CERT_OPTIONAL
            elif words[0] == "cafile":
              ca_certs = words[1]; TLS=True
            elif words[0] == "certfile":
              certfile = words[1]; TLS=True
            elif words[0] == "keyfile":
              keyfile = words[1]; TLS=True
          if protocol == "mqtt":
            servers_to_create.append((TCPListeners, {"port": port, "TLS":TLS, "cert_reqs":cert_reqs,
                        "ca_certs":ca_certs, "certfile":certfile, "keyfile":keyfile}))
          elif protocol == "mqttsn":
            servers_to_create.append((UDPListeners, {"port":port}))
      servers_to_create[-1][1]["serve_forever"] = True    
      for server in servers_to_create:
        servers.append(server[0].create(**server[1]))

  except KeyboardInterrupt:
    pass
  except:
    logger.exception("startBroker")

  # Stop incoming communications
  for server in servers:
    try:
      logger.info("Stopping the MQTT server %s", str(server))
      server.socket.shutdown(socket.SHUT_RDWR)
      server.socket.close()
    except:
      pass #traceback.print_exc()
  
  # Disconnect any still connected clients
  broker3.disconnectAll()
  filter.measure()

  logger.debug("Ending sharedData %s", sharedData)
  if persistence:
    sharedData._p_changed = True
    import transaction
    transaction.commit()
    connection.close()

def measure():
  return filter.getmeasures()

def stop():
  global server
  server.shutdown()

def reinitialize():
  global broker3, broker5, brokerSN
  broker3.reinitialize()
  broker5.reinitialize()
  brokerSN.reinitialize()

def read_config(filename):
  infile = open(filename)
  lines = infile.readlines()
  infile.close()
  return lines

def main(argv):
  try:
    opts, args = getopt.gnu_getopt(argv[1:], "hp:o:d:z:c:", ["help", "publish_on_pubrel=", "overlapping_single=",
        "dropQoS0=", "port=", "zero_length_clientids=", "config-file="])
  except getopt.GetoptError as err:
    print(err) # will print something like "option -a not recognized"
    usage()
    sys.exit(2)
  publish_on_pubrel = overlapping_single = dropQoS0 = zero_length_clientids = True
  port = 1883
  cfg = None
  for o, a in opts:
    if o in ("-h", "--help"):
      usage()
      sys.exit()
    elif o in ("-p", "--publish_on_pubrel"):
      publish_on_pubrel = False if a in ["off", "false", "0"] else True
    elif o in ("-o", "--overlapping_single"):
      overlapping_single = False if a in ["off", "false", "0"] else True
    elif o in ("-d", "--dropQoS0"):
      dropQoS0 = False if a in ["off", "false", "0"] else True
    elif o in ("-z", "--zero_length_clientids"):
      zero_length_clientids = False if a in ["off", "false", "0"] else True
    elif o in ("--port"):
      port = int(a)
    elif o in ("-c", "--config-file"):
      cfg = read_config(a)
    else:
      assert False, "unhandled option"

  run(publish_on_pubrel=publish_on_pubrel, overlapping_single=overlapping_single, dropQoS0=dropQoS0, port=port,
     zero_length_clientids=zero_length_clientids, config=cfg)

def usage():
  print(
"""
Eclipse Paho combined MQTT V311 and MQTT V5 broker

 -h --help: print this message
 -p: --publish_on_pubrel=0/1 unset/set publish on pubrel, publish on publish otherwise
 -o: --overlapping_single=0/1
 -d: --dropQoS0=0/1
 -z: --zero_length_clientid=0/1 disallow/allow zero length clientid test
 --port= port number to listen to

""")

if __name__ == "__main__":
  main(sys.argv)
