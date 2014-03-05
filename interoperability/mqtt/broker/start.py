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

import socketserver, select, sys, traceback, socket, logging, getopt

from .MQTTBrokers import MQTTBrokers
from .coverage import handler, measure
from ..formats.MQTTV311 import MQTTException

broker = None
server = None

logger = None

class MyHandler(socketserver.StreamRequestHandler):

  def handle(self):
    sock = self.request
    sock_no = sock.fileno()
    terminate = keptalive = False
    logger.info("Starting communications for socket %d", sock_no)
    while not terminate:
      try:
        if not keptalive:
          logger.info("Waiting for request")
        (i, o, e) = select.select([sock], [], [], 1)
        if i == [sock]:
          terminate = broker.handleRequest(sock)
          keptalive = False
        elif (i, o, e) == ([], [], []):
          broker.keepalive(sock)
          keptalive = True
        else:
          break
      except UnicodeDecodeError:
        logger.error("[MQTT-1.4.0-1] Unicode field encoding error")
        break
      except MQTTException as exc:
        logger.error(exc.args[0])
        break
      except AssertionError as exc:
        if (len(exc.args) > 0):
          logger.error(exc.args[0])
        else:
          logger.error()
        break
      except:
        logger.exception("MyHandler")
        break
    logger.info("Finishing communications for socket %d", sock_no)

class ThreadingTCPServer(socketserver.ThreadingMixIn,
                           socketserver.TCPServer):
  pass


def run(publish_on_pubrel=True, overlapping_single=True, dropQoS0=True, port=1883):
  global logger, broker, server
  logger = logging.getLogger('MQTT broker')
  logger.setLevel(logging.INFO)
  logger.addHandler(handler)
  broker = MQTTBrokers(publish_on_pubrel=publish_on_pubrel, overlapping_single=overlapping_single, dropQoS0=dropQoS0)
  logger.info("Starting the MQTT server on port %d", port)
  try:
    server = ThreadingTCPServer(("", port), MyHandler, False)
    server.allow_reuse_address = True
    server.server_bind()
    server.server_activate()
    server.serve_forever()
  except KeyboardInterrupt:
    pass 
  except:
    logger.exception("startBroker")
  finally:
    try:
      server.socket.shutdown(socket.SHUT_RDWR)
      server.socket.close()
    except:
      pass
  server = None
  logger.info("Stopping the MQTT server on port %d", port)
  handler.measure()
  

def stop():
  global server
  server.shutdown()

def reinitialize():
  global broker
  broker.reinitialize()

def main(argv):
  try:
    opts, args = getopt.gnu_getopt(argv[1:], "hp:o:d:", ["help", "publish_on_pubrel=", "overlapping_single=", "dropQoS0=", "port="])
  except getopt.GetoptError as err:
    print(err) # will print something like "option -a not recognized"
    usage()
    sys.exit(2)
  publish_on_pubrel = overlapping_single = dropQoS0 = True
  port = 1883
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
    elif o in ("--port"):
      port = int(a)
    else:
      assert False, "unhandled option"

  run(publish_on_pubrel=publish_on_pubrel, overlapping_single=overlapping_single, dropQoS0=dropQoS0, port=port)


if __name__ == "__main__":
  main(sys.argv)
