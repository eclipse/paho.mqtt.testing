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

import socketserver, select, sys, traceback, socket, logging

logging.basicConfig(format='%(levelname)s %(asctime)s %(message)s',  datefmt='%Y%m%d %H%M%S', level=logging.INFO)

from .MQTTBrokers import MQTTBrokers
from ..formats.MQTTV311 import MQTTException

broker = MQTTBrokers(publish_on_pubrel = False)

class MyHandler(socketserver.StreamRequestHandler):

  def handle(self):
    sock = self.request
    sock_no = sock.fileno()
    terminate = False
    logging.info("Starting communications for socket %d", sock_no)
    while not terminate:
      try:
        logging.debug("Waiting for request")
        (i, o, e) = select.select([sock], [], [], 1)
        if i == [sock]:
          terminate = broker.handleRequest(sock)
        elif (i, o, e) == ([], [], []):
          broker.keepalive(sock)
        else:
          break
      except UnicodeDecodeError:
        logging.error("[MQTT-1.4.0-1] Unicode field encoding error")
        break
      except MQTTException as exc:
        logging.error(exc.args[0])
        break
      except AssertionError as exc:
        logging.error(exc.args[0])
        break
      except:
        logging.exception("MyHandler")
        break
    logging.info("Finishing communications for socket %d", sock_no)

class ThreadingTCPServer(socketserver.ThreadingMixIn,
                           socketserver.TCPServer):
  pass


def run():
  port = 1883
  logging.info("Starting the MQTT server on port %d", port)
  try:
    s = ThreadingTCPServer(("", port), MyHandler, False)
    s.allow_reuse_address = True
    s.server_bind()
    s.server_activate()
    s.serve_forever()
  except KeyboardInterrupt:
    pass 
  except:
    logging.exception("startBroker")
  finally:
    try:
      s.socket.shutdown(socket.SHUT_RDWR)
      s.socket.close()
    except:
      pass
  logging.info("Stopping the MQTT server on port %d", port)


if __name__ == "__main__":
  run()
