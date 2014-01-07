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

"""
def report_exception(exc):
  print exc, "exception:"
  debuglines = traceback.format_exception(sys.exc_type, \
               sys.exc_value, sys.exc_traceback)
  for dd in debuglines:
    print dd[:-1]
"""

from .MQTTProtocolNodes import MQTTProtocolNodes

broker = MQTTProtocolNodes()

class MyHandler(socketserver.StreamRequestHandler):

  def handle(self):
    sock = self.request
    sock_no = sock.fileno()
    logging.info("Starting communications for socket %d", sock_no)
    while True:
      try:
        logging.info("Waiting for request")
        (i, o, e) = select.select([sock], [], [])
        if i == [sock]:
          broker.handleRequest(sock)
        else:
          break
      except socket.error: 
        # this is received from select if the client has normally disconnected
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
