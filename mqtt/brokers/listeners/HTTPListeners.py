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

import sys, traceback, socket, logging, getopt, hashlib, base64
import threading, ssl, json, re
import http.server, urllib, urllib.request, urllib.parse

from mqtt.brokers.SN import MQTTSNBrokers
from mqtt.brokers.V311 import MQTTBrokers as MQTTV3Brokers
from mqtt.brokers.V5 import MQTTBrokers as MQTTV5Brokers

logger = logging.getLogger('MQTT broker')

def jsonize(obj):
  name = None
  if "__class__" in dir(obj):
    name = obj.__class__.__name__
  data = {}
  for attrname in dir(obj):
    if not attrname.startswith("__"):
      value = getattr(obj, attrname)
      try:
        json.dumps(value)
        if attrname.startswith("_"+name+"__"):
          attrname = attrname[len(name)+3:] # remove class name for private attributes
        data[attrname] = value
      except:
        traceback.print_exc()
  return data

def get_client(clientid, *args):
  rc = 404; value = None
  client = broker3.broker.getClient(clientid)
  if client:
    value = json.dumps(jsonize(client))
    rc = 200
  return rc, value

def get_clients(*args):
  clientids = broker3.broker.getClients()
  clients = {}
  for clientid in clientids:
    clients[clientid] = jsonize(broker3.broker.getClient(clientid))
  return 200, json.dumps(clients)

def get_subscriptions(*args):
  return 200, json.dumps([jsonize(s) for s in sharedData["subscriptions"]])

def get_retained_messages(*args):
  out = {}
  for topic in sharedData["retained"].keys():
    value = list(sharedData["retained"][topic])
    value[0] = value[0].decode()
    out[topic] = value
  return 200, json.dumps(out)

class APIs:

  def __init__(self):
    self.gets = [
      ("/api/v0001/clients$", get_clients),   
      ("/api/v0001/clients/([^/]*)$", get_client),   
      ("/api/v0001/subscriptions$", get_subscriptions),  
      ("/api/v0001/retained$", get_retained_messages), 
      ]

    self.puts = [
      ]

    self.patches = [
      ]

    self.posts = [ 
      ]

    self.deletes = [
      ]

  def operation(self, name, url, body=None):
    logger.debug("Calling HTTP "+name+" "+url+(" with "+str(body) if (body != None) else ""))
    rc = (404, None)
    ops = eval("self."+name+("es" if name.endswith("ch") else "s"))
    found = False
    for op in ops:
      expr = re.compile(op[0])
      res = expr.match(url)
      if res:
        args = res.groups()
        if body:
          if args:
            args = tuple(list(args) + [body])
          else:
            args = (body,)
        rc = op[1](*args)
        found = True
        break
    if not found:
      logger.error("Request operation not found: %s %s %s" % (name, url, str(body)))
    logger.debug("Result "+str(rc))
    return rc

  def get(self, url):
    return self.operation("get", url)

  def put(self, url, body):
    return self.operation("put", url, body)

  def post(self, url, body):
    return self.operation("post", url, body)

  def patch(self, url, body):
    return self.operation("patch", url, body)

  def delete(self, url):
    return self.operation("delete", url)

api = APIs()

class requestHandler(http.server.BaseHTTPRequestHandler):

  #def __init__(self, request, client_address, server):
  #  BaseHTTPRequestHandler.__init__(self, request, client_address, server)

  def log_request(self, *args):
    # Redefing this method to remove logging of request
    #BaseHTTPRequestHandler.log_request(self, *args)
    pass

  def do_GET(self):
    logger.debug("do_GET")
    logger.info("GET %s", urllib.request.unquote(self.path))
    rc = api.get(urllib.request.unquote(self.path))
    value = None
    if type(rc) == type((0,)):
      rc, value = rc
    logger.debug("Response %s", str((rc, value)))
    self.send_response(rc)
    self.send_header("Content-type", "application/json")
    self.end_headers()
    if value:
      self.wfile.write(bytes(value + "\n", 'utf8'))

  def do_DELETE(self):
    logger.debug("do_DELETE")
    rc = http.delete(urllib.request.unquote(self.path))
    value = None
    if type(rc) == type((0,)):
      rc, value = rc
    self.send_response(rc)
    self.send_header("Content-type", "application/json")
    self.end_headers()
    if value:
      self.wfile.write(bytes(value + "\n", 'utf8'))

  def do_PATCH(self):
    logger.debug("do_PATCH")
    self.headers["Content-Disposition"] = 'inline; filename=""'
    form = cgi.FieldStorage(fp=self.rfile,
                            headers=self.headers,
                            environ={"REQUEST_METHOD": 'PATCH',
                                      "CONTENT_TYPE": self.headers['Content-Type'], })
    if self.headers["Content-Type"] == "application/json":
      if type(form.value) == type(b""):
        form.value = form.value.decode()
      body = json.loads(form.value)
    rc = http.patch(urllib.request.unquote(self.path), body)
    value = None
    if type(rc) == type((0,)):
      rc, value = rc
    self.send_response(rc)
    self.send_header("Content-type", "application/json")
    self.end_headers()
    if value:
      self.wfile.write(bytes(value + "\n", 'utf8'))
    if persistence:
      deviceAndThingDB._p_changed = True
      transaction.commit()

  def do_POST(self):
    logger.debug("do_POST")
    self.do_postput("post")

  def do_PUT(self):
    logger.debug("do_PUT")
    self.do_postput("put")

  def do_postput(self, op):
    # next line is workaround for bug: https://bugs.python.org/issue27308
    self.headers["Content-Disposition"] = 'inline; filename=""'
    form = cgi.FieldStorage(fp=self.rfile,
                            headers=self.headers,
                            environ={"REQUEST_METHOD": op.upper(),
                                      "CONTENT_TYPE": self.headers['Content-Type'], })
    body = {"Content-Type": self.headers['Content-Type']}
    if self.headers["Content-Type"] == "application/json":
      if type(form.value) == type(b""):
        form.value = form.value.decode()
      body = json.loads(form.value)
    elif type(form.value) == type([]):
      for key in form.keys():
        if key == "schemaFile":
          # a bit ugly, but everyone has to do it
          # retrieve filename from the Content-Disposition header of the "file" part
          body["schemaFileName"] = form[key].disposition_options["filename"]
          body["Content-Type"] = form[key].type
        body[key] = form[key].value
    rc = getattr(http, op)(self.path, body)
    value = None
    if type(rc) == type((0,)):
      rc, value = rc
    self.send_response(rc)
    self.send_header("Content-type", "application/json")
    self.end_headers()
    if value:
      self.wfile.write(bytes(value + "\n", 'utf8'))
    """
    if persistence:
      deviceAndThingDB._p_changed = True
      transaction.commit()
    """

def setBrokers(aBroker3, aBroker5, aBrokerSN):
  global broker3, broker5, brokerSN
  broker3 = aBroker3
  broker5 = aBroker5
  brokerSN = aBrokerSN

def setSharedData(aLock, aSharedData):
  global lock, sharedData
  lock = aLock
  sharedData = aSharedData

def create(port, host="", TLS=False, serve_forever=False,
    cert_reqs=ssl.CERT_REQUIRED,
    ca_certs=None, certfile=None, keyfile=None):
  logger.info("Starting HTTP listener on address '%s' port %d %s", host, port, "with TLS support" if TLS else "")
  bind_address = ""
  if host not in ["", "INADDR_ANY"]:
    bind_address = host
  httpd = http.server.HTTPServer((bind_address, port), requestHandler)
  if TLS:
    httpd.socket = ssl.wrap_socket(httpd.socket,
      ca_certs=ca_certs, certfile=certfile, keyfile=keyfile,
      cert_reqs=cert_reqs, server_side=True)
  if serve_forever:
    httpd.serve_forever()
  else:
    thread = threading.Thread(target = httpd.serve_forever)
    thread.daemon = True
    thread.start()
