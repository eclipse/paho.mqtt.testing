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

import inspect, logging

from . import *
from ..formats import *

logger = logging.getLogger('MQTT broker')

modules = [m for m in locals().values() if inspect.ismodule(m) and m.__name__.startswith("mqtt.")]


"""

    assert self.messageIdentifier > 0, "[MQTT-2.3.1-1] packet indentifier must be > 0"
    logger.info("[MQTT-3.9.3-1] the order of return codes must match order of topics in subscribe")

"""


def between(str, str1, str2):
  start = str.find(str1)+len(str1)
  end = str.find(str2, start)
  if end == -1:
    rc = str[start:]
  else:
    rc = str[start:end]
  return rc


def getSources(anObject, depth=0):
  members = [m for n, m in inspect.getmembers(anObject) \
              if inspect.isclass(m) or inspect.isfunction(m) or inspect.ismethod(m)]
  total = []
  for member in members:
    if inspect.isfunction(member) or inspect.ismethod(member):
      lines = inspect.getsourcelines(member)[0]
      total += lines
    elif inspect.ismodule(member) or inspect.isclass(member) and depth < 10:
      total += getSources(member, depth+1)
  return total


def getCoverage():

  exceptions = set([])
  coverages = set([])

  for module in modules:
    lines = getSources(module)
    for line in lines:
      line = line.strip()
      if line.find("[MQTT") != -1:
        statement = "[MQTT"+between(line, "[MQTT", "]")+"]"
        if line.startswith("assert") or line.startswith("raise"):
          exceptions.add(statement)
        else:
          coverages.add(statement)
  return ({"exceptions" : exceptions, "coverages" : coverages})

class Handlers(logging.Handler):

  def __init__(self):
    logging.Handler.__init__(self)
    self.coverages = getCoverage()
    self.found = set([])
    #self.results = {}

  def emit(self, record):
    line = record.message
    if line.find("[MQTT") != -1:
      statement = "[MQTT"+between(line, "[MQTT", "]")+"]"
      self.found.add(statement)

  def measure(self):
    for key in self.coverages.keys():
       found = self.coverages[key].intersection(self.found)
       logger.info("%s %d out of %d = %d%%" % \
          ("coverage statements" if key == "coverages" else key,
               len(found), len(self.coverages[key]), (len(found) * 100) / len(self.coverages[key])))

    for key in self.coverages.keys():
       found = self.coverages[key].intersection(self.found)
       notfound = self.coverages[key].difference(self.found)
       logger.info("%s found %s" % ("coverage statements" if key == "coverages" else key, found))
       logger.info("%s not found %s" % ("coverage statements" if key == "coverages" else key, notfound))
    #print self.results

handler = Handlers()

def measure():
  handler.measure()


