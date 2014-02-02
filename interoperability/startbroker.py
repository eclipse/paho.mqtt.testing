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

import mqtt.broker, sys, logging

if __name__ == "__main__":
  formatter = logging.Formatter(fmt='%(levelname)s %(asctime)s %(message)s',  datefmt='%Y%m%d %H%M%S')
  ch = logging.StreamHandler()
  ch.setFormatter(formatter)
  ch.setLevel(logging.INFO)
  broker_logger = logging.getLogger('MQTT broker')
  broker_logger.addHandler(ch)
  broker_logger.propagate = False # don't pass log entries up to the root logger 
  mqtt.broker.main(sys.argv)
