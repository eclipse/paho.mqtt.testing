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

import mbt, sys, mqtt, glob, time, logging, getopt, os

import MQTTV311_spec, client_test

def socket_check(a, b):
	print("socket_check", a, b)
	# <socket.socket object, fd=3, family=2, type=1, proto=0>
	awords = str(a).split()
	del awords[2]
	astr = ''.join(awords)
	bwords = str(b).split()
	del bwords[2]
	bstr = ''.join(bwords)
	#print("checking sockets", astr, "and", bstr)
	return astr == bstr

def exception_check(a, b):
	return True

def cleanup(hostname="localhost", port=1883):
	#print("Cleaning up broker state")
	# clean all client state
	clientids = ("", "normal", "23 characters4567890123", "A clientid that is too long - should fail", 
                 "A clientid that is longer than 23 chars - should work in 3.1.1")

	for clientid in clientids:
		aclient = mqtt.client.Client(clientid.encode("utf-8"))
		try:
			aclient.connect(host=hostname, port=port, cleansession=True)
		except:
			pass
		time.sleep(.1)
		aclient.disconnect()
		time.sleep(.1)

	# clean retained messages 
	callback = client_test.Callbacks()
	aclient = mqtt.client.Client("clean retained".encode("utf-8"))
	aclient.registerCallback(callback)
	aclient.connect(host=hostname, port=port, cleansession=True)
	aclient.subscribe(["#"], [0])
	time.sleep(2) # wait for all retained messages to arrive
	for message in callback.messages:  
		if message[3]: # retained flag
		  aclient.publish(message[0], b"", 0, retained=True)
	aclient.disconnect()
	time.sleep(.1)

	MQTTV311_spec.restart()
	#print("Finished cleaning up")

def usage():
	print(
"""Options: testname, testdir|testdirectory, hostname, port

		A testname or test directory must be specified.

""")


if __name__ == "__main__":
	try:
		opts, args = getopt.gnu_getopt(sys.argv[1:], "t:d:h:p:", ["testname=", "testdir=", "testdirectory=", "hostname=", "port="])
	except getopt.GetoptError as err:
		print(err) # will print something like "option -a not recognized"
		usage()
		sys.exit(2)

	testname = testdirectory = None
	hostname = "localhost"
	port = 1883
	for o, a in opts:
		if o in ("--help"):
			usage()
			sys.exit()
		elif o in ("-t", "--testname"):
			testname = a
		elif o in ("-s", "--testdir", "--testdirectory"):
			testdirectory = a
		elif o in ("-h", "--hostname"):
			hostname = MQTTV311_spec.hostname = a
		elif o in ("-p", "--port"):
			port = MQTTV311_spec.port = int(a)
		else:
			assert False, "unhandled option"

	if testname:
		testnames = [testname]
	elif testdirectory:
		testnames = [name for name in glob.glob(testdirectory+os.sep+"*") if not name.endswith("~")]
	else:
		usage()
		sys.exit()

	testnames.sort(key=lambda x: int(x.split(".")[-1])) # filename index order
	for testname in testnames:
		cleanup(hostname, port)
		#checks = {"socket": socket_check, "exception": exception_check}
		checks = {"exception": exception_check}
		MQTTV311_spec.test = mbt.Tests(mbt.model, testname, checks, 
				observationMatchCallback = MQTTV311_spec.observationCheckCallback,
				callCallback = MQTTV311_spec.callCallback)
		MQTTV311_spec.test.run(stepping=False)


