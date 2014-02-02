import mbt, sys, mqtt, glob, time

import MQTTV311_spec, client_test

def socket_check(a, b):
	# <socket.socket object, fd=3, family=2, type=1, proto=0>
	awords = str(a).split()
	del awords[2]
	astr = ''.join(awords)
	bwords = str(b).split()
	del bwords[2]
	bstr = ''.join(bwords)
	print("checking sockets", astr, "and", bstr)
	return astr == bstr

def exception_check(a, b):
	return True

def cleanup():

	# clean all client state
	clientids = ("", "normal", "23 characters4567890123", "A clientid that is too long - should fail")
	hostname = "localhost" #"9.20.87.54"
	port = 1883 #18883

	for clientid in clientids:
		aclient = mqtt.client.Client("myclientid".encode("utf-8"))
		aclient.connect(host=hostname, port=port, cleansession=True)
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

	MQTTV311_spec.client.__init__()


if __name__ == "__main__":
	if len(sys.argv) > 1:
		testnames = [sys.argv[1]]
	else:
		testnames = glob.glob("tests/*")

	cleanup()
	for testname in testnames:
		checks = {"socket": socket_check, "exception": exception_check}
		testname = testname if testname.startswith("tests") else "tests/"+testname
		MQTTV311_spec.test = mbt.Tests(mbt.model, testname, checks, observationMatchCallback=MQTTV311_spec.observationCheckCallback)
		MQTTV311_spec.test.run(stepping=False)
		cleanup()

