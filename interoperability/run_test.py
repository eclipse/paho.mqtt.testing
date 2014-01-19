import mbt, sys, mqtt

import MQTTV311_spec 

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


if __name__ == "__main__":
  testname = sys.argv[1]
  checks = {"socket": socket_check, "exception": exception_check}
  MQTTV311_spec.test = mbt.Tests(mbt.model, "tests/"+testname, checks, observationMatchCallback=MQTTV311_spec.observationCheckCallback)
  MQTTV311_spec.test.run(stepping=False)
