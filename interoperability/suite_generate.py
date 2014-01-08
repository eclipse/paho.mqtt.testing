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

"""

1. Start the model broker.  Clean state.

2. Run the spec, one step at a time until conformance statement hit.

3. Reduce sequence to shortest.

4. Store.


Repeat until all conformance statements hit.

Do store tests that reach a conformance statement in a different way.

"""

import os, shutil, subprocess, time, MQTTV311_spec

output = None

def create():
	global output
	if not output:
		broker = subprocess.Popen(["python3", "startbroker.py"], stderr=subprocess.PIPE)
		output = broker.stderr
		print(output.readline().decode("utf-8"))
	conformances = []
	restart = False
	while not restart:
		print("stepping\n")
		restart = MQTTV311_spec.mbt.step()
		print("stepped\n")
		data = output.readline().decode("utf-8")
		#data = data.split(" ", 3) if data else data
		print("data", data)
		while data and data.find("Waiting for request") == -1 and data.find("Finishing communications") == -1:
			if data.find("[MQTT") != -1:
				data = data.split(" ", 3) if data else data
				print("Conformance statement ", data[3])
				conformances.append(data[3])
			data = output.readline().decode("utf-8")
			print("data", data)
		#if input("--->") == "q":
		#		return
	#broker.terminate()
	return conformances


if __name__ == "__main__":
	#try:
	os.system("kill `ps -ef | grep startbroker | grep -v grep | awk '{print $2}'`")
	os.system("rm -rf tests")
	#except:
	#	pass
	os.mkdir("tests")
	test_no = 0
	
	while True:
		test_no += 1
		conformance_statements = create()
		print("Test created", conformance_statements)

		#MQTTV3_spec.mbt.log.file.close()

		# now tests/test.%d has the test
		filename = "tests/test.log.%d" % (test_no,)
		infile = open(filename)
		lines = infile.readlines()
		infile.close()
		outfile = open(filename, "w")
		outfile.writelines(conformance_statements + lines)
		#print(conformance_statements + lines)
		outfile.close()
		#MQTTV3_spec.mbt.log = open("test.log", "w")
	
		#shorten()
		#store()
