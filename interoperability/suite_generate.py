"""

1. Start the model broker.  Clean state.

2. Run the spec, one step at a time until conformance statement hit.

3. Reduce sequence to shortest.

4. Store.


Repeat until all conformance statements hit.

Do store tests that reach a conformance statement in a different way.

"""

import os, shutil, subprocess, MQTTV3_spec

def create():
	broker = subprocess.Popen(["python", "pybroker/startBroker.py"], stderr=subprocess.PIPE)
	output = broker.stderr
	conformances = []
	print(output.readline().decode("utf-8"))
	restart = False
	while not restart:
		print("stepping\n")
		restart = MQTTV3_spec.mbt.step()
		print("stepped\n")
		data = output.readline().decode("utf-8")
		data = data.split(" ", 3) if data else data
		print("data", ' '.join(data))
		while data and data[3] != "Waiting for request\n" and data[3][:9] != "Finishing":
			if data[3].startswith("[MQTT"):
				print("Conformance statement ", data[3])
				conformances.append(data[3])
			data = output.readline().decode("utf-8")
			data = data.split(" ", 3) if data else data
			print("data", ' '.join(data))
		#if input("--->") == "q":
		#		return
	broker.terminate()
	return conformances


if __name__ == "__main__":
	#try:
	os.system("kill `ps -ef | grep startBroker | grep -v grep | awk '{print $2}'`")
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
