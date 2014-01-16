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

import mbt, socket, time, _thread, sys, traceback, pdb, select, random, mqtt, logging

import mqtt.formats.MQTTV311 as MQTTV3

clientlist = {}

test = None

logger = logging.getLogger("MQTTV311_spec")
logger.setLevel(logging.DEBUG)

class Clients:
	
	def __init__(self):
		self.msgid = 1
		self.running = False
		self.packets = []

	def getNextMsgid(self):
		def getWrappedMsgid():
			id = self.msgid + 1
			if id == 65535:
				id = 1
			return id
		self.msgid = getWrappedMsgid()
		return self.msgid

	def __call__(self, sock):
		logger.debug("*** running")
		clientlist[sock] = self
		self.running = True
		try:
			while True:		
				packet = MQTTV3.unpackPacket(MQTTV3.getPacket(sock))
				if packet == None:
					break
				if test:
					logger.debug("received result %s", (sock, packet))
					test.addResult((sock, packet))
				else:
					mbt.observe((sock, packet))
					if packet.fh.MessageType == MQTTV3.PUBREC:
						mbt.execution.pools["pubrecs"].append(mbt.Choices((sock, packet)))
					elif packet.fh.MessageType == MQTTV3.PUBLISH and packet.fh.QoS in [1, 2]:
						mbt.execution.pools["publishes"].append(mbt.Choices((sock, packet)))
					elif packet.fh.MessageType == MQTTV3.PUBREL:
						mbt.execution.pools["pubrels"].append(mbt.Choices((sock, packet)))
					elif packet.fh.MessageType == MQTTV3.CONNACK:
						self.packets.append(packet)
		except:
			if sys.exc_info()[0] != socket.error:
				logger.debug("unexpected exception %s", sys.exc_info())
			#mbt.log(traceback.format_exc())
		self.running = False
		del clientlist[sock]
		logger.debug("*** stopping "+str(packet))

client = Clients()


@mbt.action
def socket_create(hostname : "hostnames", port : "ports") -> "socket":
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock.connect((hostname, port))
	id = _thread.start_new_thread(client, (sock,))
	return sock


"""
	After socket_close, the socket object is not valid any more, so we need to indicate that it should be
	thrown away.
"""
@mbt.action
def socket_close(sock : "socket"):
	sock.shutdown(socket.SHUT_RDWR)
	sock.close()

mbt.finishedWith(socket_close, "sock")
	

"""
	protocol name           valid, invalid
	protocol version        valid, invalid
	clientID	        lengths 0, 1, 22, 23; characters?
	cleansession	        true, false
	will: topic, message, qos, retained
	keepAlive                0, 60, 
	username                 None, 
	password                 None
"""
@mbt.action
def connect(sock : "socket", clientid : "clientids", cleansession : "boolean", #willmsg : "willmsgs",
#	    username : "usernames", password : "passwords"
) -> "connackrc":
	connect = MQTTV3.Connects()
	connect.ClientIdentifier = clientid
	connect.CleanStart = cleansession
	connect.KeepAliveTimer = 60
	#if username:
	#	self.usernameFlag = True
	#	self.username = username
	#if password:
	#	self.passwordFlag = True
	#	self.password = password
	sock.send(connect.pack())	
	time.sleep(0.1)
	response = clientlist[sock].packets.pop(0) #MQTTV3.unpackPacket(MQTTV3.getPacket(sock))
	logger.debug("+++connect response", response)
	if response == None or response.returnCode not in [0, 2]:
		raise Exception("Return code "+str(response.returnCode)+" in connack")

	#id = _thread.start_new_thread(client, (sock,))
	return response.returnCode

def checksocket(sock):
	time.sleep(0.1)
	if sock not in clientlist.keys():
		raise Exception("Must have been socket error")

@mbt.action
def disconnect(sock : "socket"):
	disconnect = MQTTV3.Disconnects()
	sock.send(disconnect.pack())
	checksocket(sock)
	#time.sleep(0.2)


@mbt.action
def subscribe(sock : "socket", topics : "topicLists", qoss : "qosLists"):
	subscribe = MQTTV3.Subscribes()
	subscribe.messageIdentifier = client.getNextMsgid()
	count = 0
	for t in topics:
		subscribe.data.append((t, qoss[count]))
		count += 1
	sock.send(subscribe.pack())
	checksocket(sock)
	return subscribe.messageIdentifier


@mbt.action
def unsubscribe(sock : "socket", topics : "topicLists"):
	unsubscribe = MQTTV3.Unsubscribes()
	unsubscribe.messageIdentifier = client.getNextMsgid()
	unsubscribe.data = topics
	sock.send(unsubscribe.pack())
	checksocket(sock)
	return unsubscribe.messageIdentifier


@mbt.action
def publish(sock : "socket", topic : "topics", payload : "payloads", qos : "QoSs", retained : "boolean"):
	publish = MQTTV3.Publishes()
	publish.fh.QoS = qos
	publish.fh.RETAIN = retained
	if qos == 0:
		publish.messageIdentifier = 0
	else:
		publish.messageIdentifier = client.getNextMsgid()
	publish.topicName = topic
	publish.data = payload
	sock.send(publish.pack())
	checksocket(sock)
	return publish.messageIdentifier


@mbt.action
def pubrel(pubrec : "pubrecs"): # pubrecs are observable events
	sock, pubrec = pubrec
	pubrel = MQTTV3.Pubrels()
	pubrel.messageIdentifier = pubrec.messageIdentifier
	sock.send(pubrel.pack())

mbt.finishedWith(pubrel, "pubrec")

@mbt.action
def puback(publish : "publishes"):
	sock, publish = publish
	if publish.fh.QoS == 1:
		puback = MQTTV3.Pubacks()
		puback.messageIdentifier = publish.messageIdentifier
		sock.send(puback.pack())
	elif publish.fh.QoS == 2:
		pubrec = MQTTV3.Pubrecs()
		pubrec.messageIdentifier = publish.messageIdentifier
		sock.send(pubrec.pack())
		
mbt.finishedWith(puback, "publish")		

@mbt.action
def pubcomp(pubrel : "pubrels"):
	sock, pubrel = pubrel
	pubcomp = MQTTV3.Pubcomps()
	pubcomp.messageIdentifier = pubrel.messageIdentifier
	sock.send(pubcomp.pack())

mbt.finishedWith(pubcomp, "pubrel")

def pingreq():
	pingreq = MQTTV3.Pingreqs()
	sock.send(pingreq.pack())


#print(mbt.model.getActionNames())

"""
 choice lists should be ordered but unique - ordered sets
   options: 
   sequenced - add sequence number
   frequency of choices (somehow)


"""

mbt.choices("boolean", (True, False))

mbt.choices("hostnames", ("localhost",))
mbt.choices("ports", (1883,))
mbt.choices("clientids", ("", "normal", "23 characters4567890123", "A clientid that is too long - should fail"))

topics =  ("TopicA", "TopicA/B", "Topic/C", "TopicA/C", "/TopicA")
wildTopics =  ("TopicA/+", "+/C", "#", "/#", "/+", "+/+")

mbt.choices("topics", topics)
mbt.choices("QoSs", (0, 1, 2))

mbt.choices("topicLists", [(t,) for t in topics + wildTopics])
mbt.choices("qosLists", [(0,), (1,), (2,)])


mbt.choices("payloads", (b"", b"1", b"333", b"long"*512), sequenced=True)

mbt.choices("connackrc", (0, 2), output=True)

mbt.model.addReturnType("pubrecs")
mbt.model.addReturnType("pubrels")
mbt.model.addReturnType("publishes")

mbt.model.maxobjects["socket"] = 1

last_free_names = set()
after_socket_create = set()

def select(frees):
	global last_free_names, after_socket_create
	free_names = set([f[0].getName() for f in frees])
	logger.debug("*** after_socket_create %s %s", after_socket_create, last_free_names)
	if last_free_names == set(['socket_create']):
		diff = set(free_names).difference(after_socket_create)
		logger.debug("*** diff %s", diff)
		if diff == set():
			frees = [f for f in frees if f[0].getName() == "connect"]
		else:
			curname = random.choice(list(diff))
			frees = [f for f in frees if f[0].getName() == curname]
			after_socket_create.add(curname)
	else:	
		for f in frees:
			if f[0].getName() in ["pubrel", "puback", "pubcomp"]:
				frees = [f]
				break
	last_free_names = free_names
	return frees

mbt.model.selectCallback = select


"""
stepping = False
if len(sys.argv) > 1:
	stepping = True

#mbt.run(stepping=stepping)


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

checks = {"socket": socket_check, "exception": exception_check}

test = mbt.Tests(mbt.model, "spec.log", checks)

test.run(stepping=False)

"""

