"""
*******************************************************************
  Copyright (c) 2013, 2015 IBM Corp.
 
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
sockets = []

test = None

logger = logging.getLogger("MQTTV311_spec")
#logger.setLevel(logging.INFO)

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

	def __call__(self, sockid):
		logger.debug("*** running")
		self.running = True
		packet = None
		try:
			while True:		
				packet = MQTTV3.unpackPacket(MQTTV3.getPacket(sockets[sockid]))
				if packet == None:
					break
				if test:
					logger.debug("received result %s", (sockid, packet))
					test.addResult((sockid, packet))
					if packet.fh.MessageType == MQTTV3.CONNACK:
						self.packets.append(packet)
				else:
					mbt.observe((sockid, packet))
					if packet.fh.MessageType == MQTTV3.PUBREC:
						mbt.execution.pools["pubrecs"].append(mbt.Choices((sockid, packet)))
					elif packet.fh.MessageType == MQTTV3.PUBLISH and packet.fh.QoS in [1, 2]:
						mbt.execution.pools["publishes"].append(mbt.Choices((sockid, packet)))
					elif packet.fh.MessageType == MQTTV3.PUBREL:
						mbt.execution.pools["pubrels"].append(mbt.Choices((sockid, packet)))
					elif packet.fh.MessageType == MQTTV3.CONNACK:
						self.packets.append(packet)
		except:
			if sys.exc_info()[0] != socket.error:
				logger.debug("unexpected exception %s", sys.exc_info())
			#mbt.log(traceback.format_exc())
		self.running = False
		logger.debug("*** stopping "+str(packet))

mbt.model.maxobjects["socket"] = 2
clients = []
for i in range(mbt.model.maxobjects["socket"]):
	clients.append(Clients())
next_client = 0


"""
	Sockets are created in sequence -- we should use their id(), or a sequence number of them stored in a list.
	This will also avoid platform specific formats in the test logs. *****

"""
@mbt.action
def socket_create(hostname : "hostnames", port : "ports") -> "socket":
	global next_client
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sockets.append(sock)
	sockid = len(sockets) - 1
	sock.connect((hostname, port))
	id = _thread.start_new_thread(clients[next_client], (sockid,))
	clientlist[sockid] = clients[next_client]
	next_client = (next_client + 1) % mbt.model.maxobjects["socket"]
	return sockid


"""
	After socket_close, the socket object is not valid any more, so we need to indicate that it should be
	thrown away.
"""
@mbt.action
def socket_close(sockid : "socket"):
	sock = sockets[sockid]
	sock.shutdown(socket.SHUT_RDWR)
	sock.close()

mbt.finishedWith(socket_close, "sockid")
	

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
def connect(sockid : "socket", clientid : "clientids", cleansession : "boolean", #willmsg : "willmsgs",
#	    username : "usernames", password : "passwords"
) -> "connackrc":
	sock = sockets[sockid]
	connect = MQTTV3.Connects()
	connect.ClientIdentifier = clientid
	connect.CleanSession = cleansession
	connect.KeepAliveTimer = 60
	#if username:
	#	self.usernameFlag = True
	#	self.username = username
	#if password:
	#	self.passwordFlag = True
	#	self.password = password
	sock.send(connect.pack())	
	time.sleep(0.5)
	checksocket(sockid)
	response = clientlist[sockid].packets.pop(0) #MQTTV3.unpackPacket(MQTTV3.getPacket(sock))
	logger.debug("+++connect response", response)
	if response == None or response.returnCode not in [0, 2]:
		raise Exception("Return code "+str(response.returnCode)+" in connack")

	return response.returnCode

def checksocket(sockid):
	"""
	Check that the socket is still open - has not been closed
	Throw an execption if the socket is not connected
	"""
	time.sleep(0.1) # allow the broker to close the connection if necessary
	sockets[sockid].getpeername() # throws an exception if the socket is not connected
	#if sockid not in clientlist.keys():
	#	raise Exception("Must have been socket error")

@mbt.action
def disconnect(sockid : "socket"):
	sock = sockets[sockid]
	disconnect = MQTTV3.Disconnects()
	sock.send(disconnect.pack())
	checksocket(sockid)
	#time.sleep(0.2)


@mbt.action
def subscribe(sockid : "socket", topics : "topicLists", qoss : "qosLists"):
	sock = sockets[sockid]
	subscribe = MQTTV3.Subscribes()
	subscribe.messageIdentifier = clientlist[sockid].getNextMsgid()
	count = 0
	for t in topics:
		subscribe.data.append((t, qoss[count]))
		count += 1
	sock.send(subscribe.pack())
	checksocket(sockid)
	return subscribe.messageIdentifier


@mbt.action
def unsubscribe(sockid : "socket", topics : "topicLists"):
	sock = sockets[sockid]
	unsubscribe = MQTTV3.Unsubscribes()
	unsubscribe.messageIdentifier = clientlist[sockid].getNextMsgid()
	unsubscribe.data = topics
	sock.send(unsubscribe.pack())
	checksocket(sockid)
	return unsubscribe.messageIdentifier


@mbt.action
def publish(sockid : "socket", topic : "topics", payload : "payloads", qos : "QoSs", retained : "boolean"):
	sock = sockets[sockid]
	publish = MQTTV3.Publishes()
	publish.fh.QoS = qos
	publish.fh.RETAIN = retained
	if qos == 0:
		publish.messageIdentifier = 0
	else:
		publish.messageIdentifier = clientlist[sockid].getNextMsgid()
	publish.topicName = topic
	publish.data = payload
	sock.send(publish.pack())
	checksocket(sockid)
	return publish.messageIdentifier


@mbt.action
def pubrel(pubrec : "pubrecs"): # pubrecs are observable events
	sockid, pubrec = pubrec
	sock = sockets[sockid]
	pubrel = MQTTV3.Pubrels()
	pubrel.messageIdentifier = pubrec.messageIdentifier
	sock.send(pubrel.pack())

mbt.finishedWith(pubrel, "pubrec")

@mbt.action
def puback(publish : "publishes"):
	sockid, publish = publish
	sock = sockets[sockid]
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
	sockid, pubrel = pubrel
	sock = sockets[sockid]
	pubcomp = MQTTV3.Pubcomps()
	pubcomp.messageIdentifier = pubrel.messageIdentifier
	sock.send(pubcomp.pack())

mbt.finishedWith(pubcomp, "pubrel")

def pingreq():
	pingreq = MQTTV3.Pingreqs()
	sockets[0].send(pingreq.pack())


"""
 choice lists should be ordered but unique - ordered sets
   options: 
   sequenced - add sequence number
   frequency of choices (somehow)


"""

mbt.choices("boolean", (True, False))

mbt.choices("hostnames", ("localhost",))
mbt.choices("ports", (1883,))
mbt.choices("clientids", ("", "normal", "23 characters4567890123", 
               "A clientid that is longer than 23 chars - should work in 3.1.1"))

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

def restart():
	global clients, next_client, sockets
	clients = []
	for i in range(mbt.model.maxobjects["socket"]):
		clients.append(Clients())
	next_client = 0
	for i in range(len(sockets)):
		sockets[i].close() # just to make sure 
	while len(sockets) > 0:
		del sockets[0]

mbt.model.restartCallback = restart	


def between(str, str1, str2):
  start = str.find(str1)+len(str1)
  end = str.find(str2, start)
  if end == -1:
    rc = str[start:]
  else:
    rc = str[start:end]
  return rc


def replace(str, str1, str2, replace_str):
  rc = str
  start = str.find(str1)+len(str1)
  end = str.find(str2, start)
  if start != -1 and end != -1:
    rc = str[:start] + replace_str + str[end:]
  return rc
  

def observationCheckCallback(observation, results):
	# results is a list of tuples (str(observation), observation)
	# observation will be string representation of (socket, packet)
	if (observation.find("Publishes(") != -1 and observation.find("MsgId=") != -1) or observation.find("Pubrels(") != -1:
		# look for matches in everything but MsgId
		endchar = ")" if observation.find("Pubrels") != -1 else ","
		changed_observation = replace(observation, "MsgId=", endchar, "000")
		for k in [x for x, y in results]:
			if changed_observation == replace(k, "MsgId=", endchar, "000"):
				logger.debug("observation found")
				return k
		return None
	else:	
		return observation if observation in [x for x, y in results] else None

hostname = None
port = None

def callCallback(action, kwargs):
	if action.getName() == "socket_create" and (hostname or port):
		for parm in kwargs.keys():
			if parm == "hostname":
				kwargs[parm] = hostname
			elif parm == "port":
				kwargs[parm] = port
	return action, kwargs

if __name__ == "__main__":
	stepping = False
	if len(sys.argv) > 1:
		stepping = True

	mbt.run(stepping=stepping)


