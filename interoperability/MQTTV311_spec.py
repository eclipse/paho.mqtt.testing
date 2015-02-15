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

import mbt, socket, time, _thread, sys, traceback, pdb, select, random, mqtt, logging, copy

import mqtt.formats.MQTTV311 as MQTTV3

test = None

logger = logging.getLogger("MQTTV311_spec")
#logger.setLevel(logging.INFO)

class Clients:
	
	def __init__(self):
		self.msgid = 0
		self.running = False
		self.packets = []
		
	def __eq__(self, aclient):
		try:
			assert self.msgid == aclient.msgid
			assert self.running == aclient.running
			assert self.packets == aclient.packets
			return True
		except:
			return False

	def getNextMsgid(self):
		def getWrappedMsgid():
			id = self.msgid + 1
			if id == 65535:
				id = 1
			return id
		self.msgid = getWrappedMsgid()
		return self.msgid
	
	def getPacket(self, sockid):
		packet = MQTTV3.unpackPacket(MQTTV3.getPacket(state.sockets[sockid]))
		if packet == None:
			return
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
		return packet
	
	def getResponses(self, sockid):
		# used in generation mode to get responses from broker
		while len(state.sockets[sockid].buffer) > 0:
			self.getPacket(sockid) 

	def __call__(self, sockid):
		logger.debug("*** running")
		print("**** running", id(self))
		self.running = True
		packet = None
		try:
			while True:		
				packet = self.getPacket(sockid)
		except:
			if sys.exc_info()[0] != socket.error:
				logger.debug("unexpected exception %s", sys.exc_info())
		self.running = False
		logger.debug("*** stopping "+str(packet))
		print("*** stopping "+str(packet))

mbt.model.maxobjects["socket"] = 2

class States:

	def __init__(self):
		self.set()
		self.broker = None
		if not test:
			broker_logger = logging.getLogger('MQTT broker')
			broker_logger.setLevel(logging.INFO)
			broker_logger.addHandler(mqtt.broker.coverage.handler)
			self.broker = mqtt.broker.MQTTBrokers() # add parameters later
		self.clientlist = {} # map of sockid to Clients objects
		
	def set(self):
		self.sockets = [] # list of ClientSockets objects
		self.clients = [] # list of Clients objects
		for i in range(mbt.model.maxobjects["socket"]):
			self.clients.append(Clients())
		self.next_client = 0
		self.last_free_names = set()         # used in selection callback
		self.after_socket_create = set()     # used in selection callback
		
	def reset(self):
		for i in range(len(self.sockets)):
			self.sockets[i].close() # just to make sure 
		self.set()
		if self.broker:
			self.broker.reinitialize()
			

	def setState(self, aState):
		"""
			When we set the state, we have to make sure the references between the objects are correct.	
			
		"""
		"""
		self.broker.setState(aState.broker)
		
		for c in self.clients:
			c.setState(aState.clients)
		
		self.clientlist = copy.copy(aState.clientlist)
		
		self.sockets = copy.copy(aState.sockets)
		
		self.next_client = aState.next_client
		self.last_free_names = copy.copy(aState.last_free_names)
		self.after_socket_create = copy.copy(aState.after_socket_create)
		assert self == aState
		"""
		pass
		
	def __eq__(self, aState):
		try:
			assert self.next_client == aState.next_client
			assert self.last_free_names == aState.last_free_names
			assert self.after_socket_create == aState.after_socket_create 
			assert self.sockets == aState.sockets
			assert self.clients == aState.clients
			assert self.clientlist == aState.clientlist
			assert self.broker == aState.broker
		except:
			raise
		return True
				
state = States()

def getState():
	global state
	return copy.deepcopy(state) 

def setState(aState):
	global state
	state = copy.deepcopy(aState)

def restart():
	global state
	state.reset()


mbt.model.getState = getState
mbt.model.setState = setState	
mbt.model.restartCallback = restart	

"""
	Wrap sockets so that in the case of test generation we do not use sockets but a buffer, to allow back tracking.
"""
class BrokerSockets:

	def __init__(self, aClientSocket):
		self.buffer = b""
		self.clientSocket = aClientSocket
		self.sending = True
		
	def __hash__(self):
		return id(self)
		
	def __eq__(self, a):
		try:
			assert self.buffer == a.buffer
			#assert self.clientSocket == a.clientSocket
			assert id(self.clientSocket.brokerSocket) == id(self)
			assert self.sending == a.sending
			return True
		except:
			traceback.print_exc()
			raise
			return False

	def recv(self, length):
		#while len(self.buffer) < length:
		#	print("broker waiting", length, self.buffer)
		#	time.sleep(.1)
		data = self.buffer[:length]
		self.buffer = self.buffer[length:]
		return data

	def send(self, data):
		if self.sending:
			self.clientSocket.buffer += data

	def shutdown(self, mode):
		self.sending = False

	def close(self):
		pass # don't close the client socket - to match real socket behaviour

	#def __getattr__(self, name):
	#	print("requesting broker attribute", name)
	#	raise AttributeError

	
class ClientSockets:

	def __init__(self, real=False):
		global state, test
		if test:
			real = True
		self.real = real
		self.sock = None
		if self.real:
			self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		else:
			self.connected = False
			self.failurePending = False
			self.buffer = b""
			self.brokerSocket = BrokerSockets(self)
			
	def __eq__(self, a):
		try:
			assert self.real == a.real
			assert self.connected == a.connected
			assert self.failurePending == a.failurePending
			assert self.buffer == a.buffer
			assert self.brokerSocket == a.brokerSocket
			assert id(self.brokerSocket.clientSocket) == id(self)
			return True
		except:
			raise
			return False
		
	def connect(self, destination):
		if self.real:
			self.sock.connect(destination)
		else:
			self.connected = True	 

	def recv(self, length):
		if self.real:
			return self.sock.recv(length)
		else:
			while len(self.buffer) < length:
				time.sleep(.1)
			data = self.buffer[:length]
			self.buffer = self.buffer[length:]
			return data

	def send(self, data):
		if self.real:
			return self.sock.send(data)
		else:
			if self.failurePending:
				self.connected = False
				self.failurePending = False
			if not self.connected:
				raise Exception("not connected")
			self.brokerSocket.buffer += data
			try:
				global state
				state.broker.handleRequest(self.brokerSocket)
				response = self.client.getResponses(self.id)
			except:
				self.failurePending = True # only indicate connection failure next time around, like a real socket
			return len(data)

	def shutdown(self, mode):
		if self.real:
			self.sock.shutdown(mode)

	def close(self):
		if self.real:
			self.sock.close()
		else:
			try:
				global state
				if state.broker:
					state.broker.handleRequest(self.brokerSocket)	# to cause the client to be disconnected
			except:
				traceback.print_exc()
		self.buffer = b""
		self.connected = False

	def getpeername(self):
		if self.real:
			return self.sock.getpeername()
		else:
			if self.connected:
				return "peer"
			else:
				raise Exception("not connected")

	#def __getattr__(self, name):
	#	print("attribute", name)
	#	raise AttributeError

"""
	Sockets are created in sequence -- we use a sequence number of them stored in a list.
	This will also avoid platform specific formats in the test logs. *****

"""
@mbt.action
def socket_create(hostname : "hostnames", port : "ports", **kwargs) -> "socket":
	global state
	#sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock = ClientSockets()
	state.sockets.append(sock)
	sockid = len(state.sockets) - 1 # starts with 0
	sock.connect((hostname, port))
	if test:
		print("starting client thread")
		thread_id = _thread.start_new_thread(state.clients[state.next_client], (sockid,))
	else:
		sock.client = state.clients[state.next_client]
		sock.id = sockid
	state.clientlist[sockid] = state.clients[state.next_client]
	state.next_client = (state.next_client + 1) % mbt.model.maxobjects["socket"]
	return sockid


"""
	After socket_close, the socket object is not valid any more, so we need to indicate that it should be
	thrown away.
"""
@mbt.action
def socket_close(sockid : "socket", **kwargs):
	global state
	sock = state.sockets[sockid]
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
**kwargs) -> "connackrc":
	global state
	sock = state.sockets[sockid]
	connect = MQTTV3.Connects()
	connect.ClientIdentifier = clientid
	connect.CleanSession = cleansession
	connect.KeepAliveTimer = 60
	#if username:
	#	connect.usernameFlag = True
	#	connect.username = username
	#if password:
	#	connect.passwordFlag = True
	#	connect.password = password
	#if willmsg:
	#	connect.willFlag = True
	#   connect.WillQoS = 0
    #	connect.WillRETAIN = 0
	#   connect.WillTopic = None        # UTF-8
	#	connect.WillMessage = None      # binary
	sock.send(connect.pack())
	if test:
		time.sleep(0.5)
	checksocket(sockid)
	if test:
		response = state.clientlist[sockid].packets.pop(0)
	else:
		response = None
		if len(sock.client.packets) > 0:
			response = sock.client.packets.pop(0)
	logger.debug("+++connect response", response)
	if response == None:
		raise Exception("No connack received") # can happen if second connect sent, for instance 
	if response.returnCode not in [0, 2]:
		raise Exception("Return code "+str(response.returnCode)+" in connack")

	return response.returnCode

def checksocket(sockid):
	"""
	Check that the socket is still open - has not been closed
	Throw an execption if the socket is not connected
	"""
	global state, test
	if test:
		time.sleep(0.1) # allow the broker to close the connection if necessary
	state.sockets[sockid].getpeername() # throws an exception if the socket is not connected
	#if sockid not in clientlist.keys():
	#	raise Exception("Must have been socket error")

@mbt.action
def disconnect(sockid : "socket", **kwargs):
	global state
	sock = state.sockets[sockid]
	disconnect = MQTTV3.Disconnects()
	sock.send(disconnect.pack())
	checksocket(sockid)
	#time.sleep(0.2)


@mbt.action
def subscribe(sockid : "socket", packetid : "packetids", topics : "topicLists", qoss : "qosLists", **kwargs):
	global state
	sock = state.sockets[sockid]
	subscribe = MQTTV3.Subscribes()
	subscribe.messageIdentifier = packetid
	count = 0
	for t in topics:
		subscribe.data.append((t, qoss[count]))
		count += 1
	sock.send(subscribe.pack())
	checksocket(sockid)
	return subscribe.messageIdentifier


@mbt.action
def unsubscribe(sockid : "socket", packetid : "packetids", topics : "topicLists", **kwargs):
	global state
	sock = state.sockets[sockid]
	unsubscribe = MQTTV3.Unsubscribes()
	unsubscribe.messageIdentifier = packetid
	unsubscribe.data = topics
	sock.send(unsubscribe.pack())
	checksocket(sockid)
	return unsubscribe.messageIdentifier


@mbt.action
def publish(sockid : "socket", packetid : "packetids", topic : "topics", payload : "payloads", qos : "QoSs", retained : "boolean", **kwargs):
	global state
	sock = state.sockets[sockid]
	publish = MQTTV3.Publishes()
	publish.fh.QoS = qos
	publish.fh.RETAIN = retained
	publish.messageIdentifier = packetid
	publish.topicName = topic
	publish.data = payload
	sock.send(publish.pack())
	checksocket(sockid)
	return publish.messageIdentifier


@mbt.action
def pubrel(pubrec : "pubrecs", **kwargs): # pubrecs are observable events
	global state
	sockid, pubrec = pubrec
	sock = state.sockets[sockid]
	pubrel = MQTTV3.Pubrels()
	pubrel.messageIdentifier = pubrec.messageIdentifier
	sock.send(pubrel.pack())

mbt.finishedWith(pubrel, "pubrec")

@mbt.action
def puback(publish : "publishes", **kwargs):
	global state
	sockid, publish = publish
	sock = state.sockets[sockid]
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
def pubcomp(pubrel : "pubrels", **kwargs):
	global state
	sockid, pubrel = pubrel
	sock = state.sockets[sockid]
	pubcomp = MQTTV3.Pubcomps()
	pubcomp.messageIdentifier = pubrel.messageIdentifier
	sock.send(pubcomp.pack())

mbt.finishedWith(pubcomp, "pubrel")


"""
This is not active yet.  How to use ping?
"""
def pingreq():
	global state
	pingreq = MQTTV3.Pingreqs()
	state.sockets[0].send(pingreq.pack())


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
mbt.choices("packetids", (0,)) # we use deterministic packet ids, to concentrate on good paths

mbt.choices("topicLists", [(t,) for t in topics + wildTopics])
mbt.choices("qosLists", [(0,), (1,), (2,)])


mbt.choices("payloads", (b"", b"1", b"333", b"long"*512), sequenced=True)

mbt.choices("connackrc", (0, 2), output=True)

mbt.choices("willmsgs", (None, (1, 0, "will topic", "will message"))) # simple choice to limit options

mbt.model.addReturnType("pubrecs")
mbt.model.addReturnType("pubrels")
mbt.model.addReturnType("publishes")


"""

 Selection callback.  Pick the next step from a list of options.

 This makes sure 

	1) connect is called after socket_create
	2) pubrel, puback, pubcomp are processed as quickly as possible

"""

def selectCallback(frees):
	global state 
	free_names = set([f[0].getName() for f in frees])
	logger.debug("*** after_socket_create %s %s", state.after_socket_create, state.last_free_names)
	if state.last_free_names == set(['socket_create']):
		diff = set(free_names).difference(state.after_socket_create)
		logger.debug("*** diff %s", diff)
		if diff == set():
			frees = [f for f in frees if f[0].getName() == "connect"]
		else:
			curname = random.choice(list(diff))
			frees = [f for f in frees if f[0].getName() == curname]
			state.after_socket_create.add(curname)
	else:	
		for f in frees:
			if f[0].getName() in ["pubrel", "puback", "pubcomp"]:
				frees = [f]
				break
	state.last_free_names = free_names
	return frees

mbt.model.selectCallback = selectCallback


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


"""

callback to modify call, used during test running 

"""
hostname = None
port = None

def callCallback(action, kwargs):
	# this is to allow redirection to another server on playback
	if action.getName() == "socket_create" and (hostname or port):
		for parm in kwargs.keys():
			if parm == "hostname":
				kwargs[parm] = hostname
			elif parm == "port":
				kwargs[parm] = port
	return action, kwargs


"""

callback to modify API call, used during test generation

Chooses the packetid parameter, to 

"""
def generateCallCallback(action, kwargs):
	# constrain the selection of packet ids to a valid sequence
	if action.getName() in ["publish", "subscribe", "unsubscribe"]:
		if action.getName() != "publish" or kwargs["qos"] > 0:
			kwargs["packetid"] = state.clientlist[kwargs["sockid"]].getNextMsgid()
	return action, kwargs

mbt.model.callCallback = generateCallCallback

max_progress = None
last_broker_state = None

def evaluateCallback():
	global max_progress, last_broker_state, state
	
	rc = 0
	"""
	if last_broker_state == None:
		last_broker_state = copy.copy(state.broker)
	else:
		#print("state.clients", len(state.broker.clients), len(last_broker_state.clients))
		rc += abs(len(last_broker_state.clients) - len(state.broker.clients))
		rc += abs(len(last_broker_state.broker.se.getSubscriptionList()) - len(state.broker.broker.se.getSubscriptionList()))
		last_broker_state = copy.copy(state.broker)
	"""
	
	current_coverage = mqtt.broker.coverage.getCovered()
	exceptions_found, exceptions_not_found, statements_found, statements_not_found = current_coverage 
	
	if max_progress == None:
		max_progress = current_coverage
		new_exception_count = len(exceptions_found - max_progress[0])
		new_statement_count = len(statements_found - max_progress[2])
	else:
		new_exception_count = len(exceptions_found - max_progress[0])
		if new_exception_count > 0:
			max_progress[0] = max_progress[0].union(exceptions_found)
		new_statement_count = len(statements_found - max_progress[2])
		if new_statement_count > 0:
			max_progress[2] = max_progress[2].union(statements_found)
			
	#print("evaluate", new_exception_count, new_statement_count)
	return rc + len(max_progress[0]) + len(max_progress[2])
	

mbt.model.evaluateCallback = evaluateCallback	

if __name__ == "__main__":
	stepping = False
	if len(sys.argv) > 1:
		stepping = True

	mbt.run(stepping=stepping)


