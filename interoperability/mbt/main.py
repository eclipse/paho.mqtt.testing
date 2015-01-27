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

	values of parameters to controllable actions can come from several sources:
	
		1) created as a result of other actions, either controllable or observable
			(keep a pool of results obtained)
		
		2) specific sets of values enumerated in the model


"natural choices"
    maximise the "distance" between last and next chosen routes from a particular node
      academic notion of distance is well-defined
      this is so that publish with many options is not tried immediately after a create too often - will this work?  what will work?  is this desirable?


"""

import random, traceback, time, sys, copy, shutil, logging

logger = logging.getLogger("mbt")
logger.setLevel(logging.INFO)
#formatter = logging.Formatter(fmt='%(levelname)s %(asctime)s %(name)s %(message)s',  datefmt='%Y%m%d %H%M%S')

class TraceNodes:

	def __init__(self, index):
		self.arcsAdded = False # indicates if we have set the arcs field
		self.arcs = {} # arcs leading from this node to another
		self.used = False # have we executed this particular node?
		self.leaf = False # is this the end of the line for this particular path?
		self.index = index # we are counting the nodes

	def addArc(self, key, value):
		# add an arc from the current node
		if type(value) != self.__class__:
			logger.error("error adding value %s", value)
			#traceback.print
		self.arcs[key] = value

	def isFree(self):
		# determine whether the current node has some untrodden paths
		#logger.debug("isFree curnode %s", self)
		if self.leaf:
			return None
		elif not self.used:
			return self
		for arc in self.arcs.keys():
			#logger.debug("isFree enabled %s", enabled)
			rc = self.arcs[arc].isFree()
			if rc:
				return rc
		return None

	def __repr__(self):
		return str(self.index)+", "+str(self.isSet)+", "+str(self.arcs)+\
              ", "+str(self.used)+", "+str(self.reset)


class Traces:
	"""
	A dynamic tree representing all the paths taken through the model up to now.
	The purpose is to avoid repeating paths which have already been taken.
	"""

	def __init__(self):
		self.nodeCount = 1
		self.root = TraceNodes(self.nodeCount)
		self.root.used = True
		self.curnode = self.root
		logger.debug("NODE index is now %d", self.curnode.index)

	def restart(self):
		"""
		Go back to the beginning, indicating that this node is an end of the line.
		"""
		self.curnode.leaf = True
		self.curnode = self.root
		logger.debug("NODE index is now %d", self.curnode.index)

	def addArcs(self, arcs):
		""" 
		In a particular state, enumerate all the paths available to leave that state.  
		"""
		if not self.curnode.arcsAdded:
			for arc in arcs:
				self.nodeCount += 1
				self.curnode.addArc(arc, TraceNodes(self.nodeCount))
			self.curnode.arcsAdded = True
			logger.debug("Total number of nodes now %d", self.nodeCount)

	def selectAction(self, action, args):
		"""
		In the current state, goto the next state via (action, args)
		"""
		key = tuple([action] + args)
		self.curnode = self.curnode.arcs[key]
		self.curnode.used = True
		logger.debug("NODE index is now %d", self.curnode.index)

	def findNextPath(self, callback):
		"return one next path that isn't fully exercised"
		found = None
		frees = []
		for arc in self.curnode.arcs.keys():
			if self.curnode.arcs[arc].isFree():
				frees.append(arc)

		if len(frees) > 0:
			if callback:
				frees = callback(frees) # allow the test model to restrict choice
			found = random.choice(frees) 

		return found


def combine(lista, listb):
	""" 
	lista is a list of lists
	listb is a list of any sort of elements
	"""
	product = []
	for elementb in listb:
		product += [elementa + [elementb] for elementa in lista]
	return product

class Choices:

	def __init__(self, value, returned=False, output=False):
		self.value = value
		self.returned = returned
		self.used = 0
		self.output = output

	def valueOf(self):
		self.used += 1
		return self.value

	def equals(self, value):
		if self.output:
			logger.debug("return value %s %s", value, self.value)
			input("input")
		return self.value == value

	def __repr__(self):
		return str(self.value)

class Actions:

	def __init__(self, fn):
		self.called = 0
		self.fn = fn
		self.parm_names = self.fn.__annotations__.keys() - set(["return"])

	def __call__(self, *args, **kwargs):
		self.called += 1
		return self.fn(*args, **kwargs)

	def __str__(self):
		return self.getName()

	def __repr__(self):
		return self.getName()

	def getName(self):
		return self.fn.__name__

	def getParmNames(self):
		return self.parm_names

	def getParmType(self, parm_name):
		return self.fn.__annotations__[parm_name]

	def getReturnType(self):
		rc = None
		if "return" in self.fn.__annotations__.keys():
			rc = self.fn.__annotations__["return"]
		return rc

	def enumerateChoices(self, choices):
		args = []
		for parm_name in self.getParmNames():
			parm_type = self.getParmType(parm_name)
			if args == []:
				args = [[(parm_type, i)] for i in range(len(choices[parm_type]))]
			else:
				args = combine(args, [(parm_type, i) for i in range(len(choices[parm_type]))])
		return args

class Models:

	def __init__(self):
		self.actions = [] # list of controllable actions defined in the model
		self.choices = {} # data choices for parameters, including those returned from calls
		self.return_types = [] # names of return_types
		self.finisheds = {}
		self.maxobjects = {}
		self.selectCallback = None	
		self.restartCallback = None
		self.callCallback = None
	
	def getActionNames(self):
		return [action.getName() for action in self.actions]

	def addReturnType(self, return_type):
		""" 
		return types should not be in the datapools if they
		can be created by having been mentioned in the data 
		inputs
		"""
		if return_type not in self.choices.keys():
			self.choices[return_type] = []
			self.return_types.append(return_type)

	def addAction(self, fn):
		self.actions.append(Actions(fn))
		if "return" in fn.__annotations__.keys():
			self.addReturnType(fn.__annotations__["return"])
		
	def addChoice(self, varname, values, output=False):
		""" 
		Need to track choices for coverage - wrap each choice in a class so we can
		keep track
		"""
		self.choices[varname] = [Choices(v) for v in values]
		if output:
			self.choices[varname] = tuple(self.choices[varname])   # indicate not mutable

	def finishedWith(self, action, parm_name):
		if action not in self.finisheds:
			self.finisheds[action] = [parm_name]
		else:
			self.finisheds[action].append(parm_name)


"""
Model execution class.

Used in the generation of tests.

"""
class Executions:

	def __init__(self, model):
		self.model = model
		self.trace = Traces()
		self.finished = False
		self.steps = 0
		self.observations = []

		self.pools = copy.deepcopy(self.model.choices)

	def addObservation(self, observation):
		""" 
		Used during model exploration to add an observation.
		"""
		self.observations.append(observation)
		logger.info("OBSERVED EVENT %s", observation)

	def removeFinisheds(self, action, kwargs):
		"""
		used during execution to remove choices which 
		"""
		removed = False
		for parm_name in action.getParmNames():
			parm_type = action.getParmType(parm_name)
			if action.fn in self.model.finisheds.keys():
				if parm_name in self.model.finisheds[action.fn]:
					choice = kwargs[parm_name]
					for c in self.pools[parm_type]:
						if c.equals(choice):
							self.pools[parm_type].remove(c)
					removed = True

	def coverage(self):
		total = 0
		used = 0
		unused = []
		counts = {}
		for choice in self.pools.keys():
			for c in self.pools[choice]:
				total += 1
				if c.used > 0:
					used += 1
				else:
					unused.append((choice, c))
		return int((used / total) * 100)

	def getEnabledActions(self):
		enableds = set()
		for action in self.model.actions:
			all_parms_available = True
			for parm_name in action.getParmNames():
				parm_type = action.getParmType(parm_name)
				if parm_type not in self.pools.keys() or len(self.pools[parm_type]) == 0:
					all_parms_available = False
					break
			if all_parms_available:
				return_type = action.getReturnType()
				#if return_type in self.choices.keys():
				#	print("return type "+return_type+" pool class: "+str(self.choices[return_type].__class__.__name__))
				if return_type in self.model.maxobjects.keys():
					maxobjects = self.model.maxobjects[return_type]
				else:
					maxobjects = 1
				if return_type == None or return_type not in self.pools.keys() or \
					    (self.pools[return_type].__class__.__name__ != "list" or \
					    len(self.pools[return_type]) < maxobjects):
					enableds.add(action)
		return enableds

	def restartIfSameStateAsStart(self):
		restart = True
		for choice in self.model.choices.keys():
			if self.pools[choice] != len(self.model.choices[choice]):
				restart = False
				break
		if restart:
			self.restart()

	def restart(self):
		logger.info("RESTART")
		self.trace.restart()
		self.pools = copy.deepcopy(self.model.choices)
		#for type_name in self.model.return_types:
		#	if self.pools[type_name].__class__.__name__ == "list":
		#		self.pools[type_name] = []
		if self.model.restartCallback:
			self.model.restartCallback()

	def printStats(self):
		logger.debug("action counts %s", str([(a.getName(), a.called) for a in self.model.actions]))
		counts = {}
		for choice in self.pools.keys():
			counts[choice] = [(c.value, c.used) for c in self.pools[choice]]
		logger.debug("choice counts %d", counts)
		logger.info("coverage %d%%", self.coverage())
		
	def __run__(self, interactive=True):
		while not self.finished:
			self.step(interactive)

	def step(self, interactive=False):
		"""
			Take one step in the model execution.

			1. Select an action
			2. Select parameter values
			3. Execute the action with the parameters
			4. Process the result

		"""
		restart = False
		self.steps += 1 
		logger.debug("Steps: %d coverage %d%%", self.steps, self.coverage())
		enableds = self.getEnabledActions()
		logger.debug("Enabled %s", [e.getName() for e in enableds])
			
		self.trace.addArcs([tuple([action] + choice) for action in enableds \
					    for choice in action.enumerateChoices(self.pools)])
			
		next = self.trace.findNextPath(self.model.selectCallback)
		if next == None:
			logger.debug("No more options available")
			return True
		action = next[0]; args = list(next[1:])

		kwargs = {}
		exec_kwargs = {}
		index = 0
		for parm_name in action.getParmNames():
			#print("kwargs" + str( self.choices[args[index][0]][args[index][1]]))
			#print("kwargs "+parm_name+" "+str(self.choices[args[index][0]]))
			try:
				kwargs[parm_name] = self.pools[args[index][0]][args[index][1]].valueOf()
				exec_kwargs[parm_name] = "self.pools['"+args[index][0]+"']["+str(args[index][1])+"]"
			except:
				logger.info("exception %s" % traceback.format_exc())
				import pdb
				pdb.set_trace()
			index += 1

		if self.model.callCallback:
			action, kwargs = self.model.callCallback(action, kwargs)
		logger.info("CALL %s with %s", action.getName(), kwargs)	
		#logger.debug("EXEC_CALL %s with %s", action.getName(), exec_kwargs)
		if interactive and input("--->") == "q":
			return
		self.trace.selectAction(action, args)
		rc = None

		try:
			rc = action(**kwargs)
		except:
			# an exception indicates an unexpected result, and the end of the test
			logger.info("RESULT from %s is exception %s", action.getName(), traceback.format_exc())
			self.restart()
			restart = True
		else:
			if rc != None:
				logger.info("RESULT from %s is %s", action.getName(), rc)
				ret_type = action.getReturnType()
					
				if ret_type:
					updated = False
					for c in self.pools[ret_type]:
						if c.equals(rc):
							c.used += 1 
							updated = True
							break
					if not updated and hasattr(self.pools[ret_type], "append"):
						self.pools[ret_type].append(Choices(rc, returned=True))

			self.removeFinisheds(action, kwargs)
		return restart		

	def run(self, interactive=True):
		try:
			self.__run__(interactive)
		except KeyboardInterrupt:
			self.printStats()

from mqtt.formats.MQTTV311 import Pubrecs, Publishes, Pubrels

test_logger = logging.getLogger("mbt-test")
test_logger.propagate = False
test_logger.setLevel(logging.DEBUG)

formatter = logging.Formatter(fmt='%(levelname)s %(asctime)s %(name)s %(message)s',  datefmt='%Y%m%d %H%M%S')
ch = logging.StreamHandler()
ch.setFormatter(formatter)
ch.setLevel(logging.INFO)
test_logger.addHandler(ch)

formatter = logging.Formatter(fmt='%(levelname)s %(asctime)s %(name)s %(message)s',  datefmt='%Y%m%d %H%M%S')
fh = logging.FileHandler("test.log")
fh.setFormatter(formatter)
fh.setLevel(logging.DEBUG)
test_logger.addHandler(fh)

class Tests:

	def __init__(self, model, logfilename, checks={}, observationMatchCallback=None, callCallback=None):
		self.model = model
		self.logfilename = logfilename
		self.checks = checks
		self.added_results = []
		self.passes = 0
		self.failures = 0
		self.observationMatchCallback = observationMatchCallback
		self.callCallback = callCallback
		self.logger = logging.getLogger("mbt-test")

	def replaceResults(self, aString, strresult=False):
		# returns a string suitable for call to eval()
		result_keys = sorted(self.results.keys(), key=len, reverse=False) # sort keys to do the shortest first
		change_count = 0
		valstring = aString
		for result in result_keys:
			loc = aString.find(result)
			if loc != -1:
				change_count += 1
				self.logger.debug("%d replace %s with %s" % (change_count, result, 'self.results["'+result+'"]'))
				valstring = aString[:loc] + 'self.results["'+result+'"]'  + aString[loc + len(result):]
				aString = aString[:loc] + str(self.results[result]) + aString[loc + len(result):]
				self.logger.debug("%s", aString)
		return aString if strresult else valstring

	def handleCall(self, words):
		action = self.actions[words[1]]
		strargs = self.replaceResults(words[3])
		try:
			kwargs = eval(strargs)
		except:
			self.logger.info("strargs %s" % strargs)
			self.logger.info("results %s" % self.results)
			raise
		if self.callCallback:
			action, kwargs = self.callCallback(action, kwargs)
		self.logger.debug("CALL %s with %s", action, kwargs)
		if self.stepping and input("--->") == "q":
			return
		try:
			rc = action(**kwargs)           
		except:
			rc = "exception "+traceback.format_exc()
		self.last_action = action
		self.last_rc = rc

	def handleResult(self, curline, words):
		action = self.last_action
		rc = self.last_rc
		if action.getReturnType() in self.checks.keys():
			check_result = self.checks[action.getReturnType()](rc, words[3].split(" ", 1)[1])
		elif type(rc) == type('') and rc.startswith("exception") and "exception" in self.checks.keys():
			check_result = self.checks["exception"](rc, words[3].split(" ", 1)[1])
		else:
			check_result = (curline == "RESULT from "+action.getName()+" is "+str(rc))
		if check_result:
			self.logger.debug("correct result %s", rc)
			self.passes += 1
		else:
			self.logger.warn("### incorrect result %s", rc)
			self.logger.warn("### expected %s", curline)
			self.logger.warn("### for action %s", action)
			self.failures += 1
			return

		value = words[3].strip().split(" ", 1)[1]
		if not value.startswith("exception") and type(rc) not in [type(3), type('')]:
			self.logger.debug("recording value of %s", value)
			self.results[value] = rc

	def handleObserved(self, curline):
		max_wait_count = 10 # how many iterations to wait for an observation before flagging a failure
		wait_interval = 1 # 

		# wait for result to be added to added_results, then move it to results
		observation = curline.split(" ", 2)[2].strip()
		observation = self.replaceResults(observation, True)

		count = 1
		if self.observationMatchCallback:
			observation1 = self.observationMatchCallback(observation, self.added_results)
			while observation1 == None and count < max_wait_count:
				self.logger.debug("Waiting for observation '%s' in %s", observation, [x for x, y in self.added_results])
				time.sleep(wait_interval)
				observation1 = self.observationMatchCallback(observation, self.added_results)
				count += 1
		else:
			observation1 = observation
			while observation not in [x for x, y in self.added_results] and count < max_wait_count:
				self.logger.debug("Waiting for observation '%s' in %s", observation, [x for x, y in self.added_results.keys()])
				time.sleep(wait_interval)
				count += 1
		if count == max_wait_count:
			self.logger.warn("### line %d, observation not found %s", self.lineno, observation)
			self.failures += 1
		else:
			self.logger.debug("*** line %d, observation found %s", self.lineno, observation)
			if observation != observation1:
				self.logger.debug("*** observations equal %s %s" % (observation, observation1))
			self.passes += 1
			matched_added_result = [(x, y) for x, y in self.added_results if observation1 == x][0]
			self.results[observation] = matched_added_result[1]
			self.added_results.remove(matched_added_result)

	def handleRestart(self):
		self.logger.debug("Restarting")
		self.results = {}
		self.added_results = []
		if self.model.restartCallback:
			self.model.restartCallback()

	def run(self, stepping=True):
		self.logger.info("Starting test %s", self.logfilename)
		self.actions = {}
		self.results = {}
		self.stepping = stepping
		self.lineno = 0
		for action in self.model.actions:
			self.actions[action.getName()] = action
		logfile = open(self.logfilename)
		curline = logfile.readline()
		while curline:
			self.lineno += 1
			curline = curline.strip()
			#if curline.startswith("INFO"):
			#	words = curline.split(" ", 4)[4] # remove level, date, time, logname
			#curline = ''.join(words)
			#words = words.split(" ", 3)
			words = curline.split(" ", 3)
			self.logger.debug("%d %s", self.lineno, curline)
			if words[0] == "CALL":
				self.handleCall(words)
			elif words[0] == "RESULT":
				self.handleResult(curline, words)
			elif words[0] == "OBSERVED":
				self.handleObserved(curline)
			elif words[0] == "RESTART":
				self.handleRestart()
			curline = logfile.readline()
		logfile.close()
		if self.failures == 0:
			self.logger.info("Test %s successful. Tests passed %d", self.logfilename, self.passes)
		else:
			self.logger.info("Test %s unsuccessful.  Tests passed %d, failed %s", self.logfilename, self.passes, self.failures)

	def addResult(self, result):
		self.logger.debug("adding result %s", result)
		self.added_results.append((str(result), result))
		

			

				

			

		
		
	
		
		
