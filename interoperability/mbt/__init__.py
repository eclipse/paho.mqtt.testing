"""

	values of parameters to controllable actions can come from several sources:
	
		1) created as a result of other actions, either controllable or observable
			(keep a pool of results obtained)
		
		2) specific sets of values enumerated in the model



"""

__version__ = 0.1

from mbt.main import Models, Executions, Choices, Tests, log

model = Models()
execution = None

def action(fn):
    model.addAction(fn)
    return fn

def choices(varname, values, sequenced=False, output=False):
    model.addChoice(varname, values, output)

def finishedWith(fn, parm_name):
    model.finishedWith(fn, parm_name)

def observe(event):
    execution.addObservation(event)

def run(stepping=True):
	global execution
	execution = Executions(model)
	execution.run(stepping)

def step(interactive=False):
	global execution
	if not execution:
		execution = Executions(model)
	return execution.step(interactive)
		



    
    
