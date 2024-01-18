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

__version__ = 0.1

from mbt.main import Models, Executions, Choices, Tests, logger

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
		



    
    
