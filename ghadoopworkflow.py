#!/usr/bin/env python2.5

"""
GreenHadoop makes Hadoop aware of solar energy availability.
http://www.research.rutgers.edu/~goiri/
Copyright (C) 2012 Inigo Goiri, Rutgers University

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>
"""

from ghadoopcommons import *

class WorkflowElement:
	def __init__(self, id, cmd, deps):
		self.id = id
		self.cmd = cmd
		self.pre = deps
		self.pos = []
		self.deadline = None

def readWorkflow(workflowName, deadline):
	works = {}
	# Read workflow
	file = open(workflowName, 'r')
	for line in file:
		if not line.startswith("#") and line != "\n":
			splitLine = line.replace("\t", " ").strip().split(" ")
			# Job info: id
			id = splitLine[0]			
			# Command
			auxCmd = line.strip().split("\"")
			cmd = auxCmd[1]
			for i in range(2, len(auxCmd)-1):
				cmd += "\""+auxCmd[i]
			# Dependencies
			deps = auxCmd[len(auxCmd)-1].strip().split(" ")
			if deps[0] == "":
				deps = []
			works[id] = WorkflowElement(id, cmd, deps)

	# Calculate dependencies
	for work in works.values():
		for dep in work.pre:
			works[dep].pos.append(work.id)
		work.deadline = deadline
	
	# Calculate start and end
	start = []
	end = []
	for work in works.values():
		if len(work.pre) == 0:
			start.append(work.id)
		if len(work.pos) == 0:
			end.append(work.id)
	
	# Calculate deadlines
	for workId in end:
		calculateDeadline(workId, works, deadline)

	# Sort works
	workSorted = []
	addSorted(start, works, workSorted)
	
	#for work in workSorted:
		#print str(work.id)+" cmd="+str(work.deadline)+" pre="+str(work.pre)+" dl="+str(work.deadline)
	
	return workSorted
	
def calculateDeadline(workId, works, deadline):
	work = works[workId]
	if deadline < work.deadline:
		work.deadline = deadline
	for workIdPre in work.pre:
		# TODO
		# runtime = work.getRuntime()/SLOTLENGTH)*SLOTLENGTH)
		runtime = 300 # TODO change by something that makes sense
		runtime = float(TASK_JOB*AVERAGE_RUNTIME_MAP)/MAP_NODE/len(getNodes()) + float(AVERAGE_RUNTIME_RED_LONG*TASK_JOB*0.25)/RED_NODE/len(getNodes())
		calculateDeadline(workIdPre, works, deadline-runtime)

def addSorted(pre, works, workSorted):
	pos = []
	for workId in pre:
		work = works[workId]
		if work not in workSorted:
			# Check if all the dependencies are there
			all = True
			for preWorkId in work.pre:
				if works[preWorkId] not in workSorted:
					all = False
			if all:
				workSorted.append(work)
				for posWorkId in work.pos:
					pos.append(posWorkId)
	if len(pos)>0:
		addSorted(pos, works, workSorted)
