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

# Power model for a crypt node
class Node:
	POWER_S3 = 8.6 # Watts
	POWER_IDLE = 70.0 # Watts
	POWER_1JOB = 100.0 # Watts
	POWER_2JOB = 115.0 # Watts
	POWER_3JOB = 130.0 # Watts
	POWER_4JOB = 145.0 # Watts
	POWER_AVERAGE = POWER_4JOB
	POWER_FULL = POWER_4JOB

# TimeValue
class TimeValue:
	def __init__(self):
		self.t = None
		self.v = None
		
	def __init__(self, t, v):
		self.t = parseTime(t)
		self.v = v
		
	def __lt__(self, other):
		return parseTime(self.t)<parseTime(other.t)
		
	def __str__(self):
		return str(self.t)+" => "+str(self.v)

# Job data structure
class Job:
	def __init__(self, id=None, state="UNKNOWN", submit=None, start=None, end=None):
		self.id = id
		self.state = state # Date
		self.submit = submit # Date
		self.start = start # Date
		self.end = end # Date
		self.cmd = None
		self.input = None # Data: folder or file
		self.output = None # Data: folder or file
		self.priority = "NORMAL"
		self.deadline = None # Date
		self.durationMap = None # Seconds
		self.durationRed = None # Seconds
		self.tasks = [] # Ids
		# Workflow management
		self.workflow = None
		self.prevJobs = []
		self.internalDeadline = None

	def __str__(self):
		out = "job_"+str(self.id)+"\t=> "+self.state+"\tSubmit="+str(self.submit)+"\tPriority="+str(self.priority)
		if self.end != None:
			out+=" End="+str(self.end)
		if self.submit != None and self.end != None:
			out += " Length="+toTimeString(toSeconds(self.end-self.submit))
		if self.start != None and self.end != None:
			out += " ("+toTimeString(toSeconds(self.end-self.start))+")"
		return out

# Task data structure
class Task:
	def __init__(self, id=None, jobId= None, state="UNKNOWN", submit=None, start=None, end=None):
		self.id = id
		self.jobId = jobId
		self.state = state
		self.submit = submit
		self.start = start
		self.end = None
		self.node = None
		self.jobsetup = False

	def __str__(self):
		#out = str(self.id)+"("+str(self.jobId)+")\t=> "+self.state+"\tSubmit="+str(self.submit)
		out = str(self.id)+"\t=> "+self.state+"\tSubmit="+str(self.submit)
		if self.end != None:
			out+=" End="+str(self.end)
		if self.submit != None and self.end != None:
			out += " Length="+toTimeString(toSeconds(self.end-self.submit))
		if self.start != None and self.end != None:
			out += " ("+toTimeString(toSeconds(self.end-self.start))+")"
		return out

# Files data structure
class FileHDFS:
	def __init__(self, id=None, dir=False):
		self.id = id
		self.dir = dir
		self.blocks = {}
		# isintance(object, class)

	def __str__(self):
		out = self.id
		return out
		
	def getLocation(self):
		# Get file location
		ret = []
		for block in self.blocks.values():
			if isinstance(block, FileHDFS):
				for nodeId in block.getLocation():
					if nodeId not in ret:
						ret.append(nodeId)
			else:
				# Blocks
				for nodeId in block.nodes:
					if nodeId not in ret:
						ret.append(nodeId)
		return ret
	
	def getReplication(self):
		# Get file location
		ret = []
		for block in self.blocks.values():
			if isinstance(block, FileHDFS):
				for repl in block.getReplication():
					if repl not in ret:
						ret.append(repl)
			else:
				if len(block.nodes) not in ret:
					ret.append(len(block.nodes))
		return ret
		
	def getMaxReplication(self):
		ret = None
		for block in self.blocks.values():
			if isinstance(block, FileHDFS):
				if ret==None or ret<block.getMaxReplication():
					ret = block.getMaxReplication()
			else:
				if ret==None or ret>len(block.nodes):
					ret = len(block.nodes)
		if ret == None:
			ret = 1
		return ret
		
	def getMinReplication(self):
		ret = None
		for block in self.blocks.values():
			if isinstance(block, FileHDFS):
				if ret==None or ret>block.getMinReplication():
					ret = block.getMinReplication()
			else:
				if ret==None or ret>len(block.nodes):
					ret = len(block.nodes)
		if ret == None:
			ret = 1
		return ret
	
	def size(self):
		return len(self.blocks)
	
	def isDirectory(self):
		return self.dir
		
	def isInDirectory(self, dirPath):
		return self.id.startswith(dirPath)
	
	def isFile(self):
		return not self.dir

class BlockHDFS:
	def __init__(self, id=None):
		self.id = id
		self.file = None
		self.nodes = []
	
	
# Time in/out management
# Aux function to parse a time data
def parseTime(time):
	ret = 0
	if isinstance(time, str):
		aux = time.strip()
		if aux.find('d')>=0:
			index = aux.find('d')
			ret += 24*60*60*int(aux[0:index])
			if index+1<len(aux):
				ret += parseTime(aux[index+1:])
		elif aux.find('h')>=0:
			index = aux.find('h')
			ret += 60*60*int(aux[0:index])
			if index+1<len(aux):
				ret += parseTime(aux[index+1:])
		elif aux.find('m')>=0:
			index = aux.find('m')
			ret += 60*int(aux[0:index])
			if index+1<len(aux):
				ret += parseTime(aux[index+1:])
		elif aux.find('s')>=0:
			index = aux.find('s')
			ret += int(aux[0:index])
			if index+1<len(aux):
				ret += parseTime(aux[index+1:])
		else:
			ret += int(aux)
	else:
		ret = time
	return ret

def toSeconds(td):
	ret = td.seconds
	ret += 24*60*60*td.days
	if td.microseconds > 500*1000:
		ret += 1
	return ret

# From time to string
def toTimeString(time):
	surplus=time%1
	time = int(time)
	
	ret = ""
	# Day
	aux = time/(24*60*60)
	if aux>=1.0:
		ret += str(int(aux))+"d"
		time = time - aux*(24*60*60)
		
	# Hour
	aux = time/(60*60)
	if aux>=1.0:
		ret += str(int(aux))+"h"
		time = time - aux*(60*60)
		
	# Minute
	aux = time/(60)
	if aux>=1.0:
		ret += str(int(aux))+"m"
		time = time - aux*(60)
		
	# Seconds
	if time>=1.0:
		ret += str(time)+"s"
	
	if ret == "":
		ret = "0"
	
	# Add surplus
	if surplus>0.0:
		ret+=" +%.2f" % (surplus)
	
	return ret

def diffHours(dt1,dt2):
	sec1 = time.mktime(dt1.timetuple())	
	sec2 = time.mktime(dt2.timetuple())		

	return int((sec2-sec2)/3600)
