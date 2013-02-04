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

import math
import time
import sys
import os
import threading
import string
import random
import signal
import subprocess
import socket

from operator import itemgetter

from datetime import datetime,timedelta
from subprocess import call, PIPE, Popen

from ghadoopdata import *

#DEBUG = 3
DEBUG = 0

USER = "goiri"
HADOOP_HOME = "/home/"+USER+"/hadoop-0.21.0"
MASTER_NODE = "crypt15"

GREEN_PREDICTOR="/home/goiri/ghadoop/greenavailability/"

HADOOP_SLAVES = HADOOP_HOME+"/conf/slaves"
PORT_MAPRED = 50060
PORT_HDFS = 50075
HADOOP_DECOMMISSION_MAPRED = HADOOP_HOME+"/conf/disabled_nodes"
HADOOP_DECOMMISSION_HDFS = HADOOP_HOME+"/conf/disabled_nodes_hdfs"
HADOOP_OFF_HDFS = HADOOP_HOME+"/conf/disabled_nodes_off"

#MAX_SUBMISSION = 10
MAX_SUBMISSION = 7
MAX_SUBMISSION_RETRIES = 10

# Scheduling flags
SCHEDULE_GREEN = False
SCHEDULE_BROWN_PRICE = False
SCHEDULE_BROWN_PEAK = False

# Files
#GREEN_AVAILABILITY_FILE = None
#GREEN_AVAILABILITY_FILE = 'data/greenpower.none'
GREEN_AVAILABILITY_FILE = 'data/greenpower.solar'
#GREEN_AVAILABILITY_FILE = 'data/greenpower9.solar'
#GREEN_AVAILABILITY_FILE = 'data/greenpower9-07-03-2011'
#GREEN_AVAILABILITY_FILE = 'data/greenpower9-07-03-2011-fake'
#GREEN_AVAILABILITY_FILE = 'data/greenpower9-14-06-2011'

#GREEN_AVAILABILITY_FILE = 'data/solarpower-31-05-2010' # More energy
#GREEN_AVAILABILITY_FILE = 'data/solarpower-23-08-2010' # Best for us
#GREEN_AVAILABILITY_FILE = 'data/solarpower-01-11-2010' # Worst for us
#GREEN_AVAILABILITY_FILE = 'data/solarpower-24-01-2011' # Less energy

#BROWN_PRICE_FILE = 'data/browncost.ryan'
#BROWN_PRICE_FILE = 'data/browncost9.ryan'
BROWN_PRICE_FILE = 'data/browncost.nj'
#BROWN_PRICE_FILE = 'data/browncost.none'
#BROWN_PRICE_FILE = 'data/browncost.zero'
#BROWN_PRICE_FILE = 'data/browncost.none'

WORKLOAD_FILE = 'workload/workload.genome'
#WORKLOAD_FILE = 'workload/workload.test'
#WORKLOAD_FILE = None

# Base day
#BASE_DATE = datetime(2010, 6, 18, 9, 0, 0) # 18/06/20110 9:00 AM
BASE_DATE = datetime(2010, 5, 31, 9, 0, 0) # 18/06/20110 9:00 AM


# Maximum scheduling period
#TIME_LIMIT = (4*24+8)*3600
#TIME_LIMIT = 10*60 # 10 minutes
#TIME_LIMIT = 90*60 # 70 minutes
TIME_LIMIT = 2*60*60 # 2 hours
#TIME_LIMIT = 20 # 2 hours
#TIME_LIMIT = 60 # 2 hours
#TIME_LIMIT = 60*60 # 1 hour
#TIME_LIMIT = None

#TOTALTIME = 2*24*60*60
#TOTALTIME = 10*60 # Window size
#TOTALTIME = 1*60*60 # Window size
#TOTALTIME = 30*60 # Window size
#TOTALTIME = 2160 # Window size
TOTALTIME = 60*60 # Window size
#SLOTLENGTH = 10

DEADLINE = TOTALTIME

#SLOTLENGTH = 900
#SLOTLENGTH = 900
SLOTLENGTH = 10
#SLOTLENGTH = 3600

TESTAPP = True


SCHEDULE_EVENT = False
SCHEDULE_SLOT = True
CYCLE_SLOT = True

WORKLOADGEN = True
#WORKLOADGEN = False


# Maximum graphic size
MAXSIZE = 16

# Price of the peak $/kW
PEAK_COST = 0
# PEAK_COST = 5.5884 # Winter
# PEAK_COST = 13.6136 # Summer
PEAK_PERIOD = 15*60 # 15 minutes
#PEAK_PERIOD = 60 # 15 minutes

# Power of the Gslurm system
#POWER_IDLE_GHADOOP = 55+33 # Watts: switch + scheduler
POWER_IDLE_GHADOOP = 55 # Watts: switch + scheduler
#POWER_IDLE_GHADOOP = 0 # Watts: switch + scheduler

#BASE_POWER = 1347*1000 # Watts
#MAX_POWER = 2300 # Watts
#MAX_POWER = POWER_IDLE_GHADOOP + 16*Node.POWER_FULL
#MAX_POWER = 11*230.0 # 11 panels at 230W
MAX_POWER = 14*230.0 # 12 panels at 230W


# Hadoop parameters
MAP_NODE = 4
RED_NODE = 2
TASK_NODE = MAP_NODE + RED_NODE # 2 maps and 1 reduce
TASK_JOB = 38 # TODO
#AVERAGE_RUNTIME = 100 # seconds
#AVERAGE_RUNTIME = 70 # seconds
AVERAGE_RUNTIME_MAP = 35.0 # seconds per map
AVERAGE_RUNTIME_RED_LONG = 1.0 # 2.5 # seconds per reduce/map short
#AVERAGE_RUNTIME_RED_SHORT = 2.5 # 2.5 # seconds per reduce/map long
AVERAGE_RUNTIME_RED_SHORT = 6.0 # 2.5 # seconds per reduce/map long

#ALPHA_EWMA = 0.2
ALPHA_EWMA = None

MAX_WAITING_QUEUE = 900 # Seconds

REPLICATION_DEFAULT = 2

MAX_DECOMMISSION_NODES = 4

ALWAYS_NODE = [MASTER_NODE]

# Phase to clean all the replicated data
PHASE_CLEAN_PERIOD = 100*3600 # seconds
#PHASE_CLEAN_PERIOD = 400 # seconds

PHASE_NONE = 0
PHASE_CLEAN = 1
PHASE_TURN_ON = 2
phase = PHASE_NONE


# Maximum scheduling period
numSlots = int(math.ceil(TOTALTIME/SLOTLENGTH))

#numIdleNodes = 0.0

# Data structures
# Nodes
nodes = None
lastNodeUpdate = datetime.now()
updatingNodes = False
nodeHdfsReady = []

# Jobs
jobs = {} # id -> job
tasks = {} # taskId -> task
#requiredFiles = [] # List of required files

nodeTasks = {} # nodeId -> [taskId, taskId...]
nodeJobs = {} # nodeId -> jobId

# Waiting queue
waitingQueue = []


# Files
filesUpdated = False
files = {}

nodeBlock = {}
nodeFile = {}


# Log management
# Write a line in a log
openLogs = {}
def writeLog(filename, txt):
	if filename not in openLogs:
		file = open(filename, 'a')
		openLogs[filename] = file
	else:
		file = openLogs[filename]
	file.write(txt+"\n")
	file.flush() # TODO comment

def closeLogs():
	for filename in openLogs:
		file = openLogs[filename]
		file.close()

#class Popen(subprocess.Popen):
	#def kill(self, signal = signal.SIGTERM):
		#os.kill(self.pid, signal)

# Output management
# Screen colors
class bcolors:
	HEADER = '\033[95m'
	OKBLUE = '\033[94m'
	OKGREEN = '\033[92m'
	WARNING = '\033[93m'
	FAIL = '\033[91m'
	WHITEBG = '\033[47m'
	BLUEBG = '\033[44m'
	GREENBG = '\033[42m'
	REDBG = '\033[41m'
	ENDC = '\033[0m'

	def disable(self):
		self.HEADER = ''
		self.OKBLUE = ''
		self.OKGREEN = ''
		self.WARNING = ''
		self.FAIL = ''
		self.WHITEBG = ''
		self.BLUEBG = ''
		self.GREENBG = ''
		self.ENDC = ''

# Clean screen
def clearscreen(numlines=100):
	import os
	if os.name == "posix":
		# Unix/Linux/MacOS/BSD/etc
		os.system('clear')
	elif os.name in ("nt", "dos", "ce"):
		# DOS/Windows
		os.system('CLS')
	else:
		# Fallback for other operating systems.
		print '\n' * numlines

def getDebugLevel():
	global DEBUG
	return DEBUG
	
def setDebugLevel(level):
	global DEBUG
	DEBUG = level

def getPhase():
	global phase
	return phase

def setPhase(argPhase):
	global phase
	phase = argPhase

def filesUpdated():
	global filesUpdated
	return filesUpdated

def setFilesUpdated(argFilesUpdated):
	global filesUpdated
	filesUpdated = argFilesUpdated

def cleanFileInfo(fileId):
	if fileId in files:
		file = files[fileId]
		
		auxBlocks = file.blocks
		file.blocks = {}
		for nodeId in nodeBlock:
			for block in auxBlocks.values():
				if block.id in nodeBlock[nodeId]:
					nodeBlock[nodeId].remove(block.id)
		for nodeId in nodeFile:
			for block in auxBlocks.values():
				if block.file in nodeFile[nodeId]:
					nodeFile[nodeId].remove(block.file)


def cleanFileLocation(fileId):	
	if fileId in files:
		file = files[fileId]
		for block in file.blocks.values():
			for nodeId in nodeBlock:
				if block.id in nodeBlock[nodeId]:
					nodeBlock[nodeId].remove(block.id)
				if block.file in nodeFile[nodeId]:
					nodeFile[nodeId].remove(block.file)
			block.nodes = []
		#for block in file.blocks.values():
			#block.nodes = []
		#for nodeId in nodeBlock:
			#for block in file.blocks.values():
				#if block.id in nodeBlock[nodeId]:
					#nodeBlock[nodeId].remove(block.id)
		#for nodeId in nodeFile:
			#for block in file.blocks.values():
				#if block.file in nodeFile[nodeId]:
					#nodeFile[nodeId].remove(block.file)

#def isOnlyIn(filesId, nodeList):
	#ret = False
	#for fileId in filesId:
		#file = files[fileId]
		#if file.isDirectory():
			#for fileDir in file.blocks:
				#ret = isOnlyIn([fileDir], nodeList)
				#if ret==True:
					#break
		#else:
			#ret = True
			#for nodeId in file.getLocation():
				#if nodeId not in nodeList:
					#ret = False
					#break
	#return ret

def filesOnlyIn(filesId, nodeList):
	ret = []
	for fileId in filesId:
		file = files[fileId]
		if file.isDirectory():
			for fileDir in file.blocks:
				for auxFileId in filesOnlyIn([fileDir], nodeList):
					if auxFileId not in ret:
						ret.append(auxFileId)
		else:
			isOnlyIn = True
			for nodeId in file.getLocation():
				if nodeId not in nodeList:
					isOnlyIn = False
					break
			if isOnlyIn:
				ret.append(fileId)
	return ret

def minNodesFiles(checkFiles, offNodes):
	# Input
	checkFiles = getFilesInDirectories(checkFiles)
	auxOffNodes = list(offNodes)
	
	# Get which files are missing and which nodes have them
	missingFiles = []
	missingNodes = {}
	for fileId in checkFiles:
		file = files[fileId]
		locations = file.getLocation()
		if len(locations)>0 and fileId not in missingFiles:
			# Check if the file is missing
			missing = True
			for nodeId in locations:
				if nodeId not in auxOffNodes:
					missing=False
					break
			if missing:
				missingFiles.append(fileId)
				# Check where is the file
				for nodeId in locations:
					if nodeId in auxOffNodes:
						if nodeId not in missingNodes:
							missingNodes[nodeId]=0
						missingNodes[nodeId]+=1
	# Check nodes with missing files
	auxMissingNodes = []
	for nodeId in missingNodes:
		auxMissingNodes.append((nodeId, missingNodes[nodeId]))
	missingNodes = sorted(auxMissingNodes, key=itemgetter(1), reverse=True)
	
	# We need the minimum set of nodes that have the files required by a job
	dataNodes=[]
	# Checking if there is more files to check
	while len(missingFiles)>0:
		# Turn on the nodes with the replicas with more data in
		nodeId = missingNodes[0][0]
		auxOffNodes.remove(nodeId)
		dataNodes.append(nodeId)
		# Update which files are missing and which nodes have them
		missingFiles = []
		missingNodes = {}
		for fileId in checkFiles:
			file = files[fileId]
			locations = file.getLocation()
			if len(locations)>0 and fileId not in missingFiles:
				# Check if the file is missing
				missing = True
				for nodeId in locations:
					if nodeId not in auxOffNodes:
						missing=False
						break
				if missing:
					missingFiles.append(fileId)
					# Check where is the file
					for nodeId in locations:
						if nodeId in auxOffNodes:
							if nodeId not in missingNodes:
								missingNodes[nodeId]=0
							missingNodes[nodeId]+=1
		# Check nodes with missing files
		auxMissingNodes = []
		for nodeId in missingNodes:
			auxMissingNodes.append((nodeId, missingNodes[nodeId]))
		missingNodes = sorted(auxMissingNodes, key=itemgetter(1), reverse=True)
	# Remove those nodes that already have data on
	change = True
	while change:
		change = False
		for nodeId in dataNodes:
			free = True
			if nodeId in nodeFile:
				for fileId in nodeFile[nodeId]:
					if fileId in checkFiles and fileId in files:
						# Check if file is available somewhere else
						fileAvailable = False
						file = files[fileId]
						for otherNodeId in file.getLocation():
							if otherNodeId!=nodeId and otherNodeId not in auxOffNodes:
								fileAvailable = True
								break
						if not fileAvailable:
							free = False
							break
			if free:
				auxOffNodes.append(nodeId)
				dataNodes.remove(nodeId)
				change = True
				break	
	return dataNodes

# Interacting with Hadoop
# Get all the tasks
def getTasks():
	global tasks
	return tasks

# Get all the jobs
def getJobs():
	global jobs
	return jobs

def getTaskPriority(task):
	ret = "NORMAL"
	if task.jobId in jobs:
		ret=jobs[task.jobId].priority
	return ret

def getRequiredFiles():
	ret = []
	for job in getJobs().values():
		if job.state == "DATA" or job.state == "PREP" or job.state == "RUNNING":
			if job.input != None:
				for inputfile in job.input:
					ret.append(inputfile)
	return getFilesInDirectories(ret)

def getFilesInDirectories(dirs):
	ret = []
	if dirs != None:
		for fileId in dirs:
			# Adding those that are not already there
			for auxFileId in getFilesInDirectory(fileId):
				if auxFileId not in ret:
					ret.append(auxFileId)
	return ret

def getFilesInDirectory(fileId):
	ret = []
	try:
		file = files[fileId]
		if file.isDirectory():
			for auxFileId in file.blocks:
				# Adding those that are not already there
				for auxFileId2 in getFilesInDirectory(auxFileId):
					if auxFileId2 not in ret:
						ret.append(auxFileId2)
		else:
			ret.append(fileId)
	except KeyError:
		None
	return ret

def getJobsHadoop():
	ret = {}
	
	# Read those in the queue
	pipe=Popen([HADOOP_HOME+"/bin/mapred", "job", "-list", "all"], stdout=PIPE, stderr=open('/dev/null', 'w'))
	#pipe=Popen(["scontrol", "-o", "show", "jobs"], stdout=PIPE)
	
	success = False
	waitingCycle = 0
	while True:
		pipe.poll()
		if pipe.returncode != None:
			success = True
			break
		waitingCycle+=1
		# Wait a maximum 5 seconds
		if waitingCycle>50:
			break
		time.sleep(0.5)
	# If it has finish, read the output
	if success:
		content = False
		while True:
			line = pipe.stdout.readline() #block / wait
			#print "[getJobsHadoop()] "+str(line)
			if line:
				if not content:
					if line.startswith("JobId"):
						content=True
				else:
					# Read attributes
					splitLine = line.split('\t')
					jobId = splitLine[0]
					jobId = jobId.replace("job_","")
					
					job = Job(jobId)
					ret[job.id] = job
					
					state = splitLine[1] # PREP RUNNING SUCCEEDED
					# 5 hours difference to UTC
					job.submit = datetime(1970, 1, 1, 1, 0)+timedelta(seconds=int(splitLine[2])/1000-5*3600) # StartTime
					job.start = job.submit
					if state == "SUCCEEDED":
						job.end = datetime.now()
					job.state = state
					job.priority = splitLine[4] # Priority
			else:
				break
	# Kill if the process is zombie
	if pipe.poll() == None:
		#pipe.terminate()
		os.kill(pipe.pid, signal.SIGTERM)

	return ret

# Gets the runtime of a job
def getRuntime(job):
	ret = 0
	start = None
	end = None
	
	for taskId in job.tasks:
		task = getTasks()[taskId]
		if task.jobsetup == False:
			if start==None or (task.start != None and task.start<start):
				start = task.start
			if end==None or (task.end != None and task.end>end):
				end = task.end
	if start!=None and end!=None:
		ret = toSeconds(start-end)
	
	return ret

# Gets the map runtime of a job
def getMapRuntime(job):
	ret = 0
	start = None
	end = None
	
	for taskId in job.tasks:
		if taskId.find("_m_")>=0:
			task = getTasks()[taskId]
			if task.jobsetup == False:
				if start==None or (task.start != None and task.start<start):
					start = task.start
				if end==None or (task.end != None and task.end>end):
					end = task.end
	if start!=None and end!=None:
		ret = toSeconds(start-end)
	
	return ret

# Gets the reduce runtime of a job
def getRedRuntime(job):
	ret = 0
	start = None
	end = None
	
	for taskId in job.tasks:
		task = getTasks()[taskId]
		if task.jobsetup == False:
			if taskId.find("_m_")>=0:
				# The start is the end of the last map
				if start==None or (task.end != None and task.end>start):
					start = task.end
			elif taskId.find("_r_")>=0:
				# The end is the end of the last reduce
				if end==None or (task.end != None and task.end>end):
					end = task.end
	if start!=None and end!=None:
		ret = toSeconds(start-end)
	
	return ret


def isOpen(host, port):
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.settimeout(1)
	try:
		s.connect((host, int(port)))
		s.shutdown(2)
		return True
	except:
		return False

def storeIsOpenValue(host, port, store):
	store[(host, port)] = isOpen(host, port)

def getSlaves():
	ret = []
	file=open(HADOOP_SLAVES, "r")
	for line in file:
		name=line.replace("\n", "")
		if name != "":
			if name not in ret:
				ret.append(name)
	return ret

# Return the list of nodes: nodeId -> [MapReduce, HDFS]
#def getNodes(update=False):
def getNodes():
	global nodes
	global lastNodeUpdate
	global updatingNodes
	
	if nodes==None and updatingNodes:
		nodes = {}
	elif nodes==None or (toSeconds(datetime.now()-lastNodeUpdate) >= 1 and not updatingNodes):
		updatingNodes = True
		ret = updateNodes()
		lastNodeUpdate = datetime.now()
		updatingNodes = False
		nodes = ret
	
	return nodes

def updateNodes():
	ret = {}
	store = {}
	threads = []
	for slave in getSlaves():
		# Threading
		thread = threading.Thread(target=storeIsOpenValue,args=(slave, PORT_MAPRED, store))
		thread.start()
		threads.append(thread)
		thread = threading.Thread(target=storeIsOpenValue,args=(slave, PORT_HDFS, store))
		thread.start()
		threads.append(thread)
	# While threads to finish
	while len(threads)>0:
		threads.pop().join()
	# Save information
	for (host, port) in store:
		#host = key[0]
		#port = key[1]
		if host not in ret:
			ret[host] = ["UNKNOWN", "UNKNOWN"]
		if store[(host, port)]:
			if port == PORT_MAPRED:
				ret[host][0] = "UP"
			elif port ==PORT_HDFS:
				ret[host][1] = "UP"
		else:
			if port == PORT_MAPRED:
				ret[host][0] = "DOWN"
			elif port ==PORT_HDFS:
				ret[host][1] = "DOWN"
	# Get decommissioning MapReduce
	for line in open(HADOOP_DECOMMISSION_MAPRED, "r"):
		if line != "\n":
			name=line.replace("\n", "")
			if name not in ret:
				ret[name] = ["UNKNOWN", "UNKNOWN"]
			if ret[name][0] == "UP":
				ret[name][0] = "DEC"
	# Get decommissioning MapReduce
	for line in open(HADOOP_DECOMMISSION_HDFS, "r"):
		if line != "\n":
			name=line.replace("\n", "")
			if name not in ret:
				ret[name] = ["UNKNOWN", "UNKNOWN"]
			if ret[name][1] == "UP":
				ret[name][1] = "DEC"
	return ret


def setNodeStatus(nodeId, running, decommissioned=False):
	threads = []
	
	# Do it with threads
	thread = threading.Thread(target=setNodeMapredStatus,args=(nodeId, running, decommissioned))
	thread.start()
	threads.append(thread)
	
	thread = threading.Thread(target=setNodeHdfsStatus,args=(nodeId, running, decommissioned))
	thread.start()
	threads.append(thread)
	
	# Wait for threads to finish...
	while len(threads)>0:
		threads.pop().join()
	
def setNodeDecommission(nodeId, decommission):
	setNodeMapredDecommission(nodeId, decommission)
	setNodeHdfsDecommission(nodeId, decommission)

# Change the decommission state of a set of nodes
def setNodeListDecommission(nodeList, decommission):
	global nodes
	# Change MapReduce
	change=False
	# Read file
	auxNodes = []
	file=open(HADOOP_DECOMMISSION_MAPRED, "r")
	for line in file:
		line=line.replace("\n", "")
		if line not in auxNodes:
			auxNodes.append(line)
		else:
			change=True
	file.close()
	# Make changes
	if decommission:
		# Decommission nodes
		for nodeId in nodeList:
			if nodeId not in auxNodes:
				auxNodes.append(nodeId)
				change=True
	else:
		# Recomision
		for nodeId in nodeList:
			if nodeId in auxNodes:
				auxNodes.remove(nodeId)
				change=True
	# Write changes into file
	if change:
		file=open(HADOOP_DECOMMISSION_MAPRED, "w")
		for line in sorted(auxNodes):
			file.write(line+"\n")
		file.close()
	# Update node topology
	for nodeId in nodeList:
		if decommission:
			nodes[nodeId][0] = "DEC"
		elif nodes[nodeId][0] == "DEC":
			nodes[nodeId][0] = "UP"
	
	# Change HDFS
	change=False
	# Read file
	auxNodes = []
	file=open(HADOOP_DECOMMISSION_HDFS, "r")
	for line in file:
		line=line.replace("\n", "")
		if line not in auxNodes:
			auxNodes.append(line)
		else:
			change=True
	file.close()
	# Make changes
	if decommission:
		# Decommission nodes
		for nodeId in nodeList:
			if nodeId not in auxNodes:
				auxNodes.append(nodeId)
				change=True
	else:
		# Recomision
		for nodeId in nodeList:
			if nodeId in auxNodes:
				auxNodes.remove(nodeId)
				change=True
	# Write changes into file
	if change:
		file=open(HADOOP_DECOMMISSION_HDFS, "w")
		for line in sorted(auxNodes):
			file.write(line+"\n")
		file.close()

	# Refresh nodes
	#call([HADOOP_HOME+"/bin/hdfs", "dfsadmin", "-refreshNodes"], stderr=open('/dev/null', 'w'))
	
	# Update node topology
	if decommission:
		nodes[nodeId][1] = "DEC"
	elif nodes[nodeId][1] == "DEC":
		nodes[nodeId][1] = "UP"

def cleanNodeDecommission():
	cleanNodeMapredDecommission()
	cleanNodeHdfsDecommission()

# Get a node status
def getNodeMapredStatus(nodeId):
	if isOpen(nodeId, PORT_MAPRED):
		ret = "UP"
	else:
		ret = "DOWN"
		
	# Get decommissioning MapReduce
	if ret == "UP":
		file=open(HADOOP_DECOMMISSION_MAPRED, "r")
		for line in file:
			name=line.replace("\n", "")
			if name != "":
				if name==nodeId:
					ret = "DEC"
					break
		file.close()
	# Update node info
	global nodes
	if nodeId not in nodes:
		nodes[nodeId] = ["UNKNOWN", "UNKNOWN"]
	nodes[nodeId][0] = ret
	
	return ret

# Set a node status: turn on or off
def setNodeMapredStatus(nodeId, running, decommissioned=False):
	if not decommissioned or not running:
		setNodeMapredDecommission(nodeId, False)
	else:
		setNodeMapredDecommission(nodeId, True)
	# Turn on
	current = getNodeMapredStatus(nodeId)
	while running and current=="DOWN":
		exit = call([HADOOP_HOME+"/bin/manage_node.sh", "start", "mapred", nodeId], stdout=open('/dev/null', 'w'), stderr=open('/dev/null', 'w'))
		current = getNodeMapredStatus(nodeId)
	# Turn off
	while not running and (current=="UP" or current=="DEC"):
		exit = call([HADOOP_HOME+"/bin/manage_node.sh", "stop", "mapred", nodeId], stdout=open('/dev/null', 'w'), stderr=open('/dev/null', 'w'))
		current = getNodeMapredStatus(nodeId)

# Send a node to Decommission status
def setNodeMapredDecommission(nodeId, decommission):
	change=False
	# Read file
	auxNodes = []
	file=open(HADOOP_DECOMMISSION_MAPRED, "r")
	for line in file:
		line=line.replace("\n", "")
		if line not in auxNodes:
			auxNodes.append(line)
		else:
			change=True
	file.close()
	
	# Make changes
	if decommission:
		# Decommission node
		if nodeId not in auxNodes:
			auxNodes.append(nodeId)
			change=True
	else:
		# Recomision
		if nodeId in auxNodes:
			auxNodes.remove(nodeId)
			change=True
	# Write into file
	if change:
		file=open(HADOOP_DECOMMISSION_MAPRED, "w")
		for line in sorted(auxNodes):
			file.write(line+"\n")
		file.close()
	# Refresh nodes: no need, scheduler automatically does it
	#call([HADOOP_HOME+"/bin/mapred", "mradmin", "-refreshNodes"], stderr=open('/dev/null', 'w'))
	
	# Update node topology
	global nodes
	if decommission:
		nodes[nodeId][0] = "DEC"
	elif nodes[nodeId][0] == "DEC":
		nodes[nodeId][0] = "UP"

def cleanNodeMapredDecommission():
	file=open(HADOOP_DECOMMISSION_MAPRED, "w")
	file.write("")
	file.close()

# Get a node status
def getNodeHdfsStatus(nodeId):
	if isOpen(nodeId, PORT_HDFS):
		ret = "UP"
	else:
		ret = "DOWN"

	# Get decommissioning HDFS
	if ret == "UP":
		file=open(HADOOP_DECOMMISSION_HDFS, "r")
		for line in file:
			name=line.replace("\n", "")
			if name != "":
				if name==nodeId:
					ret = "DEC"
		file.close()
	# Update node info
	global nodes
	if nodeId not in nodes:
		nodes[nodeId] = ["UNKNOWN", "UNKNOWN"]
	nodes[nodeId][1] = ret
	
	return ret

# Set a node status: turn on or off
def setNodeHdfsStatus(nodeId, running, decommissioned=False):
	current = getNodeHdfsStatus(nodeId)
	if decommissioned:
		setNodeHdfsDecommission(nodeId, True)
	# Turn on
	while running and current=="DOWN":
		exit = call([HADOOP_HOME+"/bin/manage_node.sh", "start", "hdfs", nodeId], stdout=open('/dev/null', 'w'), stderr=open('/dev/null', 'w'))
		if decommissioned:
			setNodeHdfsDecommission(nodeId, True)
		else:
			setNodeHdfsDecommission(nodeId, False)
		setNodeHdfsON(nodeId)
		current = getNodeHdfsStatus(nodeId)
	# Turn off
	while not running and (current=="UP" or current=="DEC"):
		exit = call([HADOOP_HOME+"/bin/manage_node.sh", "stop", "hdfs", nodeId], stdout=open('/dev/null', 'w'), stderr=open('/dev/null', 'w'))
		setNodeHdfsDecommission(nodeId, False)
		setNodeHdfsOFF(nodeId)
		current = getNodeHdfsStatus(nodeId)
		global nodeHdfsReady
		if nodeId in nodeHdfsReady:
			nodeHdfsReady.remove(nodeId)

# Notify HDFS to remove the next datanode
def setNodeHdfsOFF(nodeId):
	change=False
	# Read file
	auxNodes = []
	file=open(HADOOP_OFF_HDFS, "r")
	for line in file:
		line=line.replace("\n", "")
		if line not in auxNodes:
			auxNodes.append(line)
		else:
			change=True
	file.close()
	
	# Make changes
	# Decommission node
	if nodeId not in auxNodes:
		auxNodes.append(nodeId)
		change=True
	# Write into file
	if change:
		file=open(HADOOP_OFF_HDFS, "w")
		for line in sorted(auxNodes):
			file.write(line+"\n")
		file.close()
	# Refresh nodes
	#call([HADOOP_HOME+"/bin/hdfs", "dfsadmin", "-refreshNodes"], stderr=open('/dev/null', 'w'))

# Notify HDFS to stop removing the next datanode
def setNodeHdfsON(nodeId):
	change=False
	# Read file
	auxNodes = []
	file=open(HADOOP_OFF_HDFS, "r")
	for line in file:
		line=line.replace("\n", "")
		if line not in auxNodes:
			auxNodes.append(line)
		else:
			change=True
	file.close()
	
	# Make changes
	# Decommission node
	if nodeId in auxNodes:
		auxNodes.remove(nodeId)
		change=True
	# Write into file
	if change:
		file=open(HADOOP_OFF_HDFS, "w")
		for line in sorted(auxNodes):
			file.write(line+"\n")
		file.close()
	# Refresh nodes
	#call([HADOOP_HOME+"/bin/hdfs", "dfsadmin", "-refreshNodes"], stderr=open('/dev/null', 'w'))


# Send a node to Decommission status
def setNodeHdfsDecommission(nodeId, decommission):
	change=False
	# Read file
	auxNodes = []
	file=open(HADOOP_DECOMMISSION_HDFS, "r")
	for line in file:
		line=line.replace("\n", "")
		if line not in auxNodes:
			auxNodes.append(line)
		else:
			change=True
	file.close()
	
	# Make changes
	if decommission:
		# Decommission node
		if nodeId not in auxNodes:
			auxNodes.append(nodeId)
			change=True
	else:
		# Recomision
		if nodeId in auxNodes:
			auxNodes.remove(nodeId)
			change=True
	# Write into file
	if change:
		file=open(HADOOP_DECOMMISSION_HDFS, "w")
		for line in sorted(auxNodes):
			file.write(line+"\n")
		file.close()
	# Refresh nodes
	#call([HADOOP_HOME+"/bin/hdfs", "dfsadmin", "-refreshNodes"], stderr=open('/dev/null', 'w'))
	
	# Update node topology
	global nodes
	if decommission:
		nodes[nodeId][1] = "DEC"
	elif nodes[nodeId][1] == "DEC":
		nodes[nodeId][1] = "UP"

def cleanNodeHdfsDecommission():
	file=open(HADOOP_DECOMMISSION_HDFS, "w")
	file.write("")
	file.close()
	
def getNodesHdfsReady():
	global nodeHdfsReady
	return nodeHdfsReady

def cleanNodesHdfsReady():
	global nodeHdfsReady
	while len(nodeHdfsReady)>0:
		nodeHdfsReady.pop()

def submitJob(jobId=None, command=None, input=None, output=None, priority=None, waiting=True, prevJobs=[], deadline=None):
	# Generate temporary Job
	if jobId == None:
		jobId = ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(6))
	job = Job(jobId)
	job.input = input
	job.output = output
	job.submit = datetime.now()
	job.state = "WAITING"
	job.prevJobs = prevJobs
	job.internalDeadline = deadline
	if priority!=None:
		job.priority = priority
	jobs[jobId] = job
	
	# Check wether to directly submit
	if priority=="VERY_HIGH" or priority=="HIGH" or not waiting:
		submitJobHadoop(jobId, job.submit, command, input, output, priority)
	else:
		# Queue job in the waiting queue
		waitingQueue.append((jobId, job.submit, command, input, output, priority))

def submitJobHadoop(jobId, submitTime, command, input=None, output=None, priority=None):
	if command.find("index")>=0:
		cmd = HADOOP_HOME+"/bin/nutch "+command
		if output!=None:
			for outputfile in output:
				cmd+=" "+outputfile
		if input!=None:
			for inputfile in input:
				cmd+=" "+inputfile
	else:
		cmd = HADOOP_HOME+"/bin/hadoop "+command
		if input!=None:
			if command.find("loadgen")>=0:
				for inputfile in input:
					cmd+=" -indir "+inputfile
			else:
				for inputfile in input:
					cmd+=" "+inputfile
		if output!=None:
			if command.find("loadgen")>=0:
				for outputfile in output:
					cmd+=" -outdir "+outputfile
			else:
				for outputfile in output:
					cmd+=" "+outputfile
	# Turning on required nodes
	if jobId not in jobs:
		job = Job(jobId)
		job.input = input
		job.output = output
		job.submit = submitTime
		job.state = "DATA"
		jobs[jobId] = job
	else:
		jobs[jobId].state = "DATA"
		
	# Wait until all data is actually available (Hadoop dispatcher hadoop.py->dispatch() turns on the nodes)
	#missingFiles = True
	#while missingFiles:
		## Get nodes not ready yet
		#nodes = getNodes()
		#ready = getNodesHdfsReady()
		#notReady = []
		#for nodeId in nodes:
			#if nodeId not in ready:
				#notReady.append(nodeId)
		## Get missing files
		#missingFiles = False
		#for fileId in filesOnlyIn(input, notReady):
			#file = files[fileId]
			#if len(file.getLocation())>0:
				#missingFiles = True
				#break
		#if missingFiles:
			#time.sleep(1.0)
	missingFiles = True
	while missingFiles:
		reqFiles = getFilesInDirectories(input)
		ready = getNodesHdfsReady()
		# Get nodes not ready yet
		#nodes = getNodes()
		#notReady = []
		#for nodeId in nodes:
			#if nodeId not in ready:
				#notReady.append(nodeId)
		#print jobId + " " + str(ready)+"   +> "+str(getNodes())
		#for nodeId in sorted(getNodes()):
			#print str(nodeId)+":\t"+str(nodes[nodeId][0])+"\t"+str(nodes[nodeId][1])
		
		# Get missing files
		missingFiles = False
		for fileId in reqFiles:
			file = files[fileId]
			isAvailable = False
			for nodeId in file.getLocation():
				if nodeId in ready:
					isAvailable = True
					break
			if len(file.getLocation())==0 and len(file.blocks)==0:
				isAvailable = True
			if not isAvailable:
				missingFiles = True
				break
		if missingFiles:
			time.sleep(1.0)
	
	writeLog("logs/ghadoop-scheduler.log", str(datetime.now())+"\tWaiting->Data: "+str(jobId)+": data is ready")
	
	#print "Waiting->Data: "+str(jobId)+": data is ready"
	
	# Submitting to Hadoop
	# Retries...
	hadoopJobId = None
	tries = 0
	while hadoopJobId==None and tries<MAX_SUBMISSION_RETRIES:
		pipeRun=Popen(cmd.split(" "), stderr=PIPE, stdout=PIPE)#, stdout=open('/dev/null', 'w')) # , stderr=open('/dev/null', 'w')
		#print str(jobId)+" -> submit"+str(cmd.split(" "))+"   "
		outmsg = ""
		while True:
			line = pipeRun.stderr.readline() #block / wait
			if line:
				#print str(jobId)+" -err-> "+line.replace("\n","")
				outmsg += line
				if line.find("Running job:")>0:
					hadoopJobId = line.split(" ")[6].replace("job_", "").replace("\n", "")
					break
				# Out
				#line = pipeRun.stdout.readline() #block / wait
				#if line:
					#print str(jobId)+" -out-> "+line.replace("\n","")
			else:
				pipeRun.poll()
				if pipeRun.returncode != None:
					break
				else:
					time.sleep(0.1)
		tries += 1 # Retry
		if hadoopJobId==None:
			call([HADOOP_HOME+"/bin/hdfs", "dfsadmin", "-refreshNodes"], stderr=open('/dev/null', 'w'))
			if tries==MAX_SUBMISSION_RETRIES:
				writeLog("logs/ghadoop-error.log", "Job: "+str(jobId)+"\nReturn code: "+str(pipeRun.returncode)+"\nCommand:"+command+"\nOutput:\n"+outmsg)
				print "Error submitting job: "+str(pipeRun.returncode)+" -> "+str(cmd)
				print outmsg
	
	if hadoopJobId == None:
		# After max number of tries, it cannot be submit
		jobs[jobId].state = "FAILED"
	else:
		writeLog("logs/ghadoop-scheduler.log", str(datetime.now())+"\tData->Hadoop: "+str(jobId)+" -> "+str(hadoopJobId))
		
		# Store information
		if jobId in jobs:
			# Remove temporary info
			job = jobs[jobId]
			del jobs[jobId]
		else:
			# Store data
			job = Job(jobId)
			job.input = input
			job.output = output
			job.submit = submitTime
		# Update dependent jobs
		for auxJob in jobs.values():
			if auxJob.state == "WAITING": 
				for i in range(0, len(auxJob.prevJobs)):
					if auxJob.prevJobs[i] == jobId:
						auxJob.prevJobs[i] = hadoopJobId
		# Assign the actual id
		jobId = hadoopJobId
		job.id = jobId
		job.state = "PREP"
		jobs[jobId] = job
		
		# Change priority
		if priority!=None:
			jobs[jobId].priority = priority
			call([HADOOP_HOME+"/bin/hadoop", "job", "-set-priority", "job_"+jobId, priority], stdout=PIPE, stderr=open('/dev/null', 'w'))
	return hadoopJobId

def killJob(jobId):
	call([HADOOP_HOME+"/bin/mapred", "job", "-kill", "job_"+jobId], stderr=open('/dev/null', 'w'))

def setJobPriotity(jobId, priority):
	call([HADOOP_HOME+"/bin/mapred", "job", "-set-priority", "job_"+jobId, priority], stdout=open('/dev/null', 'w'), stderr=open('/dev/null', 'w'))

# File management
def rmFile(path):
	call([HADOOP_HOME+"/bin/hadoop", "fs", "-rmr", path], stdout=open('/dev/null', 'w'), stderr=open('/dev/null', 'w'))

def setFileReplication(path, replication):
	call([HADOOP_HOME+"/bin/hadoop", "fs", "-setrep", "-w", str(replication), "-R", str(path)], stdout=open('/dev/null', 'w'), stderr=open('/dev/null', 'w'))

def getFileReplication(path):
	replication = 0.0
	
	pipe = Popen([HADOOP_HOME+"/bin/hadoop", "fsck", path], stdout=PIPE, stderr=open('/dev/null', 'w'))
	text = pipe.communicate()[0]
	for line in text.split('\n'):
		if line != "":
			if line.find("Average block replication:")>0:
				while line.find("  ")>0:
					line = line.replace("  ", " ")
				line = line.strip()
				splitLine = line.split("\t")
				replication = float(splitLine[1])
				break
	return replication

def signal_handler(signal, frame):
	#print 'Killing!'
	sys.exit(0)


def getNodeReport():
	report = {}
	
	pipe = Popen([HADOOP_HOME+"/bin/hdfs", "dfsadmin", "-report"], stdout=PIPE, stderr=open('/dev/null', 'w'))
	text = pipe.communicate()[0]
	nodeId = None
	liveNodes = False
	for line in text.split('\n'):
		if line != "":
			if line.startswith("Live datanodes:"):
				liveNodes = True
			elif line.startswith("Dead datanodes:"):
				liveNodes = False
			elif line.startswith("Name:"):
				if line.find("(")>=0:
					nodeId = line.split(" ")[2]
					nodeId = nodeId[1:len(nodeId)-1]
				else:
					nodeId = line.split(" ")[1]
				if liveNodes:
					report[nodeId] = (0, 100, 0)
				else:
					report[nodeId] = None
			elif nodeId != None and report[nodeId]!=None:
				if line.startswith("DFS Remaining:"):
					# GB
					#report[nodeId] = (report[nodeId][0], report[nodeId][1], report[nodeId][2])
					value = float(line.split(" ")[2])/(1024.0*1024.0*1024.0)
					report[nodeId] = (value, report[nodeId][1], report[nodeId][2])
				elif line.startswith("DFS Remaining%:"):
					value = line.split(" ")[2]
					value = float(value[0:len(value)-1])
					report[nodeId] = (report[nodeId][0], value, report[nodeId][2])
				elif line.startswith("DFS Used%:"):
					value = line.split(" ")[2]
					value = float(value[0:len(value)-1])
					report[nodeId] = (report[nodeId][0], report[nodeId][1], value)
	return report


def checkNodesStatus(name, i):
	print i
	while True:
		startaux = datetime.now()
		nodes = getNodes()
		print str(i)+" "+str(datetime.now())+": "+str(datetime.now()-startaux)
		time.sleep(0.1)

if __name__=='__main__':
	report = getNodeReport()
	for nodeId in sorted(report):
		print nodeId+" => "+str(report[nodeId])
	'''
	print "Start"
	nodes = getNodes()
	for nodeId in sorted(nodes):
		print nodeId+": "+str(nodes[nodeId])
		
	for nodeId in sorted(nodes):
		if nodeId != "crypt01":
			setNodeStatus(nodeId, False)
	#setNodeHdfsStatus("crypt10", False)
	
	print "Mid"
	nodes = getNodes()
	for nodeId in sorted(nodes):
		print nodeId+": "+str(nodes[nodeId])
		
	for nodeId in sorted(nodes):
		if nodeId != "crypt01":
			setNodeStatus(nodeId, True)
	print "End"
	'''
	
	nodes = getNodes()
	for nodeId in sorted(nodes):
		print nodeId+": "+str(nodes[nodeId])
		
	#print "Thread 1"
	#i=1
	#t = threading.Thread(target=checkNodesStatus,args=("Mola", i))
	#t.setDaemon(True)
	#t.start()
	#print "Thread 2"
	#i=2
	#t = threading.Thread(target=checkNodesStatus,args=("Mola", i))
	#t.setDaemon(True)
	#t.start()
	##print "Thread 3"
	##i=3
	##t = threading.Thread(target=checkNodesStatus,args=("Mola", i))
	##t.setDaemon(True)
	##t.start()
	
	#time.sleep(100)
		
		
	
	#for file in ["/user/goiri/testeando3", "/user/goiri/testeando4", "/user/goiri/testeando5", "/user/goiri/testeando6", "/user/goiri/testeando7"]:
		#print file+":\t"+str(getFileReplication(file))
	#for node in getNodes():
		#print node
	
	
	print "Jobs"
	for job in getJobsHadoop().values():
		print job
	
	#submitJob("jar "+HADOOP_HOME+"/hadoop-mapred-examples-0.21.0.jar wordcount", "input", "output2", "VERY_HIGH")
	
