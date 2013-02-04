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

import threading
import signal
import subprocess

from operator import itemgetter
from datetime import datetime, timedelta

from ghadoopcommons import *


class ReplicationThread(threading.Thread):
	def __init__(self, timeStart, replThreads=40):
		threading.Thread.__init__(self)
		self.running = True
		self.timeStart = timeStart
		self.replThreads = replThreads
		self.MAX_REPLICATION_TIME = 20 # seconds
		
		self.lastSpaceCheck = None
		self.MAX_SPACECHECK_TIME = 120 # seconds
		
		self.threads = {}
	
	def kill(self):
		self.running = False

	def run(self):
		# threadId = fileId => (thread,timestart)
		retries = {}
		while self.running:
			# Cleaning threads
			for threadId in list(self.threads.keys()):
				thread = self.threads[threadId]
				if not thread[0].isAlive():
					del self.threads[threadId]
				elif toSeconds(datetime.now()-thread[1])>self.MAX_REPLICATION_TIME:
					thread[0].kill()
					if not thread[0].isAlive():
						del self.threads[threadId]

			# Check if there is enough space
			if self.lastSpaceCheck == None or toSeconds(datetime.now()-self.lastSpaceCheck) > self.MAX_SPACECHECK_TIME:
				self.doSpaceCheck()
				self.lastSpaceCheck = datetime.now()
			
			# If there is free replications threads, look for a file requiring replication
			#change = False
			if len(self.threads)<self.replThreads:
				# Required files list
				requiredFiles = getRequiredFiles()
				
				# Collect node info
				nodes = getNodes()
				onNodes = []
				decNodes = []
				for nodeId in nodes:
					if nodes[nodeId][1]=="UP" and nodeId not in onNodes:
						onNodes.append(nodeId)
					elif nodes[nodeId][1]=="DEC" and nodeId not in decNodes:
						decNodes.append(nodeId)
						
				# Account required files
				decNodesAux = []
				for nodeId in nodes:
					if nodes[nodeId][1]=="DEC" and nodeId not in decNodesAux:
						nJobs = len(nodeJobs.get(nodeId, []))
						# Get the number of required missing files of the node
						nFiles = 0
						for fileId in nodeFile.get(nodeId, []):
							if fileId in requiredFiles:
								# Check if file is available somewhere else
								fileAvailable = False
								file = files[fileId]
								for otherNodeId in file.getLocation():
									if otherNodeId!=nodeId:
										if (otherNodeId in onNodes or otherNodeId in decNodes):
											fileAvailable = True
											break
								if not fileAvailable:
									nFiles +=1
						decNodesAux.append((nodeId, nFiles, nJobs))
				# Sort decommission nodes according to the number of running jobs and the number of required files
				decNodesAux = sorted(decNodesAux, key=itemgetter(1), reverse=False)
				decNodes = sorted(decNodesAux, key=itemgetter(2), reverse=False)
				decNodes = [nodeId for nodeId, nFiles, nJobs in decNodes]
				# Auxiliar list of nodes
				decNodes1 = [nodeId for nodeId in decNodes]
				decNodes2 = [nodeId for nodeId in decNodes]
				
				# Check data in decommission nodes which is not available anywhere else
				change = True
				while len(decNodes1)>0 and len(self.threads)<self.replThreads and change:
					change = False
					nodeId = decNodes1.pop(0)
					# Check if it is actually in decommission and ready
					nodeHdfsReady = getNodesHdfsReady()
					# Check if the node is available
					if nodes[nodeId][1]=="DEC" and nodeId in nodeFile and len(nodeJobs.get(nodeId, []))==0 and nodeId in nodeHdfsReady:
						# Check if the files are available
						it = 0
						while not filesUpdated() and it<10:
							time.sleep(0.5)
							it += 1
						if filesUpdated and nodes[nodeId][1]=="DEC" and nodeId in nodeHdfsReady:
							# Check all files in the node
							replicateFiles = []
							for fileId in nodeFile[nodeId]:
								# First files only not available
								file = files[fileId]
								if fileId not in self.threads and file.isFile() and fileId in requiredFiles and fileId not in replicateFiles:
									# Not too many replications
									if fileId not in retries or retries[fileId]<len(nodes):
										# Check if file is available in UP nodes ()
										replicate = True
										for otherNodeId in file.getLocation():
											#if otherNodeId!=nodeId and nodes[otherNodeId][1]=="UP":# and nodes[otherNodeId][1]=="DEC":
											if otherNodeId!=nodeId and (nodes[otherNodeId][1]=="UP" or nodes[otherNodeId][1]=="DEC"):
												replicate = False
												break
										if replicate:
											replicateFiles.append(fileId)
								if len(self.threads)+len(replicateFiles) > self.replThreads:
									break
							# Check if it is required to replicate
							while len(replicateFiles)>0 and len(self.threads)<self.replThreads:
								fileId = replicateFiles.pop(0)
								file = files[fileId]
								# Check if there are enough nodes available
								availableNodes = False
								for otherNodeId in onNodes:
									if otherNodeId in nodeHdfsReady and otherNodeId in nodeFile and fileId not in nodeFile[otherNodeId]:
										availableNodes = True
										break
								if availableNodes:
									# Increase replication
									newrepl = file.getMaxReplication()+1
									if fileId not in retries:
										retries[fileId] = 0
									else:
										retries[fileId] += 1
									thread = SetReplicationThread(self.timeStart, fileId, newrepl, retries[fileId])
									thread.start()
									self.threads[fileId] = (thread, datetime.now())
									change = True
				# Check data in decommission nodes (is the previous one with no check on decommission nodes)
				change = True
				while len(decNodes2)>0 and len(self.threads)<self.replThreads and change:
					change = False
					nodeId = decNodes2.pop(0)
					# Check if it is actually in decommission and ready
					nodeHdfsReady = getNodesHdfsReady()
					# Check if the node is available
					if nodes[nodeId][1]=="DEC" and nodeId in nodeFile and len(nodeJobs.get(nodeId, []))==0 and nodeId in nodeHdfsReady:
						# Check if the files are available
						it = 0
						while not filesUpdated() and it<10:
							time.sleep(0.5)
							it += 1
						if filesUpdated and nodes[nodeId][1]=="DEC" and nodeId in nodeHdfsReady:
							# Check all files in the node
							replicateFiles = []
							for fileId in nodeFile[nodeId]:
								# First files only not available
								file = files[fileId]
								if fileId not in self.threads and file.isFile() and fileId in requiredFiles and fileId not in replicateFiles:
									# Not too many replications
									if fileId not in retries or retries[fileId]<len(nodes):
										# Check if file is available in UP nodes ()
										replicate = True
										for otherNodeId in file.getLocation():
											if otherNodeId!=nodeId and nodes[otherNodeId][1]=="UP":
												replicate = False
												break
										if replicate:
											replicateFiles.append(fileId)
								if len(self.threads)+len(replicateFiles) > self.replThreads:
									break
							# Check if it is required to replicate
							while len(replicateFiles)>0 and len(self.threads)<self.replThreads:
								fileId = replicateFiles.pop(0)
								file = files[fileId]
								# Check if there are enough nodes available
								availableNodes = False
								for otherNodeId in onNodes:
									if otherNodeId in nodeHdfsReady and otherNodeId in nodeFile and fileId not in nodeFile[otherNodeId]:
										availableNodes = True
										break
								if availableNodes:
									# Increase replication
									newrepl = file.getMaxReplication()+1
									if fileId not in retries:
										retries[fileId] = 0
									else:
										retries[fileId] += 1
									thread = SetReplicationThread(self.timeStart, fileId, newrepl, retries[fileId])
									thread.start()
									self.threads[fileId] = (thread, datetime.now())
									change = True

			# If not change:
			time.sleep(2.0)
		
		# Killing waiting threads
		for threadId in list(self.threads.keys()):
			thread = self.threads[threadId]
			thread[0].kill()
	

	def doSpaceCheck(self):
		# Check if there is enough space in the Always ON nodes
		global ALWAYS_NODE
		enoughSpace = False
		nodesReport = getNodeReport()
		nodes = getNodes()
				
		for nodeId in ALWAYS_NODE:
			# Check if the value is right
			if nodeId not in nodesReport:
				enoughSpace = True
				break
			elif nodesReport[nodeId]==None:
				enoughSpace = True
				break
			#elif nodesReport[nodeId][0]==0.0 and nodesReport[nodeId][2]==100.0:
				#enoughSpace = True
				#break
			elif nodesReport[nodeId][0]>3.0 and nodesReport[nodeId][2]<85.0:
				# minimum 3 GB and occupation maximum 85%
				enoughSpace = True
				break
		if not enoughSpace:
			#print "There is no enough space.... "
			#print nodesReport
			print "Nodes report:"
			# Select
			selectNodeId = None
			for nodeId in nodesReport:
				if nodeId in nodes and nodesReport!=None and nodesReport[nodeId]!=None:
					print "\t%s -> %.2fGB %.2f" % (nodeId, nodesReport[nodeId][0], nodesReport[nodeId][2])
					if selectNodeId==None or nodesReport[nodeId][0]>nodesReport[selectNodeId][0]:
						if nodeId not in ALWAYS_NODE:
							selectNodeId = nodeId
			if selectNodeId != None and selectNodeId not in ALWAYS_NODE:
				print "Selected node: "+str(selectNodeId)
				ALWAYS_NODE.append(selectNodeId)
	
	def getCurrentTime(self):
		timeNow = datetime.now()
		timeNow = datetime(timeNow.year, timeNow.month, timeNow.day, timeNow.hour, timeNow.minute, timeNow.second)
		return toSeconds(timeNow-self.timeStart)


class CleanDataThread(threading.Thread):
	def __init__(self, timeStart):
		threading.Thread.__init__(self)
		self.running = True
		self.timeStart = timeStart
		self.lastclean = datetime.now()
		self.replThreads = 5
		self.MAX_REPLICATION_TIME = 20 # seconds

		#self.threads = {}
		
	def kill(self):
		self.running = False
	
	def run(self):
		while self.running:
			# Cleaning threads
			#for threadId in list(self.threads.keys()):
				#thread = self.threads[threadId]
				#if not thread[0].isAlive():
					#del self.threads[threadId]
				#elif toSeconds(datetime.now()-thread[1])>self.MAX_REPLICATION_TIME:
					#thread[0].kill()
					#del self.threads[threadId]
			# Cleaning
			#if len(self.threads)<self.replThreads:
			requiredFiles = getRequiredFiles()
			for fileId in list(files.keys()):
				# File not required anymore
				if fileId not in requiredFiles and fileId.startswith("/user/"+str(USER)+"/"):
					file = files[fileId]
					if file.isFile():
						locations = file.getLocation()
						if len(locations)>REPLICATION_DEFAULT:
							writeLog("logs/ghadoop-scheduler.log", str(self.getCurrentTime())+"\tCleaning replication "+str(fileId))
							#file.blocks = {}
							cleanFileInfo(fileId)
							replicator = Popen([HADOOP_HOME+"/bin/hadoop", "fs", "-setrep", "-w", str(REPLICATION_DEFAULT), "-R", str(fileId)], stdout=open('/dev/null', 'w'), stderr=open('/dev/null', 'w'))
							for i in range(0,4):
								if replicator.poll()==None:
									time.sleep(0.5)
								else:
									break
							if  replicator.poll()==None:
								os.kill(replicator.pid, signal.SIGTERM)
							#cleanFileInfo(fileId)
							setFilesUpdated(False)
						#thread = SetReplicationThread(self.timeStart, fileId, REPLICATION_DEFAULT)
						#thread.start()
						#self.threads[fileId] = (thread, datetime.now())
						#setFilesUpdated(False)
					#if file.isFile() and fileId.startswith("/user/goiri/") and len(locations)>REPLICATION_DEFAULT:
						## Check if all locations area available
						#nodeHdfsReady = getNodesHdfsReady()
						#availSources = True
						#for location in locations:
							#if location not in nodeHdfsReady:
								#availSources = False
								#break
						#if availSources:
							## Reduce replication
							#writeLog("logs/ghadoop-scheduler.log", str(self.getCurrentTime())+"\tCleaning replication "+str(fileId))
							#thread = SetReplicationThread(self.timeStart, fileId, REPLICATION_DEFAULT)
							#thread.start()
							#self.threads[fileId] = (thread, datetime.now())
							#file.blocks = {}
				#if len(self.threads)>=self.replThreads:
					#break
			time.sleep(2.0)
	
		# Killing waiting threads
		#for threadId in list(self.threads.keys()):
			#thread = self.threads[threadId]
			#thread[0].kill()
			
	def getCurrentTime(self):
		timeNow = datetime.now()
		timeNow = datetime(timeNow.year, timeNow.month, timeNow.day, timeNow.hour, timeNow.minute, timeNow.second)
		return toSeconds(timeNow-self.timeStart)


class RemoveExtraReplication(threading.Thread):
	def __init__(self, fileId, replication):
		threading.Thread.__init__(self)
		self.fileId = fileId
		self.repl = repl
	
	def run(self):
		replicator = Popen([HADOOP_HOME+"/bin/hadoop", "fs", "-setrep", "-w", str(self.repl), "-R", str(self.fileId)], stdout=open('/dev/null', 'w'), stderr=open('/dev/null', 'w'))
		time.sleep(2.0)

class SetReplicationThread(threading.Thread):
	def __init__(self, timeStart, fileId, replication, retries=0):
		threading.Thread.__init__(self)
		self.timeStart = timeStart
		self.fileId = fileId
		self.replication = replication
		self.retries = retries
		self.replicator = None
		self.running = True
		
	def kill(self):
		self.running = False
		if self.replicator != None:
			try:
				os.kill(self.replicator.pid, signal.SIGTERM)
				writeLog("logs/ghadoop-scheduler.log", str(self.getCurrentTime())+"\tKilled replication "+self.fileId)
			except OSError:
				None
				#print "Error killing "+str(self.fileId)
	
	def run(self):
		file = files[self.fileId]

		# Check if the nodes are available		
		nodeList = file.getLocation()
		
		writeLog("logs/ghadoop-scheduler.log", str(self.getCurrentTime())+"\tChange replication "+self.fileId+" @ "+str(nodeList))
		
		# Waiting for one of the nodes to be available
		ready = False
		while not ready and self.running:
			nodeHdfsReady = getNodesHdfsReady()
			for nodeId in nodeList:
				if nodeId in nodeHdfsReady:
					ready = True
					break
			if not ready:
				time.sleep(0.5)
		
		# Change replication
		if self.running:
			writeLog("logs/ghadoop-scheduler.log", str(self.getCurrentTime())+"\tChanging replication "+self.fileId+" @ "+str(getNodesHdfsReady()))

			extra=self.retries
			setrep = self.replication+extra
			nodes = getNodes()
			if setrep > len(nodes):
				setrep = len(nodes)
			self.replicator = Popen([HADOOP_HOME+"/bin/hadoop", "fs", "-setrep", "-w", str(setrep), "-R", str(self.fileId)], stdout=open('/dev/null', 'w'), stderr=open('/dev/null', 'w'))
			self.replicator.wait()
			# Cleaning the extra....
			if extra>0:
				self.replicator = Popen([HADOOP_HOME+"/bin/hadoop", "fs", "-setrep", "-w", str(self.replication), "-R", str(self.fileId)], stdout=open('/dev/null', 'w'), stderr=open('/dev/null', 'w'))
				time.sleep(0.5)
				try:
					os.kill(self.replicator.pid, signal.SIGTERM)
				except OSError:
					None
			
			# Remove all replica locations, updated later
			setFilesUpdated(False)
			
			# Waiting until the file list is updated...
			while not filesUpdated() and self.running:
				time.sleep(0.5)
			writeLog("logs/ghadoop-scheduler.log", str(self.getCurrentTime())+"\tChanged replication "+self.fileId+" @ "+str(file.getLocation()))
			
			# Kill thread if it is still alive
			if self.replicator.poll() == None:
				os.kill(self.replicator.pid, signal.SIGTERM)

	def getCurrentTime(self):
		timeNow = datetime.now()
		timeNow = datetime(timeNow.year, timeNow.month, timeNow.day, timeNow.hour, timeNow.minute, timeNow.second)
		return toSeconds(timeNow-self.timeStart)

