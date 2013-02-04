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
import time
from datetime import datetime,timedelta

from ghadoopcommons import *
from itertools import *



# Thread that takes care of the queue and flushes it whenever is needed
class WaitingQueueManager(threading.Thread):
	def __init__(self, timeStart):
		threading.Thread.__init__(self)
		self.running = True
		self.timeStart = timeStart
		
		# Scheduler headers
		writeLog("logs/ghadoop-scheduler.log", "# Waiting queue manager")
	
	def kill(self):
		print "Finishing waiting queue manager..."
		self.running = False
	
	def run(self):
		while self.running:
			change = False
			if len(waitingQueue)>0:
				submittedJobs = 0
				threads = []
				
				# Check if the time limit has been reach
				for waitingJob in list(waitingQueue):
					jobId = waitingJob[0]
					jobs = getJobs()
					job = jobs[jobId]
					if job.deadline!=None and job.deadline<=datetime.now():
						# Check if all previous jobs are already finished
						prevJobFinished = True
						for prevJobId in job.prevJobs:
							if jobs[prevJobId].state != "SUCCEEDED":
								prevJobFinished = False
						if prevJobFinished:
							input = waitingJob[3]
							
							if getDebugLevel() >= 2:
								print "Waiting->Data: Job "+waitingJob[0]+" now="+str(datetime.now())+" deadline="+str(job.deadline)
							writeLog("logs/ghadoop-scheduler.log", str(self.getCurrentTime())+"\tWaiting->Data: Job "+waitingJob[0]+" now="+str(datetime.now())+" deadline="+str(job.deadline)+" input="+str(input))
							
							# Change priority
							# jobId,0
							# submitTime, 1
							# command, 2
							# input=None,3 
							# output=None,4 
							# priority=None 5
							# (jobId, submit, command, input, output, priority)
							sWaitingJob = (waitingJob[0], waitingJob[1], waitingJob[2], waitingJob[3], waitingJob[4], "VERY_HIGH")
							thread = threading.Thread(target=submitJobHadoop,args=sWaitingJob)
							thread.start()
							threads.append(thread)
							waitingQueue.remove(waitingJob)
							submittedJobs += 1
							change = True
				
				# Collect system information
				nodes = getNodes()
				offNodes = []
				decNodes = []
				onNodes = []
				for nodeId in nodes:
					if nodes[nodeId][1]=="DOWN":
						offNodes.append(nodeId)
					elif nodes[nodeId][1]=="DEC":
						decNodes.append(nodeId)
					elif nodes[nodeId][1]=="UP":
						onNodes.append(nodeId)
				
				# Account tasks and jobs waiting in hadoop
				mapsHadoopWaiting = 0
				mapsGHadoopWaiting = 0
				numJobsWaiting = 0
				for job in getJobs().values():
					if job.state!="SUCCEEDED" and job.state!="FAILED" and len(job.tasks)>0:
						for taskId in job.tasks:
							task = tasks[taskId]
							if task.state!="SUCCEEDED" and taskId.find("_m_")>=0:
								mapsHadoopWaiting += 1
					elif job.state=="DATA" or job.state=="PREP" or job.state=="RUNNING":
						# Default value for the number of tasks
						numJobTasks = len(getFilesInDirectories(job.input))
						mapsHadoopWaiting += numJobTasks
					elif job.state=="WAITING":
						#mapsGHadoopWaiting += TASK_JOB
						numJobTasks = len(getFilesInDirectories(job.input))
						mapsGHadoopWaiting += numJobTasks
					if job.state=="DATA" or job.state=="PREP" or job.state=="RUNNING":
						numJobsWaiting += 1
				# Account maps slots
				mapsSlotsTotal = 0
				mapsSlotsUsed = 0
				for nodeId in onNodes:
					if nodes[nodeId][0]=="UP":
						mapsSlotsTotal += MAP_NODE
						if nodeId in nodeTasks:
							for taskId in nodeTasks[nodeId]:
								if taskId.find("_m_")>=0:
									mapsSlotsUsed += 1
				mapsSlotsFree = mapsSlotsTotal - mapsSlotsUsed
				if mapsSlotsFree<0:
					mapsSlotsFree = 0
				
				# Nodes that cannot or should not provide data
				unavailableNodes = list(chain(offNodes, decNodes))
				
				# Account the number of extra nodes that would be required for data
				sortWaitingQueue = []
				for waitingJob in list(waitingQueue):
					jobId = waitingJob[0]
					job = getJobs()[jobId]
					if job.deadline!=None and job.submit!=None and datetime.now() > (job.deadline - (job.deadline-job.submit)/2):
						sortWaitingQueue.append((jobId, 0))
					else:
						input = waitingJob[3]
						# Get missing files from required files by job
						dataNodes = minNodesFiles(input, unavailableNodes)
						sortWaitingQueue.append((jobId, len(dataNodes)))
				sortWaitingQueue = sorted(sortWaitingQueue, key=itemgetter(1))

				# Check if any job has all data available
				while len(sortWaitingQueue)>0 and sortWaitingQueue[0][1]==0 and numJobsWaiting<MAX_SUBMISSION:
					jobId = sortWaitingQueue.pop(0)[0]
					# Search job in the queue
					index = -1
					for i in range(0, len(waitingQueue)):
						waitingJob = waitingQueue[i]
						if waitingJob[0] == jobId:
							index = i
							break
					if index>=0:
						# Check if all previous jobs are already finished
						jobs = getJobs()
						job =jobs[jobId]
						prevJobFinished = True
						for prevJobId in job.prevJobs:
							if jobs[prevJobId].state != "SUCCEEDED":
								prevJobFinished = False
						if prevJobFinished:
							# Job with no extra nodes required found
							waitingJob = waitingQueue[index]
							input = waitingJob[3]
							if getDebugLevel() >= 2:
								print "Waiting->Data: Job "+jobId+" has all data available input="+str(input)
							writeLog("logs/ghadoop-scheduler.log", str(self.getCurrentTime())+"\tWaiting->Data: Job "+waitingJob[0]+" has all data available "+str(input))
							waitingQueue.remove(waitingJob)
							thread = threading.Thread(target=submitJobHadoop,args=waitingJob)
							thread.start()
							threads.append(thread)
							numJobsWaiting += 1
							numJobTasks = len(getFilesInDirectories(input))
							mapsHadoopWaiting += numJobTasks + 1
							change = True
				
				#extraJobs = math.ceil(auxIdleNodes*TASK_NODE/TASK_JOB)
				#extraJobs -= submittedJobs
				if len(sortWaitingQueue)>0 and getDebugLevel() > 0:
					print "WaitingMaps="+str(mapsHadoopWaiting)+"+"+str(mapsGHadoopWaiting)+" Slots="+str(mapsSlotsFree)+"/"+str(mapsSlotsTotal)+" WaitingQueue="+str(len(waitingQueue))+" ["+str(sortWaitingQueue[0])+"...]"
				
				# Fill idle nodes
				TIMES_FULL = 3
				while len(sortWaitingQueue)>0 and mapsHadoopWaiting<=TIMES_FULL*mapsSlotsTotal:
					if getDebugLevel() > 0:
						print "There are idle resources! waiting="+str(mapsHadoopWaiting)+" capacity="+str(mapsSlotsTotal)+" limit="+str(TIMES_FULL*mapsSlotsTotal)
					jobId = sortWaitingQueue.pop(0)[0]
					# Search job in the queue
					index = -1
					for i in range(0, len(waitingQueue)):
						waitingJob = waitingQueue[i]
						if waitingJob[0] == jobId:
							index = i
							break
					if index>=0:
						# Choose the waiting job with less nodes required
						waitingJob = waitingQueue[index]
						waitingQueue.remove(waitingJob)
						jobId = waitingJob[0]
						input = waitingJob[3]
						# Check if all previous jobs are already finished
						jobs = getJobs()
						job =jobs[jobId]
						prevJobFinished = True
						for prevJobId in job.prevJobs:
							if jobs[prevJobId].state != "SUCCEEDED":
								prevJobFinished = False
						if prevJobFinished:
							if getDebugLevel() >= 2:
								print "Waiting->Data: there are idle resources to run job "+jobId+" input="+str(input)
							writeLog("logs/ghadoop-scheduler.log", str(self.getCurrentTime())+"\tWaiting->Data: there are idle resources to run job "+jobId+" input="+str(input))
							# Select first job, it may bring other jobs
							thread = threading.Thread(target=submitJobHadoop,args=waitingJob)
							thread.start()
							threads.append(thread)
							numJobsWaiting += 1
							numJobTasks = len(getFilesInDirectories(input))
							mapsHadoopWaiting += numJobTasks + 1
							change = True
				# Wait for everything to be submitted
				#while len(threads)>0:
					#threads.pop().join()
			if not change:
				queueSize = len(waitingQueue)
				# A maximum of 5 seconds
				waitTime = 5
				checkTime = 0.5
				for i in range(0, int(waitTime/checkTime)):
					time.sleep(checkTime)
					if queueSize != len(waitingQueue):
						break
	
	def flush(self):
		threads = []
		while len(waitingQueue)>0:
			waitingJob = waitingQueue.pop()
			thread = threading.Thread(target=submitJobHadoop,args=waitingJob)
			thread.start()
			threads.append(thread)
		
		# Wait for everything to be submitted
		while len(threads)>0:
			threads.pop().join()

	def getCurrentTime(self):
		timeNow = datetime.now()
		timeNow = datetime(timeNow.year, timeNow.month, timeNow.day, timeNow.hour, timeNow.minute, timeNow.second)
		return toSeconds(timeNow-self.timeStart)
