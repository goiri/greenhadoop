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

from ghadoopcommons import *

# Logs the queue status at every moment
class SchedulerLogger(threading.Thread):
	def __init__(self, timeStart):
		threading.Thread.__init__(self)
		self.running = True
		self.timeStart = timeStart
		self.taskRunning = 0
		self.taskSucceed = 0
		
		# Scheduler headers
		writeLog("logs/ghadoop-scheduler.log", "# Queue logger")

	
	def getTasksRunning(self):
		return self.taskRunning
		
	def getTasksFinished(self):
		return self.taskSucceed
		
	def kill(self):
		self.running = False

	def run(self):
		while self.running:
			# Log number of jobs and tasks
			# Tasks
			taskWaiting = 0
			taskData = 0
			taskPending = 0
			self.taskRunning = 0
			self.taskSucceed = 0
			taskUnknown = 0
			for task in getTasks().values():
				if task.state == "WAITING":
					taskWaiting += 1
				elif task.state == "DATA":
					taskData += 1
				elif task.state == "PREP":
					taskPending += 1
				elif task.state == "RUNNING":
					self.taskRunning += 1
				elif task.state == "SUCCEEDED":
					self.taskSucceed += 1
				else:
					taskUnknown += 1
			# Jobs
			jobWaiting = 0
			jobData = 0
			jobPending = 0
			jobRunning = 0
			jobSucceed = 0
			jobFailed = 0
			jobUnknown = 0
			for job in getJobs().values():
				if job.state == "WAITING":
					jobWaiting += 1
					numJobTasks = len(getFilesInDirectories(job.input)) + 1
					taskWaiting += numJobTasks
				elif job.state == "DATA":
					jobData += 1
					numJobTasks = len(getFilesInDirectories(job.input)) + 1
					taskData += numJobTasks
				elif job.state == "PREP":
					jobPending += 1
				elif job.state == "RUNNING":
					jobRunning += 1
				elif job.state == "SUCCEEDED":
					jobSucceed += 1
				elif job.state == "FAILED":
					jobFailed += 1
				else:
					jobUnknown += 1
			writeLog("logs/ghadoop-scheduler.log", "%d\tQueues\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\tTasks\t%d\t%d\t%d\t%d\t%d\t%d\t%d" % (self.getCurrentTime(), jobWaiting, jobData, jobPending, jobRunning, jobSucceed, jobFailed, jobUnknown, jobWaiting+jobData+jobPending+jobRunning+jobSucceed+jobFailed, taskWaiting, taskData, taskPending, self.taskRunning, self.taskSucceed, taskUnknown, taskWaiting+taskData+taskPending+self.taskRunning+self.taskSucceed+taskUnknown))
			
			time.sleep(2.0)

	def getCurrentTime(self):
		timeNow = datetime.now()
		timeNow = datetime(timeNow.year, timeNow.month, timeNow.day, timeNow.hour, timeNow.minute, timeNow.second)
		return toSeconds(timeNow-self.timeStart)

# Logs when tasks and jobs finish
class JobLogger(threading.Thread):
	def __init__(self, timeStart):
		threading.Thread.__init__(self)
		
		self.running = True
		self.timeStart = timeStart
		
		self.loggedTask = []
		self.loggedJobs = []
		
		#writeLog("logs/ghadoop-jobs.log", "# "+str(timeStart)+" Workload="+str(workloadFile)+" SlotLength="+str(SLOTLENGTH)+" TotalTime="+str(TOTALTIME)+" SchedEvent="+str(SCHEDULE_EVENT)+" SchedSlot="+str(SCHEDULE_SLOT)+" WorkloadGen="+str(WORKLOADGEN))
		writeLog("logs/ghadoop-jobs.log", "# Deadline = "+str(DEADLINE))
		writeLog("logs/ghadoop-jobs.log", "# t\tid\twork\tnode\tpriority\tsubmit\tstart\tend\twait\trun\ttotal")

	def getTasksFinished():
		return len(self.loggedTask)
	
	def kill(self):
		self.running = False

	def run(self):
		while self.running:
			for task in getTasks().values():
				if task.id not in self.loggedTask:
					if task.state == 'SUCCEEDED' and toSeconds(task.submit-self.timeStart)>=0:
						# Fixing missing data
						if task.submit==None:
							if task.start==None:
								if task.end==None:
									task.end=datetime.now()
								task.start=task.end
							task.submit=task.start
						if task.start==None:
							if task.end==None:
								task.end=datetime.now()
							task.start=task.end
						if task.end==None:
							task.end=datetime.now()
						# Task info
						t = self.getCurrentTime()
						submit = toSeconds(task.submit-self.timeStart)
						start = toSeconds(task.start-self.timeStart)
						end = toSeconds(task.end-self.timeStart)
						waittime = toSeconds(task.start-task.submit)
						runtime = toSeconds(task.end-task.start)
						totaltime = toSeconds(task.end-task.submit)
						priority = jobs[task.jobId].priority
						# Save job info
						if task.jobsetup:
							writeLog("logs/ghadoop-jobs.log", "%d\t%s\t%s\t%s\t%s\t%d\t%d\t%d\t%d\t%d\t%d\tJobSetup" % (t, task.id, task.jobId, task.node, priority, submit, start, end, waittime, runtime, totaltime))
						else:
							writeLog("logs/ghadoop-jobs.log", "%d\t%s\t%s\t%s\t%s\t%d\t%d\t%d\t%d\t%d\t%d" % (t, task.id, task.jobId, task.node, priority, submit, start, end, waittime, runtime, totaltime))
						# Logged
						self.loggedTask.append(task.id)
			for job in getJobs().values():
				if job.id not in self.loggedJobs:
					if job.state == 'SUCCEEDED' and toSeconds(job.submit-self.timeStart)>=0:
						# Fixing missing data
						if job.submit==None:
							if job.start==None:
								if job.end==None:
									job.end=datetime.now()
								job.start=job.end
							job.submit=job.start
						if job.start==None:
							if job.end==None:
								job.end=datetime.now()
							job.start=job.end
						if job.end==None:
							job.end=datetime.now()
						# Job info
						t = self.getCurrentTime()
						id = job.id.replace("job_","")
						submit = toSeconds(job.submit-self.timeStart)
						start = toSeconds(job.start-self.timeStart)
						end = toSeconds(job.end-self.timeStart)
						waittime = toSeconds(job.start-job.submit)
						runtime = toSeconds(job.end-job.start)
						totaltime = toSeconds(job.end-job.submit)
						priority = job.priority
						nodes = []
						for taskId in job.tasks:
							task = getTasks()[taskId]
							nodeId = task.node
							if nodeId not in nodes:
								nodes.append(nodeId)
						# Save job info
						writeLog("logs/ghadoop-jobs.log", "%d\t%s\t%s\t%s\t%s\t%d\t%d\t%d\t%d\t%d\t%d" % (t, job.id, id, str(sorted(nodes)), priority, submit, start, end, waittime, runtime, totaltime))
						# Logged
						self.loggedJobs.append(job.id)
					elif job.state == 'FAILED' and toSeconds(job.submit-self.timeStart)>=0:
						t = self.getCurrentTime()
						id = job.id
						submit = -1
						if job.submit != None:
							submit = toSeconds(job.submit-self.timeStart)
						priority = job.priority
						# Save job info
						writeLog("logs/ghadoop-jobs.log", "%d\t%s\t%s\t%s\t%s\t%d\t%d\t%d\t%d\t%d\t%d" % (t, job.id, id, "-", priority, submit, -1, -1, -1, -1, -1))
						# Logged
						self.loggedJobs.append(job.id)				
			time.sleep(2.0)
	
	def getCurrentTime(self):
		timeNow = datetime.now()
		timeNow = datetime(timeNow.year, timeNow.month, timeNow.day, timeNow.hour, timeNow.minute, timeNow.second)
		return toSeconds(timeNow-self.timeStart)


# Logs when tasks and jobs finish
class EnergyLogger(threading.Thread):
	def __init__(self, timeStart, data):
		threading.Thread.__init__(self)
		
		self.running = True
		self.timeStart = timeStart
		self.data = data
		
		self.reqPower = 0.0
		self.reqBrownPower = 0.0
		self.reqGreenPower = 0.0
		
		# Write log headers
		# Energy headers
		writeLog("logs/ghadoop-energy.log", "# set style fill solid")
		writeLog("logs/ghadoop-energy.log", "# plot \"testenergy\" using 1:7 lc rgb \"brown\" w boxes title \"Brown\", \"testenergy\" using 1:5 lc rgb \"green\" w boxes title \"Green\", \"testenergy\" using 1:2 w steps lw 3 lc rgb \"blue\" title \"Green availability\"")

		writeLog("logs/ghadoop-energy.log", "# "+str(self.timeStart))
		writeLog("logs/ghadoop-energy.log", "# t\tgreen\tpredi\tbrown\trun nodes\tnodes\tgreenUse\tbrownUse\ttotalUse")
		writeLog("logs/ghadoop-energy.log", "0\t0.0\t0.0\t0.0\t0\t0\t0\t0.0\t0.0\t0.0")

	def getReqPower(self):
		return self.reqPower
		
	def getReqBrownPower(self):
		return self.reqBrownPower

	def getReqGreenPower(self):
		return self.reqGreenPower

	def getCurrentTime(self):
		timeNow = datetime.now()
		timeNow = datetime(timeNow.year, timeNow.month, timeNow.day, timeNow.hour, timeNow.minute, timeNow.second)
		return toSeconds(timeNow-self.timeStart)
	
	def kill(self):
		self.running = False

	def run(self):
		while self.running:
			# Actual power (recalculate)
			reqPower = POWER_IDLE_GHADOOP
			# Check current nodes and number of tasks
			workNodes = 0
			onNodes = 0
			decNodes = 0
			nodes = getNodes()
			for nodeId in nodes:
				up = nodes[nodeId]
				if up[0] == "UP" or up[1] == "UP":
					onNodes += 1
					if nodeId in nodeTasks and len(nodeTasks[nodeId])>0:
						workNodes += 1
						if len(nodeTasks[nodeId]) == 1:
							reqPower += Node.POWER_1JOB
						elif len(nodeTasks[nodeId]) == 2:
							reqPower += Node.POWER_2JOB
						elif len(nodeTasks[nodeId]) == 3:
							reqPower += Node.POWER_3JOB
						elif len(nodeTasks[nodeId]) >= 4:
							reqPower += Node.POWER_4JOB
					else:
						reqPower += Node.POWER_IDLE
				elif up[0] == "DEC" or up[1] == "DEC":
					decNodes += 1
					reqPower += Node.POWER_IDLE
				else:
					reqPower += Node.POWER_S3
			
			# Calculate used energy and power
			greenEnergyAvail = self.data.greenAvailArray[0] # Wh
			#greenEnergyPredi = self.data.greenPrediArray[4] # Wh
			greenEnergyPredi = self.data.greenPrediArray[4] # Wh
			greenEnergyPredi2 = self.data.greenPrediArray[1] # Wh
			greenEnergyPredi3 = self.data.greenPrediArray[5] # Wh
			greenEnergyPredi4 = self.data.greenPrediArray[12] # Wh
			
			brownEnergyPrice = self.data.brownPriceArray[0] # $/KWh
			
			# Accounting requireds
			brownReqPower = 0.0
			if reqPower>0:
				greenPowerAvail = greenEnergyAvail
				if reqPower>greenEnergyAvail:
					greenReqPower = greenPowerAvail
					brownReqPower = reqPower-greenEnergyAvail
				else:
					greenReqPower = reqPower
		
			# Update information
			self.reqPower = reqPower
			self.reqBrownPower = brownReqPower
			self.reqGreenPower = greenReqPower
			
			writeLog("logs/ghadoop-energy.log", "%d\t%.2f\t%.2f\t%.5f\t%d\t%d\t%d\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f" % (self.getCurrentTime(), greenEnergyAvail, greenEnergyPredi, brownEnergyPrice, workNodes, onNodes, onNodes+decNodes, greenReqPower, brownReqPower, reqPower, greenEnergyPredi2, greenEnergyPredi3, greenEnergyPredi4))
			
			time.sleep(2.0)
		# Last line in the log
		writeLog("logs/ghadoop-energy.log", "%d\t0.0\t0.0\t0.0\t0\t0\t0\t0.0\t0.0\t0.0" % (self.getCurrentTime()))
