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
import threading
import os
import sys
import signal

from datetime import datetime,timedelta
from subprocess import call, PIPE, Popen

from ghadoopcommons import *


class MonitorMapred(threading.Thread):
	def __init__(self):
		threading.Thread.__init__(self)
		self.running = True
		
		# JobTracker
		self.logfileMapr = HADOOP_HOME+"/logs/hadoop-"+USER+"-jobtracker-"+MASTER_NODE+".log"
		#self.logfileMapr = "/scratch/muhammed/hadoop_log/hadoop-muhammed-jobtracker-crypt10.log"
		
		# http://forums.devshed.com/python-programming-11/how-to-monitor-a-file-for-changes-85767.html
		self.fileMapr = open(self.logfileMapr, 'r')
		self.watcherMapr = os.stat(self.logfileMapr)
		self.this_modifiedMapr = self.last_modifiedMapr = self.watcherMapr.st_mtime

		# Get nodes:
		nodes = getNodes()
		for nodeId in nodes:
			nodeTasks[nodeId] = []
			nodeJobs[nodeId] = []
			if nodes[nodeId][1] == "UP" or nodes[nodeId][1] == "DEC":
				if nodeId not in getNodesHdfsReady():
					getNodesHdfsReady().append(nodeId)

		# Read previous state of the system
		self.startTime = None
		# TODO read previous history
		if False:
			while True:
				line = self.fileMapr.readline()
				if not line: break
				#print line
				change = self.parseLine(line)
			#print "Tasks "+str(len(tasks))
			#print "Jobs "+str(len(jobs))
			print "Ready!"
		# Go to the end of the file
		self.fileMapr.seek(0,2)

		# Start helper threads
		self.checkstatus = MonitorMapredCheckStatus(self)
		self.checkstatus.start()
		self.nodestatus = MonitorNodeCheckStatus()
		self.nodestatus.start()

	def kill(self):
		self.running = False
		self.checkstatus.kill()
		self.nodestatus.kill()

	def run(self):
		# Monitor
		lastUpdate = 0
		change = True
		while self.running:
			# Update from log: JobTracker
			if self.this_modifiedMapr > self.last_modifiedMapr:
				self.last_modifiedMapr = self.this_modifiedMapr
				# File was modified, so read new lines, look for error keywords
				while 1:
					line = self.fileMapr.readline()
					if not line: break
					auxChange = self.parseLine(line)
					if auxChange:
						change = True
			self.watcherMapr = os.stat(self.logfileMapr)
			self.this_modifiedMapr = self.watcherMapr.st_mtime
			
			# Updates
			lastUpdate -= 1
			if lastUpdate<0:
				lastUpdate=5
				change=True
				
			if DEBUG>3 and change:
				self.printOutput()
			change=False
			
			time.sleep(1.0)

	def parseLine(self, line):
		change = False
		try:
			date = datetime.strptime(line.split(",")[0], "%Y-%m-%d %H:%M:%S")
			if self.startTime == None:
				self.startTime = date
			
			line = line.replace("\n", "")
			lineSplit = line.split(" ")
			if lineSplit[3].startswith("org.apache.hadoop.mapred.JobTracker") and len(lineSplit)>5:
				if line.find("added successfully")>=0:
					# Add job
					jobId = lineSplit[5]
					jobId = jobId[jobId.find("_")+1:]
					
					if jobId not in jobs:
						jobs[jobId] = Job(jobId, "PREP", date)
					else:
						jobs[jobId].state = "PREP"
						if jobs[jobId].submit == None:
							jobs[jobId].submit = date
					
					#job = addMonitorJob(id, jobId)
					#job.submit = date
					#job.state = "PREP"
					
					## Store in data structures
					#runningJobs.append(id)
					
					if DEBUG>3:
						print str(date)+" Job "+jobId+"("+jobId+") started."
					change=True
				elif line.find("Adding task")>=0 and len(lineSplit)>13:
					# Add task to job
					taskId = lineSplit[10]
					if taskId.endswith(","):
						taskId = taskId[0:len(taskId)-1]
					taskIdSplit= taskId.split("_")
					jobId = taskIdSplit[1]+"_"+taskIdSplit[2]
					
					nodeId = lineSplit[13]
					nodeId = nodeId.replace("'tracker_","")
					nodeId = nodeId[0:nodeId.find(":")]
				
					if taskId not in tasks:
						task = Task(taskId, jobId, "RUNNING", date)
						task.start = date
						if task.submit == None:
							task.submit = date
						tasks[taskId] = task
					else:
						task = tasks[taskId]
						task.state = "RUNNING"
						task.start = date
						if task.submit == None:
							task.submit = date
					task.node = nodeId
					
					# check if this is actual job or just setting up
					if line.find("JOB_SETUP")>=0 or line.find("TASK_CLEANUP")>=0 or line.find("JOB_CLEANUP")>=0:
						task.jobsetup = True
					
					# Check if job existed and update status
					if jobId not in jobs:
						if not task.jobsetup:
							job = Job(jobId, "RUNNING", date, date)
						else:
							job = Job(jobId, "PREP", date)
					else:
						job = jobs[jobId]
						if not task.jobsetup:
							job.state = "RUNNING"
						if job.submit == None:
							job.submit = date
						if not task.jobsetup and job.start == None:
							job.start = date
					if taskId not in job.tasks:
						job.tasks.append(taskId)
					
					# Removing from other nodes
					for otherNodeId in nodeTasks:
						if taskId in nodeTasks[otherNodeId]:
							nodeTasks[otherNodeId].remove(taskId)
					
					# Assign task to node
					if nodeId not in nodeTasks:
						nodeTasks[nodeId] = []
					if taskId not in nodeTasks[nodeId]:
						nodeTasks[nodeId].append(taskId)
					# Assign job to node
					if nodeId not in nodeJobs:
						nodeJobs[nodeId] = []
					if jobId not in nodeJobs[nodeId] and not task.jobsetup:
						nodeJobs[nodeId].append(jobId)
					if DEBUG>3:
						print str(date)+" Task "+taskId+"("+jobId+") to "+nodeId+": "+str(nodeTasks[nodeId])
					change=True
				elif line.find("Removing task")>=0 and len(lineSplit)>6:
					#2011-07-13 17:18:51,532 INFO org.apache.hadoop.mapred.JobTracker: Removing task 'attempt_201107131634_0017_m_000126_0'
					#0 2011-07-13 17:18:51,532
					#2 INFO
					#3 org.apache.hadoop.mapred.JobTracker:
					#4 Removing
					#5 task
					#6 'attempt_201107131634_0017_m_000126_0'
					
					# Add task to job
					attemptId = lineSplit[6]
					if attemptId.startswith("\'"):
						attemptId = attemptId[1:len(attemptId)]
					if attemptId.endswith("\'"):
						attemptId = attemptId[0:len(attemptId)-1]
					if attemptId.endswith(","):
						attemptId = attemptId[0:len(attemptId)-1]
					attemptIdSplit= attemptId.split("_")
					taskId = "task_"+attemptIdSplit[1]+"_"+attemptIdSplit[2]+"_"+attemptIdSplit[3]+"_"+attemptIdSplit[4]
					jobId = attemptIdSplit[1]+"_"+attemptIdSplit[2]
					
					# Removing failed task nodes
					for nodeId in nodeTasks:
						if taskId in nodeTasks[nodeId]:
							nodeTasks[nodeId].remove(taskId)
				elif line.find("Retired job with id")>=0 and len(lineSplit)>8:
					# Job finished
					jobId = lineSplit[8]
					jobId = jobId.replace("'","")
					jobId = jobId[jobId.find("_")+1:]
					
					if jobId not in jobs:
						job = Job(jobId)
						jobs[jobId] = job
					else:
						job = jobs[jobId]
					job.end = date
					if job.submit == None:
						job.submit = date
					if job.start == None:
						job.start = date
					job.state = "SUCCEEDED"
					
					# Remove node -> Job
					for nodeId in nodeJobs:
						if jobId in nodeJobs[nodeId]:
							nodeJobs[nodeId].remove(jobId)
					
					if DEBUG>3:
						print str(date)+" Job "+jobId+"("+jobId+") finished"
					change=True
			elif lineSplit[3].startswith("org.apache.hadoop.mapred.JobInProgress"):
				if line.find("Task")>=0 and line.find("has completed")>=0:
					# Task finished
					taskId = lineSplit[8]
					taskIdSplit= taskId.split("_")
					jobId = taskIdSplit[1]+"_"+taskIdSplit[2]
					
					if taskId in tasks:
						task = tasks[taskId]
						task.end = date
						if task.submit == None:
							task.submit = date
						if task.start == None:
							task.start = date
						task.state = "SUCCEEDED"
					
						# Cleaning running task nodes
						if task.node in nodeTasks:
							if taskId in nodeTasks[task.node]:
								nodeTasks[task.node].remove(taskId)
					
					if DEBUG>3:
						print str(date)+" Task "+taskId+"("+jobId+") finished"
					change=True
				elif line.find("has split on")>=0:
					# Tasks generated
					taskId = lineSplit[4]
					taskId = taskId.replace("tip:","")
					taskIdSplit= taskId.split("_")
					jobId = taskIdSplit[1]+"_"+taskIdSplit[2]
					
					if taskId not in tasks:
						task = Task(taskId, jobId, "PREP", date)
						tasks[taskId] = task
					else:
						task = tasks[taskId]
						task.state = "PREP"
						if task.submit == None:
							task.submit = date
					
					# Check if job existed and update status
					if jobId not in jobs:
						job = Job(jobId, "PREP", date, date)
					else:
						job = jobs[jobId]
						if job.state == "UNKNOWN":
							job.state = "PREP"
						if job.submit == None:
							job.submit = date
					if taskId not in job.tasks:
						job.tasks.append(taskId)
					
					#task = addMonitorTask(id, taskId)
					#if task.submit == None:
						#task.submit = date
					#task.state = "PREP"
					
					if DEBUG>3:
						print str(date)+" Task "+taskId+"("+jobId+") created"
					change=True
				elif line.find("initialized successfully with")>=0:
					# Generate maps and reduces
					jobId = lineSplit[5]
					jobId = jobId.replace("job_", "")
					nmap=int(lineSplit[9])
					nred=int(lineSplit[13])
					
					# Check if job existed and update status
					if jobId not in jobs:
						job = Job(jobId, "PREP", date, date)
					else:
						job = jobs[jobId]
						if job.state == "UNKNOWN":
							job.state = "PREP"
						if job.submit == None:
							job.submit = date
					
					for i in range(0, nred):
						taskId = "task_"+jobId+"_r_"+str(i).zfill(6)
						task = Task(taskId, jobId, "PREP", date)
						tasks[taskId] = task
						if taskId not in job.tasks:
							job.tasks.append(taskId)
					for i in range(0, nmap):
						taskId = "task_"+jobId+"_m_"+str(i).zfill(6)
						task = Task(taskId, jobId, "PREP", date)
						tasks[taskId] = task
						if taskId not in job.tasks:
							job.tasks.append(taskId)
					change=True
		except ValueError:
			if DEBUG>3:
				print "Error line: "+line
		except Exception, e:
			print e
			print "Error line: "+line
			
		return change
	
	def printOutput(self):
		print "========================================================="
		print "Tasks ("+str(len(tasks))+"):"
		runn = 0
		prep = 0
		comp = 0
		unkn = 0
		for taskId in sorted(tasks):
			task = tasks[taskId]
			if task.state == "SUCCEEDED":
				comp+=1
			elif task.state == "RUNNING":
				runn+=1
			elif task.state == "PREP":
				prep+=1
			else:
				unkn+=1
			if len(tasks)<30:
				print "  "+str(task)
		if len(tasks)>=30:
			print "  Unknown:  "+str(unkn)
			print "  Queue:    "+str(prep)
			print "  Running:  "+str(runn)
			print "  Complete: "+str(comp)
		
		nodes = getNodes()
		
		print "Nodes->Tasks ("+str(len(nodeTasks))+"):"
		for nodeId in sorted(nodeTasks):
			out = "\t"+str(nodeId)
			if nodeId in nodes:
				for status in nodes[nodeId]:
					out += " "+status
			if nodeId in nodeTasks and len(nodeTasks[nodeId])>0:
				out+=":\t"+str(nodeTasks[nodeId])
			print out
			
		print "Nodes->Jobs ("+str(len(nodeJobs))+"):"
		for nodeId in sorted(nodeJobs):
			out = "\t"+str(nodeId)
			if nodeId in nodes:
				for status in nodes[nodeId]:
					out += " "+status
			if nodeId in nodeJobs and len(nodeJobs[nodeId])>0:
				out+=":\t"+str(nodeJobs[nodeId])
			print out
		
		print "Jobs ("+str(len(jobs))+"):"
		for jobId in sorted(jobs):
			out = ""
			job = jobs[jobId]
			for taskId in job.tasks:
				task = tasks[taskId]
				if task.state == "RUNNING":
					out +=bcolors.BLUEBG+" "+bcolors.ENDC
				elif task.state == "SUCCEEDED":
					out +=bcolors.GREENBG+" "+bcolors.ENDC
				else:
					out +=" "
			print "\t"+str(job)+"\t"+out+" "+str(len(job.tasks))
		
		#print "Required files ("+str(len(requiredFiles))+"):"
		#for fileId in sorted(requiredFiles):
			#print "\t"+str(fileId)


class MonitorMapredCheckStatus(threading.Thread):
	def __init__(self, monitor):
		threading.Thread.__init__(self)
		self.monitor = monitor
		self.running = True
		self.times = 0

	def kill(self):
		self.running = False

	def run(self):
		# Monitor
		while self.running:
			self.checkStatus() 
			
			time.sleep(5.0)
	
	def checkStatus(self):
		self.times += 1
		if self.times%3 == 0:
			# Check with Hadoop info
			for job in getJobsHadoop().values():
				# Update info in local structure
				if job.id not in jobs:
					jobs[job.id] = job
				else:
					if job.state=="SUCCEEDED" and jobs[job.id].end == None:
						jobs[job.id].end = job.end
					jobs[job.id].state = job.state
					jobs[job.id].priority = job.priority
		# Update jobs succeeded
		for job in jobs.values():
			if job.state == "SUCCEEDED":
				for taskId in job.tasks:
					task = tasks[taskId]
					if task.end == None:
						task.end = job.end
					task.state = "SUCCEEDED"
		# Update tasks succeeded
		for task in tasks.values():
			if task.state == "SUCCEEDED":
				try:
					if task.id in nodeTasks[task.node]:
						nodeTasks[task.node].remove(task.id)
				except KeyError:
					None
		# Update node->job
		for nodeId in nodeJobs:
			for jobId in list(nodeJobs[nodeId]):
				try:
					job = jobs[jobId]
					if job.state == "SUCCEEDED":
						nodeJobs[nodeId].remove(jobId)
				except KeyError:
					None
				except ValueError:
					None
		# Update node->task
		for nodeId in nodeTasks:
			for taskId in list(nodeTasks[nodeId]):
				try:
					task = tasks[taskId]
					if task.state == "SUCCEEDED" or task.node != nodeId:
						nodeTasks[nodeId].remove(taskId)
				except KeyError:
					None
				except ValueError:
					None


class MonitorNodeCheckStatus(threading.Thread):
	def __init__(self):
		threading.Thread.__init__(self)
		self.running = True
		
		# Namenode
		self.logfileHdfs = HADOOP_HOME+"/logs/hadoop-"+USER+"-namenode-"+MASTER_NODE+".log"
		self.fileHdfs = open(self.logfileHdfs, 'r')
		self.watcherHdfs = os.stat(self.logfileHdfs)
		self.this_modifiedHdfs = self.last_modifiedHdfs = self.watcherHdfs.st_mtime

		# Go to the end of the file
		self.fileHdfs.seek(0,2)
		
		# Read nodes
		pipe=Popen([HADOOP_HOME+"/bin/hdfs", "dfsadmin", "-printTopology"], stdout=PIPE, stderr=open('/dev/null', 'w'))
		text = pipe.communicate()[0]
		
		self.nodeName = {}
		for line in text.split('\n'):
			if line !="" and not line.startswith("Rack:"):
				line = line.strip()
				lineSplit = line.split(" ")
				if len(lineSplit)>=2:
					nodeId = lineSplit[1].replace("(", "").replace(")", "")
					self.nodeName[lineSplit[0]] = nodeId	
	def kill(self):
		self.running = False
		
	def run(self):
		# Monitor
		while self.running:
			# Update from log: Namenode
			if self.this_modifiedHdfs > self.last_modifiedHdfs:
				self.last_modifiedHdfs = self.this_modifiedHdfs
				# File was modified, so read new lines, look for error keywords
				while True:
					line = self.fileHdfs.readline()
					if not line:
						break
					try:
						if line.find("org.apache.hadoop.net.NetworkTopology")>0:
							lineSplit = line.split(" ")
							if len(lineSplit)>3 and lineSplit[3].startswith("org.apache.hadoop.net.NetworkTopology"):
								date = datetime.strptime(line.split(",")[0], "%Y-%m-%d %H:%M:%S")
								if line.find("Removing a node")>=0:
									nodeId = lineSplit[7]
									nodeId = nodeId.replace("\n", "")
									nodeId = nodeId[nodeId.rindex("/")+1:]
									
									if nodeId in self.nodeName:
										nodeId = self.nodeName[nodeId]
									if nodeId in getNodesHdfsReady():
										getNodesHdfsReady().remove(nodeId)
										change = True
								elif line.find("Adding a new node")>=0:
									nodeId = lineSplit[8]
									nodeId = nodeId.replace("\n", "")
									nodeId = nodeId[nodeId.rindex("/")+1:]
									
									if nodeId in self.nodeName:
										nodeId = self.nodeName[nodeId]
									if nodeId not in getNodesHdfsReady():
										getNodesHdfsReady().append(nodeId)
										change = True
					except ValueError:
						if DEBUG>3:
							print "Error line: "+line
					except TypeError:
						if DEBUG>3:
							print "Error line: "+line
			self.watcherHdfs = os.stat(self.logfileHdfs)
			self.this_modifiedHdfs = self.watcherHdfs.st_mtime
			
			time.sleep(2.0)

if __name__=='__main__':
	DEBUG=4
	
	thread = MonitorMapred()
	thread.start()
	
	signal.signal(signal.SIGINT, signal_handler)
	
	while True:
		time.sleep(10.0)
	
	thread.join()
