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

# bash: for file in `ls logs/`; do python ghadoopparser.py logs/$file/$file > logs/$file/$file-summary.log; done

import sys
import math

from ghadoopcommons import *

ALPHA = 0.8
PERIOD = 100
NUM_SAMPLE_TASKS = 300

LOG_ENERGY = "ghadoop-energy.log"
LOG_JOBS = "ghadoop-jobs.log"
LOG_SCHEDULER = "ghadoop-scheduler.log"

if len(sys.argv)>1:
	LOG_ENERGY = sys.argv[1]+"-energy.log"
	LOG_JOBS = sys.argv[1]+"-jobs.log"
	LOG_SCHEDULER = sys.argv[1]+"-scheduler.log"

# Read queue and calculate average
prevLineSplit = None
timeQueueSize = []
timeRunSize = []
avgSum = 0
avgTim = 0
for line in open(LOG_SCHEDULER, "r"):
	if not line.startswith("#") and line!="\n":
		line = line.replace("\n", "")
		lineSplit = line.split("\t")
		if lineSplit[1]=="Queues":
			lineSplit[0] = int(lineSplit[0])
			
			current = lineSplit[0]
			taskW = int(lineSplit[11])
			taskD = int(lineSplit[12])
			taskP = int(lineSplit[13])
			taskR = int(lineSplit[14])
			taskSum = taskW + taskD + taskP + taskR
			
			if len(timeQueueSize)>0 and timeQueueSize[len(timeQueueSize)-1][0]==current:
				timeQueueSize[len(timeQueueSize)-1] = (current, taskSum)
			else:
				timeQueueSize.append((current, taskSum))
			
			if len(timeRunSize)>0 and timeRunSize[len(timeRunSize)-1][0]==current:
				timeRunSize[len(timeRunSize)-1] = (current, taskR)
			else:
				timeRunSize.append((current, taskR))
			
			
			if prevLineSplit != None:
				#t = (lineSplit[0]-prevLineSplit[0])/3600.0
				t = (lineSplit[0]-prevLineSplit[0])
							
				avgSum += taskSum
				avgTim += t
				#print str(t)+" "+str(lineSplit)+" queues="+str(taskSum)
				#print str(t)+" "+str(taskSum)
				
			prevLineSplit = lineSplit

avgQueue = int(round(avgSum/avgTim))
print "Average queue size = "+str(avgQueue)



# Read energy file, line by line
timePower = []
for line in open(LOG_ENERGY, "r"):
	if not line.startswith("#") and line!="\n":
		line = line.replace("\n", "")
		lineSplit = line.split("\t")
		# Getting info
		lineSplit[0] = int(lineSplit[0]) # Time
		lineSplit[9] = float(lineSplit[9]) # Total use
		
		t = lineSplit[0]
		#t = (lineSplit[0]-prevLineSplit[0])/3600.0
		timePower.append((t, lineSplit[9]))
# Read job file
timeTasks = []
for line in open(LOG_JOBS, "r"):
	if not line.startswith("#") and line!="\n":
		line = line.replace("\n", "")
		lineSplit = line.split("\t")
		# Getting info
		lineSplit[0] = int(lineSplit[0]) # Time
		lineSplit[1] = lineSplit[1] # TaskId
		lineSplit[2] = lineSplit[2] # JobId
		lineSplit[3] = lineSplit[3] # Node
		lineSplit[4] = lineSplit[4] # Priority
		lineSplit[5] = int(lineSplit[5]) # Submit
		lineSplit[6] = int(lineSplit[6]) # Start
		lineSplit[7] = int(lineSplit[7]) # End
		lineSplit[8] = int(lineSplit[8]) # Wait
		lineSplit[9] = int(lineSplit[9]) # Run
		lineSplit[10] = int(lineSplit[10]) # Total
		
		#if not lineSplit[3].startswith("['") and lineSplit[1].find("_m_")>0:
		if not lineSplit[3].startswith("['"):
			t = lineSplit[0]
			timeTasks.append((t, lineSplit))



# Output
PERIOD = 100
print "Information in periods of "+str(PERIOD)+" seconds"
print "Start\tEnd\tTasks\tWh\tWh/Task\tTask/s\tQueue\tWh/Task"
for i in range(0, 7200/PERIOD):
	t = i*PERIOD
	tnext = (i+1)*PERIOD
	
	# Task
	tasks = 0
	for tv in timeTasks:
		if t<=tv[0] and tv[0]<tnext:
			tasks+=1
		if tnext<=tv[0]:
			break
	# Energy
	energy = 0
	prevT = 0
	for tv in timePower:
		if t<=tv[0] and tv[0]<tnext:
			power = tv[1]
			period = tv[0]-prevT
			energy += power * period/3600.0
		prevT = tv[0]
	# Queue
	queueSize = 0
	num = 0
	for tv in timeQueueSize:
		if tv[0]>=t and tv[0]<tnext:
			num += 1
			queueSize += tv[1]
	avgQueueSize = 0
	if num>0:
		avgQueueSize = float(queueSize)/num
		
	# Compute with queue size
	reft = t
	# Time to complete queue
	compQueue = 0
	auxAvgQueueSize = avgQueueSize
	for tv in timeTasks:
		if tv[0]>=reft:
			auxAvgQueueSize -= 1
			if auxAvgQueueSize<0:
				break
			compQueue = tv[0]-reft
	# Energy to complete queue
	tasks2 = 0
	for tv in timeTasks:
		if tv[0]>=reft and tv[0]<(reft+compQueue):
			tasks2+=1
		if tv[0]>=(reft+compQueue):
			break
	energy2 = 0
	prevT = 0
	for tv in timePower:
		if tv[0]>=reft and tv[0]<(reft+compQueue):
			power = tv[1]
			period = tv[0]-prevT
			energy2 += power * period/3600.0
		prevT = tv[0]
	
	# Energy per task
	energyTask = 0
	if tasks>0:
		energyTask = energy/tasks
	energyTask2 = 0
	if tasks2>0:
		energyTask2 = float(energy2)/tasks2
	#print "%d\t%d\t%d\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%d" % (t, tnext, tasks, energy, energyTask, 1.0*tasks/PERIOD, avgQueueSize, energyTask2, energy2, tasks2)
	print "%d\t%d\t%d\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f" % (t, tnext, tasks, energy, energyTask, 1.0*tasks/PERIOD, avgQueueSize, energyTask2)
	
	#numRunTasks = 0
	#for tv in timeRunSize:
		#if t<tv[0]:
			#break
		#numRunTasks = tv[1]
		
	#numTasks = 0
	#for tv in timeQueueSize:
		#if t<tv[0]:
			#break
		#numTasks = tv[1]
		
	#power = 0
	#for tv in timePower:
		#if t<tv[0]:
			#break
		#power = tv[1]
	#print str(t)+"\t"+str(numRunTasks)+"\t"+str(numTasks)+"\t"+str(power)


energy = 0.0
prevT = 0
for tv in timePower:
	power = tv[1]
	period = tv[0]-prevT
	energy += power * period/3600.0
	prevT = tv[0]
	
print "Summary:"
print "Tasks: "+str(len(timeTasks))
print "Energy: "+str(energy)+"Wh"
print "Energy/Task: %.3f Wh/task" % (energy/len(timeTasks))






sys.exit(0)
















# Read job file
output = []
tasks = []
prevTasks = []
tasksPeriod = []
numTotalTasks = 0
sumPeriod = 0
totPeriod = 0
for line in open(LOG_JOBS, "r"):
	if not line.startswith("#") and line!="\n":
		line = line.replace("\n", "")
		lineSplit = line.split("\t")
		# Getting info
		lineSplit[0] = int(lineSplit[0]) # Time
		lineSplit[1] = lineSplit[1] # TaskId
		lineSplit[2] = lineSplit[2] # JobId
		lineSplit[3] = lineSplit[3] # Node
		lineSplit[4] = lineSplit[4] # Priority
		lineSplit[5] = int(lineSplit[5]) # Submit
		lineSplit[6] = int(lineSplit[6]) # Start
		lineSplit[7] = int(lineSplit[7]) # End
		lineSplit[8] = int(lineSplit[8]) # Wait
		lineSplit[9] = int(lineSplit[9]) # Run
		lineSplit[10] = int(lineSplit[10]) # Total
		
		#if not lineSplit[3].startswith("['") and lineSplit[1].find("_m_")>0:
		if not lineSplit[3].startswith("['"):
			jobId = lineSplit[2]
			t = lineSplit[0]
			
			numTotalTasks += 1
			tasks.append(lineSplit)
			while len(tasks)>avgQueue:
				prevTasks.append(tasks.pop(0))
			# Filling previous tasks
			while len(prevTasks)>NUM_SAMPLE_TASKS:
				prevTasks.pop(0)
			
			avgEnergyQueue = 0
			t0 = t
			if len(tasks)==avgQueue:
				# Calculate period
				start = tasks[0][0]
				end   = tasks[len(tasks)-1][0]
				# Calculate energy in that period
				avgSum = 0
				prevValue = None
				for i in range(0, len(timePower)):
					if timePower[i][0]>=start and timePower[i][0]<=end:
						if prevValue!= None:
							t = (timePower[i][0] - prevValue[0])/3600.0 # hours
							e = prevValue[1]*t # Watts * hour
							avgSum += e # Wh
						prevValue = timePower[i]
				avgEnergyQueue = avgSum/len(tasks)
			
			avgEnergyPrevQueue = 0
			if len(prevTasks)==NUM_SAMPLE_TASKS:
				# Calculate prev period
				start = prevTasks[0][0]
				end   = prevTasks[len(prevTasks)-1][0]
				# Calculate energy in that period
				avgSum = 0
				prevValue = None
				for i in range(0, len(timePower)):
					if timePower[i][0]>=start and timePower[i][0]<=end:
						if prevValue!= None:
							t = (timePower[i][0] - prevValue[0])/3600.0 # hours
							e = prevValue[1]*t # Watts * hour
							avgSum += e # Wh
						prevValue = timePower[i]
				avgEnergyPrevQueue = avgSum/len(prevTasks)
				
			#print "%d\t%.2f\t%.2f\t%.2f" % (t0, avgEnergyQueue, avgEnergyPrevQueue, avgEnergyPrevQueue-avgEnergyQueue)
			if len(output)>0 and output[len(output)-1][0] == t0:
				output[len(output)-1] = (t0, avgEnergyQueue, avgEnergyPrevQueue)
			else:
				output.append((t0, avgEnergyQueue, avgEnergyPrevQueue))
				
				
prevLine = None
total1 = 0
total2 = 0
totalD = 0
totalN = 0
for v in output:
	if prevLine != None:
		if v[1]>0 and v[2]>0:
			period = v[0]-prevLine[0]
			total1 += v[1]*period
			total2 += v[2]*period
			dif = 100*(v[1]-v[2])/v[1]
			if dif<0:
				dif = -dif
			totalD += dif*period
			totalN += period
			#print "%d\t%.2f\t%.2f\t%.2f" % (v[0], v[1], v[2], v[1]-v[2])
	prevLine = v
print "%.2f %.2f %.2f / %.2f = %.2f%% " % (total1, total2, totalD, totalN, totalD/totalN)
