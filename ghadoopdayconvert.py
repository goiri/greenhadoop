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





def readGreenAvailFile(filename):
	greenAvailability = []
	file = open(filename, 'r')
	for line in file:
		if line != '' and line.find("#")!=0 and line != '\n':
			lineSplit = line.strip().expandtabs(1).split(' ')
			t=lineSplit[0]
			p=float(lineSplit[1])
			# Apply scale factor TODO
			p = (p/2300.0)*MAX_POWER
			
			greenAvailability.append(TimeValue(t,p))
	file.close()
	return greenAvailability




LOG_ENERGY = "ghadoop-energy.log"
LOG_JOBS = "ghadoop-jobs.log"
LOG_SCHEDULER = "ghadoop-scheduler.log"

if len(sys.argv)>1:
	LOG_ENERGY = sys.argv[1]+"-energy.log"
	LOG_JOBS = sys.argv[1]+"-jobs.log"
	LOG_SCHEDULER = sys.argv[1]+"-scheduler.log"


greenAvailability = None
path = LOG_ENERGY[0:LOG_ENERGY.rfind("/")]
for file in os.listdir(path):
	if file.startswith("solarpower"):
		greenAvailability = readGreenAvailFile(path+"/"+file)

# Energy file
prevLineSplit = None
totalEnergyGreen = 0.0
totalEnergyBrown = 0.0
totalEnergyTotal = 0.0
totalEnergyBrownCost = 0.0
totalEnergyGreenAvail = 0.0

totalTime = 0
totalTime = 7200
timeFinishWorkload = 7200

# Nodes
runNodes = 0.0
upNodes = 0.0
decNodes = 0.0

# Peak
maxPowerPeak = 0.0

peakControlTime = []
peakControlPowe = []

SPEEDUP = 24
ACCOUNTING_PERIOD=(15*60/3600.0)/SPEEDUP # hours

numNodes = 0


# Read energy file, line by line
for line in open(LOG_ENERGY, "r"):
	if not line.startswith("#") and line!="\n":
		line = line.replace("\n", "")
		lineSplit = line.split("\t")
		# Getting info
		lineSplit[0] = int(lineSplit[0]) # Time
		lineSplit[1] = float(lineSplit[1]) # Green availability
		lineSplit[2] = float(lineSplit[2]) # Green prediction
		lineSplit[3] = float(lineSplit[3]) # Brown price
		lineSplit[4] = int(lineSplit[4]) # Run Nodes
		lineSplit[5] = int(lineSplit[5]) # Up Nodes
		lineSplit[6] = int(lineSplit[6]) # Dec Nodes
		lineSplit[7] = float(lineSplit[7]) # Green use
		lineSplit[8] = float(lineSplit[8]) # Brown use
		lineSplit[9] = float(lineSplit[9]) # Total use
		
		
		t = lineSplit[0]
		
		newGreen = 0
		for tv in greenAvailability:
			#print tv
			if (tv.t/24)>t:
				break
			newGreen = tv.v
		
		totaluse = lineSplit[9]
		if newGreen >= totaluse:
			greenuse = totaluse
			brownuse = 0
		else:
			greenuse = newGreen
			brownuse = totaluse-newGreen
		print str(t)+"\t"+str(newGreen)+"\t"+str(newGreen)+"\t"+str(lineSplit[3])+"\t"+str(lineSplit[4])+"\t"+str(lineSplit[5])+"\t"+str(lineSplit[6])+"\t"+str(greenuse)+"\t"+str(brownuse)+"\t"+str(lineSplit[9])
		
		# Nodes
		if numNodes<lineSplit[6]:
			numNodes = lineSplit[6]
		
		# Parse info
		if prevLineSplit != None:
			t = (lineSplit[0]-prevLineSplit[0])/3600.0
			
			energyGreen = t * prevLineSplit[7]
			energyBrown = t * prevLineSplit[8]
			energyTotal = t * prevLineSplit[9]
			energyBrownCost = (energyBrown/1000.0)*prevLineSplit[3]
			energyGreenAvail = t * prevLineSplit[1]
			
			# Nodes
			runNodes += 3600.0 * t * prevLineSplit[4]
			upNodes += 3600.0 * t * prevLineSplit[5]
			decNodes += 3600.0 * t * prevLineSplit[6]

			# Peak power accounting
			#print prevLineSplit
			peakControlTime.append(t)
			peakControlPowe.append(prevLineSplit[8])
			#while sum(peakControlTime)-peakControlTime[0]>ACCOUNTING_PERIOD:
			while sum(peakControlTime)>ACCOUNTING_PERIOD:
				peakControlTime.pop(0)
				peakControlPowe.pop(0)
			while len(peakControlTime)>1 and peakControlTime[0] == 0:
				peakControlTime.pop(0)
				peakControlPowe.pop(0)
			powerPeak = 0
			for i in range(0, len(peakControlTime)):
				#print str(peakControlTime[i])+"h "+str(peakControlPowe[i])+"W"
				#powerPeak += (peakControlTime[i]/ACCOUNTING_PERIOD) * peakControlPowe[i]
				if sum(peakControlTime)>0:
					powerPeak += (peakControlTime[i]/sum(peakControlTime)) * peakControlPowe[i]
			if powerPeak>maxPowerPeak:
				maxPowerPeak = powerPeak
			#if prevLineSplit[0]>totalTime:
				#totalTime = prevLineSplit[0]
			#print "Current = "+str(powerPeak)+" Peak = "+str(maxPowerPeak) 
			
			# Totals
			totalEnergyGreen += energyGreen
			totalEnergyBrown += energyBrown
			totalEnergyTotal += energyTotal
			totalEnergyBrownCost += energyBrownCost
			totalEnergyGreenAvail += energyGreenAvail
		prevLineSplit = lineSplit


runNodes = runNodes/totalTime
upNodes = upNodes/totalTime
decNodes = decNodes/totalTime

# Read job file, line by line
jobs = {}
jobsFinished = []
jobsStartEnd = {}

tasks = {}
timeTasks = []
timeTasksHigh = []
timeTasksNone = []
timeJobs = []
totalRuntime = 0
totalMapRuntime = 0
#timeFinishWorkload = 0

speedTasks = []
speedTasksHigh = []
speedTasksNone = []
speedJobs = []
#totalSpeed = 0.0
#totalNumber = 0

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
		
		jobId = lineSplit[2]
		if jobId not in jobsStartEnd:
			jobsStartEnd[jobId] = (lineSplit[5], None)
		
		t = lineSplit[0]
		if lineSplit[3].startswith("['"):
			# Job
			#if timeFinishWorkload< (lineSplit[5]+lineSplit[10]):
				#timeFinishWorkload = lineSplit[0]
			timeJobs.append((t, lineSplit[2]))
			jobId = lineSplit[2]
			if jobId not in jobsFinished:
				jobsFinished.append(jobId)
			
			jobsStartEnd[jobId] = (jobsStartEnd[jobId][0], lineSplit[10])
		else:
			# Task
			if lineSplit[1] not in tasks:
				tasks[lineSplit[1]] = None
			if lineSplit[2] not in jobs:
				jobs[lineSplit[2]] = []
			jobs[lineSplit[2]].append(lineSplit[1])
			totalRuntime += lineSplit[9]
			if lineSplit[1].find("_m_")>=0:
				totalMapRuntime += lineSplit[9]
			
			timeTasks.append((t, lineSplit[1]))
			if lineSplit[4].find("HIGH")>=0:
				timeTasksHigh.append((t, lineSplit[1]))
			else:
				timeTasksNone.append((t, lineSplit[1]))
			
		# Clean too old tasks
		MEASURE_INTERVAL = 300.0
		while len(timeTasks)>0 and t-timeTasks[0][0]>MEASURE_INTERVAL: # 5 minutes
			del timeTasks[0]
		speedTasks.append((t, len(timeTasks)/MEASURE_INTERVAL))
		while len(timeTasksHigh)>0 and t-timeTasksHigh[0][0]>MEASURE_INTERVAL: # 5 minutes
			del timeTasksHigh[0]
		speedTasksHigh.append((t, len(timeTasksHigh)/MEASURE_INTERVAL))
		while len(timeTasksNone)>0 and t-timeTasksNone[0][0]>MEASURE_INTERVAL: # 5 minutes
			del timeTasksNone[0]
		speedTasksNone.append((t, len(timeTasksNone)/MEASURE_INTERVAL))
		
		# Clean too old jobs
		while len(timeJobs)>0 and t-timeJobs[0][0]>MEASURE_INTERVAL: # 5 minutes
			del timeJobs[0]
		speedJobs.append((t, len(timeJobs)/MEASURE_INTERVAL))
		
# Get speed
# Tasks speed
auxSpeed = []
for s in speedTasks:
	if len(auxSpeed)>0 and auxSpeed[len(auxSpeed)-1][0] == s[0]:
		auxSpeed[len(auxSpeed)-1] = s
	else:
		auxSpeed.append(s)
speedTasks = auxSpeed
a = 0.0
n = 0
now = 0
for s in speedTasks:
	interval = s[0]-now
	#print str(interval)+"\t"+str(s)
	a += s[1]*interval
	n += interval
	now = s[0]
finalSpeedTasks = 0.0
if n>0:
	finalSpeedTasks = a/n
# Tasks speed high
auxSpeed = []
for s in speedTasksHigh:
	if len(auxSpeed)>0 and auxSpeed[len(auxSpeed)-1][0] == s[0]:
		auxSpeed[len(auxSpeed)-1] = s
	else:
		auxSpeed.append(s)
speedTasksHigh = auxSpeed
a = 0.0
n = 0
now = 0
for s in speedTasksHigh:
	interval = s[0]-now
	#print str(interval)+"\t"+str(s)
	a += s[1]*interval
	n += interval
	now = s[0]
finalSpeedTasksHigh = 0.0
if n>0:
	finalSpeedTasksHigh = a/n
# Tasks speed
auxSpeed = []
for s in speedTasksNone:
	if len(auxSpeed)>0 and auxSpeed[len(auxSpeed)-1][0] == s[0]:
		auxSpeed[len(auxSpeed)-1] = s
	else:
		auxSpeed.append(s)
speedTasksNone = auxSpeed
a = 0.0
n = 0
now = 0
for s in speedTasksNone:
	interval = s[0]-now
	#print str(interval)+"\t"+str(s)
	a += s[1]*interval
	n += interval
	now = s[0]
finalSpeedTasksNone = 0.0
if n>0:
	finalSpeedTasksNone = a/n
	
	
	
	

# Jobs speed
auxSpeed = []
for s in speedJobs:
	if len(auxSpeed)>0 and auxSpeed[len(auxSpeed)-1][0] == s[0]:
		auxSpeed[len(auxSpeed)-1] = s
	else:
		auxSpeed.append(s)
speedJobs = auxSpeed
a = 0.0
n = 0
now = 0
for s in speedJobs:
	interval = s[0]-now
	#print str(interval)+"\t"+str(s)
	a += s[1]*interval
	n += interval
	now = s[0]
finalSpeedJobs = 0.0
if n>0:
	finalSpeedJobs = a/n

# Read scheudler file, line by line
replications = []
replStart = 0
replDone = 0
for line in open(LOG_SCHEDULER, "r"):
	if not line.startswith("#") and line!="\n":
		line = line.replace("\n", "")
		lineSplit = line.split("\t")
		# Getting info
		lineSplit[0] = int(lineSplit[0]) # Time
		lineSplit[1] = lineSplit[1] # Message
		# File replication
		if lineSplit[1].startswith("Changed"):
			replDone += 1
			fileName = lineSplit[1].split(" ")[2]
			if fileName not in replications:
				replications.append(fileName)
		elif lineSplit[1].startswith("Change"):
			replStart +=1
			fileName = lineSplit[1].split(" ")[2]
			if fileName not in replications:
				replications.append(fileName)
		# Queues
		#if lineSplit[1].startswith("Queues"):
			#print line

totalEnergyIdle = ((numNodes*Node.POWER_S3)+POWER_IDLE_GHADOOP) * totalTime/3600.0

greenUtilization = 0.0
if totalEnergyGreenAvail>0.0:
	greenUtilization = 100.0*totalEnergyGreen/totalEnergyGreenAvail

# Print summary
#print "Power:"
#print "\tGreen: %.2f kWh (%.2f/%.2f Wh %.2f%%)" % (totalEnergyGreen/1000.0, totalEnergyGreen, totalEnergyGreenAvail, greenUtilization)
#print "\tBrown: %.2f kWh (%.2f Wh)" % (totalEnergyBrown/1000.0, totalEnergyBrown)
#print "\tIdle:  %.2f kWh (%.2f Wh)" % (totalEnergyIdle/1000.0, totalEnergyIdle)
#print "\tWork:  %.2f kWh (%.2f Wh)" % ((totalEnergyTotal-totalEnergyIdle)/1000.0, totalEnergyTotal-totalEnergyIdle)
#print "\tTotal: %.2f kWh (%.2f Wh)" % (totalEnergyTotal/1000.0, totalEnergyTotal)
#print "\tCost:  $%.4f" % (totalEnergyBrownCost)
#print "\tPeak:  %.2f W" % (maxPowerPeak)
#print "Nodes: %d" % (numNodes)
#print "Time:  %d s" % (totalTime)
#print "\tWork:  %d s" % (timeFinishWorkload)
#print "\tRun:   %d s" % (totalRuntime)
#print "\tOccup Total: %.2f%% = %.2f / %.2fx%.2f" % (100.0*totalRuntime/(totalTime*numNodes), totalRuntime, totalTime, numNodes)
#occup = 0.0
#if timeFinishWorkload>0.0:
	#occup = 100.0*totalRuntime/(timeFinishWorkload*numNodes)
#print "\tOccup Work:  %.2f%%" % (occup)

#print "Jobs:  %d" % (len(jobs))
#if len(jobs)>0:
	#print "\tTasks:  %.2f" % (float(len(tasks))/len(jobs))
	#print "\tLength: %.2f s" % (totalRuntime/float(len(jobs)))
	#print "\tEnergy: %.2f Wh" % ((totalEnergyTotal-totalEnergyIdle)/len(jobs))
#print "Tasks: %d" % (len(tasks))
#if len(tasks)>0:
	#print "\tLength: %.2f s" % (totalRuntime/float(len(tasks)))
	#print "\tEnergy: %.2f Wh" % ((totalEnergyTotal-totalEnergyIdle)/len(tasks))

#print "Performance:"
#print "\t%.2f tasks/second " % (finalSpeedTasks)
#if totalEnergyBrown>0:
	#print "\t%.2f tasks/hour / Wh" % (finalSpeedTasks*3600.0/totalEnergyBrown)
#if totalEnergyBrownCost>0:
	#print "\t%.2f tasks/hour / $" % (finalSpeedTasks*3600.0/totalEnergyBrownCost)
#print "\t%.2f jobs/hour " % (finalSpeedJobs*3600.0)
#if totalEnergyBrown>0:
	#print "\t%.2f jobs/hour / Wh" % (finalSpeedJobs*3600.0/totalEnergyBrown)
#if totalEnergyBrownCost>0:
	#print "\t%.2f jobs/hour / $" % (finalSpeedJobs*3600.0/totalEnergyBrownCost)


#print "Repl:  %d/%d" % (replStart, replDone)
#print "Repl:  %d" % (len(replications))



