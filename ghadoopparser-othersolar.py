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



LOG_ENERGY = "ghadoop-energy.log"
LOG_JOBS = "ghadoop-jobs.log"
LOG_SCHEDULER = "ghadoop-scheduler.log"

if len(sys.argv)>1:
	LOG_ENERGY = sys.argv[1]+"-energy.log"
	LOG_JOBS = sys.argv[1]+"-jobs.log"
	LOG_SCHEDULER = sys.argv[1]+"-scheduler.log"


SOLAR_0 = "data/solarpower-15-05-2011"
SOLAR_1 = "data/solarpower-09-05-2011"
SOLAR_2 = "data/solarpower-12-05-2011"
SOLAR_3 = "data/solarpower-14-06-2011"
SOLAR_4 = "data/solarpower-16-06-2011"

LOG_SOLAR = SOLAR_1


# Extra functions
# Get the available green power
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


# Energy file
prevLineSplit = None
totalEnergyGreen = 0.0
totalEnergyBrown = 0.0
totalEnergyTotal = 0.0
totalEnergyBrownCost = 0.0
totalEnergyGreenAvail = 0.0

totalTime = 0
#totalTime = 7200
#timeFinishWorkload = 7200

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


# Read solar
solarAvailable = readGreenAvailFile(LOG_SOLAR)

options = {}
# Read energy file, line by line
for line in open(LOG_ENERGY, "r"):
	if line.startswith("#"):
		if line.find("Options = ")>=0:
			optionsLine = line[line.find("{")+1:line.find("}")]
			for option in optionsLine.split(", "):
				option = option.replace("'", "")
				optionSplit = option.split(": ")
				options[optionSplit[0]] = optionSplit[1]
	elif line!="\n":
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
		
		# Get solar from external file...
		solarAvailableNow = 0.0
		for i in range(0, len(solarAvailable)):
			if t*24 >= solarAvailable[i].t:
				solarAvailableNow = solarAvailable[i].v
			else:
				break
		
		# Nodes
		if numNodes<lineSplit[6]:
			numNodes = lineSplit[6]
		
		# Parse info
		if prevLineSplit != None:
			t = (lineSplit[0]-prevLineSplit[0])/3600.0
			
			#energyGreenAvail = t * prevLineSplit[1]
			energyGreenAvail = t * solarAvailableNow
			energyTotal = t * prevLineSplit[9]
			if energyTotal>energyGreenAvail:
				energyGreen = energyGreenAvail
				energyBrown = energyTotal - energyGreenAvail
			else:
				energyGreen = energyTotal
				energyBrown = 0.0
			
			#energyGreen = t * prevLineSplit[7]
			#energyBrown = t * prevLineSplit[8]
			energyBrownCost = (energyBrown/1000.0)*prevLineSplit[3]
			
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
			if prevLineSplit[0]>totalTime:
				totalTime = prevLineSplit[0]
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
taskMap = []
taskRed = []
tasksHigh = []
tasksNormal = []
timeTasks = []
timeTasksHigh = []
timeTasksNone = []
timeJobs = []
totalRuntime = 0
totalMapRuntime = 0
totalRedRuntime = 0
#timeFinishWorkload = 0

totalTimeJobs = 0.0
totalTimeJobsNum = 0
totalTimeJobsNormal = 0.0
totalTimeJobsNormalNum = 0
totalTimeJobsHigh = 0.0
totalTimeJobsHighNum = 0

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
			timeJobs.append((t, lineSplit[2]))
			if jobId not in jobsFinished:
				jobsFinished.append(jobId)
			
			jobsStartEnd[jobId] = (jobsStartEnd[jobId][0], lineSplit[10])
			
			totalTimeJobs += lineSplit[10]
			totalTimeJobsNum += 1
			
			if lineSplit[4].find("HIGH")>=0:
				totalTimeJobsHigh += lineSplit[10]
				totalTimeJobsHighNum += 1
			else:
				totalTimeJobsNormal += lineSplit[10]
				totalTimeJobsNormalNum += 1
		# If the task is not a job setup task
		elif line.find("JobSetup")<0:
			# Task
			taskId = lineSplit[1]
			if taskId not in tasks:
				tasks[taskId] = None
			if jobId not in jobs:
				jobs[jobId] = []
			jobs[jobId].append(taskId)
			totalRuntime += lineSplit[9]
			# Map or Reduce
			if taskId.find("_m_")>=0:
				taskMap.append(taskId)
				totalMapRuntime += lineSplit[9]
			elif taskId.find("_r_")>=0:
				taskRed.append(taskId)
				totalRedRuntime += lineSplit[9]
			# Priority
			timeTasks.append((t, taskId))
			if lineSplit[4].find("HIGH")>=0:
				timeTasksHigh.append((t, taskId))
				if taskId not in tasksHigh:
					tasksHigh.append(taskId)
			else:
				timeTasksNone.append((t, taskId))
				if taskId not in tasksNormal:
					tasksNormal.append(taskId)
			
		# Clean too old tasks
		MEASURE_INTERVAL = 300.0
		while len(timeTasks)>0 and t-timeTasks[0][0]>MEASURE_INTERVAL: # 5 minutes
			del timeTasks[0]
		while len(timeTasksHigh)>0 and t-timeTasksHigh[0][0]>MEASURE_INTERVAL: # 5 minutes
			del timeTasksHigh[0]
		while len(timeTasksNone)>0 and t-timeTasksNone[0][0]>MEASURE_INTERVAL: # 5 minutes
			del timeTasksNone[0]
		
		# Clean too old jobs
		while len(timeJobs)>0 and t-timeJobs[0][0]>MEASURE_INTERVAL: # 5 minutes
			del timeJobs[0]


# Read scheudler file, line by line
replications = []
replStart = 0
replDone = 0
submittedJobs = 0
deadlineActions = 0
for line in open(LOG_SCHEDULER, "r"):
	if not line.startswith("#") and line!="\n":
		line = line.replace("\n", "")
		lineSplit = line.split("\t")
		# Getting info
		try:
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
			if lineSplit[1].startswith("Queues"):
				if int(lineSplit[9]) > submittedJobs:
					submittedJobs = int(lineSplit[9])
			if line.find("eadline")>=0:
				deadlineActions += 1
		except:
			None

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


# Print summary HTML
print "<ul>"
print "<li>Energy consumption: %.2f kWh (%.2f Wh)</li>" % (totalEnergyTotal/1000.0, totalEnergyTotal)
print "\t<ul>"
print "\t<li>Green: %.2f kWh (%.2f/%.2f Wh %.2f%%)</li>" % (totalEnergyGreen/1000.0, totalEnergyGreen, totalEnergyGreenAvail, greenUtilization)
print "\t<li>Brown: %.2f kWh (%.2f Wh)</li>" % (totalEnergyBrown/1000.0, totalEnergyBrown)
print "\t<li>Idle: %.2f kWh (%.2f Wh)</li>" % (totalEnergyIdle/1000.0, totalEnergyIdle)
print "\t<li>Work: %.2f kWh (%.2f Wh)</li>" % ((totalEnergyTotal-totalEnergyIdle)/1000.0, totalEnergyTotal-totalEnergyIdle)
pricePeak = 5.5884
if "peakCost" in options:
	pricePeak = float(options["peakCost"])
print "\t<li>Energy cost: $%.4f</li>" % (totalEnergyBrownCost)
print "\t<li>Peak power: %.2f W (%.2f kW x $%.2f/kW = $%.2f)</li>" % (maxPowerPeak, maxPowerPeak/1000.0, pricePeak, (maxPowerPeak/1000.0)*pricePeak)
totalCost = totalEnergyBrownCost*SPEEDUP*15+(maxPowerPeak/1000.0)*pricePeak
print "\t<li>Total cost: $%.2f + $%.2f = $%.2f</li>" % (totalEnergyBrownCost*SPEEDUP*15, (maxPowerPeak/1000.0)*pricePeak, totalCost)
print "\t</ul>"
print "<li>Nodes: %d</li>" % (numNodes)
print "\t<ul>"
print "\t<li>Run:   %.2f</li>" % (runNodes)
print "\t<li>Up:    %.2f</li>" % (upNodes)
print "\t<li>On:    %.2f</li>" % (decNodes)
print "\t<li>Total: %.2f</li>" % (numNodes)
print "\t<li>Utilization: %.2f%%</li>" % (100.0*runNodes/numNodes)
print "\t</ul>"

# Slot utilization*1.5 / Node utilization (UP) = Utilization of the available slots

print "<li>Experiment duration: %d s</li>" % (totalTime)
print "\t<ul>"
#print "\t<li>Working time: %d s</li>" % (timeFinishWorkload)
if totalTime > 0:
	#print "\t<li>Utilization: %.2f%% = %.2f / %.2fx%dx%d</li>" % (100.0*totalRuntime/(totalTime*numNodes*TASK_NODE), totalRuntime, totalTime, numNodes, TASK_NODE)
	print "\t<li>Utilization: %.2f%% = %.2f / %.2fx%dx%d</li>" % (100.0*totalRuntime/(totalTime*numNodes*TASK_NODE), totalRuntime, totalTime, numNodes, TASK_NODE)
	print "\t<li>Utilization maps: %.2f%% = %.2f / %.2fx%dx%d</li>" % (100.0*totalMapRuntime/(totalTime*numNodes*MAP_NODE), totalMapRuntime, totalTime, numNodes, MAP_NODE)
print "\t</ul>"

print "<li>Jobs: %d</li>" % (len(jobsFinished))
# Deadline stats
violations = 0
if len(jobs)>0:
	possibleViolations = 0
	percentageViolation = 0
	totalViolation = 0
	
	if "deadline" in options:
		DEADLINE = float(options["deadline"])
	for jobId in jobsStartEnd:
		if jobsStartEnd[jobId] == None:
			# The job is violating the deadline
			if (jobsStartEnd[jobId][0]+DEADLINE) < totalTime:
				possibleViolations += 1
				violations += 1
				percentageViolation += 100.0*(DEADLINE-jobsStartEnd[jobId][1])/DEADLINE
				print "%s %s violation: %d %.2f%%" % (jobId, str(jobsStartEnd[jobId]), DEADLINE-jobsStartEnd[jobId][1], 100.0*(DEADLINE-jobsStartEnd[jobId][1])/DEADLINE)
		else:
			possibleViolations += 1
			if jobsStartEnd[jobId][1]>DEADLINE:
				violations += 1
				totalViolation += DEADLINE-jobsStartEnd[jobId][1]
				percentageViolation += 100.0*(DEADLINE-jobsStartEnd[jobId][1])/DEADLINE
				print "%s %s violation: %ds %.2f%%" % (jobId, str(jobsStartEnd[jobId]), DEADLINE-jobsStartEnd[jobId][1], 100.0*(DEADLINE-jobsStartEnd[jobId][1])/DEADLINE)
	print "\t<ul>"
	print "\t<li>Run: %d</li>" % (len(jobs))
	print "\t<li>Finished: %d/%d</li>" % (len(jobsFinished), submittedJobs)
	print "\t<li>Deadline actions: %d</li>" % (deadlineActions)
	aux = 0.0
	if violations>0:
		aux = percentageViolation/violations
	print "\t<li>Violate (%ds): %d/%d (%ds %.2f%%)</li>" % (DEADLINE, violations, possibleViolations, totalViolation, aux)
	print "\t<li>Avg tasks: %.2f tasks/job</li>" % (float(len(tasks))/len(jobs))
	print "\t<li>Avg length: %.2f seconds/job</li>" % (totalRuntime/float(len(jobs)))
	print "\t<li>Avg energy: %.2f Wh/job</li>" % ((totalEnergyTotal-totalEnergyIdle)/len(jobs))
	print "\t<li>Avg total time: %.2f seconds</li>" % (totalTimeJobs/totalTimeJobsNum)
	print "\t<ul>"
	aux = 0.0
	if totalTimeJobsHighNum>0:
		aux = totalTimeJobsHigh/totalTimeJobsHighNum
	print "\t\t<li>High: %.2f seconds</li>" % (aux)
	aux = 0.0
	if totalTimeJobsNormalNum>0:
		aux = totalTimeJobsNormal/totalTimeJobsNormalNum
	print "\t\t<li>Normal: %.2f seconds</li>" % (aux)
	print "\t</ul>"
	print "\t</ul>"

print "<li>Tasks: %d (%d + %d)</li>" % (len(tasks), len(taskMap), len(taskRed))
if len(tasks)>0:
	print "\t<ul>"
	print "\t<li>Running time: %d s (%d + %d)</li>" % (totalRuntime, totalMapRuntime, totalRedRuntime)
	print "\t<li>Avg length: %.2f s (%.2f + %.2f)</li>" % (totalRuntime/float(len(tasks)), totalMapRuntime/float(len(taskMap)), totalRedRuntime/float(len(taskRed)))
	print "\t<li>Avg energy: %.2f Wh (%.2f Wh)</li>" % ((totalEnergyTotal-totalEnergyIdle)/len(tasks), totalEnergyTotal/len(tasks))
	print "\t</ul>"

print "<li>Task performance:</li>"
print "\t<ul>"
print "\t<li>%.2f tasks/second</li>" % (1.0*len(tasks)/totalTime)
print "\t<ul>"
print "\t\t<li>High:   %.2f tasks/second</li>" % (1.0*len(tasksHigh)/totalTime)
print "\t\t<li>Normal: %.2f tasks/second</li>" % (1.0*len(tasksNormal)/totalTime)
print "\t</ul>"
if totalEnergyBrown>0:
	print "\t<li>%.2f tasks/hour / Wh</li>" % ((3600.0*len(tasks)/totalTime)/totalEnergyBrown)
if totalEnergyBrownCost>0:
	print "\t<li>%.2f tasks/hour / $</li>" % (3600.0*len(tasks)/totalTime/totalEnergyBrownCost)
#print "\t<li>%.2f jobs/hour</li>" % (finalSpeedJobs*3600.0)
print "\t</ul>"


print "<li>Job performance:</li>"
print "\t<ul>"
print "\t<li>%.2f jobs/hour</li>" % (3600.0*len(jobsFinished)/totalTime)
if totalEnergyBrown>0:
	print "\t<li>%.2f jobs/hour / Wh</li>" % (3600.0*len(jobsFinished)/totalTime/totalEnergyBrown)
if totalEnergyBrownCost>0:
	print "\t<li>%.2f jobs/hour / $</li>" % (3600.0*len(jobsFinished)/totalTime/totalEnergyBrownCost)
if len(jobsFinished)>0:
	print "\t<li>$%.2f/job</li>" % (totalCost/len(jobsFinished))	
print "\t</ul>"

print "<li>Replicated files: %d</li>" % (len(replications))
print "\t<ul>"
print "\t<li>Start: %d</li>" % (replStart)
print "\t<li>Done: %d</li>" % (replDone)
print "\t</ul>"

print "</ul>"

# Summaryzing
if len(tasks)>0 and numNodes>0 and totalTime>0:
	print "Summary spreadsheet<br/>"
	summary = "%.2f %.2f %.2f %.2f %.5f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %d %.2f %.2f %d %.2f" % (totalEnergyGreen/1000.0, totalEnergyGreenAvail/1000.0, totalEnergyBrown/1000.0, totalEnergyTotal/1000.0, totalEnergyBrownCost, maxPowerPeak/1000.0, pricePeak, (maxPowerPeak/1000.0)*pricePeak, 100.0*totalMapRuntime/(totalTime*numNodes*MAP_NODE), 100.0*totalRuntime/(totalTime*numNodes*TASK_NODE), upNodes, decNodes, 100.0*runNodes/numNodes, len(jobsFinished), 1.0*len(tasks)/totalTime, 3600.0*len(jobsFinished)/totalTime, violations, (totalEnergyTotal-totalEnergyIdle)/len(tasks))
	print summary+"<br/>"
	print summary+"<br/>"


