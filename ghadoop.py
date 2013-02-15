#!/usr/bin/env python2.5

"""
GreenHadoop makes Hadoop aware of solar energy availability.
http://www.research.rutgers.edu/~goiri/
Copyright (C) 2012 Inigo Goiri and Ricardo Bianchini.
All rights reserved. Dept. of Computer Science, Rutgers University.

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

from subprocess import call, PIPE, Popen
from datetime import datetime, timedelta
from math import *
from operator import itemgetter

try:
	Set = set
except NameError:
	from sets import Set

import threading

from ghadoopcommons import *


# Get the available green power
def getGreenPowerAvailability():
	greenAvailability = []
	file = open('greenpower', 'r')
	for line in file:
		if line != '' and line.find("#")!=0:
			lineSplit = line.strip().expandtabs(1).split(' ')
			t=int(lineSplit[0])
			p=float(lineSplit[1])
			greenAvailability.append(TimeValue(t,p))
	file.close()
	return greenAvailability

# Get the cost of the brown energy
def getBrownPowerPrice():
	brownPrice = []
	file = open('browncost', 'r')
	for line in file:
		if line != '' and line.find("#")!=0:
			lineSplit = line.strip().expandtabs(1).split(' ')
			t=int(lineSplit[0])
			p=float(lineSplit[1])
			brownPrice.append(TimeValue(t,p))
	file.close()
	return brownPrice

# Calculate the cost of allocating a given percentage to cheap and expensive
def calculateCost(peak, previousPeak, peakCost, queueEnergy, consumedBrownCheap, surplusBrownCheap, consumedBrownExpen, surplusBrownExpen, cheapPrice, expenPrice, brownPriceArray):
	consumedBrownCheap = list(consumedBrownCheap)
	surplusBrownCheap = list(surplusBrownCheap)
	consumedBrownExpen = list(consumedBrownExpen)
	surplusBrownExpen = list(surplusBrownExpen)
	
	peakEnergy = peak*(SLOTLENGTH/3600.0)
	
	# Cheap
	for i in range(0, len(brownPriceArray)):
		auxPeakEnergy = peakEnergy-consumedBrownCheap[i]
		if surplusBrownCheap[i]>queueEnergy:
			if auxPeakEnergy>queueEnergy:
				surplusBrownCheap[i] -= queueEnergy
				consumedBrownCheap[i] += queueEnergy
				queueEnergy = 0.0
			else:
				queueEnergy -= auxPeakEnergy
				surplusBrownCheap[i] -= auxPeakEnergy
				consumedBrownCheap[i] += auxPeakEnergy
		else:
			if auxPeakEnergy>surplusBrownCheap[i]:
				queueEnergy -= surplusBrownCheap[i]
				consumedBrownCheap[i] += surplusBrownCheap[i]
				surplusBrownCheap[i] = 0.0
			else:
				queueEnergy -= auxPeakEnergy
				surplusBrownCheap[i] -= auxPeakEnergy
				consumedBrownCheap[i] += auxPeakEnergy
	# Expensive
	for i in range(0, len(brownPriceArray)):
		auxPeakEnergy = peakEnergy-consumedBrownExpen[i]
		if surplusBrownExpen[i]>queueEnergy:
			if auxPeakEnergy>queueEnergy:
				surplusBrownExpen[i] -= queueEnergy
				consumedBrownExpen[i] += queueEnergy
				queueEnergy = 0.0
			else:
				queueEnergy -= auxPeakEnergy
				surplusBrownExpen[i] -= auxPeakEnergy
				consumedBrownExpen[i] += auxPeakEnergy
		else:
			if auxPeakEnergy>surplusBrownExpen[i]:
				queueEnergy -= surplusBrownExpen[i]
				consumedBrownExpen[i] += surplusBrownExpen[i]
				surplusBrownExpen[i] = 0.0
			else:
				queueEnergy -= auxPeakEnergy
				surplusBrownExpen[i] -= auxPeakEnergy
				consumedBrownExpen[i] += auxPeakEnergy
	
	# Compute peak power and energy cost
	energyCost = 0
	energyPeak = 0
	for i in range(0, len(brownPriceArray)):
		energyCost += consumedBrownCheap[i]*cheapPrice/1000.0
		energyCost += consumedBrownExpen[i]*expenPrice/1000.0
		if consumedBrownCheap[i]>energyPeak:
			energyPeak = consumedBrownCheap[i]
		if consumedBrownExpen[i]>energyPeak:
			energyPeak = consumedBrownExpen[i]
	
	powerPeak = energyPeak/(SLOTLENGTH/3600.0)
	totalPeakCost = (powerPeak-previousPeak) * peakCost/1000.0
	if totalPeakCost<0.0:
		totalPeakCost = 0.0
	
	cost = (round((energyCost+totalPeakCost)*100000))/100000.0

	return (cost, consumedBrownCheap, consumedBrownExpen, queueEnergy)



# Schedule jobs.
def schedule(timeElapsed, peakBrown, greenAvailArray, brownPriceArray, options=None):
	# Parse options
	peakCost = 0.0
	flagScheduleGreen = True
	flagScheduleBrown = True
	deadline = TOTALTIME
	if options != None:
		# Green availability
		if options.schedNoGreen == True:
			flagScheduleGreen = False
		# Brown price
		if options.schedNoBrown == True:
			flagScheduleBrown = False
		# Schedule peak price
		if options.peakCost != None:
			peakCost = options.peakCost
		if options.deadline != None:
			deadline = options.deadline
	
	# Current date
	#timeNow = datetime.now()
	#timeNow = datetime(timeNow.year, timeNow.month, timeNow.day, timeNow.hour, timeNow.minute, timeNow.second)

	# Generate schedule array
	nodes = getNodes()
	numNodes = len(nodes)

	# Calculate idle power
	powerIdle = POWER_IDLE_GHADOOP
	for i in range(0,numNodes):
		powerIdle += Node.POWER_S3
	
	# Calculate always on power
	powerAlwaysOn = 0.0
	for nodeId in ALWAYS_NODE:
		powerAlwayOn = (Node.POWER_AVERAGE-Node.POWER_S3)

	# Get cheap and expensive prices
	if flagScheduleBrown:
		# Actual pricing
		cheapPrice = brownPriceArray[0]
		expenPrice = brownPriceArray[0]
		for i in range(1, len(brownPriceArray)):
			if brownPriceArray[i]<cheapPrice:
				cheapPrice = brownPriceArray[i]
			elif brownPriceArray[i]>expenPrice:
				expenPrice = brownPriceArray[i]
	else:
		# Static brown
		cheapPrice = brownPriceArray[0]
		expenPrice = brownPriceArray[0]
		for i in range(1, len(brownPriceArray)):
			brownPriceArray[i] = brownPriceArray[0]
	
	# Static green
	if not flagScheduleGreen:
		for i in range(0, len(greenAvailArray)):
			greenAvailArray[i] = 0.0
	
	# Copy green energy availability and initialize consumption arrays (including idle power)
	totalPower = Node.POWER_AVERAGE*numNodes + POWER_IDLE_GHADOOP
	energySlot = totalPower*(SLOTLENGTH/3600.0)
	
	totalAvailEnergy = 0.0
	totalAvailGreenEnergy = 0.0
	totalAvailBrownEnergyCheap = 0.0
	totalAvailBrownEnergyExpen = 0.0
	
	totalAvailAlwaysOnEnergy = 0.0
	
	# Create base working arrays
	surplusGreen = [0.0]*numSlots
	surplusBrownCheap = [0.0]*numSlots
	surplusBrownExpen = [0.0]*numSlots
	
	consumedGreen = [0.0]*numSlots
	consumedBrownCheap = [0.0]*numSlots
	consumedBrownExpen = [0.0]*numSlots
	for i in range(0, numSlots):
		availableGreen = greenAvailArray[i]*(SLOTLENGTH/3600.0)
		if availableGreen>energySlot:
			availableGreen=energySlot
		availableBrown = energySlot-availableGreen
		
		# Surplus
		surplusGreen[i] = availableGreen
		if brownPriceArray[i]==cheapPrice:
			surplusBrownCheap[i] = availableBrown
		else:
			surplusBrownExpen[i] = availableBrown
		
		# Use system power: S3 and Scheduler
		reqEnergySlot = (powerIdle+powerAlwayOn) * SLOTLENGTH/3600.0 # Wh
		totalAvailAlwaysOnEnergy += powerAlwayOn * SLOTLENGTH/3600.0
		
		if availableGreen>reqEnergySlot:
			# Only green
			consumedGreen[i] += reqEnergySlot
			surplusGreen[i] -= reqEnergySlot
		else:
			# Green and brown
			consumedGreen[i] += surplusGreen[i]
			reqEnergySlot -= surplusGreen[i]
			surplusGreen[i] = 0.0
			
			if brownPriceArray[i]==cheapPrice:
				if surplusBrownCheap[i]>reqEnergySlot:
					consumedBrownCheap[i] += reqEnergySlot
					surplusBrownCheap[i] -= reqEnergySlot
				else:
					consumedBrownCheap[i] += surplusBrownCheap[i]
					surplusBrownCheap[i] = 0.0
			else:
				if surplusBrownExpen[i]>reqEnergySlot:
					consumedBrownExpen[i] += reqEnergySlot
					surplusBrownExpen[i] -= reqEnergySlot
				else:
					consumedBrownExpen[i] += surplusBrownExpen[i]
					surplusBrownExpen[i] = 0.0
		# Total amounts
		totalAvailEnergy += surplusGreen[i]+surplusBrownCheap[i]+surplusBrownExpen[i]
		totalAvailGreenEnergy += surplusGreen[i]
		totalAvailBrownEnergyCheap += surplusBrownCheap[i]
		totalAvailBrownEnergyExpen += surplusBrownExpen[i]
	# New scheduling
	queueEnergy = 0.0 # Wh

	# Getting task and job information
	jobs = getJobs()
	tasks = getTasks()
	
	# Setting deadlines
	# Sort jobs according to submission date (last submitted to newest submission)
	jobSubmit = []
	for job in jobs.values():
		if job.state != "SUCCEEDED" and job.state != "FAILED":
			if job.submit==None:
				job.submit = datetime.now()
			jobSubmit.append((job.id, job.submit))
	jobSubmit = sorted(jobSubmit, key=itemgetter(1), reverse=True)
	
	# Assign internal deadlines
	assignedSlots = []
	for jobId, submit in jobSubmit:
		try:
			job = jobs[jobId]
			if job.internalDeadline == None:
				job.internalDeadline = deadline
			if job.durationMap==None or job.durationRed==None:
				# Calculate job duration
				numJobMaps = len(getFilesInDirectories(job.input)) + 1
				totalLenMap = float(numJobMaps*AVERAGE_RUNTIME_MAP) # Add time for maps
				if numJobMaps>20:
					# Long jobs (more than 20 maps)
					totalLenRed = float(numJobMaps*AVERAGE_RUNTIME_RED_LONG) # Add time for reduces
				else:
					# Short jobs
					totalLenRed = float(numJobMaps*AVERAGE_RUNTIME_RED_SHORT) # Add time for reduces
				job.durationMap = totalLenMap/(TASK_NODE*numNodes)
				job.durationRed = max(totalLenRed, 30.0)# Add minimnal time
			# Get the deadline. Take into account overlaps
			startdeadline = submit + timedelta(seconds=(job.internalDeadline - job.durationMap - job.durationRed))
			# Start point for slot
			startMap = startdeadline
			endMap = startdeadline+timedelta(seconds=job.durationMap)
			# Assign chunk from the back
			assigned = False
			for i in range(0, len(assignedSlots)):
				usedSlot = assignedSlots[i]
				startSlot = usedSlot[0]
				endSlot = usedSlot[1]
				if (startSlot<startMap and startMap<endSlot) or (startSlot<endMap and endMap<endSlot):
					# Move a litle earlier and keep trying
					startMap = startSlot-timedelta(seconds=job.durationMap)
					endMap = startSlot
				elif startMap>=endSlot:
					# It fits in this slot, assign it
					assignedSlots.insert(i, [startMap, endMap])
					assigned = True
					break
			if not assigned:
				assignedSlots.append([startMap, endMap])
			# Finally assign starting deadline
			job.deadline = startMap
		except KeyError:
			print "KeyError: "+str(jobId)+" not found."
	
	# Manage tasks and jobs
	taskEnergy = options.taskEnergy
	
	numJobsWaiting = 0
	numJobsData = 0
	numJobsQueue = 0
	numJobsRun = 0
	numJobsSucc = 0
	numJobsFail = 0
	
	numJobsVeryHigh = 0
	numJobsHigh = 0
	numTasksVeryHigh = 0
	numTasksHigh = 0
	
	numTasksHadoop = 0
	
	# Evaluating tasks and jobs
	# Jobs
	for job in jobs.values():
		# Move to very high priority if the deadline has been reached
		if job.priority!="VERY_HIGH" and (job.state=="RUNNING" or job.state=="PREP") and datetime.now()>job.deadline:
			setJobPriotity(job.id, "VERY_HIGH")
			job.priority = "VERY_HIGH"
			writeLog("logs/ghadoop-scheduler.log", str(timeElapsed)+"\tRunning job "+str(job.id)+" now="+str(datetime.now())+" deadline="+str(job.deadline)+": move to very high priority!")
		# Account jobs
		if job.state == "RUNNING" or len(job.tasks)>0:
			# Add the part of the tasks that still need to run
			for taskId in job.tasks:
				task = tasks[taskId]
				if task.state!="SUCCEEDED":
					queueEnergy += taskEnergy
					if job.priority == "VERY_HIGH":
						numJobsVeryHigh += 1.0/len(job.tasks)
						numTasksVeryHigh += 1
					elif job.priority == "HIGH":
						numJobsHigh += 1.0/len(job.tasks)
						numTasksHigh += 1
		elif job.state == "PREP" or job.state == "DATA" or job.state == "WAITING":
			#queueEnergy += jobEnergy
			numJobTasks = len(getFilesInDirectories(job.input)) + 1
			queueEnergy += taskEnergy * numJobTasks
			if job.priority == "VERY_HIGH":
				numJobsVeryHigh += 1
				numTasksVeryHigh += numJobTasks
			elif job.priority == "HIGH":
				numJobsHigh += 1
				numTasksHigh += numJobTasks
		# Accounting number of jobs
		if job.state == "WAITING":
			numJobsWaiting+=1
		elif job.state == "DATA":
			numJobsData+=1
		elif job.state == "PREP":
			numJobsQueue+=1
		elif job.state == "RUNNING":
			numJobsRun+=1
		elif job.state == "SUCCEEDED":
			numJobsSucc+=1
		elif job.state == "FAILED":
			numJobsFail+=1
	# Tasks
	for task in tasks.values():
		if task.state == "RUNNING" or task.state == "PREP":
			numTasksHadoop += 1
	
	# Output
	if getDebugLevel() > 1:
		#print "Tasks:   %d %d %d" % (numTasksQueue, numTasksRun, numTasksSucc)
		print "Jobs: %d->%d->%d->%d->%d (X:%d T:%d)" % (numJobsWaiting, numJobsData, numJobsQueue, numJobsRun, numJobsSucc, numJobsFail, len(jobs))
		print "  **: %1.2f  *: %1.2f" % (numJobsVeryHigh, numJobsHigh)
		print "  **: %d  *: %d" % (numTasksVeryHigh, numTasksHigh)
		print "Queue: %1.2fWh" % (queueEnergy)
		#print "Jobs:"
		#for job in jobs.values():
			#print "\t"+str(job.id)+"\t"+str(job.state)+"\t"+str(job.prevJobs)+"\tinput="+str(job.input)+"\toutput="+str(job.output)
	# Evaluate always on nodes available energy...
	queueEnergy = queueEnergy-totalAvailAlwaysOnEnergy
	if queueEnergy<0.0:
		queueEnergy = 0.0
	# Assign green: always
	if flagScheduleGreen:
		for i in range(0, numSlots):
			if queueEnergy>0.0:
				if queueEnergy >= surplusGreen[i]:
					consumedGreen[i] += surplusGreen[i]
					queueEnergy -= surplusGreen[i]
					surplusGreen[i] = 0.0
				else:
					consumedGreen[i] += queueEnergy
					surplusGreen[i] -= queueEnergy
					queueEnergy -= queueEnergy
	if getDebugLevel() > 1:
		#print "Queue: %.2f Wh" % (queueEnergy)
		print "Always ON: %.2f Wh" % (totalAvailAlwaysOnEnergy)
		print "Green+Cheap+Brown = %.2f+%.2f+%.2f = %.2f Wh" % (totalAvailGreenEnergy,totalAvailBrownEnergyCheap,totalAvailBrownEnergyExpen,totalAvailEnergy)
	# Assign brown
	# Maximum power
	minPeakN = totalPower
	minCostN,minBrownCheapN,minBrownExpenN,minQueueTodoN = calculateCost(minPeakN, peakBrown, peakCost,\
		queueEnergy,\
		consumedBrownCheap, surplusBrownCheap,\
		consumedBrownExpen, surplusBrownExpen,\
		cheapPrice, expenPrice, brownPriceArray)
	minPeak = minPeakN
	minCost = minCostN
	minBrownCheap = minBrownCheapN
	minBrownExpen = minBrownExpenN
	minQueueTodo = minQueueTodoN
	
	# Minimum power
	minPeak0 = peakBrown
	minCost0,minBrownCheap0,minBrownExpen0,minQueueTodo0 = calculateCost(minPeak0, peakBrown, peakCost,\
		queueEnergy,\
		consumedBrownCheap, surplusBrownCheap,\
		consumedBrownExpen, surplusBrownExpen,\
		cheapPrice, expenPrice, brownPriceArray)
	if minCost0<minCost and minQueueTodo0==0.0:
		minPeak = minPeak0
		minCost = minCost0
		minBrownCheap = minBrownCheap0
		minBrownExpen = minBrownExpen0
		minQueueTodo = minQueueTodo0
	
	# Search minimum cost
	if peakCost>0.0:
		finish = False
		for i in range(0,100):
			auxMinPeak = (minPeak0+minPeakN)/2.0
			
			auxCost,auxConsumedBrownCheap,auxConsumedBrownExpen,auxQueueTodo = calculateCost(auxMinPeak, peakBrown, peakCost,\
			queueEnergy,\
			consumedBrownCheap, surplusBrownCheap,\
			consumedBrownExpen, surplusBrownExpen,\
			cheapPrice, expenPrice, brownPriceArray)
			
			if auxQueueTodo>0:
				minPeak0 = auxMinPeak
			else:
				if auxCost<minCost:
					minPeakN = auxMinPeak
				else:
					minPeak0 = auxMinPeak
				
			if auxCost<minCost and auxQueueTodo==0.0:
				if (minCost-auxCost)<0.0001:
					finish = True
				minCost = auxCost
				minBrownCheap = auxConsumedBrownCheap
				minBrownExpen = auxConsumedBrownExpen
				minQueueTodo = auxQueueTodo
				minPeak = auxMinPeak
			
			#print "%.2fW =>\t$%.4f\ttodo:%.2fWh [%.2f]" % (peak, auxCost, auxQueueTodo, minPeak)
			
			if (minPeakN-minPeak0)<0.001 or finish:
				break
	
	# Update consume (and surplus is no needed)
	consumedBrownCheap = minBrownCheap
	consumedBrownExpen = minBrownExpen
		
	# Evaluate priorities
	#reqPowerVeryHigh = (float(numTasksVeryHigh)/TASK_NODE)*(Node.POWER_AVERAGE-Node.POWER_S3) + powerIdle
	#reqPowerHigh = (float(numTasksHigh)/2.0/TASK_NODE)*(Node.POWER_AVERAGE-Node.POWER_S3) + powerIdle
	reqPower = (consumedGreen[0] + consumedBrownCheap[0] + consumedBrownExpen[0])/(SLOTLENGTH/3600.0)
	
	#reqPowerVeryHigh = (float(numJobsVeryHigh*TASK_JOB)/TASK_NODE)*(Node.POWER_AVERAGE-Node.POWER_S3) + powerIdle
	#reqPowerVeryHigh = (float(numTasksVeryHigh)/TASK_NODE)*(Node.POWER_AVERAGE-Node.POWER_S3) + powerIdle
	if numTasksVeryHigh>0:
		#reqPowerVeryHigh = (len(nodes)*(Node.POWER_IDLE-Node.POWER_S3)) + (float(numTasksVeryHigh)/TASK_NODE)*(Node.POWER_AVERAGE-Node.POWER_IDLE) + powerIdle
		reqPowerVeryHigh = (float(numTasksVeryHigh)/TASK_NODE)*(Node.POWER_AVERAGE-Node.POWER_S3) + powerIdle
		if reqPowerVeryHigh > totalPower:
			reqPowerVeryHigh = totalPower
		if reqPowerVeryHigh>reqPower:
			reqPower = reqPowerVeryHigh
		
	#reqPowerHigh = (float(numJobsHigh*TASK_JOB)/TASK_NODE)*(Node.POWER_AVERAGE-Node.POWER_S3) + powerIdle
	#reqPowerHigh = (float(numTasksHigh)/TASK_NODE)*(Node.POWER_AVERAGE-Node.POWER_S3) + powerIdle
	if numTasksHigh>0:
		#reqPowerHigh = (len(nodes)*(Node.POWER_IDLE-Node.POWER_S3)) + (float(numTasksHigh)/TASK_NODE)*(Node.POWER_AVERAGE-Node.POWER_IDLE) + powerIdle
		reqPowerHigh = (float(numTasksHigh)/TASK_NODE)*(Node.POWER_AVERAGE-Node.POWER_S3) + powerIdle
		maxPowerHigh = (len(nodes)/2.0)*(Node.POWER_AVERAGE-Node.POWER_S3) + POWER_IDLE_GHADOOP
		if reqPowerHigh > maxPowerHigh:
			reqPowerHigh = maxPowerHigh
		if reqPowerHigh>reqPower:
			reqPower = reqPowerHigh	
	
	# Update consumptions with high priority stuff
	reqExtraEnergy = reqPower*(SLOTLENGTH/3600.0) - (consumedGreen[0] + consumedBrownCheap[0] + consumedBrownExpen[0])
	if surplusGreen[0]>reqExtraEnergy:
		consumedGreen[0] += reqExtraEnergy
		surplusGreen[0] -= reqExtraEnergy
	else:
		consumedGreen[0] += surplusGreen[0]
		reqExtraEnergy -= surplusGreen[0]
		surplusGreen[0] = 0
		if surplusBrownCheap[0]>reqExtraEnergy:
			consumedBrownCheap[0] += reqExtraEnergy
			surplusBrownCheap[0] -= reqExtraEnergy
		else:
			consumedBrownCheap[0] += surplusBrownCheap[0]
			reqExtraEnergy -= surplusBrownCheap[0]
			surplusBrownCheap[0] = 0
			if surplusBrownExpen[0]>reqExtraEnergy:
				consumedBrownExpen[0] += reqExtraEnergy
				surplusBrownExpen[0] -= reqExtraEnergy
			else:
				consumedBrownExpen[0] += surplusBrownExpen[0]
				reqExtraEnergy -= surplusBrownExpen[0]
				surplusBrownExpen[0] = 0
	
	# Output
	if getDebugLevel() >= 1:
		print "Energy usage: "+str(numNodes)+"x"+str(Node.POWER_FULL)+"W + "+str(POWER_IDLE_GHADOOP)+"W"
		maxPower = (numNodes * Node.POWER_FULL + POWER_IDLE_GHADOOP) * SLOTLENGTH/3600.0 # Wh
		for i in range(MAXSIZE,0,-1):
			out=""
			# TODO change the scale factor of the figure
			scale = 2
			for j in range(0, numSlots/scale):
				index = j*scale
				if consumedGreen[index]>(1.0*(i-1)*maxPower/MAXSIZE):
					out += bcolors.GREENBG+" "+bcolors.ENDC
				elif consumedGreen[index]+consumedBrownExpen[index]>(1.0*(i-1)*maxPower/MAXSIZE):
					out += bcolors.REDBG+" "+bcolors.ENDC
				elif consumedGreen[index]+consumedBrownCheap[index]>(1.0*(i-1)*maxPower/MAXSIZE):
					out += bcolors.BLUEBG+" "+bcolors.ENDC
				elif consumedGreen[index]+surplusGreen[index]>(1.0*(i-1)*maxPower/MAXSIZE):
					out += bcolors.WHITEBG+" "+bcolors.ENDC
				else:
					out += " "
			print out+" %.1fW" % ((1.0*i*maxPower/MAXSIZE)*(3600.0/SLOTLENGTH))
		totalCost = 0
		totalGreen = 0
		totalBrown = 0
		for i in range(0, numSlots):
			totalGreen += consumedGreen[i]
			totalBrown += consumedBrownCheap[i] + consumedBrownExpen[i]
			totalCost += consumedBrownCheap[i] * brownPriceArray[i]/1000.0 # Wh * $/kWh
			totalCost += consumedBrownExpen[i] * brownPriceArray[i]/1000.0 # Wh * $/kWh
		print "Green: %.2fWh  Brown: %.2fWh ($%.2f)" % (totalGreen, totalBrown, totalCost)#, peakBrown, peakBrown*peakCost/1000.0)   Peak: %.1fW ($%.2f)
	
	return reqPower


# Take actions
def dispatch(reqPower, availableGreenPower):
	startaux = datetime.now()
	
	done = False
	if getDebugLevel() > 0:
		print "Required power by the scheduler: " + str(reqPower) 
		print "Available green power: " + str(availableGreenPower) 

	# If there is enough green energy, use the nodes
	if reqPower<availableGreenPower:
		reqPower = availableGreenPower

	# Turn on/off Nodes
	nodes = getNodes()

	# Sorting lists
	onNodes = []
	onNodesNoSort = []
	decNodes = []
	decNodesNoSort = []
	offNodes = []
	requiredFiles = getRequiredFiles()
	for nodeId in nodes:
		if nodes[nodeId][1]=="UP" and nodeId not in onNodesNoSort:
			onNodesNoSort.append(nodeId)
		elif nodes[nodeId][1]=="DEC" and nodeId not in decNodesNoSort:
			decNodesNoSort.append(nodeId)
	for nodeId in sorted(nodes):
		auxFiles=[]
		nFiles = 0
		nFilesExcl = 0
		# Get the number of required files of the node
		for fileId in nodeFile.get(nodeId, []):
			if fileId not in auxFiles and fileId in requiredFiles:
				auxFiles.append(fileId)
		for fileId in auxFiles:
			nFiles += 1
			file = files[fileId]
			missing = True
			for otherNodeId in file.getLocation():
				if nodeId != otherNodeId:
					if otherNodeId in onNodesNoSort or otherNodeId in decNodesNoSort:
						missing = False
						break
			if missing:
				nFilesExcl +=1
		nJobs = len(nodeJobs.get(nodeId, []))
		if nodes[nodeId][1]=="UP":
			onNodes.append((nodeId, nFiles, nJobs))
		elif nodes[nodeId][1]=="DOWN":
			offNodes.append((nodeId, nFilesExcl, nJobs))
		elif nodes[nodeId][0]=="DEC" or nodes[nodeId][1]=="DEC":
			decNodes.append((nodeId, nFilesExcl, nJobs))
			if nodes[nodeId][0]=="UP":
				# It should be not running anything
				setNodeMapredDecommission(nodeId, True)
		else:
			offNodes.append((nodeId, nFilesExcl, nJobs))
		# Output
		if getDebugLevel() > 1:
			if nodes[nodeId][0] == "UP" or nodes[nodeId][0] == "DEC" or nodes[nodeId][1] == "UP" or nodes[nodeId][1] == "DEC":
				if nodeId in nodeTasks:
					out = ""
					for taskId in nodeTasks[nodeId]:
						if getTaskPriority(tasks[taskId]) == "VERY_HIGH":
							out += " "+taskId.replace("task_", "")+"**"
						elif getTaskPriority(tasks[taskId]) == "HIGH":
							out += " "+taskId.replace("task_", "")+"*"
						else:
							out += " "+taskId.replace("task_", "")
					print "\t"+str(nodeId)+"\t"+str(nodes[nodeId])+"\tfile="+str(nFilesExcl)+"("+str(nFiles)+"),job="+str(nJobs)+":\t"+out
				else:
					print "\t"+str(nodeId)+"\t"+str(nodes[nodeId])+"\tfile="+str(nFilesExcl)+"("+str(nFiles)+"),job="+str(nJobs)

	# Sort onNodes:  1:Less data required in the future 2:Executed less tasks of still running jobs
	onNodes = sorted(onNodes, key=itemgetter(2))
	onNodes = sorted(onNodes, key=itemgetter(1))
	onNodes = [nodeId for nodeId, nFiles, nJobs in onNodes]
	
	# Sort decNodes: 1:Executed most tasks of still running jobs 2:Most data required in the future 
	decNodes = sorted(decNodes, key=itemgetter(1), reverse=True)
	decNodes = sorted(decNodes, key=itemgetter(2), reverse=True)
	decNodes = [nodeId for nodeId, nFiles, nJobs in decNodes]
	
	# Sort offNodes: Containing more required data
	offNodes = sorted(offNodes, key=itemgetter(1), reverse=True)
	offNodes = [nodeId for nodeId, nFiles, nJobs in offNodes]

	# Decomission/Recomission:
	# UP->DEC Decommission nodes:
	#   MapReduce: Executed less tasks of still running jobs
	#   HDFS:      Less data required in the future
	# DEC->UP Recommission nodes:
	#   MapReduce: Executed most tasks of still running jobs
	#   HDFS:      Most data required in the future
	
	# Background cycle:
	# Decommission HDFS nodes :
	#   Replicate required data in UP nodes
	
	# Turn ON/OFF
	# DEC->DOWN Turn off decommission nodes:
	#   MapReduce: Hasn't executed tasks of running jobs
	#   HDFS:      All its required data available in UP nodes
	# DOWN->UP Turn on S3 nodes:
	#   MapReduce: -
	#   HDFS:      Containing more required data
	threads = {}
	# Turn on ALWAYS ON Nodes
	for nodeId in ALWAYS_NODE:
		if getDebugLevel() > 1:
			print "Turning on always on nodes: "+str(ALWAYS_NODE)
		toRecommission = []
		if nodeId in offNodes:
			if getDebugLevel() > 1:
				print "\tTurn on "+nodeId+" [ALWAYS]"
			thread = threading.Thread(target=setNodeStatus,args=(nodeId, True))
			thread.start()
			threads["off->on "+nodeId] = thread
			# Update data structure
			offNodes.remove(nodeId)
		elif nodeId in decNodes:
			if getDebugLevel() > 1:
				print "\tRecommission "+nodeId+" [ALWAYS]"
			#thread = threading.Thread(target=setNodeDecommission,args=(nodeId, False))
			#thread.start()
			#threads["dec->on "+nodeId] = thread
			# Update data structure
			toRecommission.append(nodeId)
			decNodes.remove(nodeId)
		if len(toRecommission)>0:
			setNodeListDecommission(toRecommission, False)
		# We all know this node is up, no need to do it again
		if nodeId in onNodes:
			onNodes.remove(nodeId)
			
	# Turn on/off nodes
	# Clean data phase
	if getPhase() == PHASE_CLEAN:
		# Turn on everything
		if (len(decNodes)+len(offNodes))>0:
			if getDebugLevel() > 1:
				print "Turning on everything: cleanning phase!"
			# Start all
			for nodeId in list(offNodes):
				if getDebugLevel() > 1:
					print "\tTurn on "+nodeId
				thread = threading.Thread(target=setNodeStatus,args=(nodeId, True))
				thread.start()
				threads["off->on "+nodeId] = thread
				# Update data structure
				offNodes.remove(nodeId)
				onNodes.append(nodeId)
			# Recommission all
			toRecommission = []
			for nodeId in list(decNodes):
				if getDebugLevel() > 1:
					print "\tRecommission "+nodeId
				# Update data structure
				toRecommission.append(nodeId)
				decNodes.remove(nodeId)
				onNodes.append(nodeId)
			if len(toRecommission)>0:
				setNodeListDecommission(toRecommission, False)
	#elif getPhase() != PHASE_TURN_ON:
	else:
		# Checking missing files
		if getDebugLevel() > 1:
			print "Checking missing files..."
		requiredFiles = getRequiredFiles()
		dataNodes = minNodesFiles(requiredFiles, offNodes)
		
		# Turn into decommission the required nodes
		for nodeId in dataNodes:
			# Turn on (in decommission state) just one of the replicas, the replica manager will automatically turn on the other nodes
			if getDebugLevel() > 1:
				print "\tGet data from "+nodeId
			thread = threading.Thread(target=setNodeStatus,args=(nodeId, True, True))
			thread.start()
			threads["off->data "+nodeId] = thread
			# Update data structure
			decNodes.append(nodeId)
			if nodeId in offNodes:
				offNodes.remove(nodeId)
		
		# Turn on/off Nodes
		idlePower = len(nodes)*Node.POWER_S3 + POWER_IDLE_GHADOOP
		currentPower = (len(onNodes)+len(ALWAYS_NODE))*(Node.POWER_AVERAGE-Node.POWER_S3) + len(decNodes)*(Node.POWER_IDLE-Node.POWER_S3) + idlePower
		
		#reqPower = 2000
		if getDebugLevel()>0:
			print "Power: current="+str(currentPower)+" req="+str(reqPower)
		# Turn ON nodes
		if reqPower>currentPower:
			# Account the number of tasks to run
			mapsHadoopWaiting = 0
			redsHadoopWaiting = 0
			for job in getJobs().values():
				if job.state!="SUCCEEDED" and job.state!="FAILED" and len(job.tasks)>0:
					for taskId in job.tasks:
						task = tasks[taskId]
						if task.state!="SUCCEEDED" and taskId.find("_m_")>=0:
							mapsHadoopWaiting += 1
						if task.state!="SUCCEEDED" and taskId.find("_r_")>=0:
							redsHadoopWaiting += 1
				elif job.state=="DATA" or job.state=="PREP" or job.state=="RUNNING":
					# Default value for the number of tasks
					numJobTasks = len(getFilesInDirectories(job.input))
					mapsHadoopWaiting += numJobTasks
					redsHadoopWaiting += numJobTasks*0.25
			maxRequiredNodesMap = math.ceil(1.0*mapsHadoopWaiting/MAP_NODE)
			maxRequiredNodesRed = math.ceil(1.0*redsHadoopWaiting/RED_NODE)
			maxRequiredNodes = max(maxRequiredNodesMap, maxRequiredNodesRed)
			
			while reqPower>currentPower and (len(decNodes)+len(offNodes))>0 and (len(ALWAYS_NODE)+len(onNodes))<maxRequiredNodes:
				# DEC->UP Recommission nodes:
				#   MapReduce: Executed most tasks of still running jobs
				#   HDFS:      Most data required in the future
				if len(decNodes)>0:
					nodeId = decNodes[0]
					if getDebugLevel() > 1:
						print "\tRecommission "+nodeId
					setNodeDecommission(nodeId, False)
					# Update data structure
					decNodes.remove(nodeId)
					onNodes.append(nodeId)
					currentPower += (Node.POWER_AVERAGE-Node.POWER_IDLE)
				else:
					# DOWN->UP Turn on S3 nodes:
					#   MapReduce: -
					#   HDFS:      Containing more required data in the future
					if len(offNodes)>0:
						nodeId = offNodes[0]
						if getDebugLevel() > 1:
							print "\tTurn on "+nodeId
						thread = threading.Thread(target=setNodeStatus,args=(nodeId, True))
						thread.start()
						threads["off->on "+nodeId] = thread
						# Update data structure
						offNodes.remove(nodeId)
						onNodes.append(nodeId)
						currentPower += (Node.POWER_AVERAGE-Node.POWER_S3)
		# Turn OFF nodes
		elif reqPower<currentPower:
			# UP->DEC Decommission nodes:
			#   MapReduce: Executed less tasks of still running jobs
			#   HDFS:      Less data required in the future
			while reqPower<currentPower and len(onNodes)>0:
				nodeId = onNodes[0]
				if getDebugLevel() > 1:
					print "\tDecommission "+nodeId
				setNodeDecommission(nodeId, True)
				# Update data structure
				onNodes.remove(nodeId)
				decNodes.append(nodeId)
				currentPower -= (Node.POWER_AVERAGE-Node.POWER_S3)

		if len(decNodes)>0:
			if getDebugLevel() > 1:
				print "Checking decommission "+str(decNodes)+"..."
			requiredFiles = getRequiredFiles()
			for nodeId in reversed(decNodes):
				# DEC->DOWN Turn off decommission nodes:
				#   MapReduce: Hasn't executed tasks of running jobs
				#   HDFS:      All its required data available in UP nodes
				if len(nodeTasks[nodeId])==0 and len(nodeJobs[nodeId])==0:
					auxFiles=[]
					required = False
					# Get the number of required files of the node
					for fileId in nodeFile.get(nodeId, []):
						if fileId not in auxFiles and fileId in requiredFiles:
							auxFiles.append(fileId)
					for fileId in auxFiles:
						file = files[fileId]
						missing = True
						for otherNodeId in file.getLocation():
							if nodeId != otherNodeId:
								if otherNodeId in ALWAYS_NODE or otherNodeId in onNodes or otherNodeId in decNodes:
									missing = False
									break
						if missing:
							required = True
							break
					if not required:
						if getDebugLevel() > 1:
							print "\tTurn off "+nodeId
						thread = threading.Thread(target=setNodeStatus,args=(nodeId, False))
						thread.start()
						threads["dec->off "+nodeId] = thread
						# Update data structure
						offNodes.append(nodeId)
						decNodes.remove(nodeId)
		# Too many nodes in decommission... run some jobs in there
		if False:
			if len(decNodes)>MAX_DECOMMISSION_NODES:
				target = len(decNodes)-MAX_DECOMMISSION_NODES
				for nodeId in list(decNodes):
					if len(nodeJobs[nodeId])>0:
						setNodeMapredDecommission(nodeId, False)
						decNodes.remove(nodeId)
						target -= 1
						if target<=0:
							break
				if False:
					# Try again with nodes with no jobs
					if target>0:
						for nodeId in list(decNodes):
							setNodeMapredDecommission(nodeId, False)
							decNodes.remove(nodeId)
							target -= 1
							if target<=0:
								break

	# Wait actions to be performed
	#threads.pop().join()
	tstart = datetime.now()
	while len(threads)>0 and datetime.now()-tstart < timedelta(seconds=SLOTLENGTH):
		change = True
		while change and len(threads)>0:
			change = False
			for threadId in threads:
				if not threads[threadId].isAlive():
					del threads[threadId]
					change = True
					break
		if not change:
			time.sleep(0.5)
		else:
			call([HADOOP_HOME+"/bin/hdfs", "dfsadmin", "-refreshNodes"], stderr=open('/dev/null', 'w'))
	
	# Checking inconsistencies
	nodes = getNodes()
	for nodeId in nodes:
		if nodes[nodeId][0]=="DOWN" and nodes[nodeId][1]=="UP":
			# It should be turned on
			#print "Inconsistent state for "+str(nodeId)+": HDFS UP and Mapred should be UP"
			#setNodeMapredStatus(nodeId, True)
			thread = threading.Thread(target=setNodeMapredStatus,args=(nodeId, True))
			thread.start()
			threads["down->up "+nodeId] = thread
		elif nodes[nodeId][0]=="DEC" and nodes[nodeId][1]=="UP":
			# It should be everything on decommission
			#print "Inconsistent state for "+str(nodeId)+": HDFS UP and Mapred in Decommission"
			setNodeHdfsDecommission(nodeId, True)
		elif (nodes[nodeId][0]=="UP" or nodes[nodeId][0]=="DEC") and nodes[nodeId][1]=="DOWN":
			# It should be turned off
			#print "Inconsistent state for "+str(nodeId)+": HDFS down and Mapred should be down"
			#setNodeMapredStatus(nodeId, False)
			thread = threading.Thread(target=setNodeMapredStatus,args=(nodeId, False))
			thread.start()
			threads["up->down "+nodeId] = thread
		# Not solved:
		# 	UP DEC
		# 	DOWN DEC
	
	# Wait re-check actions to be performed
	tstart = datetime.now()
	while len(threads)>0 and datetime.now()-tstart < timedelta(seconds=SLOTLENGTH):
		change = True
		while change and len(threads)>0:
			change = False
			for threadId in threads:
				if not threads[threadId].isAlive():
					del threads[threadId]
					change = True
					break
		if not change:
			time.sleep(0.5)
		else:
			call([HADOOP_HOME+"/bin/hdfs", "dfsadmin", "-refreshNodes"], stderr=open('/dev/null', 'w'))

	# Ouput
	if getDebugLevel() > 1:
		print "Double check:"
		print "  ON:  "+str(ALWAYS_NODE)+" + "+str(onNodes)
		print "  Dec: "+str(decNodes)
		print "  OFF: "+str(offNodes)

		#nodes = getNodes() # True
		#for nodeId in sorted(nodes):
			#print "\t"+str(nodeId)+":\t"+str(nodes[nodeId][0])+"\t"+str(nodes[nodeId][1])

	done = True
	return done


if __name__ == "__main__":
	schedule()
