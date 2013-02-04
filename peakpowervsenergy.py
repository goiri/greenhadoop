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

import sys

POWER_CAPACITY = 100.0 # Watts

EXPENSIVE_START = 8
EXPENSIVE_END = 23
PRICE_ENERGY_EXPEN = 0.14 # $/kWh
PRICE_ENERGY_CHEAP = 0.08 # $/kWh
PEAK_PRICE = 12 # $/kW

#SLOT_TIME = 5*60 # seconds
SLOT_TIME = 5*60 # seconds


GREEN_AVAILABILITY = [
	0.0, # 0 
	0.0, # 1
	0.0, # 2
	0.0, # 3
	0.0, # 4
	0.0, # 5
	0.0, # 6
	0.0, # 7
	10.0, # 8
	20.0, # 9
	30.0, # 10
	40.0, # 11
	50.0, # 12
	60.0, # 13
	70.0, # 14
	60.0, # 15
	50.0, # 16
	40.0, # 17
	30.0, # 18
	20.0, # 19
	10.0, # 20
	0.0, # 21
	0.0, # 22
	0.0, # 23
]

GREEN_AVAILABILITY = [
	0.0, # 0 
	0.0, # 1
	0.0, # 2
	0.0, # 3
	0.0, # 4
	0.0, # 5
	0.0, # 6
	0.0, # 7
	5.0, # 8
	20.0, # 9
	30.0, # 10
	40.0, # 11
	60.0, # 12
	65.0, # 13
	70.0, # 14
	65.0, # 15
	60.0, # 16
	40.0, # 17
	30.0, # 18
	20.0, # 19
	5.0, # 20
	0.0, # 21
	0.0, # 22
	0.0, # 23
]

# From time to string
def toTimeString(time):
	ret = ""
	# Day
	aux = time/(24*60*60)
	if aux>0:
		ret += str(aux)+"d"
		time = time - aux*(24*60*60)
	
	# Hour
	aux = time/(60*60)
	if aux>0:
		ret += str(aux)+"h"
		time = time - aux*(60*60)
		
	# Minute
	aux = time/(60)
	if aux>0:
		ret += str(aux)+"m"
		time = time - aux*(60)
		
	# Seconds
	if time>0:
		ret += str(time)+"s"
	
	if ret == "":
		ret = "0"
	
	return ret



def calculateCost(proportion, previousPeak, queueEnergy, totalCheapEnergyAvailable, totalExpenEnergyAvailable):
	brownEnergyExpen = []
	brownEnergyCheap = []
	totalCheapEnergy = 0.0
	totalExpenEnergy = 0.0
	peakBrownPower = 0.0
	
	# Initial proportion
	proportionCheap = queueEnergy/totalCheapEnergyAvailable
	proportionExpen = 0.0
	if proportionCheap>1.0:
		proportionCheap = 1.0
		proportionExpen = (queueEnergy-totalCheapEnergyAvailable)/totalExpenEnergyAvailable
		if proportionExpen>1.0:
			proportionExpen = 1.0
	
	if proportion<100.0:
		# Move cheap to expensive
		cheapEnergy = totalCheapEnergyAvailable*proportionCheap
		expenEnergy = totalExpenEnergyAvailable*proportionExpen
		expenEnergy += cheapEnergy*(100-proportion)/100.0
		if expenEnergy>totalExpenEnergyAvailable:
			expenEnergy = totalExpenEnergyAvailable
		delta = expenEnergy-(totalExpenEnergyAvailable*proportionExpen)
		cheapEnergy = (totalCheapEnergyAvailable*proportionCheap)-delta

		# Recalculate 
		proportionCheap = cheapEnergy/totalCheapEnergyAvailable
		proportionExpen = expenEnergy/totalExpenEnergyAvailable

	#print "Percentage: "+str(proportion)
	#print "  Cheap: "+str(totalCheapEnergyAvailable*proportionCheap)+"/"+str(totalCheapEnergyAvailable)
	#print "  Expen: "+str(totalExpenEnergyAvailable*proportionExpen)+"/"+str(totalExpenEnergyAvailable)
	#print "  Cheap: %.2f%%" % (proportionCheap*100.0)
	#print "  Expen: %.2f%%" % (proportionExpen*100.0)

	if proportionCheap<0.0:
		proportionCheap = 0.0
	if proportionExpen<0.0:
		proportionExpen = 0.0

	for i in range(0, (24*60*60)/SLOT_TIME):
		hour = i*SLOT_TIME/3600
		
		# Calculate brown and green power
		# Expensive
		if EXPENSIVE_START<=hour and hour<EXPENSIVE_END:
			expenEnergyAvailable = (POWER_CAPACITY-GREEN_AVAILABILITY[hour])*(SLOT_TIME/3600.0)
			
			# Assign to cheap
			aux = expenEnergyAvailable*proportionExpen
			queueEnergy -= aux
			#brownEnergyExpen[i] = aux
			brownEnergyCheap.append(0.0)
			brownEnergyExpen.append(aux)
			
			totalExpenEnergy += aux
			
			# Peak
			auxPeakBrown = aux/(SLOT_TIME/3600.0)
		# Cheap
		else:
			cheapEnergyAvailable = (POWER_CAPACITY-GREEN_AVAILABILITY[hour])*(SLOT_TIME/3600.0)
			
			# Assign to cheap
			aux = cheapEnergyAvailable*proportionCheap
			queueEnergy -= aux
			#brownEnergyCheap[i] = aux
			brownEnergyCheap.append(aux)
			brownEnergyExpen.append(0.0)
			
			totalCheapEnergy += aux
			
			# Peak
			auxPeakBrown = aux/(SLOT_TIME/3600.0)
		if auxPeakBrown>peakBrownPower:
			peakBrownPower = auxPeakBrown

	# Costs: energy and peak
	energyCost =  (totalCheapEnergy*PRICE_ENERGY_CHEAP/1000.0)
	energyCost += (totalExpenEnergy*PRICE_ENERGY_EXPEN/1000.0)
	
	peakCost = (peakBrownPower-previousPeak) * PEAK_PRICE/1000.0
	if peakCost<0.0:
		peakCost = 0.0
	
	#print "Cheap energy:     %.2fWh" % (totalCheapEnergy)
	#print "Expensive energy: %.2fWh" % (totalExpenEnergy)

	#print "$"+str(energyCost)+" + $"+str(peakCost)+" = $"+str(energyCost+peakCost)

	return (energyCost+peakCost, brownEnergyCheap, brownEnergyExpen)


def main():
	queueEnergy = 1800.0 # Wh
	if len(sys.argv)>1:
		queueEnergy = int(sys.argv[1])

	greenEnergy = []

	previousPeak = 10.0

	totalGreenEnergy = 0.0
	totalExpenEnergyAvailable = 0.0
	totalCheapEnergyAvailable = 0.0
	for i in range(0, (24*60*60)/SLOT_TIME):
		hour = i*SLOT_TIME/3600
		
		# Calculate brown and green power
		greenAvailable = GREEN_AVAILABILITY[hour]*(SLOT_TIME/3600.0)
		totalGreenEnergy += greenAvailable
		if EXPENSIVE_START<=hour and hour<EXPENSIVE_END:
			totalExpenEnergyAvailable += POWER_CAPACITY*(SLOT_TIME/3600.0)-greenAvailable
		else:
			totalCheapEnergyAvailable += POWER_CAPACITY*(SLOT_TIME/3600.0)-greenAvailable

		# Assign green energy
		if (queueEnergy>0.0):
			if (greenAvailable >= queueEnergy):
				greenEnergy.append(greenAvailable)
				queueEnergy -= greenAvailable
			else:
				greenEnergy.append(greenAvailable)
				queueEnergy -= greenAvailable
			if queueEnergy<0.0:
				queueEnergy = 0.0
		else:
			greenEnergy.append(0.0)


	# Assign brown
	#print "Queue energy: "+str(queueEnergy)+"Wh"
	#print "Total energy: "+str(24*POWER_CAPACITY)+"Wh"
	#print "Green energy: "+str(totalGreenEnergy)+"Wh"
	#print "Cheap energy: "+str(totalCheapEnergyAvailable)+"Wh"
	#print "Expen energy: "+str(totalExpenEnergyAvailable)+"Wh"

	point0 = 0.0
	pointN = 100.0
	cost0,brownEnergyCheap0,brownEnergyExpen0 = calculateCost(point0, previousPeak, queueEnergy, totalCheapEnergyAvailable, totalExpenEnergyAvailable)
	costN,brownEnergyCheapN,brownEnergyExpenN = calculateCost(pointN, previousPeak, queueEnergy, totalCheapEnergyAvailable, totalExpenEnergyAvailable)
	
	if costN<cost0:
		cost = costN
		point = pointN
		brownEnergyCheap = brownEnergyCheapN
		brownEnergyExpen = brownEnergyExpenN
	else:
		cost = cost0
		point = point0
		brownEnergyCheap = brownEnergyCheap0
		brownEnergyExpen = brownEnergyExpen0
	
	# Minimize
	for i in range(0,100):
		distance = (pointN-point0)/3.0
		point1 = point0 + 1.0*distance
		point2 = point0 + 2.0*distance

		# Reassign brown energy to reduce peak
		cost1,brownEnergyCheap1,brownEnergyExpen1 = calculateCost(point1, previousPeak, queueEnergy, totalCheapEnergyAvailable, totalExpenEnergyAvailable)
		cost2,brownEnergyCheap2,brownEnergyExpen2 = calculateCost(point2, previousPeak, queueEnergy, totalCheapEnergyAvailable, totalExpenEnergyAvailable)
		
		# Calculate slopes
		slope0 = -1
		if cost0<cost1:
			slope0 = 1
		slope1 = -1
		if cost1<cost2:
			slope1 = 1
		slope2 = -1
		if cost2<costN:
			slope2 = 1
		
		#print "====== Iteration %d point=%.2f-%.2f cost=$%.4f" % (i, point0, pointN, cost)
		#print " Point 0: %.2f $%.2f" % (point0, cost0)
		#print "  Slope 0: "+str(slope0)
		#print " Point 1: %.2f $%.2f" % (point1, cost1)
		#print "  Slope 1: "+str(slope1)
		#print " Point 2: %.2f $%.2f" % (point2, cost2)
		#print "  Slope 2: "+str(slope2)
		#print " Point N: %.2f $%.2f" % (pointN, costN)
		
		# Calculate new points
		auxcost = cost0
		if slope0>0:
			point0 = point0
			pointN = point1
			cost = cost0
			point = point0
			brownEnergyCheap = brownEnergyCheap0
			brownEnergyExpen = brownEnergyExpen0
		elif slope0<0 and slope1>0:
			point0 = point0
			pointN = point2
			cost = cost1
			point = point1
			brownEnergyCheap = brownEnergyCheap1
			brownEnergyExpen = brownEnergyExpen1
		elif slope1<0 and slope2>0:
			point0 = point1
			pointN = pointN
			cost = cost2
			point = point2
			brownEnergyCheap = brownEnergyCheap2
			brownEnergyExpen = brownEnergyExpen2
		elif slope2<0:
			point0 = point2
			pointN = pointN
			cost = costN
			point = pointN
			brownEnergyCheap = brownEnergyCheapN
			brownEnergyExpen = brownEnergyExpenN
			
		# Close enough
		if pointN-point0<0.1:
			break

	#print "********* "+str(point)+"% cost=$"+str(cost)

	# Output
	# plot "out" using 1:3 w steps title "Green", "out" using 1:4 w steps title "Cheap brown", "out" using 1:5 w steps title "Expensive energy", "out" using 1:6 w steps title "Total energy", "out" using 1:7 w steps title "Capacity"
	for i in range(0,len(greenEnergy)):
		out = str(i)+"\t"
		out += toTimeString(i*SLOT_TIME)+"\t"
		out += str(greenEnergy[i]/(SLOT_TIME/3600.0))+"\t"
		out += str(brownEnergyCheap[i]/(SLOT_TIME/3600.0))+"\t"
		out += str(brownEnergyExpen[i]/(SLOT_TIME/3600.0))+"\t"
		out += str((greenEnergy[i]+brownEnergyCheap[i]+brownEnergyExpen[i])/(SLOT_TIME/3600.0))+"\t"
		out += str(POWER_CAPACITY)
		print out
	print "%.2f+%.2f+%.2f = %.2f" % (greenEnergy[i]/(SLOT_TIME/3600.0), brownEnergyCheap[i]/(SLOT_TIME/3600.0), brownEnergyExpen[i]/(SLOT_TIME/3600.0), (greenEnergy[i]+brownEnergyCheap[i]+brownEnergyExpen[i])/(SLOT_TIME/3600.0))


if __name__ == "__main__":
	main()