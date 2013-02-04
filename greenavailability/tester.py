#!/usr/bin/python

import sys
import math
sys.path.append(r'./')
import model,setenv

setenv.init()
from datetime import timedelta, datetime

MAX = 49

BASE_DATE = datetime(2010, 7, 12, 9, 0, 0)

ep = model.EnergyPredictor('.', 2, 2070,debugging=True)

values = {}


def addValue(t, v):
	if t not in values:
		values[t] = []
	values[t].append(v)

for i in range(0, 7*24):
	#date = BASE_DATE + timedelta(seconds=i*900)
	date = BASE_DATE + timedelta(seconds=i*900*4)
	greenAvail,change = ep.getGreenAvailability(date, MAX)
	
	
	#print "Prediction at "+str(date)+" ("+str(change)+")"
	#print "  %s %.2f" % (str(date), greenAvail[0])
	#print "%d\t%.2f" % (toSeconds(date-BASE_DATE), greenAvail[0])
	#addValue(toSeconds(date-BASE_DATE), greenAvail[0])
	
	for i in range(1, len(greenAvail)):
		d = date + timedelta(hours=i)
		d = datetime(d.year, d.month, d.day, d.hour)
		#print "  %s %d %.2f" % (str(d), toSeconds(d-BASE_DATE), greenAvail[i])
		#print "%s\t%.2f" % (str(d-BASE_DATE), greenAvail[i])
		#addValue(toSeconds(d-BASE_DATE), greenAvail[i])	


#for t in sorted(values.keys()):
	#aux = ""
	#for v in values[t]:
		#aux += "%.2f\t" % v
	#print str(t)+"\t"+aux
	
	
	
