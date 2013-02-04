#!/usr/bin/python
import os,sys,re,os.path,time
from datetime import datetime,timedelta

class IntellicastRecord:
	def __init__(self,d,condition,cloudCover,temp):
		self.date = d
		self.cond = condition
		self.cc = cloudCover
		self.temp = temp
	


dataDir = '/net/wonko/home/muhammed/intellicast/tempdata'


fileTimeFormat = '%Y_%m_%d_%H'
tspFormat = '%Y_%m_%d_%H_%M'
def toSeconds(td):
	ret = td.seconds
	ret += 24*60*60*td.days
	if td.microseconds > 500*1000:
		ret += 1
	return ret


def diffHours(dt1,dt2):
	
	sec1 = time.mktime(dt1.timetuple())	
	sec2 = time.mktime(dt2.timetuple())		
	
	return int((sec2-sec1)/3600)

if __name__ == '__main__':
	
	times = []
        count = 0
	filesLessThan48  = []
        filesHaveGap = []
	for f in os.listdir(dataDir):
		d = datetime.strptime(f,fileTimeFormat)
		times.append(d)
		
		filename = os.path.join(dataDir,f)
		fd = open(filename,'r')


		count+=1
		lastHour = None		
		c=0
		alreadyIncluded = False
		for line in fd:
			line = line.strip()
			if not line:
				continue
			c+=1

			elements = line.split()
			currentHour = datetime.strptime(elements[0],tspFormat)

			if currentHour==None:
				print f
			     	print elements[0]
				sys.exit()			

			if not lastHour==None and not alreadyIncluded:
                            diff = diffHours(lastHour,currentHour)
                            if diff>1:
				filesHaveGap.append(f)
				alreadyIncluded = True
			
			lastHour = currentHour	
			
			

		if c<48:
#			print f,len(lines)
			filesLessThan48.append((f,c))
		fd.close()		

	times.sort()


	print "collecting time starts",times[0]
	print "collecting time ends",times[-1],
	print "number of hours between start and end",diffHours(times[0],times[-1])
	print "number of actual hours (files)",len(times)
	print "number of files has hours less than 48 entries (filenames output in 'lessThan48.txt')",len(filesLessThan48)
        print "number of files having gap inside ",len(filesHaveGap)
	if len(filesLessThan48):
		
		fd = open('lessThan48.txt','w')
                for f,c in filesLessThan48:
			print >>fd,f,c
		fd.close()
	if len(filesHaveGap):
		
		fd = open('gap.txt','w')
                for f in filesHaveGap:
			print >>fd,f
		fd.close()
	#let's check for gaps
	for i in range(1,len(times)):
		prevHour = times[i-1]
		currentHour = times[i]
                diff = diffHours(prevHour,currentHour)
		if diff>1:
			print "hole starts",prevHour,"ends",currentHour,"number of hours",diff

