#!/usr/bin/python

import os, fnmatch, tempfile, sys,os.path,pickle
#import singledayweather
import weatherPrediction, pastWeather
#import datetime
from datetime import timedelta, datetime

#the input is a tab separated text file containing weather and solar data


princetonDataStart = datetime(year=2010,month=5,day=28)
princetonDataEnd = datetime(year=2010,month=10,day=4)
pisDataStart = datetime(year=2010,month=12,day=14)
pisDataEnds = datetime.now()

#error entering and exiting method
normal = 0
enter_on_thresh = 1
enter_on_error = 2

#states
normal_state = 0


hourFormat = '%m_%d_%Y_%H_%M'

class PredictionProvider:
	def __init__(self,dataPath,shiftFactor):
		self.dataPath = dataPath
		self.shiftFactor = shiftFactor
		
	def getPredictions(self,date,hours):
		
		numData = hours-self.shiftFactor
		start = date+timedelta(hours=self.shiftFactor)
		
		inp = weatherPrediction.process(start, numData, self.dataPath,self.shiftFactor)
		
		foretag = []
		foreweather = []
		hour_pre = []
#		shift = -1
		#inp = open("fore.txt","r")
		for line in inp.readlines():
#			print line.strip()
			#print line.split("\t")[5], line.split("\t")[15]
			#print int(line.split()[0].split("_")[-1])
#			if shift == -1:
#				shift = int(line.split()[0].split("_")[-1])
#				start = datetime.strptime(line.split()[0],"%Y_%m_%d_%H")
			foretag.append(int(line.split("\t")[2]))
			foreweather.append(line.split("\t")[1])
			hour_pre.append(int(line.split()[0].split("_")[-1]))
			#day.append(line.split("\t")[4])
		inp.close()
		
		return foretag,foreweather,hour_pre

class ActualConditions(PredictionProvider):
	
	def __init__(self,dataPath):
		self.datapath = dataPath
		self.timeToCondition = {}
		
	def getPredictions(self,date,hours):
		
		conditions = self.getConditions(date, hours)
		foretag = []
		foreweather = []
		hour_pre = []
#		shift = -1
		#inp = open("fore.txt","r")
		for (d,c) in conditions:
			#print line
			#print line.split("\t")[5], line.split("\t")[15]
			#print int(line.split()[0].split("_")[-1])
#			if shift == -1:
#				shift = int(line.split()[0].split("_")[-1])
#				start = datetime.strptime(line.split()[0],"%Y_%m_%d_%H")
			foretag.append(int(c.conditionGroup))
			foreweather.append(c.conditionString)
			hour_pre.append(d.hour)
			#day.append(line.split("\t")[4])
		return foretag,foreweather,hour_pre
		
		
	def populateConditions(self,date,numberOfHours):
		
		retval = pastWeather.process(date,numberOfHours,self.datapath)
		
#		print "populate",date,numberOfHours
		
		self.timeToCondition.update(retval)
	
	def getConditionString(self,date):
			try:
				v = self.timeToCondition[date]
			except KeyError:
				self.populateConditions(date,24)
				try:
					v = self.timeToCondition[date]
				except KeyError:
					print date
					print self
					raise	
			return v.conditionString
	def getConditions(self,start, num_hours):
		retval = []

		tdelta = timedelta(hours=1)
		d = datetime(start.year,start.month,start.day,start.hour)
		
		for i in range(num_hours):
			# TODO there was a bug in here: you check the size and then you increase the number
			try:
				v = self.timeToCondition[d]
			except KeyError:
				self.populateConditions(d,num_hours-i)
				v = self.timeToCondition[d]
			retval.append((d,v))
			d = d+tdelta
		#print len(retval)
		return retval			
	def __str__(self):
		keys = self.timeToCondition.keys()
		keys.sort()
		retval = ""
		for k in keys:
			retval+="%s\t%s\n"%(str(k),str(self.timeToCondition[k]))
		return retval
class EnergyProduction:
	def __init__(self,dataPath):
		self.dataPath = dataPath
		self.timeToEnergy = {}
	
	def populateEnergy(self,date):
		path = self.dataPath		
		filepath = os.path.join(path,"pastdata",`date.year`,`date.month`,"data.txt")
		if not os.path.isfile(filepath):
			print filepath," is not a file"
			raise Exception()
			sys.exit(1)
			
		datafile = open(filepath,'r')		
		startDate = None
#		print "populate energy",date,filepath
		tdelta = timedelta(hours=1)
		for line in datafile:
			#print lines[i]
			if line[0]=='#':
				continue
			line = line.strip()
			if not line:
				continue
			
			
			if not startDate:
				startDate = datetime.strptime(line.strip(),"%Y_%m_%d_%H")
				d = startDate
			else:
				v = float(line.strip())
				self.timeToEnergy[d] = v
				d = d+tdelta			
			
				
		
	def readPastProduction(self,start, num_hours):

		#print filepath
		retval = []

		tdelta = timedelta(hours=1)
		d = datetime(start.year,start.month,start.day,start.hour)
		
		for i in range(num_hours):
			# TODO there was a bug in here: you check the size and then you increase the number
			try:
				v = self.timeToEnergy[d]
			except KeyError:
				self.populateEnergy(d)
				v = self.timeToEnergy[d]
			retval.append(v)
			d = d+tdelta
		#print len(retval)
	
		return retval	

	def getProduction(self,hour):
                d = hour
                try:
                        v = self.timeToEnergy[d]
                except KeyError:
                        self.populateEnergy(d)
                        v = self.timeToEnergy[d]
                return v


class Debug:
	def __init__(self,scaling=1):
		self.reset()
		self.scaling = scaling
		
	def reset(self):
		self.haveLastTag = True
		self.baseValue = 0
		self.tagValue = 0
		self.tag = "Unknown"
		self.trackTag = False
		self.prediction = 0
		self.actual = 0
		self.actualCondition = "Unknown"
		
		
	def __str__(self):
		return "%.2f\t%s\t%.2f\t%.2f\t%s(%s)\t%s\t%s\t%.2f"%(self.scaling * self.baseValue,str(self.tagValue),self.scaling * self.prediction,self.scaling * self.actual,self.tag,self.actualCondition,str(self.haveLastTag),str(self.trackTag),self.actual)

		
class EnergyPredictor(object):

	def __init__(self,path='.',threshold=3, scalingFactor=1347, offset=15, energythreshold=20, useActualData=False, scalingBase=1347,debugging=False,error_exit=normal):
		#energythreshold=20 means we assume two energy value to be same if within range of +-20
		#global debug
		self.datapath = path
		self.scaling = scalingFactor/float(scalingBase)
#		self.scaling = scalingFactor / 1347.0

		self.threshold = threshold
		self.offset = offset
		self.energythreshold = energythreshold
		self.pasttime = datetime(2000,01,01)
		self.pastresult = []
		self.debugging = debugging
		self.debug = []
		self.energyProduction = EnergyProduction(self.datapath)
		self.shiftFactor = 2
		self.actualConditions = ActualConditions(self.datapath)
		self.useActualData = useActualData
		self.error_exit = error_exit
		self.last_call_state = normal_state
		
		if useActualData:
			self.weatherPredictor = self.actualConditions
#			print "using actual data"
		else:
			self.weatherPredictor = PredictionProvider(self.datapath,self.shiftFactor)
#			print "using predictions"
		self._1sterror = 0.4
		self._reterror = 0.35
		self.lagerror = 1
			
		
	def getGreenAvailability(self, now, hours):
		# TODO is it right?
		#yes it is correct now
		#if now<datetime(2010, 6, 16, 0, 0, 0):
		#	self.offset=21

		result, actual  = self.process(now, hours)
#		result, actual  = self.process(now, hours, self.datapath, self.threshold, self.energythreshold, self.offset,debugging=self.debugging)


		#debug code start
		#fp = file(self.datapath+"/debug/%0*d_%0*d_%0*d_%0*d_%0*d.txt"%(2,now.year,2,now.month,2,now.day,2,now.hour,2,now.minute),'w')
		#dt = datetime(now.year,now.month,now.day,now.hour)
		#debug code end
#		for i in range(len(result)):
#			#debug code start
#			#fp.write(str(dt+timedelta(hours=i))+"\t"+`actual[i]`+"\t"+`result[i]`+"\t"+`result[i]-actual[i]`+"\n")
#			#debug code end
#
#			result[i] = self.scaling * result[i]
#		
		result = map(lambda x:self.scaling * x, result)

		#fp.close()
		

		flag = False
		#print self.pastresult
		if len(self.pastresult)==0:
			flag = True
		elif self.pasttime + timedelta(hours=self.threshold) <= now:
			flag = True
		else:
			#print self.pasttime, self.pastresult
			d = datetime(now.year,now.month,now.day,now.hour)
			lasttime = datetime(self.pasttime.year, self.pasttime.month, self.pasttime.day, self.pasttime.hour)
			j = 0
			for i in range(len(self.pastresult)):
				if d==lasttime+timedelta(hours=i):
					if j<len(result) and abs(self.pastresult[i]-result[j])>self.energythreshold:
						flag = True
						break
					j += 1
					d += timedelta(hours=1)
		
		if flag==True:
			self.pasttime = now
			self.pastresult = []
			for i in range(len(result)):
				self.pastresult.append(result[i])

		#print self.pastresult, flag

		currentHour = datetime(now.year,now.month,now.day,now.hour)
 		if not currentHour == now:
 			#not the beginning of hour
 			result[0] = self.scaling * self.energyProduction.getProduction(currentHour)		
 
		#print now,result[0]
		
		
		return result, flag
		
	
		
		

	def countsunny(self,startrow):
		j = startrow
		retval = 0
		for k in range(24):
			#print j, len(weather), len(data)
			if (data[j]>0) and (weather[j]=='sunny'):
	
			#if (data[j]>data[0]) and (weather[j]=='sunny'):
				retval += 1
			
			j += 1
	
		#print j,retval
	
		return retval
	
	def selectbase(self):
		i = 0
		baserow = 0
		maxcount = 0
		while (i < len(data)):
			count = countsunny(i)
			if count > maxcount:
				baserow = i
				maxcount = count
			i += 24
	
		#print baserow, maxcount
		return baserow
	
	def constructbase(self,data, lowdata):
		#select the maximum of each hour and append it at the end of data, we will these values as base
		retval = len(data)
		#print retval
		for i in range(24):
			data.append(0.0)
		#print len(data)
		for i in range(retval):
			if data[i]>data[retval+i%24]:
				data[retval+i%24] = data[i]
			if lowdata[i]>data[retval+i%24]:
				#print (retval+i)%24,i,retval, lowdata[i]
				data[retval+i%24] = lowdata[i]
				
	
		return retval
	
	def noisefreetags(self,alltags, groups):
		tagvalues = []
		#print len(alltags)
		for i in range(0,len(alltags)):
			alltags[i].sort()
			#print alltags[i]
			if len(alltags[i]) > 0:
				#print i, len(alltags[i]), alltags[i][len(alltags[i])/2], sum_avg[i]
				tagvalues.append(alltags[i][len(alltags[i])/2])
			else:
				#print i, len(alltags[i]), 0.0, sum_avg[i]
				tagvalues.append(0.0)
		#print tagvalues, len(tagvalues)
		return tagvalues
				
	
	def calctag(self,tag, weather, data, lowdata):
		#base = selectbase()
		base = self.constructbase(data, lowdata)
		mult = []
		sum_avg = [0.0]*(max(tag)+1)
		sum_low = [0.0]*(max(tag)+1)
		count = [0]*(max(tag)+1)
		count_low = [0]*(max(tag)+1)
		mapping = ["Empty"]*(max(tag)+1)
		#debug code start
		alltags = []
		lowtags = []
		for i in range(max(tag)+1):
			#alltags[i] = []
			alltags.append([])
			lowtags.append([])
		#debug code end
		#print count
		for i in range(len(tag)):
			mapping[tag[i]] = weather[i]
			#print i, lowdata[i]
			if data[base + i%24]==0:
				mult.append(0.0)
				#print a
			else:
				#print i, lowdata[i], data[i]
				mult.append(lowdata[i]/data[base + i%24]*100)
				if mult[i]>0.0:
					lowtags[tag[i]].append(mult[i])				
				sum_low[tag[i]] += mult[i]
				#if mult[i] > 0.0:
				count_low[tag[i]] += 1
				mult[i] = (data[i]/data[base + i%24]*100)
				#debug code start
				if mult[i]>0.0:
					alltags[tag[i]].append(mult[i])				
					#use=max(lowdata[i],data[i])
					#print i/24, i%24, weather[i], use, use/data[base+i%24]
				sum_avg[tag[i]] += mult[i]
				if mult[i] > 0.0:
					count[tag[i]] += 1
	
			
	
		basevalue = []
		for i in range(24):
			basevalue.append(data[base+i])
	
		#print basevalue
				
		
		#outfile = open("tags","w")
		#print sum_avg, sum_low
		for i in range(len(sum_avg)):
			if count[i]<>0:
				sum_avg[i] /= count[i]
			if count_low[i]<>0:
				sum_low[i] /= count_low[i]
			#print i,mapping[i], count[i], sum_avg[i]
			#outfile.write(`i`+"\t"+mapping[i]+"\t"+`count[i]`+"\t"+` sum_avg[i]`+"\n")
	
		#for i in range(1,len(alltags)):
		#	alltags[i].sort()
		#	print alltags[i]
		#	if len(alltags[i]) > 0:
		#		print i, len(alltags[i]), alltags[i][len(alltags[i])/2], sum_avg[i]
		#	else:
		#		print i, len(alltags[i]), 0.0, sum_avg[i]
	
		#print len(sum_avg)	
		sum_avg = self.noisefreetags(alltags, len(sum_avg))
		sum_low = self.noisefreetags(lowtags, len(sum_low))
		#print len(sum_avg)
	
		#debug code returning same tag value for entire day
		#return basevalue, sum_avg, sum_avg, mapping
		return basevalue, sum_avg, sum_low, mapping
	
		#outfile.close()
	
		
	
	def populate(self,dirname, filename, date, offset):
	
		tag = []
		weather = []
		data = []
		lowdata = []
	
		#hardcoded to handle May 31, 2010
		if offset>15:
			prevdate = datetime(date.year,date.month,date.day) + timedelta(days=5) 
			nextdate = datetime(date.year,date.month,date.day) + timedelta(days=24)
		else:
			prevdate = datetime(date.year,date.month,date.day) - timedelta(days=offset) 
			nextdate = datetime(date.year,date.month,date.day) + timedelta(days=offset)
	
		#print prevdate, nextdate
	
		inp = open(dirname+'/'+filename,"r")
	        for line in inp.readlines():
			#print line.split("\t")[4]#, line.split("\t")[15]
			d = datetime.strptime(line.split("\t")[4],"%m/%d/%Y %H:%M")
			d = d.replace(year=date.year)
			#date = datetime(date.year,date.month,date.day)
			#print d
			if (prevdate <= d) and (nextdate > d):
				if len(line.split("\t"))<16:
					print "We got a corrupted a training file ", filename
					sys.exit(1)
	
				#if d.hour == 7:
				#	print d, line.split("\t")[15]
				#print line.split("\t")[4]
				tag.append(int(line.split("\t")[5]))
				weather.append(line.split("\t")[6])
				if d.hour < 10 or d.hour > 15:
					lowdata.append(float(line.split("\t")[15]))
					data.append(0)
					#debug code start
					#data.append(float(line.split("\t")[15]))
					#debug code end
				else:
					#lowdata.append(0.0)
					lowdata.append(float(line.split("\t")[15]))
					data.append(float(line.split("\t")[15]))
				#print line, len(data), ((len(data)-1)%24)
	
		inp.close()
		#print lowdata
		return tag, weather, data, lowdata
	

				
	
	def predictday(self,date, hours):
		#global debug
		
		
		
		#global tag
		#global weather
		#global data
		#global lowdata
		tag = []
		weather = []
		data = []
		lowdata = []
		#duration = 14
		
		start = date
		#print start
		month = start.month
		lastmonth = (start-timedelta(days=self.offset)).month
		nextmonth = (start+timedelta(days=self.offset)).month
		#lastmonth = (start+timedelta(days=offset)).month
		#nextmonth = 
	
		#print lastmonth, nextmonth
	
		
		path = self.datapath
		
		
		for datafile in sorted(os.listdir(path)):
			ste1 = os.path.join(path,datafile)
			
			#print ste1
			if os.path.isdir(ste1) and datafile.isdigit():
				if lastmonth<=int(datafile) or nextmonth>=int(datafile):
					#abc = os.listdir(ste1)
					#print abc
#					for tfile in sorted(os.listdir(path+"/"+datafile)):
					for tfile in sorted(os.listdir(ste1)):					
						if fnmatch.fnmatch(tfile,"*wea_data.txt"):
							tag1, weather1, data1, lowdata1 = self.populate(ste1, tfile, start, self.offset)
							for a in range(len(tag1)):
								tag.append(tag1[a])
								weather.append(weather1[a])
								data.append(data1[a])
								lowdata.append(lowdata1[a])
	
	
		#print tag, len(tag), data
		if len(tag)==0:
			print "Failed to locate training data"
			sys.exit(1)
		
		if len(tag)%24<>0:
			print "Training data has some unknown missing lines"
			sys.exit(1)
	
		if len(tag)<>len(weather):
			print "Tag weather mismatch"
			sys.exit(1)
		
		#print start	
		basevalue, factor, factor2, mapping = self.calctag(tag, weather, data, lowdata)
		#print factor, "and", factor2
		#singledayweather.process(date)
		
#		if self.debugging:
#		
#			actualConditions = pastWeather.process(date, hours, path)
#		 	actualw = []
#		 	for line in actualConditions.readlines():
#		 		
#				#print line
#		 		#print line.split("\t")[5], line.split("\t")[15]
#		 		#print int(line.split()[0].split("_")[-1])
#		 		#if shift == -1:
#		 		#	shift = int(line.split()[0].split("_")[-1])
#		 		#	start = datetime.strptime(line.split()[0],"%Y_%m_%d_%H")
#		 		#foretag.append(int(line.split("\t")[2]))
#		 		#foreweather.append(line.split("\t")[1])
#		 		#hour_pre.append(int(line.split()[0].split("_")[-1]))
#		 		actualw.append(line.split("\t")[1])
#		 		#day.append(line.split("\t")[4])
#		 	actualConditions.close()	
	 
	#		print len(actualw)
	
		#print "before"
	 
#	 	if past:
#	 		inp = pastWeather.process(date, hours, self.datapath) 		
#	 	else:
#			#print date, hours	
#			inp = weatherPrediction.process(date, hours, self.datapath)
	
		#print "abc"
		#for line in inp:
		#	print line.strip()
	
		#start prediction
		#newtag = []
		#newweather = []
		#inp = open("modeldata/%d_%d_%d_wea.txt"%(date.month, date.day, date.year),"r")
		#for line in inp.readlines():
		#	#print line.split("\t")[5], line.split("\t")[15]
		#	newtag.append(int(line.split("\t")[5]))
		#	newweather.append(line.split("\t")[6])
		#	#day.append(line.split("\t")[4])
		#inp.close()
	
		
		
		numData = hours+self.shiftFactor
		start = date-timedelta(hours=self.shiftFactor)
		
		endTime = date+timedelta(hours=hours-self.shiftFactor)
		#shift = endTime.hour
		shift = start.hour
#		#inp = open("fore.txt","r")
#		for line in inp.readlines():
#			#print line
#			#print line.split("\t")[5], line.split("\t")[15]
#			#print int(line.split()[0].split("_")[-1])
#			if shift == -1:
#				shift = int(line.split()[0].split("_")[-1])
#				start = datetime.strptime(line.split()[0],"%Y_%m_%d_%H")
#			foretag.append(int(line.split("\t")[2]))
#			foreweather.append(line.split("\t")[1])
#			hour_pre.append(int(line.split()[0].split("_")[-1]))
#			#day.append(line.split("\t")[4])
#		inp.close()
#	
		#print "shift", shift
		pastprod = self.energyProduction.readPastProduction(start, numData)
		foretag,foreweather,hour_pre = self.weatherPredictor.getPredictions(start, numData)
		
#		os.system("echo \"%s\" >> foretag.txt"%(str(foretag)))
#		os.system("echo \"%s\" >> foreweather.txt"%(str(foreweather)))
#		os.system("echo \"%s\" >> hour_pre.txt"%(str(hour_pre)))
#		os.system("echo \"%s\" >> pastprod.txt"%(str(pastprod)))
#		os.system("echo \"%s\" >> shift.txt"%(str(shift)))
		#print "shift by",shift
		#print pastprod, len(pastprod)
		#print len(foretag), len(actualw)
	
		if len(pastprod)<len(foretag):
			print "actual data missing after ", start, len(pastprod), len(foretag)
			sys.exit(1)
	
		predictvalue = []
		predictfore = []
#		outp = tempfile.TemporaryFile()#open("pre.txt","w")
		prevfact = factor[1] #we assume last tracked tag is factor for sunny
		track_tag = factor[1] #we assume last tracked tag is factor for sunny
		state = self.last_call_state
		
		
		#print basevalue
	
		#debug code start
		#if not os.path.exists(path+"/debug"):
		#	os.mkdir(path+"/debug")
		
		#if not os.path.isfile(path+"/debug/%0*d_%0*d_%0*d_tag.txt"%(2,date.year,2,date.month,2,date.day)):
		#	fp = open(path+"/debug/%0*d_%0*d_%0*d_tag.txt"%(2,date.year,2,date.month,2,date.day),'w')
		#	for i in range(1,len(factor)):
		#		fp.write(mapping[i]+"\t"+`factor[i]`+"\t"+`factor2[i]`+"\n")
	
		#	fp.close()
	
	
		#fp = file(path+"/debug/%0*d_%0*d_%0*d_%0*d_%0*d_wea.txt"%(2,date.year,2,date.month,2,date.day,2,date.hour,2,date.minute),'w')
		#debug code end
		#dt = datetime(now.year,now.month,now.day,now.hour)
		
		i = -1	
		for i in range(len(foretag)):
			#if newtag[i]>len(mapping) or (mapping[newtag[i]]=='Empty'):
			#	print "No value for",newweather[i]
			#	predictvalue.append(prevfact * basevalue[i%24]/100)
				
			#elif basevalue[i%24] == 0.0:
			#	predictvalue.append(0.0)
			#else:
			#	predictvalue.append(factor[newtag[i]]*basevalue[i%24]/100)
			#	prevfact = factor[newtag[i]]
				
			#print i, foretag[i], hour_pre[i], len(mapping)
			if foretag[i]>=len(mapping) or (mapping[foretag[i]]=='Empty'):
				#print "No value for",foreweather[i]
				if self.error_exit==normal:
					if i>=(self.shiftFactor+2):
						predictfore.append(prevfact * basevalue[(i+shift)%24]/100)
					else:
						predictfore.append(track_tag * basevalue[(i+shift)%24]/100)
				elif state==self.lagerror or i<(self.shiftFactor+2):
					predictfore.append(track_tag * basevalue[(i+shift)%24]/100)
				else:
					predictfore.append(prevfact * basevalue[(i+shift)%24]/100)
				if self.debugging:
					self.debug[i].haveLastTag = False
			#elif basevalue[i%24] == 0.0:
			#	predictvalue.append(0.0)
			elif hour_pre[i]<10 or hour_pre[i]>15:
				#if hour_pre[i]==8:
				#	print factor2[foretag[i]]*basevalue[(i+shift)%24]/100, factor2[foretag[i]], basevalue[(i+shift)%24]
				predictfore.append(factor2[foretag[i]]*basevalue[(i+shift)%24]/100)
				prevfact = factor2[foretag[i]]
			else:
				predictfore.append(factor[foretag[i]]*basevalue[(i+shift)%24]/100)
				#print i, predictfore[i]
				prevfact = factor[foretag[i]]
	
			if self.debugging:
				self.debug[i].tagValue = prevfact 
			
			tracked_pred = track_tag * basevalue[(i+shift)%24]/100
			forecast_pred = predictfore[i]

			if self.error_exit==normal and i<(self.shiftFactor+2) and state==self.lagerror:
				predictfore[i] = tracked_pred
				prevfact = track_tag
			elif self.error_exit==enter_on_thresh and state==self.lagerror:
				#nontemp = predictfore[i]
				predictfore[i] = tracked_pred
				prevfact = track_tag
			elif self.error_exit==enter_on_error and state==self.lagerror:
				predictfore[i] = tracked_pred
				prevfact = track_tag
	
			#print i, predictfore[i]#, pastprod[i]
			
			#print date, newweather[i], predictvalue[i]
			if basevalue[(i+shift)%24]>0 and i<(self.shiftFactor+1):
				# factor[1] is the tag for sunny, we assume tags cant go over tag for sunny
				track_tag = min(pastprod[i]/basevalue[(i+shift)%24]*100, factor[1])
	
			prev_state = state
			#exiting error correction mode
			if self.error_exit==normal and state<>normal_state:
				if i>=(self.shiftFactor+2):
					state = normal_state
				elif pastprod[i]==predictfore[i] or abs(pastprod[i]-forecast_pred)<self._reterror*pastprod[i]:
					state = normal_state
			elif self.error_exit==enter_on_thresh and state<>normal_state:
				if basevalue[(i+shift)%24]==0.0:
					state = normal_state
			elif self.error_exit==enter_on_error and state<>normal_state:
				if basevalue[(i+shift)%24]==0.0:
					state = normal_state
			#entering error correction mode
			elif self.error_exit==normal and i<(self.shiftFactor+1) and abs(pastprod[i]-forecast_pred)>self._1sterror*pastprod[i] and hour_pre[i]>8 and hour_pre[i]<17:
				state = min(self.lagerror, state+1)
			elif self.error_exit==enter_on_thresh and i<(self.shiftFactor+1) and abs(pastprod[i]-forecast_pred)>self._1sterror*pastprod[i] and hour_pre[i]>8 and hour_pre[i]<17:
				state = min(self.lagerror, state+1)
			elif self.error_exit==enter_on_error and i<(self.shiftFactor+1) and hour_pre[i]>8 and hour_pre[i]<17:
				#print abs(pastprod[i]-forecast_pred), abs(pastprod[i]-tracked_pred)
				if abs(pastprod[i]-forecast_pred) > abs(pastprod[i]-tracked_pred):
					state = min(self.lagerror, state+1)

			if i==(self.shiftFactor+2)-1:
				self.last_call_state = state
			#elif state==1 and abs(pastprod[i]-predictfore[i])>0.3*pastprod[i]:
			#	state = min(2, state+1)
			
	
#			outp.write(str(date)+"\t"+foreweather[i]+"\t"+`predictfore[i]`+"\t"+`basevalue[(i+shift)%24]`+"\n")
			#print str(date-timedelta(hours=2))+"\t"+`hour_pre[i]`+"\t"+`tracked_pred`+"\t"+`forecast_pred`+"\t"+`pastprod[i]`+"\t"+foreweather[i]+"\t"+`factor[foretag[i]]`+"\t"+`basevalue[(i+shift)%24]`, prev_state
			#debug code start
			#fp.write(str(date)+"\t"+`hour_pre[i]`+"\t"+`predictfore[i]`+"\t"+foreweather[i]+"\t"+`pastprod[i]`+"\t"+`basevalue[(i+shift)%24]`+"\n")
			#debug code end
			
		
			if self.debugging:	
				self.debug[i].baseValue = basevalue[(i+shift)%24]
				self.debug[i].tag = foreweather[i].replace(" ", "_")
				self.debug[i].prediction = predictfore[i]
				self.debug[i].actual = pastprod[i]
				self.debug[i].actualCondition = self.actualConditions.getConditionString(date).replace(" ", "_")
			date += timedelta(hours=1)
#		outp.close()
		#fp.close()
		#if i>-1 and self.debugging:
		#	print str(date-timedelta(hours=3))+"\t"+`hour_pre[i]`+"\t"+`tracked_pred`+"\t"+`predictfore[i]`+"\t"+foreweather[i]+"\t"+`pastprod[i]`+"\t"+`basevalue[(i+shift)%24]`, prev_state
	
		if hours > len(predictfore):
			hours = len(predictfore) 
		#print len(pastprod[2:hours+2])#, hours
		return predictfore[self.shiftFactor:hours+self.shiftFactor], pastprod[self.shiftFactor:hours+self.shiftFactor]
	
#	def process(self,now, hours, path, threshold, energy=20, offset=15, past=False,debugging=False):
	def process(self,now, hours):
		#global past
		#Md: control predication usage
		#if now.month == 8 or now.month==5 or now.month==6 or now.month==3:
		#	past = 0
		#else:
		#	past = 1
		now = datetime(now.year,now.month,now.day,now.hour)
		if hours > 49 and not self.useActualData:
			print "Prediction is wanted for more than 49 hours but we can only return 49 hours."
			hours = 49
			
		if self.debugging:
			for i in range(hours+self.shiftFactor):
				try:
					d = self.debug[i]
				except IndexError:
					d =  Debug(self.scaling)
#					print len(self.debug),i
					self.debug.append(d)
				d.reset()
			
		#print past
		result, actual = self.predictday(now, hours)	
	
		#flag = False
	
		#if not os.path.isfile(path+"/pastpred.txt"):	
		#	flag = True
		#else:
		#	pastfile = open(path+"/pastpred.txt", 'r')
		#	lines = pastfile.readlines()
		#	
		#	if len(lines)==0 or now > datetime.strptime(lines[0].split("\t")[0],"%Y_%m_%d_%H_%M")+timedelta(hours=threshold):
		#
		#	else:
		#		d = datetime(now.year, now.month, now.day, now.hour)
		#		j = 0
		#		for i in range(len(lines)):
		#			date = datetime.strptime(lines[i].split("\t")[0],"%Y_%m_%d_%H_%M")
		#			if d==date: 
		#				if abs(result[j]-float(lines[i].split("\t")[1]))>energy:
		#					flag = True
		#					break
		#				j += 1
		#				d += timedelta(hours=1)
						
	
		#	pastfile.close()
	
		#pastfile = open(path+"/pastpred.txt", 'w')
		
		#now = datetime(now.year, now.month, now.day, now.hour)
		#for i in range(len(result)):
		#	pastfile.write((now).strftime("%Y_%m_%d_%H_%M")+"\t"+`result[i]`+"\n")
		#	now+=timedelta(hours=1)
	
		#pastfile.close()
		
		#print len(h),len(result), len(actual)
		return result, actual


class CachedEnergyPredictor(EnergyPredictor):
	
	
	def __init__(self,startDate,endDate=None,predictionHorizon=48,path='.',threshold=3, scalingFactor=1347, offset=15,energythreshold=20, useActualData=False,scalingBase=1347,debugging=False,error_exit=normal):
		global hourFormat
		super(CachedEnergyPredictor,self).__init__(path, threshold, scalingFactor, offset, energythreshold, useActualData, scalingBase, debugging, error_exit)
		
		
		self.startDate = datetime(startDate.year,startDate.month,startDate.day,startDate.hour)
		self.predictionHorizon = predictionHorizon
		
		
#		EnergyPredictor.__init__(self, path, threshold, scalingFactor, offset, energythreshold, useActualData, scalingBase, debugging)
		if not endDate:
			self.endDate = (startDate+timedelta(days=5)-timedelta(hours=9))
		else:
			self.endDate = datetime(endDate.year,endDate.month,endDate.day,endDate.hour)
			
			if endDate.minute>0:
				self.endDate += timedelta(hours=1)
			
		
		self.cachedPredictionDir = os.path.join(self.datapath,"cachedPredictions")
		if not os.path.exists(self.cachedPredictionDir):
			os.makedirs(self.cachedPredictionDir)
		
		##let's populate the predictions
		currentDate = self.startDate
		
		self.predictions = []
	
        	self.otherPredictions = {}
    	
        	tdelta = timedelta(hours=1)
        	while currentDate < self.endDate:
			fname = os.path.join(self.cachedPredictionDir,"%s_%d"%(currentDate.strftime(hourFormat),self.predictionHorizon))

			if os.path.exists(fname):
				fd = open(fname,'r')
				(greenAvail, flag) = pickle.load(fd)
			else:               
				greenAvail, flag = super(CachedEnergyPredictor,self).getGreenAvailability(currentDate, self.predictionHorizon)
				fd = open(fname,'w')
				pickle.dump((greenAvail, flag), fd)

			fd.close()				
			self.predictions.append((greenAvail, flag))
            
			currentDate+=tdelta
		

	def lookUpInOthers(self,now,horizon):
		
		
		if self.otherPredictions.has_key(horizon):
			table = self.otherPredictions[horizon]
		else:
			table = {}
			self.otherPredictions[horizon] = table
		
		#now look up in the table
		
		if table.has_key(now):
			retval,flag = table[now]
		else:
			retval, flag = super(CachedEnergyPredictor,self).getGreenAvailability(now, horizon)
			table[now] = (retval,flag)
		return retval,flag

	def getGreenAvailability(self, now, hours):
		currentHour = datetime(now.year,now.month,now.day,now.hour)
		if hours == self.predictionHorizon:
			tdelta = currentHour - self.startDate
#			numSeconds = tdelta.total_seconds()		
			numSeconds = tdelta.days * 24*3600 + tdelta.seconds		
			index = numSeconds/3600
			
			try:
				retval,flag = self.predictions[index]
			except IndexError:
				#look in to other
				retval,flag = self.lookUpInOthers(currentHour,hours)
		else:
			retval,flag = self.lookUpInOthers(currentHour,hours)

		if not currentHour == now:
                        #not the beginning of hour
                        retval[0] = self.scaling * self.energyProduction.getProduction(currentHour)
		
		return retval,flag

if __name__ == '__main__':

	
	#now = datetime.now()
	now = datetime(2010, 7, 14, 23, 0,0)
	p = EnergyPredictor(threshold=3,energythreshold=20,offset=15)
	flag,retval = p.process(now, 48)
	print retval, len(retval)
	#start = datetime(2010,06,16)	
	#for i in range(14):
	#	tag = []
	#	weather = []
	#	data = []
		#predictday(start)
		#calctag()
	#	start += timedelta(days=1)
	
	#	if fnmatch.fnmatch(datafile,"*.txt"):
	#		inp = open(datafile,"r")
	#		for line in inp.readlines():
				#print line.split("\t")[5], line.split("\t")[15]
	#			tag.append(int(line.split("\t")[5]))
	#			weather.append(line.split("\t")[6])
	#			data.append(float(line.split("\t")[15]))

	#print data, tag
	#calctag()
	
