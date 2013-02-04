#!/usr/bin/python

import os, fnmatch, tempfile, sys,os.path,pickle
#import singledayweather
import pastWeather, get_real_data
#import datetime
from datetime import timedelta, datetime


princetonDataStart = datetime(year=2010,month=5,day=28)
princetonDataEnd = datetime(year=2010,month=10,day=4)
pisDataStart = datetime(year=2010,month=12,day=14)
pisDataEnds = datetime.now()

#error entering and exiting method
enter_on_error = 2  #kept for backward compatibility

#states
normal_state = 0
track_state = 1

MAX_HOURS = 49

int_date_format = "%Y_%m_%d_%H"

class PredictionProvider:
	def __init__(self,dataPath,shiftFactor):
		self.dataPath = dataPath
		self.shiftFactor = shiftFactor
		
	def getPredictions(self,date,hours):
		filepath = os.path.join(path,"proc_forecast",date.strftime(int_date_format))
		if not os.path.isfile(filepath):
			print filepath," forecast missing"
			raise Exception()
			sys.exit(1)

		date = []
		cloud_cov = []
		actual = []
		fh = open(os.path.join(self.datapath,filepath),'r')
		for line in fh:
			hour = int(list1[1])
			sky_cond = float(int(list1[5]))/100.0
			rain_pr = float(int(list1[4]))/100.0	
		fh.close()
		
		return date,cloud_cov,actual

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
		alldata = get_real_data.process(date, MAX_HOURS)		
		tdelta = timedelta(hours=1)
		d = date
		for i in range(len(alldata)):
			if not self.timeToEnergy.has_key(d):
				self.timeToEnergy[d] = alldata[i]
			d = d+tdelta
		return
		
	
	def populateAllEnergy(self,date, num_hours):
		alldata = get_real_data.process(date, num_hours)		
		tdelta = timedelta(hours=1)
		d = date
		for i in range(len(alldata)):
			self.timeToEnergy[d] = alldata[i]
			d = d+tdelta
		return
		
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
				self.populateAllEnergy(d, num_hours)
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


class EnergyPredictor(object):

	def __init__(self,path='.',threshold=3, scalingFactor=1347, offset=15, energythreshold=20, useActualData=False, scalingBase=1347,debugging=False,error_exit=enter_on_error):
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
		#self.shiftFactor = 2
		self.shiftFactor = 0
		self.actualConditions = ActualConditions(self.datapath)
		self.useActualData = useActualData
		self.error_exit = error_exit
		self.last_call_state = normal_state
		self.last_tag = 10    # 10 is an arbitrary number to start later we will change to be the last hour's cloud coverage
		
		if useActualData:
			self.weatherPredictor = self.actualConditions
#			print "using actual data"
		else:
			self.weatherPredictor = PredictionProvider(self.datapath,self.shiftFactor)
#			print "using predictions"
		self.lagerror = 1
		self.base = []
		self.basemonth = []

	def getGreenAvailability(self, now, hours):
		
		result, actual  = self.process(now, hours)
		result = map(lambda x:self.scaling * x, result)

		flag = False

		if len(self.pastresult)==0:
			flag = True
		elif self.pasttime + timedelta(hours=self.threshold) <= now:
			flag = True
		else:

			d = datetime(now.year,now.month,now.day,now.hour)
			lasttime = datetime(self.pasttime.year, self.pasttime.month, self.pasttime.day, self.pasttime.hour)
			j = 0
			for i in range(len(self.pastresult)):
				if d==lasttime+timedelta(hours=i):
					if j<len(result) and result[j]==-1.0:
						result[j]=self.pastresult[i]
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



		currentHour = datetime(now.year,now.month,now.day,now.hour)
 		if not currentHour == now:
 			result[0] = self.scaling * self.energyProduction.getProduction(currentHour)		
 	
		
		return result, flag

	def read_base(self,date):
		
		month = date.month
		if date.month==3:		#handle daylight savings time
			if (date.day)<13:
				month = month-1
			#if (date.day)>7 and (date.day)<14 and date.weekday()==6:
			#if (date.day)>14:
			#	month = month+1
		if date.month==11:		#handle daylight savings time
			if (date.day)<6:
				month = month-1
			#if (date.day)>7 and (date.day)<14 and date.weekday()==6:
			#if (date.day)>14:
			#	month = month+1
		if len(self.base)>0 and self.basemonth==month:
			return self.base[0:24],	self.base[24]	
		str1 = self.datapath+"/base/%d.txt"%(month)	
		if not os.path.isfile(str1):
			print "base file missing "+str1
			sys.exit(1)
		fd = open(str1,'r')
		basevalues = [0]*24
		basetotal = 0
		for line in fd:
			list1 = line.strip().split("\t")
			hour = int(list1[0])
			basevalues[hour] = float(list1[1].strip())
			basetotal += basevalues[hour]
		fd.close()
		self.base = basevalues
		self.base.append(basetotal)
		self.basemonth = month
		#print basevalues
		return basevalues,basetotal

	def read_cache(self,filename,call_date=0):
		if not os.path.isfile(filename):
			print "result file missing "+ filename

		fd = open(filename,'r')
		result = []
		actual = []
		i = 1
		for line in fd:
			list1 = line.strip().split("\t")
			hour_diff = int(list1[0])
			act_prod = float(list1[5])
			pred = float(list1[6])
			if i<hour_diff:
				while i<>hour_diff:
					result.append(-1.0) #-1 is denoting unknown value
					actual.append(-1.0)					
					i +=1
			if i==hour_diff:
				result.append(pred)
				actual.append(act_prod)				
					
				
		fd.close()
		return result, actual

	def predict(self, list1, base, call_date=datetime(2011,03,03), fd=0, state_cc=0, state_new=0):
		retval1 = False
	
		hour = int(list1[1])
		sky_cond = float(int(list1[5]))/100.0
		rain_pr = float(int(list1[4]))/100.0
		#print base, hour, list1[0]
		pred1 = base[hour]*(1-sky_cond)
		hour_diff_td = datetime.strptime(list1[0].strip(),"%Y_%m_%d_%H_%M")-call_date
		hour_diff = hour_diff_td.days*24+hour_diff_td.seconds/3600

		pred_last = base[hour]*self.last_tag
		actual = float(list1[7])
		diff1 = abs(pred1-actual)	
		diff_last = abs(pred_last-actual)
		
		pred1_out = pred1

		if state_cc==track_state:
			pred1_out = pred_last
	
		if fd<>0:
			print >>fd, hour_diff,"\t",list1[0],"\t",list1[5],"\t",list1[4],"\t",base[hour],"\t",actual,"\t",pred1_out

		
		if (diff1>diff_last):
			retval1 = True

		return retval1, hour_diff, base[hour]

	def predict_long(self,base, call_date, fr, fw):
		
		state1 = self.last_call_state

		for line in fr:
			list1 = line.strip().split("\t")
			#print fw," fw"
			flag, hour_diff, spec_base = self.predict(list1, base, call_date, fw, state1)
			
			if spec_base>100.0: 
				if hour_diff<2:
					if flag:
						self.last_call_state = track_state
					else:
						self.last_call_state = normal_state					
			else:
				state1 = normal_state
	
		return

	def predictday(self,call_date, hours):
		result = []
		actual = []

		date = call_date-timedelta(hours=1)
		#1. get base
		base, basetotal = self.read_base(date)
		#2. get forecast
		forepath = self.datapath+"/intelli_forecast/%d_%0*d_%0*d_%0*d"%(date.year,2,date.month,2,date.day,2,date.hour)
		#3. predict
		if not os.path.isfile(forepath):
			print "Forecast missing "+ forepath
			return result, actual
		resultpath = self.datapath+"/cached_result/%d_%0*d_%0*d_%0*d"%(date.year,2,date.month,2,date.day,2,date.hour)
		#if os.path.isfile(resultpath):
		#	result, actual = self.read_cache(resultpath)
		fh = open(forepath,'r')
		if not os.path.isdir(self.datapath+"/cached_result"):
			os.mkdir(self.datapath+"/cached_result")
		fw = open(resultpath,'w')
		#print base
		self.predict_long(base,date,fh,fw)
		result, actual = self.read_cache(resultpath)
		fh.close()
		fw.close()
		#return value
		return result, actual

	def process(self,now, hours):
		now = datetime(now.year,now.month,now.day,now.hour)
		if hours > 49 and not self.useActualData:
			print "Prediction is wanted for more than 49 hours but we can only return 49 hours."
			hours = 49
			
		#if self.debugging:
		#	for i in range(hours+self.shiftFactor):
		#		try:
		#			d = self.debug[i]
		#		except IndexError:
		#			d =  Debug(self.scaling)
		#			self.debug.append(d)
		#		d.reset()
			
		#print past
		result, actual = self.predictday(now, hours)	
	
		return result, actual

if __name__ == '__main__':

	now = datetime(2011, 3, 15, 0, 0,0)
	p = EnergyPredictor(threshold=3,energythreshold=20,offset=15)
	pred, actual = p.process(now, 48)
	print len(pred), pred
