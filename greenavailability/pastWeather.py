#!/usr/bin/python

#import os,sys,time
from datetime import datetime,timedelta
import os,re,sys,glob,os.path,time,string
import weatherPrediction
import tempfile,subprocess
#from datetime import datetime,timedelta


locations = weatherPrediction.locations

zipcodes = weatherPrediction.zipcodes


timezones = weatherPrediction.timezones

conditions = weatherPrediction.conditions

format = '%b_%d_%Y_%H_%M'
outputFormat = '%Y_%m_%d_%H'





currentDate = None
dayFormat = "%b_%d_%Y"


class WeatherCondition:
    def __init__(self,conditionString,conditionGroup):
        self.conditionString = conditionString
        self.conditionGroup = conditionGroup
        
    def __str__(self):
        return "%s\t%s"%(self.conditionString,str(self.conditionGroup))
    
class TemperatureRecord:
    

    
    def __init__(self,time,filename,line):
        self.time = time
        self.filename = filename
        self.line     = line
        
    def setAttribute(self,attribute,value):
        if not hasattr(self, attribute):
            setattr(self, attribute, value)
        
    def setCondition(self,cond):
	if not hasattr(self, 'condition'):
		self.condition=cond

    def setTemp(self, temp):
	if not hasattr(self, 'temp'):
	        self.temp = temp
    def setFeelLike(self, feelLike):
	if not hasattr(self, 'feelLike'):
	        self.feelLike = feelLike
    def setHumidity(self,hum):
	if not hasattr(self, 'humidity'):
	        self.humidity = hum
    def setPrecipation(self,pre):
	if hasattr(self, 'precipitation'):
		print self.precipitation
	if not hasattr(self, 'precipitation'):
	        self.precipation = pre
        
    def setWind(self,direction,speed):
	if not hasattr(self, 'direction'):
	        self.direction=direction
        	self.speed = speed
		#print str(self)
#		fout.write(str(self)+"\n")
        
    def check(self):
        try:
            a = self.direction
            a = self.speed
            a = self.condition
            a = self.feelLike
            a = self.temp
            a = self.time
        except AttributeError:
            print "bad time",self.time,self.filename
            raise
        
        
    def __str__(self):
        
        outFormat = "%Y_%m_%d_%H"#"%A_%H"
        try:
            return "%s\t%d\t%d\t%d\t%s\t%d\t%s\t%d"%(self.time.strftime(outFormat),self.temp,self.feelLike,self.humidity,self.condition,self.precipation,self.direction,self.speed)
        except AttributeError:
            print "bad",self.time,self.filename,self.line
            raise
            
            
    def getTime(self):
        return time.mktime(self.time.timetuple())
class OneDayRecords:
    
    def __init__(self):
        self.map = {}
#    def __init__(self,day):
#        self.day = day
#        self.init()
        
    def append(self,record):
        if not self.map.has_key(record.time.hour):
            self.map[record.time.hour] = record
    def getRecords(self):
	#print self.map.values()
        return self.map.values()

def roundToNereastHour(time):
	retval = datetime(time.year,time.month,time.day,time.hour)
	if time.minute>30:
		retval+=timedelta(hours=1)

	return retval

def getTimeDelta(t1,t2):
	if t1>t2:
		return t1-t2
	else:
		return t2-t1


def parseStore(htmlFile,day,timeToRecord={}):
#    pass
#
#def parseStore(htmlFile, d):
	
    
    retval = {}
    hourToTsp = {}
	
    fd = open(htmlFile,'r')
    
#    print htmlFile
#    ftemp = tempfile.TemporaryFile()#open("tempdata.txt",'w')
	# 2010-06-05 22:04:00
    format = "%I:%M %p"
	
    timeRe = re.compile('<td valign="middle" class="inDentA".*<b>(.*)</b></font></td>')
    tempRe = re.compile('<td valign="middle" align="left".*>(.+)<b>(-?\d+)&deg;F</b></td>') 
    feltRe = re.compile('<td align="center" valign="middle" class="blueFont10" bgcolor="#.+"><b>(\-*\d+)&deg;F</b></td>') 
    dewpRe = re.compile('<td align="center" valign="middle" class="blueFont10" bgcolor="#.+">(\-*\d+)&deg;F</td>') #<td align="center" valign="middle" class="blueFont10" bgcolor="#f1f4f5">30&deg;F</td>
    humiRe = re.compile('<td align="center" valign="middle" class="blueFont10" bgcolor="#.+">(\d+)%</td>') #<td align="center" valign="middle" class="blueFont10" bgcolor="#f1f4f5">92%</td>
    visiRe = re.compile('<td align="center" valign="middle" class="blueFont10" bgcolor="#.+">(\d+\.\d+)<BR>miles</td>') #<td align="center" valign="middle" class="blueFont10" bgcolor="#f1f4f5">9.0<BR>miles</td>
    presRe = re.compile('<td align="center" valign="middle" class="blueFont10" bgcolor="#.+">(\d+\.\d+)$') #<td align="center" valign="middle" class="blueFont10" bgcolor="#f1f4f5">30.01
    windRe = re.compile('<td align="center" valign="middle" class="blueFont10" bgcolor="#.+">(.+)</td>') #<td align="center" valign="middle" class="blueFont10" bgcolor="#f1f4f5">CALM</td>
	
    nextHour=0
    dayJump=False
#	arrayDate = []
#	arrayOut = []


    
    currentRecord = None

    discardRecord =  False
    
    for line in fd:
        line = line.strip()
		
        if not line:
			continue
		
		#print line
		# Get time
        r = timeRe.search(line)

        if r:
            repeat = False
			#print r.group(0)
            tsp = r.group(1)
            t = datetime.strptime(tsp,format)
            t = datetime(day.year,day.month,day.day,t.hour,t.minute)

			
            nearestHour = roundToNereastHour(t)
            currentRecord = TemperatureRecord(t, htmlFile, line)
            if timeToRecord.has_key(nearestHour):
                
                r = timeToRecord[nearestHour]
                deltaPrev = getTimeDelta(r.time,nearestHour)
                
                deltaNow  = getTimeDelta(t,nearestHour)
                
                if deltaNow<deltaPrev:
                    timeToRecord[nearestHour] = currentRecord
                    discardRecord = False
                else:
                    discardRecord = True
                
            else:
                timeToRecord[nearestHour] = currentRecord
                discardRecord = False
            			
            
#            try:
#				if prevNearestHour == nearestHour:
#					repeat = True
#            except:
#				None
#						
#            try:
#				prevRecord=currentRecord
#            except:
#				prevOut=None
			
#			out = str(nearestHour.year)+"\t"+str(nearestHour.month)+"\t"+str(nearestHour.day)+"\t"+str(nearestHour.hour)+"\t"+str(nearestHour)
			
#            if seconds<deltaNow.seconds:
#				continue
            continue

		# Get condition and temperature
		#<td valign="middle" align="left" class="blueFont10">Light Rain and Freezing Rain <b>32&deg;F</b></td>
        
        if discardRecord:
            continue
    
        r = tempRe.search(line)
        if(r):
			#print r.group(0)
            cond = r.group(1)
            temp = r.group(2)
			
            cond=cond.strip().lower()
            condOrig=cond
			
            if cond in conditions:				
                cond = conditions[cond]
				#print cond
			
#			out+="\t"+str(cond)+"\t"+str(condOrig)+"\t"+str(temp)
            c = WeatherCondition(condOrig,cond)
            currentRecord.setTemp(temp.strip())
            currentRecord.setCondition(c)

			#print out
			
            prevNearestHour = nearestHour
			
            continue
		
		#Felt like
		#<td align="center" valign="middle" class="blueFont10" bgcolor="#f1f4f5"><b>32&deg;F</b></td>
	    
        
        r = feltRe.search(line)
        
        if(r):
#			out+="\t"+r.group(1)
            currentRecord.setFeelLike(r.group(1).strip())
            continue
		
		#Dew Point
		#<td align="center" valign="middle" class="blueFont10" bgcolor="#f1f4f5">30&deg;F</td>
        r = dewpRe.search(line)
        if(r):
#			out+="\t"+r.group(1)
            currentRecord.setAttribute("Dew", r.group(1).strip())
            continue
		
		#Humidity
		#<td align="center" valign="middle" class="blueFont10" bgcolor="#f1f4f5">92%</td>
        r = humiRe.search(line)
        if(r):
#			out+="\t"+r.group(1)
            currentRecord.setHumidity(r.group(1).strip())
#            currentRecord.setAttribute("Humidity", r.group(1).strip())
            continue
		
		#Visibility
                #<td align="center" valign="middle" class="blueFont10" bgcolor="#f1f4f5">9.0<BR>miles</td>
        r = visiRe.search(line)
        if(r):
#			out+="\t"+r.group(1)
            currentRecord.setAttribute("Visibility", r.group(1))
            continue
		
		#Pressure
		#<td align="center" valign="middle" class="blueFont10" bgcolor="#f1f4f5">30.01
		#<IMG SRC="http://image.weather.com/web/common/icons/steady_pressure.gif?20061207" WIDTH="5" HEIGHT="8" BORDER="0" ALT="steady">
        r = presRe.search(line)
        if(r):
#			out+="\t"+r.group(1)
            currentRecord.setAttribute("Pressure", r.group(1))
            continue
                
		#Wind
		#<td align="center" valign="middle" class="blueFont10" bgcolor="#f1f4f5">CALM</td>
        r = windRe.search(line)
        if(r):
            wind = r.group(1)
            if wind == 'CALM':
                wind = 0
            else:
		      
                index1 = string.index(wind, '<BR>')
                index2 = string.index(wind, 'mph')
                wind = wind[index1+4:index2]
			
            currentRecord.setWind("DIRECTION", wind)
#			out+="\t"+str(wind)
			#if not repeat:
				#print out
			
			# Print things out
			#print str(nextHour)+">"+str(nearestHour.hour)+"--------"+out
			#if not repeat:
				#if nextHour==nearestHour.hour:
					#print str(nextHour-nearestHour.hour)+" - "+str(nextHour)+" - "+str(nearestHour.hour)+"\t"+out
				#else:
					#while nextHour<nearestHour.hour and (nextHour-nearestHour.hour)>0:
						#print str(nextHour-nearestHour.hour)+" - "+str(nextHour)+" - "+str(nearestHour.hour)+"\t"+out+" added"
						#nextHour = (nextHour+1)%24
					#print str(nextHour-nearestHour.hour)+" - "+str(nextHour)+" - "+str(nearestHour.hour)+"\t"+out+" after adding"
				#nextHour = (nextHour+1)%24
			
            if nearestHour.hour>2:
				dayJump=True
			
			#check = nextHour-nearestHour.hour
			#done=False
			#if check == 0:
				##print str(nextHour-nearestHour.hour)+" - "+str(nextHour)+" - "+str(nearestHour.hour)+"\t"+out
				#print out
				#done=True
				
			#if check<0 and check>-10:
				#nextHour=nextHour+1
				##print str(nextHour-nearestHour.hour)+" - "+str(nextHour)+" - "+str(nearestHour.hour)+"\t"+prevOut
				##print str(nextHour-nearestHour.hour)+" - "+str(nextHour)+" - "+str(nearestHour.hour)+"\t"+out
				#print prevOut
				#print out
				#done=True
			
			## Day jump
			#if check<=-10:
				#if not dayJump:
					##print str(nextHour-nearestHour.hour)+" - "+str(nextHour)+" - "+str(nearestHour.hour)+"\t"+out+" extrange"
					#print out
					#dayJump=True
				#nextHour=nextHour-1
				#done=True
			
			#if check>0:
				##print str(nextHour-nearestHour.hour)+" - "+str(nextHour)+" - "+str(nearestHour.hour)+"\t"+prevOut+" skipped"
				#nextHour=nextHour-1
				#done=True
			
			#if not done:
				##print str(nextHour-nearestHour.hour)+" - "+str(nextHour)+" - "+str(nearestHour.hour)+"\t"+out+" other situation"
				#print out+" other situation"
				
			#nextHour = (nextHour+1)%24
			
			#print out
#			arrayOut.append(out)
#			arrayDate.append(nearestHour)
			
            continue
	
    fd.close()
    
    
    return timeToRecord
    
#    if len(arrayDate)==0:
#	   return retval
#
#    arrayDate2 = []
#    arrayOut2 = []
#	# Remove dupe
#
#	#some times the first enrty may contain 12am of next day
#	#target = datetime(arrayDate[1].year,arrayDate[1].month,arrayDate[1].day,0)
#
#	#print arrayDate
#
#    i = 0
#    while arrayDate[i].hour<>0 or arrayDate[i].day<>arrayDate[i+1].day:
#		arrayDate.pop(i)
#		arrayOut.pop(i)
#
#	#print arrayDate
#	
#    for i in range(0,len(arrayDate)):
#		#print i, arrayDate[i], arrayDate[i]-timedelta(hours=1), arrayDate[i-1]
#		if i==0:
#			#parts = arrayDate[0].split("\t")
#			#hour = arrayDate[0].hour#int(parts[3])
#			#if hour==23:
#			#	arrayDate2.append(arrayDate[1])
#			#	arrayOut2.append(arrayOut[1])
#			#else:
#			arrayDate2.append(arrayDate[i])
#			arrayOut2.append(arrayOut[i])
#		elif (arrayDate[i]-timedelta(hours=1)) == arrayDate[i-1]:
#			arrayDate2.append(arrayDate[i])
#			arrayOut2.append(arrayOut[i])
#		#if (arrayDate[i]-timedelta(hours=23))==target or (arrayDate[i]-timeDelta[hours=1])
#
#    i=1
#    iniHour = arrayDate2[0]
#    iniHour = datetime(iniHour.year,iniHour.month,iniHour.day,0)
#    #print arrayOut2[0]
#    ftemp.write(arrayOut2[0]+"\n")
#    for h in range(1,24):
#		#print h, i, len(arrayDate2)
#		if (i<len(arrayDate2)) and arrayDate2[i].day == iniHour.day:
#			#print arrayOut2[i]
#			ftemp.write(arrayOut2[i]+"\n")
#		else:
#			i = i+1
#			if (i<len(arrayDate2)):
#				#print arrayOut2[i]
#				ftemp.write(arrayOut2[i]+"\n")
#			
#		if (i<len(arrayDate2)) and arrayDate2[i]==(iniHour+timedelta(hours=h)):
#			i = i+1
#
#	#ftemp.close()
#
#    ftemp.seek(0)# = open("tempdata.txt",'r')
#    for line in ftemp:
#		#print line.split("\t")
#		str1 = line.split("\t")[0]+"_"+line.split("\t")[1]+"_"+line.split("\t")[2]+"_"+line.split("\t")[3]
#		str1 += "\t"+line.split("\t")[7]+"\t"+line.split("\t")[8]+"\t"+line.split("\t")[10]+"\t"+line.split("\t")[6]
#		str1 += "\t"+"10"+"\t"+"NNW"+"\t"+line.split("\t")[13]
#		fout.write(str1)
#    ftemp.close()
    
	# Let's double check to see if we have all the required hour
	#startTime = datetime(day.year,day.month,day.day)
	#oneHour = timedelta(hours=1)
	#endTime = startTime + timedelta(days=1)

	#while(startTime<endTime):
		#if not retval.has_key(startTime):
			#print location,startTime
		#startTime+=oneHour
    return retval

def fromDirToTime(x):        
    return int(time.mktime(time.strptime(os.path.basename(x), getWeatherPrediction.format)))

def execCmd(cmd):
    try:
        retcode = subprocess.call(cmd, shell=True)
        if retcode < 0:
            print >>sys.stderr,"cmd return code",retcode,cmd 
            print >>sys.stderr, "Child was terminated by signal", -retcode
            raise()
    except OSError, e:
        print >>sys.stderr,"cmd failed",cmd
        print >>sys.stderr, "Execution failed:", e
	raise()
def process(date, num_hours=24, path="."):
    global outputFormat
	#global fout
	#global d
#	fout = tempfile.TemporaryFile()#= open("full_fore.txt","w")

    loc = 'nj'
    url = "http://www.weather.com/weather/pastweather/hourly/"
    d = date
    if d.minute>0:
        d = datetime(d.year,d.month,d.day,d.hour)
		#d += timedelta(hours=1)

    date = d
#    date = d - timedelta(hours=2)
    d = datetime(date.year,date.month,date.day)#date
	#print date
#    num_hours += 2
	
#    print "process",date,num_hours,d

    timeToRecords = {}
    
    while d < date+timedelta(hours=num_hours+1):
        url = "http://www.weather.com/weather/pastweather/hourly/"
        url += "%s?when=%s&stn=0"%(zipcodes[loc],d.strftime("%m%d%y"))
        htmlFile = path+"/htmlarchive/%s_%d_%d_%d.html"%(loc,d.year,d.month,d.day)
        #print htmlFile
		# Download if they cannot be found
        if not os.path.isfile(htmlFile):
            #print url
            cmd = 'wget -O %s -o /dev/null "%s"'%(htmlFile,url)
            execCmd(cmd)
        else:
            if os.path.getsize(htmlFile)==0:
                cmd = 'wget -O %s -o /dev/null "%s"'%(htmlFile,url)
                execCmd(cmd)			
	
        parseStore(htmlFile,d,timeToRecords)
		#print d
        d += timedelta(days=1)
		
    
#        keys = timeToRecords.keys()
#        keys.sort()
#        
#        print keys
#        print "++++++++++++++++++++++++++++"
	#fout.close()

#	finp = fout#open("full_fore.txt","r")
#	finp.seek(0)
#	fout = tempfile.TemporaryFile()#open("fore.txt","w")
    dates = timeToRecords.keys()
    dates.sort()
    
    
#    shift = date.hour
#    i = 0
    
#    print shift
    retval = {}
    
    
    lastDate = None
    
    tdelta = timedelta(hours=1)
    
    for d in dates:
#        if shift>0:
#			shift -= 1
#			continue
		#print line.split("\t")
        r = timeToRecords[d]
        
        if conditions.has_key(r.condition.conditionString):
			tagint = conditions[r.condition.conditionString]
        else:
			print "key missing", r.condition.conditionString
			tagint = conditions[weatherPrediction.long_substr(r.condition.conditionString)]
            
#        str1 = "%s\t%s\t%d"%(d.strftime(outputFormat),r.condition.conditionString,tagint)
        r.condition.conditionGroup = tagint
        retval[d] = r.condition
        
        if lastDate:
            lastDate=lastDate+tdelta
            while lastDate<d:
                retval[lastDate] = WeatherCondition("Unkown", -1)
                lastDate=lastDate+tdelta
        lastDate = d
        
#		fout.write(str1+"\n")
#        i += 1
#        if i==num_hours:
#            break
		
#	finp.close()
#	#fout.close()
#	fout.seek(0)
#	
#	return fout
    return retval

	

if __name__ == '__main__':

    print "past weather"
    
    now = datetime(2010, 8, 23, 9, 0,0)
    
    retval = process(now)
    
    keys = retval.keys()
    
    keys.sort()
    for k in keys:
        print k.strftime(outputFormat),retval[k]
    
#    for line in fd:
#        print line.strip()
#    fd.close()
