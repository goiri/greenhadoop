#!/usr/bin/python

#import os,sys,time
from datetime import datetime,timedelta, date
import os,re,sys,glob,os.path,time,getopt, string
import tempfile
#import getWeatherPrediction
#from datetime import datetime,timedelta


locations = ["nj", "nyc", "Boston", "atlantic", "phily", "DC", "Pittsburgh", "Charlotte", "Orlando", "Miami", "Cleveland", "Atlanta", "Indianapolis", "Nashville", "Mobile", "Chicago", "St_louis", "Memphis", "New_Orleans", "Minneapolis", "Kansas_city", "Grand_forks", "Lincoln", "Dallas", "Houston", "Austin", "Glasgow", "Casper", "Denver", "Salt_lake_city", "Spokane", "Seattle", "Las_vegas", "la", "sf"]

zipcodes = {'nj':'08854', 'nyc':'10010', 'Boston':'02120', 'atlantic':'08401', 'phily':'10110', 'DC':'20010', 'Pittsburgh':'15210', 'Charlotte':'28210', 'Orlando':'32820', 'Miami':'33010', 'Cleveland':'61241', 'Atlanta':'30310', 'Indianapolis':'46220', 'Nashville':'37210', 'Mobile':'36610', 'Chicago':'60610', 'St_louis':'63110', 'Memphis':'38120', 'New_Orleans':'70130', 'Minneapolis':'55410', 'Kansas_city':'66101', 'Grand_forks':'58201', 'Lincoln':'68510', 'Dallas':'75210', 'Houston':'77010', 'Austin':'78710', 'Glasgow':'549230', 'Casper':'82630', 'Denver':'80210', 'Salt_lake_city':'84120', 'Spokane':'99201', 'Seattle':'98110', 'Las_vegas':'89110', 'la':'90010', 'sf':'94110'}
#locations = ["nj"]
#locations = ["epfl","wa","nj"]

#urls = {
 #   'nj':'http://www.weather.com/weather/hourbyhour/08854',
#}


#timezones = {
 #   'nj':'US/Eastern',
#}

timezones = {
    'nj':'US/Eastern', 'nyc':'US/Eastern', 'Boston':'US/Eastern', 'atlantic':'US/Eastern', 'phily':'US/Eastern', 'DC':'US/Eastern', 'Pittsburgh':'US/Eastern', 'Charlotte':'US/Eastern', 'Orlando':'US/Eastern', 'Miami':'US/Eastern', 'Cleveland':'US/Eastern', 'Atlanta':'US/Eastern', 'Indianapolis':'US/Eastern', 'Nashville':'US/Central', 'Mobile':'US/Central', 'Chicago':'US/Central', 'St_louis':'US/Central', 'Memphis':'US/Central', 'New_Orleans':'US/Central', 'Minneapolis':'US/Central', 'Kansas_city':'US/Central', 'Grand_forks':'US/Central', 'Lincoln':'US/Central', 'Dallas':'US/Central', 'Houston':'US/Central', 'Austin':'US/Central', 'Glasgow':'US/Mountain', 'Casper':'US/Mountain', 'Denver':'US/Mountain', 'Salt_lake_city':'US/Mountain', 'Spokane':'US/Pacific', 'Seattle':'US/Pacific', 'Las_vegas':'US/Pacific', 'la':'US/Pacific', 'sf':'US/Pacific'
}

weekdays={
	'Monday':0,
	'Tuesday':1,
	'Wednesday':2,
	'Thursday':3,
	'Friday':4,
	'Saturday':5,
	'Sunday':6
	}

conditions = {
	'mostly sunny': 2,
	'mostly sunny / wind': 2,
	'mostly sunny/wind': 2,
	'sunny': 1,
	'sunny / wind': 1,
	'sunny/wind': 1,
	'sunny and windy': 1,
	'clear': 1,
	'clear / wind': 1,
	'clear/wind': 1,
	'mostly clear': 2,
	'mostly clear / wind': 2,
	'mostly clear/wind': 2,
	'fair': 3,
	'fair and windy': 3,
	'partly cloudy': 4,
	'partly cloudy / wind': 4,
	'partly cloudy and windy': 4,
	'partly cloudy/wind': 4,
	'flurries': 5,
	'flurries / wind': 5,
	'flurries/wind': 5,
	'mostly cloudy': 6,
	'mostly cloudy / wind': 6,
	'mostly cloudy and windy': 6,
	'mostly cloudy/wind': 6,
	'few showers': 7,
	'light freezing rain': 7,
	'light freezing rain/sleet': 7,
	'light rain': 7,
	'light rain and fog': 7,
	'light rain and freezing rain': 7,
	'light rain and windy': 7,
	'light rain with thunder': 7,
	'light rain/fog': 7,
	'light rain/wind': 7,
	'light rain / wind': 7,
	'light sleet': 7,
	'light wintry mix': 7,
	'rain / thunder / wind' : 7,
	'rain / thunder': 7,
	'rain / wind': 8,
	'rain/wind': 8,
	'rain': 8,
	'rain and freezing rain': 8,
	'rain shower': 8,
	'showers': 8,
	'wintry mix': 8,
	'few snow showers': 9,
	'few snow showers / wind': 9,
	'light snow': 9,
	'light snow / wind': 9,
	'light snow and fog': 9,
	'light snow and windy': 9,
	'light snow shower': 9,
	'rain and snow': 9,
	'rain / snow showers': 9,
	'cloudy': 10,
	'cloudy / wind': 10,
	'cloudy and windy': 10,
	'cloudy/wind': 10,
	'snow': 11,
	'snow / wind': 11,
	'snow and fog': 11,
	'snow shower': 11,
	'snow shower / wind': 11,
	'snow/blowing snow': 11,
	'snow/wind': 11,
	'drizzle and fog': 12,
	'fog': 12,
	'foggy': 12,
	'haze': 12,
	'heavy rain': 13,
	'heavy rain / wind': 13,
	'heavy t-storm': 7,
	'heavy t-storms': 7,
	'isolated t-storms': 7,
	'isolated t-storms / wind': 7,
	'scattered strong storms': 7,
	'scattered strong storms / wind': 7,
	'scattered t-storms': 7,	
	'scattered thunderstorms': 7,	
	'scattered t-storms / wind': 7,
	'squalls': 7,
	'squalls and windy': 7,
	'strong storms': 7,
	'strong storms / wind': 7,
	't-showers': 8,
	't-storm': 7,
	't-storms': 7,
	'thunder': 7,
	'thunder in the vicinity': 7,
	'blizzard': 13,
	'blowing snow and windy': 13,
	'heavy snow': 13,
	'heavy snow / wind': 13
	}

format = '%b_%d_%Y_%H_%M'





#currentDate = None
dayFormat = "%b_%d_%Y"
class TemperatureRecord:
    

    
    def __init__(self,time,filename,line):
        self.time = time
        self.filename = filename
	self.line     = line
        
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
        
    def setWind(self,direction,speed, fout):
	if not hasattr(self, 'direction'):
	        self.direction=direction
        	self.speed = speed
		#print str(self)
		fout.write(str(self)+"\n")
        
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

def getRecords(filename, fout, no_line=48):
    #global currentDate

    #no_line *= 4 #we decrease it later at some pattern, the patter occures 4 times per hour
    
    fd = open(filename,"r")
    
    hbhDateHeaderRe = re.compile("<div class=\"hbhDateHeader\">(.*?)</div>")
#<div class="hbhTDTime"><div>3 am</div></div>
    hbhTDTimeRe = re.compile("<div class=\"hbhTDTime\"><div>(\d+ .*?)</div></div>")


    hbhTDConditionRe = re.compile("<div class=\"hbhTDCondition\"><div><b>(-?\d+).*?</b><br>(.*?)</div></div>")
    hbhTDFeelsRe =re.compile("<div class=\"hbhTDFeels\"><div>(-?\d+).*?</div></div>")
    
#    <div class="hbhTDPrecip"><div>20%</div></div>
#    hbhTDPrecipRe = re.compile("<div class=\"hbhTDPrecip\"><div>(\d+?)%</div></div>")
    
    
    hbhTDHumidityRe=re.compile("<div class=\"hbhTDHumidity\"><div>(\d+)%</div></div>")
    
    hbhTDPrecipRe = re.compile('<div class="hbhTDPrecip"><div>(\d+)%</div></div>')
#    hbhTDConditionRe= re.compile('<div class="hbhTDConditionIcon"><div><img src="http://i.imwx.com/web/common/wxicons/45/gray/28.gif" alt="Mostly Cloudy" width="45" height="45" border="0"></div></div>' 

    hbhTDWindRe = re.compile('<div class="hbhTDWind"><div>From (.*?)<br> (\d+) mph</div></div>') 
    hbhTDWindRe2 = re.compile('<div class="hbhTDWind"><div>(.*?)</div></div>')
    
    currentRecord = None
    state = -1
    
    records = OneDayRecords()
    
    for line in fd:
        
        line = line.strip()
        
        if not line:
            continue
        result = hbhDateHeaderRe.search(line)
        if result:
            currentDate = result.group(1)
	    #print currentDate
            continue
        
        result = hbhTDTimeRe.search(line)
        if result:
            datetimeString = currentDate+" "+result.group(1)
	    #print filename, datetimeString
            timeTuple = time.strptime(datetimeString,"%A, %B %d %I %p")
	    #if state == -1:
		#if timeTuple.tm_hour == 0:
		#	state = 1
		#else:
		#	state =0

            currentTime = datetime(2011,timeTuple.tm_mon,timeTuple.tm_mday,timeTuple.tm_hour)
	    if state == -1:
		state = 0
		if timeTuple.tm_hour == 0:
		    currentTime += timedelta(days=1)
            #currentTime.year
            
            if not currentRecord==None:
                try:
                    currentRecord.check()
                except AttributeError:
                    print 'bad filename',filename
                    raise
            
            currentRecord = TemperatureRecord(currentTime,filename,line)
            records.append(currentRecord)
            continue
        
        if not currentRecord:
            continue
        
        result = hbhTDConditionRe.search(line)
        if result:
            temp = int(result.group(1))
            currentRecord.setTemp(temp)
            condition = result.group(2)
            currentRecord.setCondition(condition)
            continue
        
        result = hbhTDFeelsRe.search(line)
        if result:
            temp = int(result.group(1))
            currentRecord.setFeelLike(temp)
            continue
        
        result = hbhTDHumidityRe.search(line)
        if result:
            hum = int(result.group(1))
            currentRecord.setHumidity(hum)
            continue
        
        result = hbhTDPrecipRe.search(line)
        if result:
            precipation = int(result.group(1))
            currentRecord.setPrecipation(precipation)
            continue        
        result = hbhTDWindRe.search(line)
        if result:
            direction = result.group(1)            
            speed = int(result.group(2))
            #self.map.values()
            currentRecord.setWind(direction,speed, fout)
	    if no_line<=1:
		return
	    no_line -= 1
	    #print no_line," 1st", str(currentRecord)
            
            continue            
        result = hbhTDWindRe2.search(line)
        if result:
            direction = result.group(1)            
            speed = 0            
            currentRecord.setWind(direction,speed, fout)
	    if no_line<=1:
		return
	    no_line -= 1
	    #print no_line," 2nd", str(currentRecord)            
            continue                        
#        if not stripedLine:
#            continue
#        line = re.sub(r'\n','',line)
        
#        line = line.lower()
        
#        r = badRe.search(line)
#        
#        if r:
#            badLineFd.write(line)
#            continue

        
#        html+=stripedLine


    return records.getRecords()

def fromDirToTime(x):

        
    return int(time.mktime(time.strptime(os.path.basename(x), getWeatherPrediction.format)))

def getRecordsFromDir(dirname,location):
    files = glob.glob(os.path.join(dirname,location)+"*")
    files.sort()
    records = []
    
    for f in files:
        fd = open(f,'r')
        try:
            r = getRecords(fd,f)
            
            records.extend(r)
        except AttributeError:
            print "bad file",f
            raise
        
        fd.close()
        
    return records

def long_substr(allstr, newstr):
	maxkey = allstr[0]
	maxlen = 0
	if len(allstr) > 0 and len(newstr) > 0:
		#print len(allstr)
		for k in range(len(allstr)):
			#print k
			#print allstr[k]
		       	for i in range(len(allstr[k])):
            			for j in range(len(newstr)):
			                for l in range(maxlen,len(newstr)-j):
						#print l, maxlen
						if string.find(allstr[k][i:i+l],newstr[j:j+l])!=-1:
							maxlen = l
							maxkey = allstr[k]
			
			#break				
	#print maxlen	
	return maxkey

def convertdate(line, date, h):
	#print line
	list1 = line.split("\t")
	#dd = datetime(date.year,date.month,date.day,date.hour)+timedelta(hours=h)
	day = weekdays[list1[0].split("_")[0]]
	hour = int(list1[0].split("_")[1])
	sday = ((day+7)-date.weekday())%7
	dd = datetime(date.year,date.month,date.day,hour)+timedelta(days=sday)
	#print date.weekday(),day,sday
	str1 = datetime.strftime(dd,"%Y_%m_%d_%H")
	for j in range(1,len(list1)):
		str1 += "\t"+list1[j]

	#str1 += "\n"
	#print str1, date
	return str1

def conandretdate(line, date, h):
	#print line
	list1 = line.split("\t")
	#dd = datetime(date.year,date.month,date.day,date.hour)+timedelta(hours=h)
	day = weekdays[list1[0].split("_")[0]]
	hour = int(list1[0].split("_")[1])
	sday = ((day+7)-date.weekday())%7
	dd = datetime(date.year,date.month,date.day,hour)+timedelta(days=sday)
	#print date.weekday(),day,sday
	str1 = datetime.strftime(dd,"%Y_%m_%d_%H")
	for j in range(1,len(list1)):
		str1 += "\t"+list1[j]

	#str1 += "\n"
	#print str1
	return str1, dd


def readforecastpiscataway(fout, d, no_hours, path):
    
    d -= timedelta(hours=1)
    dd = datetime(d.year,d.month,d.day,d.hour,19)
    
    piscatapred = path+"/htmlcache/"+datetime.strftime(dd,"%b_%d_%Y_%H_%M") 
	#print piscatapred
    if os.path.isdir(piscatapred):
		#print piscatapred+"/nj.0"
		if os.path.isfile(piscatapred+"/nj.0"):
			#print piscatapred
			getRecords(piscatapred+"/nj.0", fout, 1)
    else:
		return 0
	
    dd += timedelta(hours=1)
    piscatapred = path+"/htmlcache/"+datetime.strftime(dd,"%b_%d_%Y_%H_%M") 
	#print piscatapred
    if os.path.isdir(piscatapred):
		if os.path.isfile(piscatapred+"/nj.0"):
			getRecords(piscatapred+"/nj.0", fout, 1)
    else:
		return 0

    dd += timedelta(hours=1)
    piscatapred = path+"/htmlcache/"+datetime.strftime(dd,"%b_%d_%Y_%H_%M") 
	#print piscatapred
    if os.path.isdir(piscatapred):
		if os.path.isfile(piscatapred+"/nj.0"):
			getRecords(piscatapred+"/nj.0", fout, 1)
    else:
		return 0

    rest_hour = no_hours-1	
    dd += timedelta(hours=1)
		
	
    piscatapred = path+"/htmlcache/"+datetime.strftime(dd,"%b_%d_%Y_%H_%M") 
	#print piscatapred
    if os.path.isdir(piscatapred):
		for i in range(0,4):
			#print piscatapred+"/nj.%d"%i
			if os.path.isfile(piscatapred+"/nj.%d"%i):
				#getRecords(piscatapred+"/nj.%d"%i, fout, 12)
				getRecords(piscatapred+"/nj.%d"%i, fout)
				rest_hour -= 12
    else:
		return 0

    if rest_hour > 0:
        print "We dont have more than 48 hours of prediction"
        raise Exception()
        sys.exit(1)

	return 1
		

def readforecastprinceton(fout, d, hours, path):
	d -= timedelta(hours=1)
	princepred = path+"/htmlcache/"+datetime.strftime(d,"%b_%d_%Y")
	princepred += "/%d"%d.hour
	if os.path.isfile(princepred):
		f1 = open(princepred,'r')
		lines = f1.readlines()
		fout.write(convertdate(lines[0],d,1))
		#fout.write(lines[1])
	else:
		#print "Prediction file missing ", princepred
		#sys.exit(1)	
		return 0 #files are not there
	f1.close()
	
	d += timedelta(hours=1)
	princepred = path+"/htmlcache/"+datetime.strftime(d,"%b_%d_%Y")
	princepred += "/%d"%d.hour
	if os.path.isfile(princepred):
		f1 = open(princepred,'r')
		lines = f1.readlines()
		fout.write(convertdate(lines[0],d,1))
		#fout.write(lines[1])
	else:
		print "Prediction file missing ", princepred
		sys.exit(1)	
	f1.close()
	
	d += timedelta(hours=1)
	princepred = path+"/htmlcache/"+datetime.strftime(d,"%b_%d_%Y")
	princepred += "/%d"%d.hour
	if os.path.isfile(princepred):
		f1 = open(princepred,'r')
		lines = f1.readlines()
		#print princepred
		fout.write(convertdate(lines[0],d,1))
		#fout.write(lines[1])
	else:
		print "Prediction file missing ", princepred
		sys.exit(1)	
	f1.close()

	
	resthour = hours-1
	d += timedelta(hours=1)
	predfor = d+timedelta(hours=1)
	while resthour>0:
		princepred = path+"/htmlcache/"+datetime.strftime(d,"%b_%d_%Y")
		if os.path.isdir(princepred):
			princepred += "/%d"%d.hour
			if os.path.isfile(princepred):
				#print princepred, predfor
				f1 = open(princepred,'r')
				i = 1
				for line in f1:
					if resthour > 0:
						wrtdata, tempdate = conandretdate(line,d,i)
						#print tempdate, princepred
						if tempdate==predfor:
							fout.write(wrtdata)
							predfor += timedelta(hours=1)
							resthour -= 1
							i += 1
						elif tempdate>predfor:
							print resthour, d, predfor
							resthour=0
							break
					#if resthour <= 0:
					#	break
				f1.close()
				
			else:
				print "Prediction file missing ", princepred
				sys.exit(1)
		else:
			print "Prediction file missing",princepred
			sys.exit(1)
		d += timedelta(hours=1)
	#print d.weekday()
	return 1

def process(date, hours, path,shiftFactor):
	#global fout
    fout = tempfile.TemporaryFile()#open("full_fore.txt","w")

    loc = 'nj'
    url = "http://www.weather.com/weather/hourbyhour/"+zipcodes[loc] #urls[loc]
    tz = timezones[loc]
    os.environ['TZ'] = tz
    time.tzset()
	
        
	#delta = timedelta(hours=12)
        #for i in range(4):
         #   t = d.timetuple()
            #cmd = 'wget -O temp.%d -o /dev/null "%s?begHour=%d&begDay=%d"'%(i,url,t.tm_hour,t.tm_yday)
	  #  htmlFile = path+"/htmlcache/%s_%d_%d_%d_%d.html"%(loc,d.year,d.month,d.day,d.hour)
	   # if not os.path.isfile(htmlFile):
	#	cmd = 'wget -O %s -o /dev/null "%s?begHour=%d&begDay=%d"'%(htmlFile,url,t.tm_hour,t.tm_yday)		
	#	os.system(cmd)
	 #   getRecords(htmlFile, fout)
    #        print cmd
         #   d += delta
	#fout.close()
	
    
    d = datetime(date.year,date.month,date.day,date.hour) - timedelta(hours=shiftFactor)
    
    status = readforecastprinceton(fout, d, hours, path)
    if status==0:
		#print d, hours
		status = readforecastpiscataway(fout, d, hours, path)
		if status==0:
			print "No forecats found"
			sys.exit(1)


    finp = fout#open("full_fore.txt","r")
    finp.seek(0)
    fout = tempfile.TemporaryFile()#open("fore.txt","w")
    count = 0
    for line in finp.readlines():
		#print line.strip()

		if conditions.has_key(line.split("\t")[4].lower()):
			tagint = conditions[line.split("\t")[4].lower()]
		else:
			print "key missing", line.split("\t")[4].lower()
			tagint = conditions[long_substr(conditions.keys(),line.split("\t")[4].lower())]
		str1 = line.split("\t")[0]+"\t"+line.split("\t")[4]+"\t"+`tagint`
		fout.write(str1+"\n")
		count += 1
		if count == hours+shiftFactor:
			break
			
    finp.close()
	#fout.close()
    fout.seek(0)

    return fout

	

if __name__ == '__main__':

    dirname = time.strftime(format)
    
    
    
    #os.mkdir(dirname)
    
    delta = timedelta(hours=12)
    for loc in locations:
        url = "http://www.weather.com/weather/hourbyhour/"+zipcodes[loc] #urls[loc]
        tz = timezones[loc]
        os.environ['TZ'] = tz
        time.tzset()
        d = datetime.now()
        for i in range(4):
            t = d.timetuple()
            cmd = 'wget -O %s/%s.%d -o /dev/null "%s?begHour=%d&begDay=%d"'%(dirname,loc,i,url,t.tm_hour,t.tm_yday)
            #os.system(cmd)
    #        print cmd
            d += delta 
