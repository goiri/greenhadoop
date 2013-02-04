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

import threading
import time

from datetime import datetime,timedelta
from optparse import OptionParser
from itertools import *

from ghadoopcommons import *
from ghadooplogger import *
from ghadoopwaiting import *
from ghadoopreplication import *
from ghadoopmonitor import *
from ghdfsmonitor import *
from ghadoop import *


def minNodesFilesNew(checkFiles, offNodes):
	startaux = datetime.now()
	
	# Input
	checkFiles = getFilesInDirectories(checkFiles)
	auxOffNodes = list(offNodes)
	
	# Get which files are missing and which nodes have them
	missingFiles = []
	missingNodes = {}
	for fileId in checkFiles:
		file = files[fileId]
		locations = file.getLocation()
		if len(locations)>0 and fileId not in missingFiles:
			# Check if the file is missing
			missing = True
			for nodeId in locations:
				if nodeId not in auxOffNodes:
					missing=False
					break
			if missing:
				missingFiles.append(fileId)
				# Check where is the file
				for nodeId in locations:
					if nodeId in auxOffNodes:
						if nodeId not in missingNodes:
							missingNodes[nodeId]=0
						missingNodes[nodeId]+=1
	# Check nodes with missing files
	auxMissingNodes = []
	for nodeId in missingNodes:
		auxMissingNodes.append((nodeId, missingNodes[nodeId]))
	missingNodes = sorted(auxMissingNodes, key=itemgetter(1), reverse=True)
	
	if toSeconds(datetime.now()-startaux)>1:
		print "\t\t\t1:"+str(datetime.now()-startaux)
	
	# We need the minimum set of nodes that have the files required by a job
	dataNodes=[]
	# Checking if there is more files to check
	while len(missingFiles)>0:
		# Turn on the nodes with the replicas with more data in
		nodeId = missingNodes[0][0]
		auxOffNodes.remove(nodeId)
		dataNodes.append(nodeId)
		# Update which files are missing and which nodes have them
		missingFiles = []
		missingNodes = {}
		for fileId in checkFiles:
			file = files[fileId]
			locations = file.getLocation()
			if len(locations)>0 and fileId not in missingFiles:
				# Check if the file is missing
				missing = True
				for nodeId in locations:
					if nodeId not in auxOffNodes:
						missing=False
						break
				if missing:
					missingFiles.append(fileId)
					# Check where is the file
					for nodeId in locations:
						if nodeId in auxOffNodes:
							if nodeId not in missingNodes:
								missingNodes[nodeId]=0
							missingNodes[nodeId]+=1
		# Check nodes with missing files
		auxMissingNodes = []
		for nodeId in missingNodes:
			auxMissingNodes.append((nodeId, missingNodes[nodeId]))
		missingNodes = sorted(auxMissingNodes, key=itemgetter(1), reverse=True)
		
	if toSeconds(datetime.now()-startaux)>1:
		print "\t\t\t3:"+str(datetime.now()-startaux)
	# Remove those nodes that already have data on
	change = True
	while change:
		change = False
		for nodeId in dataNodes:
			free = True
			if nodeId in nodeFile:
				for fileId in nodeFile[nodeId]:
					if fileId in checkFiles and fileId in files:
						# Check if file is available somewhere else
						fileAvailable = False
						file = files[fileId]
						for otherNodeId in file.getLocation():
							if otherNodeId!=nodeId and otherNodeId not in auxOffNodes:
								fileAvailable = True
								break
						if not fileAvailable:
							free = False
							break
			if free:
				auxOffNodes.append(nodeId)
				dataNodes.remove(nodeId)
				change = True
				break	
	if toSeconds(datetime.now()-startaux)>1:
		print "\t\t\t4:"+str(datetime.now()-startaux)
	return dataNodes

if __name__=='__main__':
	DEBUG = 0

	print "Starting monitoring..."
	monitorHdfs = MonitorHDFS()
	monitorHdfs.start()

	print "Waiting for files to be monitored..."
	while len(files)==0:
		time.sleep(0.2)

	offNodes = []
	for nodeId in getNodes():
		if nodeId != "crypt15" and nodeId != "crypt07":
			offNodes.append(nodeId)

	#checkPath = ["/user/goiri/input-workload1-11", "/user/goiri/input-workload1-18", "/user/goiri/input-workload1-40", "/user/goiri/input-workload1-42", "/user/goiri/input-workload1-96", "/user/goiri/input-workload1-98"]
	checkPath = []
	for i in range(1, 100):
		checkPath.append("/user/goiri/input-workload1-"+str(i).zfill(2))
		
	for i in range(0, 1000):
		#timestart = datetime.now()
		#for path in checkPath:
			#dataNodes = minNodesFiles([path], offNodes)
			#if DEBUG>0:
				#print "\t"+str(path)+": "+str(len(getFilesInDirectories([path])))+" "+str(len(dataNodes))
		#print "\tOld: "+str(datetime.now()-timestart)

		timestart = datetime.now()
		for path in checkPath:
			#dataNodes = minNodesFilesNew([path], offNodes)
			dataNodes = minNodesFilesNew([path], offNodes)
			if DEBUG>0:
				print "\t"+str(path)+": "+str(len(getFilesInDirectories([path])))+" "+str(len(dataNodes))
			#print "\t\tNew: "+str(datetime.now()-timestart)
		print "\t"+str(i)+" New: "+str(datetime.now()-timestart)

	sys.exit(0)

