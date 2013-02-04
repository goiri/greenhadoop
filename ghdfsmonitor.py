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

import time
import threading
import signal

from subprocess import call, PIPE, Popen

from ghadoopcommons import *


class MonitorHDFS(threading.Thread):
	def __init__(self):
		threading.Thread.__init__(self)
		self.running = True
		
		# Read nodes
		pipe=Popen([HADOOP_HOME+"/bin/hdfs", "dfsadmin", "-printTopology"], stdout=PIPE, stderr=open('/dev/null', 'w'))
		text = pipe.communicate()[0]
		
		self.nodeName = {}
		for line in text.split('\n'):
			if line !="" and not line.startswith("Rack:"):
				line = line.strip()
				lineSplit = line.split(" ")
				if len(lineSplit)>=2:
					self.nodeName[lineSplit[0]] = lineSplit[1].replace("(", "").replace(")", "")
		# Read for the first time
		self.getData()
	
	def kill(self):
		self.running = False

	def run(self):
		lastUpdate = 0
		while self.running:
			self.getData()
			
			# Output
			lastUpdate -= 1
			if lastUpdate<0:
				lastUpdate=1
				if DEBUG>3 :
					self.printOutput()
					
			# Wait until next cycle
			time.sleep(10.0)

	def getData(self):
		
		# Read files
		pipe = Popen([HADOOP_HOME+"/bin/hdfs", "fsck", "/", "-files", "-blocks", "-locations"], stdout=PIPE, stderr=open('/dev/null', 'w'))
		blockToRead = 0
		for line in pipe.stdout.readlines():
			if line != "\n":
				if line.find("<dir>")>=0:
					lineSplit = line.split(" ")
					fileId = lineSplit[0]
					if fileId in files:
						file = files[fileId]
					else:
						file = FileHDFS(fileId, True)
						files[fileId] = file
					
					# Parent directory
					dir = fileId[0:fileId.rfind("/")]
					if fileId != "/":
						if dir=="":
							dir="/"
						files[dir].blocks[file.id]=file
				elif line.find("bytes")>=0 and line.find("block(s):")>=0:
					lineSplit = line.split(" ")
					fileId = lineSplit[0]
					blockToRead = int(lineSplit[3])
					if fileId in files:
						file = files[fileId]
					else:
						file = FileHDFS(fileId, False)
						files[fileId] = file
					
					# Parent Directory
					dir = fileId[0:fileId.rfind("/")]
					files[dir].blocks[file.id] = file
					
					# Clean previous file location
					#cleanFileLocation(fileId)
					auxBlocks = file.blocks
					file.blocks = {}
					for block in auxBlocks.values():
						for nodeId in block.nodes:
							if block.id in nodeFile[nodeId]:
								nodeBlock[nodeId].remove(block.id)
							if block.file in nodeFile[nodeId]:
								nodeFile[nodeId].remove(block.file)
				else:
					if line.find("Under replicated")>0:
						None
					elif line != " OK\n" and blockToRead>0:
						if line.find("CORRUPT")>=0:
							None
						elif line.find("MISSING")>=0:
							blockToRead-=1
						else:
							# Update information
							lineSplit = line.split(" ")
							id = lineSplit[1]
							
							file = files[fileId]
							if id in file.blocks:
								block = file.blocks[id]
							else:
								block = BlockHDFS(id)
								block.file = fileId
								file.blocks[id] = block
							
							nodes = line[line.find("[")+1:line.find("]")]
							nodes = nodes.split(", ")
							
							# Set file location
							for node in nodes:
								nodeName = node
								if node in self.nodeName:
									nodeName = self.nodeName[node]
								if nodeName not in block.nodes:
									block.nodes.append(nodeName)
							blockToRead-=1
		# Update map nodes -> file,block
		for file in files.values():
			# Get file location
			for block in file.blocks.values():
				if isinstance(block, BlockHDFS):
					for nodeId in block.nodes:
						# Node -> Block
						if nodeId not in nodeBlock:
							nodeBlock[nodeId] = []
						if block.id not in nodeBlock[nodeId]:
							nodeBlock[nodeId].append(block.id)
						# Node -> File
						if nodeId not in nodeFile:
							nodeFile[nodeId] = []
						if block.file not in nodeFile[nodeId]:
							nodeFile[nodeId].append(block.file)
				else:
					file = block
					for nodeId in file.getLocation():
						# Node -> File
						if nodeId not in nodeFile:
							nodeFile[nodeId] = []
						if file.id not in nodeFile[nodeId]:
							nodeFile[nodeId].append(file.id)	
		
		# Files are updated
		setFilesUpdated(True)

	def printOutput(self):
		print "========================================================="
		# Output
		print "Files:"
		for fileName in sorted(files):
			file = files[fileName]
			tab=""
			for i in range(1, file.id.count("/")):
				tab+="\t"
			print tab+file.id+" blk="+str(len(file.blocks))+" repl="+str(file.getMinReplication())+" => "+str(sorted(file.getLocation()))

		nodes = getNodes()
		print "Node->Blocks:"
		for nodeId in sorted(self.nodeName.values()):
			out = "\t"+nodeId
			if nodeId in nodes:
				out+=" ["+str(nodes[nodeId][1])+"]"
			if nodeId in nodeBlock:
				out+=":\t"+str(len(nodeBlock[nodeId]))
			print out
		
		print "Node->File:"
		for nodeId in sorted(self.nodeName.values()):
			out = "\t"+nodeId
			if nodeId in nodes:
				out+=" ["+str(nodes[nodeId])+"]"
			if nodeId in nodeFile:
				out+=":\t"+str(len(nodeFile[nodeId]))
			print out

if __name__=='__main__':
	DEBUG = 4
	
	thread = MonitorHDFS()
	thread.start()
	
	signal.signal(signal.SIGINT, signal_handler)
	
	
	nodes = getNodes()
	for nodeId in nodes:
		print nodeId+": "+str(nodes[nodeId])
		
	nodes = getNodes()
	offNodes = nodes.keys()
	"""
	#offNodes.remove("crypt01")
	print "/user/goiri/input-workload1-00 => " + str(minNodesFiles(["/user/goiri/input-workload1-00"], offNodes))
	print "/user/goiri/input-workload1-01 => " + str(minNodesFiles(["/user/goiri/input-workload1-01"], offNodes))
	print "/user/goiri/input-workload1-02 => " + str(minNodesFiles(["/user/goiri/input-workload1-02"], offNodes))
	print "/user/goiri/input-workload1-03 => " + str(minNodesFiles(["/user/goiri/input-workload1-03"], offNodes))
	print "/user/goiri/input-workload1-04 => " + str(minNodesFiles(["/user/goiri/input-workload1-04"], offNodes))
	print "/user/goiri/input-workload1-05 => " + str(minNodesFiles(["/user/goiri/input-workload1-05"], offNodes))
	print "/user/goiri/input-workload1-06 => " + str(minNodesFiles(["/user/goiri/input-workload1-06"], offNodes))
	print "/user/goiri/input-workload1-07 => " + str(minNodesFiles(["/user/goiri/input-workload1-07"], offNodes))
	"""
	
	while True:
		time.sleep(5.0)
	
	thread.join()
