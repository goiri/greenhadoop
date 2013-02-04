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

from ghadoopcommons import *
from ghadoopmonitor import *

#print ["/bin/touch", HADOOP_HOME+"/logs/hadoop-"+USER+"-jobtracker-"+MASTER_NODE+".log"]

print "Nodes:\tNode\tMapRed\tHDFS"
nodes = getNodes()
for nodeId in sorted(nodes):
	print "\t"+str(nodeId)+":\t"+str(nodes[nodeId][0])+"\t"+str(nodes[nodeId][1])

"""
# Deploy the testing file
for i in `seq 1 8`; do
	for j in `seq 1 8`; do
		./hadoop fs -cp  /user/goiri/testfile /user/goiri/input$i/testfile$j
	done
done
"""

cleanNodesHdfsReady()
cleanNodeDecommission()

# Restart Hadoop
print "Stopping MapReduce..."
call([HADOOP_HOME+"/bin/stop-mapred.sh"], stdout=open('/dev/null', 'w'))
print "Stopping HDFS..."
call([HADOOP_HOME+"/bin/stop-dfs.sh"], stdout=open('/dev/null', 'w'))

print "Stopping everything..."
JAVA_BIN = "/usr/lib/jvm/java-6-sun/bin/java"
#call([HADOOP_HOME+"/bin/slaves.sh","killall","-9","/usr/lib/jvm/java-6-openjdk/bin/java"], stdout=open('/dev/null', 'w'))
#call([HADOOP_HOME+"/bin/slaves.sh","killall","-9","/usr/lib/jvm/java-6-sun/bin/java"], stdout=open('/dev/null', 'w'))
#call([HADOOP_HOME+"/bin/slaves.sh","killall","-9","/home/goiri/jre1.6.0_26/bin/java"], stdout=open('/dev/null', 'w'))
call([HADOOP_HOME+"/bin/slaves.sh","killall","-9",JAVA_BIN], stdout=open('/dev/null', 'w'))
call(["killall","-9",JAVA_BIN], stdout=open('/dev/null', 'w'), stderr=open('/dev/null', 'w'))
call([HADOOP_HOME+"/bin/slaves.sh","rm","-f","/tmp/hadoop-goiri*.pid"], stdout=open('/dev/null', 'w'))

#call(["rm","-f",HADOOP_HOME+"/logs/*"], stdout=open('/dev/null', 'w'))
print "Cleaning log files..."
call([HADOOP_HOME+"/bin/slaves.sh","rm","-Rf",HADOOP_HOME+"/logs/*"], stdout=open('/dev/null', 'w'))

call(["/bin/touch", HADOOP_HOME+"/logs/hadoop-"+USER+"-jobtracker-"+MASTER_NODE+".log"], stdout=open('/dev/null', 'w'))
call(["/bin/touch", HADOOP_HOME+"/logs/hadoop-"+USER+"-namenode-"+MASTER_NODE+".log"], stdout=open('/dev/null', 'w'))


"""
for i in range(0,5):
	if len(getJobs())!=0:
		break
	else:
		time.sleep(0.5)

print "Cancelling jobs..."
# Cancel tasks
for job in getJobs().values():
	if job.state != "SUCCEEDED" and job.state != "KILLED" and job.state != "FAILED":
		killJob(job.id)

print "Deploying regular HDFS..."
print "Deploying Green HDFS..."
call(["rm","-f",HADOOP_HOME+"/hadoop-hdfs-0.21.1-SNAPSHOT.jar"], stdout=open('/dev/null', 'w'))
call(["rm","-f",HADOOP_HOME+"/hadoop-hdfs-0.21.0.jar"], stdout=open('/dev/null', 'w'))
call(["cp",HADOOP_HOME+"/hadoop-hdfs-0.21.0.jar.bak",HADOOP_HOME+"/hadoop-hdfs-0.21.0.jar"], stdout=open('/dev/null', 'w'))
call(["cp",HADOOP_HOME+"/greenhadoop-hdfs-0.21.1-SNAPSHOT.jar.bak",HADOOP_HOME+"/hadoop-hdfs-0.21.1-SNAPSHOT.jar"], stdout=open('/dev/null', 'w'))
"""

print "Starting HDFS..."
call([HADOOP_HOME+"/bin/start-dfs.sh"], stdout=open('/dev/null', 'w'))
# Wait for safemode to be done
print "Wait until HDFS is ready..."
call([HADOOP_HOME+"/bin/hdfs", "dfsadmin", "-safemode", "wait"], stdout=open('/dev/null', 'w'), stderr=open('/dev/null', 'w'))

print "Starting MapReduce..."
call([HADOOP_HOME+"/bin/start-mapred.sh"], stdout=open('/dev/null', 'w'))

print "Start monitoring..."
thread = MonitorMapred()
thread.start()

signal.signal(signal.SIGINT, signal_handler)

# Wait until all data nodes are available
print "Waiting until every node is ready..."
auxNodes = []
while len(auxNodes)<len(nodes):
	for nodeId in getNodesHdfsReady():
		if nodeId not in auxNodes:
			auxNodes.append(nodeId)
			print "\t"+nodeId+" ready!"
	if len(auxNodes)<len(nodes):
		time.sleep(0.5)

# Clean decommission state
cleanNodeDecommission()

print "Removing output and temporary data..."
rmFile("/user/goiri/output*")
rmFile("/user/goiri/crawlMain/new_indices")
rmFile("/tmp/*")
rmFile("/jobtracker/*")

print "Set default data replication..."
setFileReplication("/user/goiri/*", REPLICATION_DEFAULT)

"""
print "Stopping HDFS..."
call([HADOOP_HOME+"/bin/stop-dfs.sh"], stdout=open('/dev/null', 'w'))

print "Deploying Green HDFS..."
call(["rm","-f",HADOOP_HOME+"/hadoop-hdfs-0.21.1-SNAPSHOT.jar"], stdout=open('/dev/null', 'w'))
call(["rm","-f",HADOOP_HOME+"/hadoop-hdfs-0.21.0.jar"], stdout=open('/dev/null', 'w'))
call(["cp",HADOOP_HOME+"/greenhadoop-hdfs-0.21.1-SNAPSHOT.jar.bak",HADOOP_HOME+"/hadoop-hdfs-0.21.1-SNAPSHOT.jar"], stdout=open('/dev/null', 'w'))

print "Starting HDFS..."
call([HADOOP_HOME+"/bin/start-dfs.sh"], stdout=open('/dev/null', 'w'))

Wait for safemode to be done
print "Wait until HDFS is ready..."
call([HADOOP_HOME+"/bin/hdfs", "dfsadmin", "-safemode", "wait"], stdout=open('/dev/null', 'w'), stderr=open('/dev/null', 'w'))


Start everything
if True:
	ALWAYS_NODE=nodes.keys()

print "Decommissioning..."
threads = []
nodes = getNodes()
for nodeId in sorted(nodes):
	if nodeId not in ALWAYS_NODE:
		thread = threading.Thread(target=setNodeStatus,args=(nodeId, False))
		thread.start()
		threads.append(thread)
	else:
		thread = threading.Thread(target=setNodeStatus,args=(nodeId, True))
		thread.start()
		threads.append(thread)

while len(threads)>0:
	threads.pop().join()
"""


"""
#Wait until all data nodes are available
print "Waiting until every node is ready..."
auxNodes = []
while len(auxNodes)<len(nodes):
	for nodeId in getNodesHdfsReady():
		if nodeId not in auxNodes:
			auxNodes.append(nodeId)
			print "\t"+nodeId+" ready!"
	if len(auxNodes)<len(nodes):
		time.sleep(0.5)


#Turn off mapred
print "Turning off nodes..."
threads = []
for nodeId in sorted(getNodes()):
	if nodeId not in ALWAYS_NODE:
		thread = threading.Thread(target=setNodeMapredStatus,args=(nodeId, False))
		thread.start()
		threads.append(thread)
		thread = threading.Thread(target=setNodeHdfsStatus,args=(nodeId, False))
		thread.start()
		threads.append(thread)
while len(threads)>0:
	thread = threads.pop()
	thread.join()

TODO for testing only
nodes = getNodes()
for nodeId in sorted(nodes):
	if nodeId != "crypt15":
		setNodeMapredStatus(nodeId, False)
"""

print "Nodes:\tNode\tMapRed\tHDFS"
#nodes = getNodes(True)
nodes = getNodes()
for nodeId in sorted(nodes):
	print "\t"+str(nodeId)+":\t"+str(nodes[nodeId][0])+"\t"+str(nodes[nodeId][1])

print "Finished!"

thread.kill()

sys.exit(0)
