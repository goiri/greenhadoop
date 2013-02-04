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


print "Nodes:\tNode\tMapRed\tHDFS"
#nodes = getNodes(True)
nodes = getNodes()
for nodeId in sorted(nodes):
	print "\t"+str(nodeId)+":\t"+str(nodes[nodeId][0])+"\t"+str(nodes[nodeId][1])
