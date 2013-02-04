#!/usr/bin/python
import sys,os

modPath = "."

def init():
	global modPath
	for p in sys.path:
		p = os.path.realpath(p)
		if p.endswith('greenavailability'):
			modPath=p
