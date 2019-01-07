#!/bin/python

import os,sys


def loadIds(path,column = 0):
	ids = set()
	f = open(path,"r")
	for line in f:
		arr = line.strip().split("\t")
		id = arr[column]
		ids.add(id)
	f.close()
	return ids

def loadIds2(path,column = 0):
	ids = set()
	f = open(path,"r")
	for line in f:
		arr = line.strip("\n").split("\t")
		idArr = arr[column].strip().split("&")
		for id in idArr:
			ids.add(id)
	f.close()
	return ids


def getDiffSet(set1,set2):
	return set1 - set2

def output(path,pathout,idSet,column):
	fp = open(path,"r")
	fout = open(pathout,"w+")
	for line in fp:
		arr = line.strip("\r\n").split("\t")
		if len(arr) < 2:
			continue
		ids = arr[column].strip()
		#for id in ids:
		if ids in idSet:
			fout.write("%s" % line)
				#break
	#	id = arr[5].strip()
	#	if id in idSet:
	#		fout.write("%s" % line)
	#	id = arr[6].strip()
	#	if id in idSet:
	#		fout.write("%s" % line)
	#	id = arr[7].strip()
	#	if id in idSet:
	#		fout.write("%s" % line)
	#	id = arr[8].strip()
	#	if id in idSet:
	#		fout.write("%s" % line)
	fp.close()
	fout.close()

if len(sys.argv) < 3:
	print "must be three parameters"
	sys.exit(0)

set1 = loadIds(sys.argv[2],0)
#set2 = loadIds(sys.argv[2],0)

output(sys.argv[1],sys.argv[3],set1,0)
