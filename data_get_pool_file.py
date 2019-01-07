#!/usr/local/bin/python
# -*- coding=utf-8 -*-

import os,sys
import MySQLdb
import traceback
from utils import *
import json
import logging
from logging.handlers import TimedRotatingFileHandler
import MySQLdb.cursors
import time
import datetime
import urllib2
from pooldb import *
from logger import *
from data_get_pool_utils import *
import threading
import Queue

reload(sys)
sys.setdefaultencoding('utf-8')

if __name__ == '__main__':
	g_connData = g_pool_TMApi.connection()
	g_curData = g_connData.cursor()
	g_connSrc = g_pool_Src.connection()
	g_curSrc = g_connSrc.cursor()

	album_set = set()
	f = open(sys.argv[1],"r")
	fout = open(sys.argv[1]+".process","a+")
	for line in fout:
		album_set.add(line.strip().split("\t")[0])
	for line in f:
		album_id = line.strip().split("\t")[0]
		logging.info("album %s processing" % (album_id))
		if album_id in album_set:
			logging.info("album %s has processed" % (album_id))
			continue
		album_dict = get_new_album_track_byid(album_id,g_connData,g_curData,20)
		data_get_pool_makeup(g_config.configinfo,7,g_connData,g_curData,g_connSrc,g_curSrc,album_dict,200000)
		fout.write("%s\n" % (album_id))
		#break
	f.close()
	fout.close()
	
	g_curData.close()
	g_connData.close()
	g_curSrc.close()
	g_connSrc.close()
