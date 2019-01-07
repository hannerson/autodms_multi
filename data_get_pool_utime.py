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
	utime = 0
	if os.path.exists("data_get_pool.utime"):
		utime = int(getTimefile("data_get_pool.utime"))-3600
	else:
		utime = int(time.time()-3600)
	if utime == 0:
		utime = int(time.time()-3600)

	while True:
		g_connData = g_pool_TMApi.connection()
		g_curData = g_connData.cursor()
		g_connSrc = g_pool_Src.connection()
		g_curSrc = g_connSrc.cursor()

		begin_last_get_time = (datetime.datetime.now()).strftime("%Y-%m-%d %H:%m:%S")
		logging.info("begin get-time:%s" % (begin_last_get_time))

		next_utime = int(time.time()-3600)

		album_dict = get_new_album_track_utime(utime,g_connData,g_curData,200000)
		end_last_get_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%m:%S")
		logging.info("end get-time:%s" % (end_last_get_time))

		createTimefile("data_get_pool.utime",next_utime)
		utime = next_utime

		data_get_pool(g_config.configinfo,g_connData,g_curData,g_connSrc,g_curSrc,album_dict,200000)
		#break
		logging.info("sleep 180s")
		time.sleep(180)
		
		g_curData.close()
		g_connData.close()
		g_curSrc.close()
		g_connSrc.close()
