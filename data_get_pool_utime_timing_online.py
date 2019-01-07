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
	if os.path.exists("data_get_pool_timing.utime"):
		utime = int(getTimefile("data_get_pool_timing.utime"))
	else:
		utime = int(time.time()-3600)
	if utime == 0:
		utime = int(time.time()-3600)
	while True:
		g_connData = g_pool_TMApi.connection()
		g_curData = g_connData.cursor()
		g_connSrc = g_pool_Src.connection()
		g_curSrc = g_connSrc.cursor()

		last_get_time = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))
		next_utime = int(time.time()-10)
		logging.info("begin last-get-time:%s" % (last_get_time))

		album_dict = get_release_date_recently_utime(utime,g_connData,g_curData,200000)

		end_last_get_time = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()))
		logging.info("end last-get-time:%s" % (end_last_get_time))

		createTimefile("data_get_pool_timing.utime",next_utime)
		utime = next_utime

		data_get_pool(g_config.configinfo,g_connData,g_curData,g_connSrc,g_curSrc,album_dict,200000)
		break
		logging.info("sleep 180s")
		time.sleep(180)
		
		g_curData.close()
		g_connData.close()
		g_curSrc.close()
		g_connSrc.close()
