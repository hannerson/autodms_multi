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
	last_get_time = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()-2400))
	while True:
		g_connData = g_pool_TMApi.connection()
		g_curData = g_connData.cursor()
		g_connSrc = g_pool_Src.connection()
		g_curSrc = g_connSrc.cursor()

		last_get_time1 = (datetime.datetime.now()-datetime.timedelta(seconds=-180)).strftime("%Y-%m-%d %H:%m:%S")
		#album_dict = get_new_album_track(last_get_time,g_connData,g_curData,200000)
		logging.info("begin last-get-time:%s" % (last_get_time))
		logging.info("next last-get-time:%s" % (last_get_time1))
		#album_dict = get_release_date_recently(last_get_time,g_connData,g_curData,200000)
		album_dict = get_new_album_track_manual(last_get_time,g_connData,g_curData,200000)
		print len(album_dict)
		logging.info("end last-get-time:%s" % (last_get_time))
		last_get_time = last_get_time1
		data_get_pool_makeup(g_config.configinfo,10,g_connData,g_curData,g_connSrc,g_curSrc,album_dict,200000)
		#logging.info("sleep 180s")
		#time.sleep(180)
		
		g_curData.close()
		g_connData.close()
		g_curSrc.close()
		g_connSrc.close()
		break
