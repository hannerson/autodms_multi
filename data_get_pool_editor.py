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
	while True:
		g_connData = g_pool_TMApi.connection()
		g_curData = g_connData.cursor()
		g_connSrc = g_pool_Src.connection()
		g_curSrc = g_connSrc.cursor()
		albumids = get_editor_album(g_connSrc,g_curSrc)
		for albumid,priority in albumids.items():
			album_dict = get_new_album_track_byid(albumid,g_connData,g_curData,200000)
			print len(album_dict)
			data_get_pool_priority(g_config.configinfo,priority,g_connData,g_curData,g_connSrc,g_curSrc,album_dict,200000)
			if len(album_dict) > 0:
				update_editor_album(albumid,g_connSrc,g_curSrc)
				logging.info("process album %s ok" % (albumid))
		logging.info("sleep 180s")
		time.sleep(180)
		
		g_curData.close()
		g_connData.close()
		g_curSrc.close()
		g_connSrc.close()
		#break
