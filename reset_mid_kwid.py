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

def reset_mid_kwid(mid,conn,cur):
	sql = '''update MusicSrc set kw_id=0,m_status=0,source_type=1,m_album_id=0 where mid=%s''' % (mid)
	cnt = cur.execute(sql)
	if cnt > 0:
		conn.commit()

def reset_albumid(from_aid,conn,cur):
	sql = '''update AlbumSrc set m_album_id=0,m_status=0 where id=%s''' % (from_aid)
	cnt = cur.execute(sql)
	if cnt > 0:
		conn.commit()

def checkMusicSrc(mid,conn,cur):
	from_aid = 0
	sql = '''select from_aid from MusicSrc where mid=%s''' % (mid)
	cnt = cur.execute(sql)
	if cnt > 0:
		rets = cur.fetchone()
		from_aid = rets["from_aid"]
	return from_aid

def mainworker(filepath):
	g_connData = g_pool_TMApi.connection()
	g_curData = g_connData.cursor()
	g_connSrc = g_pool_Src.connection()
	g_curSrc = g_connSrc.cursor()
	###1.open file
	f = open(filepath,"r")
	track_set = set()
	fout = open(filepath+".process","a+")
	for line in fout:
		track_set.add(line.strip().split("\t")[0])
	for line in f:
		arr = line.strip().split("\t")
		mid = arr[0].strip()
		logging.info("track %s processing" % (mid))
		if mid in track_set:
			logging.info("track %s has processed" % (mid))
			continue
			
		from_aid = checkMusicSrc(mid,g_connSrc,g_curSrc)
		reset_mid_kwid(mid,g_connSrc,g_curSrc)
		if int(from_aid) > 0:
			reset_albumid(from_aid,g_connSrc,g_curSrc)
		album_dict = get_new_album_track_by_trackid(mid,g_connData,g_curData)
		data_get_pool(g_config.configinfo,g_connData,g_curData,g_connSrc,g_curSrc,album_dict,200000)
		fout.write("%s\n" % (mid))
		#break
	g_curData.close()
	g_connData.close()
	g_curSrc.close()
	g_connSrc.close()

if __name__ == '__main__':
	mainworker(sys.argv[1])
