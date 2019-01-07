# -*- coding=utf-8 -*-

import os,sys
import MySQLdb
import traceback
import utils
import json
import logging
from logging.handlers import TimedRotatingFileHandler
import MySQLdb.cursors
import time
from sqlClass import *
from pooldb import *
from logger import *


def loadHighrisk(path):
	highrisk = set()
	f = open(path,"r")
	for line in f:
		arr = line.strip().split("\t")
		key = arr[0].strip()
		highrisk.add(key)
	return highrisk

def checkMusicSrc(table, conn, cur):
	taskids = []
	if table in ["MusicSrc","AlbumSrc"]:
		sql = '''select id,m_name,m_artists from %s where m_status=%s order by priority desc limit 5000''' % (table, g_status["default"])
	elif table in ["ArtistSrc"]:
		sql = '''select id,m_name from %s where m_status=%s order by priority desc limit 5000''' % (table, g_status["default"])
	cnt = cur.execute(sql)
	if cnt > 0:
		ret = cur.fetchall()
		conn.commit()
		for r in ret:
			taskids.append(r)
	return taskids


def mainworker(table):
	highriskwords = loadHighrisk("./highrisk.txt")
	g_connSrc = g_pool_Src.connection()
	g_curSrc = g_connSrc.cursor()
	sqlclass = sqlClass(g_connSrc,g_curSrc)
	###1 select info
	taskids = checkMusicSrc(table, g_connSrc, g_curSrc)
	while len(taskids) > 0:
		for info in taskids:
			logging.info("process: id %s" % (info["id"]))
			matched = False
			for key in highriskwords:
				for k,val in info.items():
					if k == "id":
						continue
					if val.find(key) != -1:
						field_dict = {}
						field_dict["m_status"] = g_status["highrisk"]
						field_dict["hf_type"] = g_status["highrisk"]
						field_dict["hf_keyword"] = key
						where = "id=%s" % (info["id"])
						sqlclass.mysqlUpdate(table,where,field_dict)
						matched = True
						break
				if matched:
					logging.info("keyword matched : id %s - %s" % (info["id"],key))
					break
			if matched:
				field_dict = {}
				field_dict["m_status"] = g_status["highrisk"]
				field_dict["hf_type"] = g_status["highrisk"]
				field_dict["hf_keyword"] = key
				where = "id=%s" % (info["id"])
				sqlclass.mysqlUpdate(table,where,field_dict)
			else:
				field_dict = {}
				field_dict["m_status"] = g_status["highrisk"]
				where = "id=%s" % (info["id"])
				sqlclass.mysqlUpdate(table,where,field_dict)
		taskids = checkMusicSrc(table, g_connSrc, g_curSrc)
		break
	g_curSrc.close()
	g_connSrc.close()
