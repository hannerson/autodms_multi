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
import threading
import Queue

reload(sys)
sys.setdefaultencoding('utf-8')

def get_new_album_track(last_get_time,conn,cur,limit):
	album_dict = {}
	#last_get_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%m:%S")
	sql = '''select * from CenterApi where ctime>"%s" and company="索尼音乐" limit %s''' % (last_get_time,limit)
	cnt = cur.execute(sql)
	if cnt > 0:
		rets = cur.fetchall()
		conn.commit()
		for ret in rets:
			if album_dict.has_key(ret["album_id"]):
				album_dict[ret["album_id"]].append(ret)
			else:
				album_dict[ret["album_id"]] = []
				album_dict[ret["album_id"]].append(ret)
	
	return album_dict

def get_frommid_MusicSrc(tm_id,albumname,artists,connSrc,curSrc):
	from_id = 0
	m_status = 0
	if int(tm_id) > 0:
		sql = '''select id,m_status from MusicSrc where mid=%s''' % (tm_id)
	else:
		sql = '''select id,m_status from MusicSrc where m_name=\"%s\" and m_artists=\"%s\"''' % (MySQLdb.escape_string(str(albumname)), MySQLdb.escape_string(str(artists)))

	cnt = curSrc.execute(sql)
	connSrc.commit()

	if cnt > 0:
		ret = curSrc.fetchone()
		from_id = ret["id"]
		m_status = ret["m_status"]
	return from_id,m_status

def get_new_album(tx_albumid,conn,cur):
	album = {}
	sql = '''select * from CenterAlbumApi where album_id=%s''' % (tx_albumid)
	cnt = cur.execute(sql)
	if cnt > 0:
		album = cur.fetchone()
		conn.commit()
	return album

def checkSrcAlbum(tx_albumid,conn,cur):
	aid = 0
	priority = 0
	sql = '''select id,priority from AlbumSrc where tx_albumid=%s''' % (tx_albumid)
	cnt = cur.execute(sql)
	if cnt > 0:
		ret = cur.fetchone()
		aid = ret["id"]
		priority = ret["priority"]
	return aid,priority

def checkSrcMusic(mid,conn,cur):
	aid = 0
	sql = '''select id from MusicSrc where mid=%s''' % (mid)
	cnt = cur.execute(sql)
	if cnt > 0:
		ret = cur.fetchone()
		aid = ret["id"]
	return aid

def insert_MusicSrc(config,from_aid,priority,info,connData,curData,connSrc,curSrc):
	sql_fields = ""
	sql_values = ""
	fields_map = config["MusicMap"]
	for k,v in fields_map.items():
		sql_fields += "%s," % v
		sql_values += "\"%s\"," % MySQLdb.escape_string(str(info[k]))
	for k,v in config["MusicConst"].items():
		sql_fields += "%s," % k
		sql_values += "%s," % v
	sql_fields += "%s," % ("from_aid")
	sql_values += "%s," % (from_aid)
	sql_fields += "%s," % ("source_type")
	sql_values += "%s," % (1)
	#url,file_type = get_resource_url(1,info["track_id"],connData,curData)
	#sql_fields += "%s," % "m_audio_url"
	#sql_values += "\"%s\"," % url
	sql_fields += "%s," % "priority"
	sql_values += "%s," % priority
	sql = '''insert into `MusicSrc` (%s) values (%s)''' % (sql_fields.rstrip(","), sql_values.rstrip(","))
	logging.info(sql)
	cnt = curSrc.execute(sql)
	connSrc.commit()
	

def get_resource_url(type,sourceid,connData,curData):
	url = ""
	file_type = ""
	sql = '''select url,file_type from CenterDownUrlR where ctype=%s and iid=%s''' % (type,sourceid)
	cnt = curData.execute(sql)
	if cnt > 0:
		ret = curData.fetchone()
		connData.commit()
		file_type = ret["file_type"]
		url = ret["url"]
	return url,file_type
	

###info:[{album_id,album_name,singer_name,track_ids,desc,language,upc,company,album_type,release_time},{}]
def insert_AlbumSrc(album_id,artistTop,config,connData,curData,connSrc,curSrc):
	from_aid = 0
	priority = 6
	sql_fields = ""
	sql_values = ""
	fields_map = config["AlbumMap"]
	album_info = get_new_album(album_id,connData,curData)
	print album_info
	if len(album_info) == 0:
		return -1,0
	inter_days = 10
	top_art = False
	artistset = set()
	if album_info.has_key("singer_name") and album_info.has_key("release_time"):
		inter_days = calTime(datetime.datetime.now().strftime("%Y-%m-%d"),album_info["release_time"][0:10])
		artistset = artist_split(album_info["singer_name"])
	for a in artistset:
		if a in artistTop:
			top_art = True
	for k,v in fields_map.items():
		sql_fields += "%s," % v
		sql_values += "\"%s\"," % MySQLdb.escape_string(str(album_info[k]))
	for k,v in config["AlbumConst"].items():
		sql_fields += "%s," % k
		if k == "priority":
			if inter_days <=7 and top_art:
				sql_values += "%s," % 10
			else:
				sql_values += "%s," % priority
		else:
			sql_values += "%s," % v

	###get resource pic url
	#url,file_type = get_resource_url(2,album_id,connData,curData)
	#print url,file_type
	#sql_fields += "%s," % "m_pic_url"
	#sql_values += "\"%s\"," % url
	sql_fields += "%s," % ("source_type")
	sql_values += "%s," % (1)
	
	sql = '''insert into `AlbumSrc` (%s) values (%s)''' % (sql_fields.rstrip(","), sql_values.rstrip(","))
	logging.info(sql)
	cnt = curSrc.execute(sql)
	connSrc.commit()
	from_aid = int(curSrc.lastrowid)
	return from_aid,priority


def data_get_pool(config,connData,curData,connSrc,curSrc,limit):
	###1.check music, return dict:albumid-trackids
	last_get_time = (datetime.datetime.now()+datetime.timedelta(days=-2)).strftime("%Y-%m-%d %H:%m:%S")
	artistTop = loadArtistTop("./artistTop3k.txt")
	while True:
		from_aid = 0
		album_dict = get_new_album_track(last_get_time,connData,curData,limit)
		last_get_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%m:%S")
		logging.info("last-get-time:%s" % (last_get_time))
		for album_id,infos in album_dict.items():
			###2.check album exists
			#print album_id
			from_aid,priority = checkSrcAlbum(album_id,connSrc,curSrc)
			if from_aid > 0:
				###3.album exists, insert music
				for info in infos:
					if checkSrcMusic(info["track_id"],connSrc,curSrc) > 0:
						logging.info("mid : %s exists" % (info["track_id"]))
						continue
					insert_MusicSrc(config,from_aid,priority,info,connData,curData,connSrc,curSrc)
			else:
				###3.insert album, insert music
				#album_info = get_new_album(album_id,connData,curData)
				from_aid,priority = insert_AlbumSrc(album_id,artistTop,config,connData,curData,connSrc,curSrc)
				if from_aid == -1:
					logging.info("no album info %s" % (album_id))
					continue
				for info in infos:
					if checkSrcMusic(info["track_id"],connSrc,curSrc) > 0:
						logging.info("mid : %s exists" % (info["track_id"]))
						continue
					insert_MusicSrc(config,from_aid,priority,info,connData,curData,connSrc,curSrc)
		break
		logging.info("sleep 180s")
		time.sleep(180)

if __name__ == '__main__':
	g_connData = g_pool_TMApi.connection()
	g_curData = g_connData.cursor()
	g_connSrc = g_pool_Src.connection()
	g_curSrc = g_connSrc.cursor()

	data_get_pool(g_config.configinfo,g_connData,g_curData,g_connSrc,g_curSrc,10000)
	
	g_curData.close()
	g_connData.close()
	g_curSrc.close()
	g_connSrc.close()
