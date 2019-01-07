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
	#sql = '''select * from CenterApi where album_id in (4951470)'''
	sql = '''select * from CenterApi where track_id in (219636677,219644093,219104794,219644522)'''
	#sql = '''select * from CenterApi where ctif_scheduled_release_time in ("201811130000","201811131000","201811131100") limit %s''' % (limit)
	cnt = cur.execute(sql)
	if cnt > 0:
		rets = cur.fetchall()
		for ret in rets:
			if album_dict.has_key(ret["album_id"]):
				album_dict[ret["album_id"]][ret["track_id"]] = ret
			else:
				album_dict[ret["album_id"]] = {}
				album_dict[ret["album_id"]][ret["track_id"]] = ret
			#if album_dict.has_key(ret["album_id"]):
			#	album_dict[ret["album_id"]].append(ret)
			#else:
			#	album_dict[ret["album_id"]] = []
			#	album_dict[ret["album_id"]].append(ret)
		conn.commit()
	return album_dict

def get_new_album_track_file(filename,conn,cur,limit):
	f = open(filename,"r")
	pro_set = set()
	for line in f:
		arr = line.strip().split("\t")
		pro_set.add(arr[0])
	f.close()

	ids_str = ""
	for id in pro_set:
		ids_str += "%s," % (id)
	
	album_dict = {}
	#last_get_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%m:%S")
	#sql = '''select * from CenterApi where album_id in (4951470)'''
	sql = '''select * from CenterApi where album_id in (%s)''' % (ids_str.rstrip(","))
	#sql = '''select * from CenterApi where ctif_scheduled_release_time in ("201811130000","201811131000","201811131100") limit %s''' % (limit)
	cnt = cur.execute(sql)
	if cnt > 0:
		rets = cur.fetchall()
		for ret in rets:
			if album_dict.has_key(ret["album_id"]):
				album_dict[ret["album_id"]][ret["track_id"]] = ret
			else:
				album_dict[ret["album_id"]] = {}
				album_dict[ret["album_id"]][ret["track_id"]] = ret
			#if album_dict.has_key(ret["album_id"]):
			#	album_dict[ret["album_id"]].append(ret)
			#else:
			#	album_dict[ret["album_id"]] = []
			#	album_dict[ret["album_id"]].append(ret)
		conn.commit()
	return album_dict

def get_release_date_recently(last_get_time,conn,cur,limit):
	album_dict = {}
	#last_get_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%m:%S")
	sql = '''select * from CenterApi where ctime>"%s" limit %s''' % (last_get_time,limit)
	cnt = cur.execute(sql)
	if cnt > 0:
		rets = cur.fetchall()
		for ret in rets:
			album_info = get_new_album(ret["album_id"],conn,cur)
			if len(album_info) == 0:
				continue
			inter_days = 10
			if album_info.has_key("release_time"):
				if album_info["release_time"][0:10] != "0000-00-00":
					inter_days = calTime(datetime.datetime.now().strftime("%Y-%m-%d"),album_info["release_time"][0:10])
			if inter_days < 7:
				if album_dict.has_key(ret["album_id"]):
					album_dict[ret["album_id"]][ret["track_id"]] = ret
				else:
					album_dict[ret["album_id"]] = {}
					album_dict[ret["album_id"]][ret["track_id"]] = ret
			#if album_dict.has_key(ret["album_id"]):
			#	album_dict[ret["album_id"]].append(ret)
			#else:
			#	album_dict[ret["album_id"]] = []
			#	album_dict[ret["album_id"]].append(ret)
	
		conn.commit()
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
	m_status = 0
	sql = '''select id,m_status from MusicSrc where mid=%s''' % (mid)
	cnt = cur.execute(sql)
	if cnt > 0:
		ret = cur.fetchone()
		aid = ret["id"]
		m_status = ret["m_status"]
	return aid,m_status

def insert_MusicSrc(config,from_aid,priority,info,connData,curData,connSrc,curSrc):
	sql_fields = ""
	sql_values = ""
	c_show_type = 10
	fields_map = config["MusicMap"]
	for k,v in fields_map.items():
		if v == "timing_online" and info.has_key(k) and len(info[k])==12:
			sql_fields += "%s," % v
			time_arr = time.strptime(info[k],"%Y%m%d%H%M")
			time_str = time.strftime("%Y-%m-%d %H:%M:00",time_arr)
			sql_values += "\"%s\"," % MySQLdb.escape_string(str(time_str))
			c_show_type = 0
			continue
		elif v == "timing_online":
			continue
		sql_fields += "%s," % v
		sql_values += "\"%s\"," % MySQLdb.escape_string(str(info[k]))
	for k,v in config["MusicConst"].items():
		sql_fields += "%s," % k
		if k == "c_show_type":
			sql_values += "%s," % c_show_type
		elif k == "priority":
			sql_values += "%s," % 9
		else:
			sql_values += "%s," % v
	sql_fields += "%s," % ("from_aid")
	sql_values += "%s," % (from_aid)
	sql_fields += "%s," % ("source_type")
	sql_values += "%s," % (1)
	pay_flag = 0x0;
	if info.has_key("ctif_is_pay") and int(info["ctif_is_pay"]) == 1:
		pay_flag = pay_flag | 0x1
	if info.has_key("ctif_is_month_pay") and int(info["ctif_is_month_pay"]) == 1:
		pay_flag = pay_flag | (0x1 << 1)
	if info.has_key("ctif_is_cache_pay") and int(info["ctif_is_cache_pay"]) == 1:
		pay_flag = pay_flag | (0x1 << 2)
	#url,file_type = get_resource_url(1,info["track_id"],connData,curData)
	#sql_fields += "%s," % "m_audio_url"
	#sql_values += "\"%s\"," % url
	sql_fields += "%s," % "pay_flag"
	sql_values += "%s," % pay_flag
	if not config["MusicConst"].has_key("priority"):
		sql_fields += "%s," % "priority"
		sql_values += "%s," % priority
	sql = '''insert into `MusicSrc` (%s) values (%s)''' % (sql_fields.rstrip(","), sql_values.rstrip(","))
	logging.info(sql)
	cnt = curSrc.execute(sql)
	connSrc.commit()
	
def update_MusicSrc(config,mid,info,connData,curData,connSrc,curSrc):
	sql_fields = ""
	sql_values = ""
	sql_update = ""
	fields_map = config["MusicMap"]
	for k,v in fields_map.items():
		sql_fields += "%s," % v
		sql_values += "\"%s\"," % MySQLdb.escape_string(str(info[k]))
		sql_update += "%s=\"%s\"," % (v,MySQLdb.escape_string(str(info[k])))
	for k,v in config["MusicConst"].items():
		sql_fields += "%s," % k
		sql_values += "%s," % v
		sql_update += "%s=\"%s\"," % (k,v)
	#sql_fields += "%s," % "priority"
	#sql_values += "%s," % priority
	#sql_update += "%s=%s," % ("retry_count",0)
	#sql_update += "%s=%s," % ("sig_count",0)
	#sql_update += "%s=%s," % ("m_status",0)
	sql = '''update `MusicSrc` set %s where mid=%s''' % (sql_update.rstrip(","),mid)
	logging.info(sql)
	cnt = curSrc.execute(sql)
	if cnt > 0:
		connSrc.commit()
	sql = '''update `MusicSrc` set retry_count=0,sig_count=0,m_status=0 where mid=%s''' % (mid)
	cnt = curSrc.execute(sql)
	if cnt > 0:
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
	inter_days = 100
	top_art = False
	artistset = set()
	if album_info.has_key("singer_name") and album_info.has_key("release_time"):
		if album_info["release_time"][0:10] != "0000-00-00":
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
				#sql_values += "%s," % 10
				priority = 10
			elif inter_days <=30 and top_art:
				#sql_values += "%s," % 9
				priority = 9
			elif top_art:
				priority = 8
			elif inter_days <=30:
				priority = 7
			sql_values += "%s," % 9
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

def checkAlbumPay(track_id,conn,cur):
	notify = 0
	sql = '''select notify from CenterAccessApi where track_id=%s limit 1''' % (track_id)
	cnt = cur.execute(sql)
	if cnt > 0:
		ret = cur.fetchone()
		notify = ret["notify"]
	return notify

def data_get_pool(config,connData,curData,connSrc,curSrc,album_dict,limit):
	###1.check music, return dict:albumid-trackids
	#last_get_time = (datetime.datetime.now()+datetime.timedelta(seconds=-3600)).strftime("%Y-%m-%d %H:%m:%S")
	#last_get_time1 = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()-3600))
	artistTop = loadArtistTop("./artistTop3k.txt")
	#while True:
	from_aid = 0
	#last_get_time = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time()-1800))
	#album_dict = get_new_album_track(last_get_time,connData,curData,limit)
	#last_get_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%m:%S")
	#last_get_time1 = last_get_time
	#logging.info("last-get-time:%s" % (last_get_time))
	for album_id,infos in album_dict.items():
		###2.check album exists
		#print album_id
		if len(infos) == 0:
			continue
		#is_digit_album = False
		#for track_id,info in infos.items():
		#	if checkAlbumPay(track_id,connData,curData) == 7:
		#		is_digit_album = True
		#		break
		#if is_digit_album:
		#	continue
		if int(album_id) > 0:
			from_aid,priority = checkSrcAlbum(album_id,connSrc,curSrc)
			if priority == 0:
				priority = 6
			if from_aid > 0:
				###3.album exists, insert music
				for track_id,info in infos.items():
					mid,m_status = checkSrcMusic(info["track_id"],connSrc,curSrc)
					if mid > 0 and m_status == 9:
						logging.info("mid : %s exists" % (info["track_id"]))
						logging.info("mid : %s update" % (info["track_id"]))
						update_MusicSrc(config,info["track_id"],info,connData,curData,connSrc,curSrc)
						continue
					elif mid > 0:
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
				for track_id,info in infos.items():
					mid,m_status = checkSrcMusic(info["track_id"],connSrc,curSrc)
					if mid > 0 and m_status == 9:
						logging.info("mid : %s exists" % (info["track_id"]))
						logging.info("mid : %s update" % (info["track_id"]))
						update_MusicSrc(config,info["track_id"],info,connData,curData,connSrc,curSrc)
						continue
					elif mid > 0:
						logging.info("mid : %s exists" % (info["track_id"]))
						continue
					insert_MusicSrc(config,from_aid,priority,info,connData,curData,connSrc,curSrc)
		else:
			from_aid = 0
			priority = 6
			for track_id,info in infos.items():
				mid,m_status = checkSrcMusic(info["track_id"],connSrc,curSrc)
				if mid > 0 and m_status == 9:
					logging.info("mid : %s exists" % (info["track_id"]))
					logging.info("mid : %s update" % (info["track_id"]))
					update_MusicSrc(config,info["track_id"],info,connData,curData,connSrc,curSrc)
					continue
				elif mid > 0:
					logging.info("mid : %s exists" % (info["track_id"]))
					continue
				insert_MusicSrc(config,from_aid,priority,info,connData,curData,connSrc,curSrc)
		#break
		#logging.info("sleep 180s")
		#time.sleep(180)

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
		album_dict = get_new_album_track_file(sys.argv[1],g_connData,g_curData,200000)
		logging.info("end last-get-time:%s" % (last_get_time))
		last_get_time = last_get_time1
		data_get_pool(g_config.configinfo,g_connData,g_curData,g_connSrc,g_curSrc,album_dict,200000)
		#logging.info("sleep 180s")
		#time.sleep(180)
		
		g_curData.close()
		g_connData.close()
		g_curSrc.close()
		g_connSrc.close()
		break
