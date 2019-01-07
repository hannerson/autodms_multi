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

def get_new_album_track_byid(albumid,conn,cur,limit):
	album_dict = {}
	sql = '''select * from CenterApi where album_id=%s''' % (albumid)
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

def get_new_album_track_by_trackid(track_id,conn,cur):
	album_dict = {}
	sql = '''select * from CenterApi where track_id=%s''' % (track_id)
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

def get_new_album_track_manual(last_get_time,conn,cur,limit):
	album_dict = {}
	#last_get_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%m:%S")
	sql = '''select * from CenterApi where album_id in (5771848)'''
	#sql = '''select * from CenterApi where album_id=0 limit 10'''
	#sql = '''select * from CenterApi where track_id in (226056685,226056686)'''
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

def get_new_album_track(last_get_time,conn,cur,limit):
	album_dict = {}
	#last_get_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%m:%S")
	sql = '''select * from CenterApi where ctime>"%s" limit %s''' % (last_get_time,limit)
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

def get_new_album_track_utime(timestamp,conn,cur,limit):
	album_dict = {}
	sql = '''select * from CenterApi where utime>%s limit %s''' % (timestamp,limit)
	cnt = cur.execute(sql)
	if cnt > 0:
		rets = cur.fetchall()
		for ret in rets:
			if album_dict.has_key(ret["album_id"]):
				album_dict[ret["album_id"]][ret["track_id"]] = ret
			else:
				album_dict[ret["album_id"]] = {}
				album_dict[ret["album_id"]][ret["track_id"]] = ret
		conn.commit()
	return album_dict

def get_editor_album(conn,cur):
	albumids = {}
	sql = '''select * from EditorQAlbum where status=0'''
	cnt = cur.execute(sql)
	if cnt > 0:
		rets = cur.fetchall()
		for r in rets:
			albumids[r["qaid"]] = r["priority"]
	return albumids

def update_editor_album(albumid,conn,cur):
	sql = '''update EditorQAlbum set status=1 where qaid=%s''' % (albumid)
	cnt = cur.execute(sql)
	if cnt > 0:
		conn.commit()

def insert_editor_album(albumid,conn,cur):
	try:
		sql = '''insert into EditorQAlbum (`qaid`) values (%s)''' % (albumid)
		cnt = cur.execute(sql)
		if cnt > 0:
			conn.commit()
	except Exception,e:
		logging.error(str(e))
		pass

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

def get_release_date_recently_utime(timestamp,conn,cur,limit):
	album_dict = {}
	#last_get_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%m:%S")
	sql = '''select * from CenterApi where utime>%s limit %s''' % (timestamp,limit)
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
	sql = '''select * from CenterAlbumApi where album_id=%s order by sid desc''' % (tx_albumid)
	cnt = cur.execute(sql)
	if cnt > 0:
		album = cur.fetchone()
		conn.commit()
	return album

def checkSrcArtist(tx_songerid,conn,cur):
	tmeid = -1
	m_artist_id = 0
	sql = '''select tmeid,m_artist_id from ArtistSrc where tmeid=%s''' % (tx_songerid)
	cnt = cur.execute(sql)
	if cnt > 0:
		ret = cur.fetchone()
		tmeid = ret["tmeid"]
		m_artist_id = ret["m_artist_id"]
	return tmeid,m_artist_id

def checkSrcAlbum(tx_albumid,name,artists,conn,cur):
	aid = 0
	priority = 0
	if int(tx_albumid) > 0:
		sql = '''select id,priority from AlbumSrc where tx_albumid=%s''' % (tx_albumid)
		cnt = cur.execute(sql)
		if cnt > 0:
			ret = cur.fetchone()
			aid = ret["id"]
			priority = ret["priority"]
	else:
		sql = '''select id,priority from AlbumSrc where m_name=\"%s\" and m_artists=\"%s\"''' % (MySQLdb.escape_string(name),MySQLdb.escape_string(artists))
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

def insert_ArtistSrc(config,info,connData,curData,connSrc,curSrc):
	sql_fields = ""
	sql_values = ""
	c_show_type = 0
	fields_map = config["ArtistMap"]
	for k,v in fields_map.items():
		sql_fields += "%s," % v
		sql_values += "\"%s\"," % MySQLdb.escape_string(str(info[k]))
	for k,v in config["ArtistConst"].items():
		sql_fields += "%s," % k
		if k == "c_show_type":
			sql_values += "%s," % c_show_type
		else:
			sql_values += "%s," % v
	sql = '''insert into `ArtistSrc` (%s) values (%s)''' % (sql_fields.rstrip(","), sql_values.rstrip(","))
	logging.info(sql)
	cnt = curSrc.execute(sql)
	connSrc.commit()

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
	
def update_MusicSrc(config,mid,info,status,connData,curData,connSrc,curSrc):
	sql_fields = ""
	sql_values = ""
	sql_update = ""
	fields_map = config["MusicMap"]
	priority = 7
	for k,v in fields_map.items():
		if v == "timing_online" and info.has_key(k) and len(info[k])==12:
			sql_fields += "%s," % v
			time_arr = time.strptime(info[k],"%Y%m%d%H%M")
			time_str = time.strftime("%Y-%m-%d %H:%M:00",time_arr)
			sql_values += "\"%s\"," % MySQLdb.escape_string(str(time_str))
			c_show_type = 0
			sql_update += "%s=\"%s\"," % (v,MySQLdb.escape_string(str(time_str)))
			priority = 10
			continue
		elif v == "timing_online":
			continue
		sql_fields += "%s," % v
		sql_values += "\"%s\"," % MySQLdb.escape_string(str(info[k]))
		sql_update += "%s=\"%s\"," % (v,MySQLdb.escape_string(str(info[k])))
	for k,v in config["MusicConst"].items():
		if k == "priority" and priority == 7:
			continue
		sql_fields += "%s," % k
		sql_values += "%s," % v
		sql_update += "%s=\"%s\"," % (k,v)
	#sql_fields += "%s," % "priority"
	#sql_values += "%s," % priority
	sql_update += "%s=%s," % ("retry_count",0)
	sql_update += "%s=%s," % ("sig_count",0)
	sql_update += "%s=%s," % ("m_status",status)
	sql = '''update `MusicSrc` set %s where mid=%s''' % (sql_update.rstrip(","),mid)
	logging.info(sql)
	cnt = curSrc.execute(sql)
	if cnt > 0:
		connSrc.commit()
	#sql = '''update `MusicSrc` set retry_count=0,sig_count=0,m_status=%s where mid=%s''' % (mid,status)
	#cnt = curSrc.execute(sql)
	#if cnt > 0:
	#	connSrc.commit()

def update_MusicSrc2(config,mid,info,status,connData,curData,connSrc,curSrc):
	sql_fields = ""
	sql_values = ""
	sql_update = ""
	fields_map = config["MusicMap"]
	priority = 7
	for k,v in fields_map.items():
		if v == "timing_online" and info.has_key(k) and len(info[k])==12:
			sql_fields += "%s," % v
			time_arr = time.strptime(info[k],"%Y%m%d%H%M")
			time_str = time.strftime("%Y-%m-%d %H:%M:00",time_arr)
			sql_values += "\"%s\"," % MySQLdb.escape_string(str(time_str))
			c_show_type = 0
			sql_update += "%s=\"%s\"," % (v,MySQLdb.escape_string(str(time_str)))
			priority = 10
			continue
		elif v == "timing_online":
			continue
		sql_fields += "%s," % v
		sql_values += "\"%s\"," % MySQLdb.escape_string(str(info[k]))
		sql_update += "%s=\"%s\"," % (v,MySQLdb.escape_string(str(info[k])))
	for k,v in config["MusicConst"].items():
		if k == "priority" and priority == 7:
			continue
		sql_fields += "%s," % k
		sql_values += "%s," % v
		sql_update += "%s=\"%s\"," % (k,v)
	#sql_fields += "%s," % "priority"
	#sql_values += "%s," % priority
	sql_update += "%s=%s," % ("retry_count",0)
	sql_update += "%s=%s," % ("sig_count",0)
	sql_update += "%s=%s," % ("m_status",status)
	sql_update += "%s=%s," % ("kw_id",0)
	sql = '''update `MusicSrc` set %s where mid=%s''' % (sql_update.rstrip(","),mid)
	logging.info(sql)
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

def getCompany(cp_id,conn,cur):
        company = ""
        sql = '''select company_name from CenterCorpApi where company_id=%s''' % (cp_id)
        cnt = cur.execute(sql)
        if cnt > 0:
                ret = cur.fetchone()
                company = ret["company_name"]
        return company

###info:[{album_id,album_name,singer_name,track_ids,desc,language,upc,company,album_type,release_time},{}]
def insert_AlbumSrc(album_id,album_info,timing_online,artistTop,config,connData,curData,connSrc,curSrc):
	from_aid = 0
	priority = 6
	sql_fields = ""
	sql_values = ""
	fields_map = config["AlbumMap"]
	if int(album_id) > 0:
		album_info = get_new_album(album_id,connData,curData)
		###check Artist and insert Artist
		if album_info["singer_name"] != "" and album_info["singer_id"] != "":
			names = album_info["singer_name"].strip().split("###")
			artids = album_info["singer_id"].strip().split("###")
			if len(names) != len(artids):
				logging.error("singername track %s: names %s - artids %s" % (track_id,str(names),str(artids)))
			else:
				for i in range(len(names)):
					artinfo = {}
					artinfo["singer_name"] = names[i]
					artinfo["singer_id"] = artids[i]
					tmeid,m_artist_id = checkSrcArtist(artinfo["singer_id"],connSrc,curSrc)
					if tmeid > -1:
						continue
					else:
						insert_ArtistSrc(config,artinfo,connData,curData,connSrc,curSrc)
	#print album_info
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
	company = ""
	for k,v in fields_map.items():
		if not album_info.has_key(k):
			continue
		if v == "basic_company":
			if album_info.has_key("cp_id") and int(album_info["cp_id"]) > 0:
				company = getCompany(album_info["cp_id"],connData,curData)
			sql_fields += "%s," % v
			sql_values += "\"%s\"," % MySQLdb.escape_string(company)
			continue
		sql_fields += "%s," % v
		sql_values += "\"%s\"," % MySQLdb.escape_string(str(album_info[k]))
	for k,v in config["AlbumConst"].items():
		sql_fields += "%s," % k
		if k == "priority":
			if inter_days <=7:
				#sql_values += "%s," % 10
				priority = 10
			elif inter_days <=30 and top_art:
				#sql_values += "%s," % 9
				priority = 9
			elif top_art:
				priority = 8
			elif inter_days <=30:
				priority = 7
			sql_values += "%s," % priority
		else:
			sql_values += "%s," % v
	if len(timing_online) == 12:
		time_arr = time.strptime(timing_online,"%Y%m%d%H%M")
		time_str = time.strftime("%Y-%m-%d %H:%M:00",time_arr)
		sql_fields += "%s," % ("timing_online")
		sql_values += "\"%s\"," % MySQLdb.escape_string(str(time_str))
		c_show_type = 0
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

def createTimefile(filename,time_str):
	f = open(filename,"w+")
	f.write("%s\n" % (time_str))
	f.close()

def getTimefile(filename):
	time_str = ""
	f = open(filename,"r")
	for line in f:
		time_str = line.strip()
	f.close()
	return time_str

def data_get_pool(config,connData,curData,connSrc,curSrc,album_dict,limit):
	###1.check music, return dict:albumid-trackids
	artistTop = loadArtistTop("./artistTop3k.txt")
	#while True:
	from_aid = 0
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
		timing_online = ""
		for track_id,info in infos.items():
			timing_online = info["ctif_scheduled_release_time"]
			break
		if int(album_id) > 0 and int(album_id) != 8623:
			##album_info = get_new_album(album_id,connData,curData)
			####check Artist and insert Artist
			#if album_info["singer_name"] != "" and album_info["singer_id"] != "":
			#	names = album_info["singer_name"].strip().split("###")
			#	artids = album_info["singer_id"].strip().split("###")
			#	if len(names) != len(artids):
			#		logging.error("singername track %s: names %s - artids %s" % (track_id,str(names),str(artids)))
			#	else:
			#		for i in range(len(names)):
			#			artinfo = {}
			#			artinfo["singer_name"] = names[i]
			#			artinfo["singer_id"] = artids[i]
			#			tmeid,m_artist_id = checkSrcArtist(artinfo["singer_id"],connSrc,curSrc)
			#			if tmeid > -1:
			#				continue
			#			else:
			#				insert_ArtistSrc(config,artinfo,connData,curData,connSrc,curSrc)
			from_aid,priority = checkSrcAlbum(album_id,"","",connSrc,curSrc)
			if priority == 0:
				priority = 7
			if from_aid > 0:
				###3.album exists, insert music
				for track_id,info in infos.items():
					###check Artist and insert Artist
					if info["singer_name"] != "" and info["singer_id"] != "":
						names = info["singer_name"].strip().split("###")
						artids = info["singer_id"].strip().split("###")
						if len(names) != len(artids):
							logging.error("singername track %s: names %s - artids %s" % (track_id,str(names),str(artids)))
						else:
							for i in range(len(names)):
								artinfo = {}
								artinfo["singer_name"] = names[i]
								artinfo["singer_id"] = artids[i]
								tmeid,m_artist_id = checkSrcArtist(artinfo["singer_id"],connSrc,curSrc)
								if tmeid > -1:
									continue
								else:
									insert_ArtistSrc(config,artinfo,connData,curData,connSrc,curSrc)

					mid,m_status = checkSrcMusic(info["track_id"],connSrc,curSrc)
					if mid > 0 and m_status in [0,2,6,9,10,11,12,4]:
						logging.info("mid : %s exists" % (info["track_id"]))
						logging.info("mid : %s update" % (info["track_id"]))
						if m_status in [6,9,10,11,12]:
							update_MusicSrc(config,info["track_id"],info,0,connData,curData,connSrc,curSrc)
						else:
							update_MusicSrc(config,info["track_id"],info,m_status,connData,curData,connSrc,curSrc)
						continue
					elif mid > 0:
						logging.info("mid : %s exists" % (info["track_id"]))
						continue
					insert_MusicSrc(config,from_aid,priority,info,connData,curData,connSrc,curSrc)
			else:
				###3.insert album, insert music
				#album_info = get_new_album(album_id,connData,curData)
				album_info = {}
				from_aid,priority = insert_AlbumSrc(album_id,album_info,timing_online,artistTop,config,connData,curData,connSrc,curSrc)
				if from_aid == -1:
					logging.info("no album info %s" % (album_id))
					###insert fail album
					insert_editor_album(album_id,connSrc,curSrc)
					continue
				for track_id,info in infos.items():
					###check Artist and insert Artist
					if info["singer_name"] != "" and info["singer_id"] != "":
						names = info["singer_name"].strip().split("###")
						artids = info["singer_id"].strip().split("###")
						if len(names) != len(artids):
							logging.error("singername track %s: names %s - artids %s" % (track_id,str(names),str(artids)))
						else:
							for i in range(len(names)):
								artinfo = {}
								artinfo["singer_name"] = names[i]
								artinfo["singer_id"] = artids[i]
								tmeid,m_artist_id = checkSrcArtist(artinfo["singer_id"],connSrc,curSrc)
								if tmeid > -1:
									continue
								else:
									insert_ArtistSrc(config,artinfo,connData,curData,connSrc,curSrc)
					mid,m_status = checkSrcMusic(info["track_id"],connSrc,curSrc)
					if mid > 0 and m_status in [0,2,6,9,10,11,12,4]:
						logging.info("mid : %s exists" % (info["track_id"]))
						logging.info("mid : %s update" % (info["track_id"]))
						if m_status in [6,9,10,11,12]:
							update_MusicSrc(config,info["track_id"],info,0,connData,curData,connSrc,curSrc)
						else:
							update_MusicSrc(config,info["track_id"],info,m_status,connData,curData,connSrc,curSrc)
						continue
					elif mid > 0:
						logging.info("mid : %s exists" % (info["track_id"]))
						continue
					insert_MusicSrc(config,from_aid,priority,info,connData,curData,connSrc,curSrc)
		else:
			from_aid = 0
			priority = 6
			for track_id,info in infos.items():
				#if info.has_key("album_name") and info["album_name"] == "" and int(album_id) != 8623:
				#	continue
				###check Artist and insert Artist
				if info["singer_name"] != "" and info["singer_id"] != "":
					names = info["singer_name"].strip().split("###")
					artids = info["singer_id"].strip().split("###")
					if len(names) != len(artids):
						logging.error("singername track %s: names %s - artids %s" % (track_id,str(names),str(artids)))
					else:
						for i in range(len(names)):
							artinfo = {}
							artinfo["singer_name"] = names[i]
							artinfo["singer_id"] = artids[i]
							tmeid,m_artist_id = checkSrcArtist(artinfo["singer_id"],connSrc,curSrc)
							if tmeid > -1:
								continue
							else:
								insert_ArtistSrc(config,artinfo,connData,curData,connSrc,curSrc)

					if int(album_id) == 8623:
						pass
					elif int(album_id) == 0:
						album_info = {}
						if info["album_name"] != "" and info["singer_name"] != "":
							album_info["album_name"] = info["album_name"]
							album_info["singer_name"] = info["singer_name"]
							from_aid,priority = checkSrcAlbum(0,album_info["album_name"],album_info["singer_name"],connSrc,curSrc)
						if from_aid == 0:
							from_aid,priority = insert_AlbumSrc(album_id,album_info,timing_online,artistTop,config,connData,curData,connSrc,curSrc)

				mid,m_status = checkSrcMusic(info["track_id"],connSrc,curSrc)
				if mid > 0 and m_status in [0,2,6,9,10,11,12,4]:
					logging.info("mid : %s exists" % (info["track_id"]))
					logging.info("mid : %s update" % (info["track_id"]))
					if m_status in [6,9,10,11,12]:
						update_MusicSrc(config,info["track_id"],info,0,connData,curData,connSrc,curSrc)
					else:
						update_MusicSrc(config,info["track_id"],info,m_status,connData,curData,connSrc,curSrc)
					continue
				elif mid > 0:
					logging.info("mid : %s exists" % (info["track_id"]))
					continue
				insert_MusicSrc(config,from_aid,priority,info,connData,curData,connSrc,curSrc)

def data_get_pool_priority(config,pri_manual,connData,curData,connSrc,curSrc,album_dict,limit):
	###1.check music, return dict:albumid-trackids
	artistTop = loadArtistTop("./artistTop3k.txt")
	#while True:
	from_aid = 0
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
		timing_online = ""
		for track_id,info in infos.items():
			timing_online = info["ctif_scheduled_release_time"]
			break
		if int(album_id) > 0 and int(album_id) != 8623:
			from_aid,priority = checkSrcAlbum(album_id,"","",connSrc,curSrc)
			if priority == 0:
				priority = pri_manual
			priority = pri_manual
			if from_aid > 0:
				###3.album exists, insert music
				for track_id,info in infos.items():
					###check Artist and insert Artist
					if info["singer_name"] != "" and info["singer_id"] != "":
						names = info["singer_name"].strip().split("###")
						artids = info["singer_id"].strip().split("###")
						if len(names) != len(artids):
							logging.error("singername track %s: names %s - artids %s" % (track_id,str(names),str(artids)))
						else:
							for i in range(len(names)):
								artinfo = {}
								artinfo["singer_name"] = names[i]
								artinfo["singer_id"] = artids[i]
								tmeid,m_artist_id = checkSrcArtist(artinfo["singer_id"],connSrc,curSrc)
								if tmeid > -1:
									continue
								else:
									insert_ArtistSrc(config,artinfo,connData,curData,connSrc,curSrc)
					mid,m_status = checkSrcMusic(info["track_id"],connSrc,curSrc)
					if mid > 0 and m_status in [0,2,6,9,12,4]:
						logging.info("mid : %s exists" % (info["track_id"]))
						logging.info("mid : %s update" % (info["track_id"]))
						if m_status in [6,9,12]:
							update_MusicSrc(config,info["track_id"],info,0,connData,curData,connSrc,curSrc)
						else:
							update_MusicSrc(config,info["track_id"],info,m_status,connData,curData,connSrc,curSrc)
						continue
					elif mid > 0:
						logging.info("mid : %s exists" % (info["track_id"]))
						continue
					insert_MusicSrc(config,from_aid,priority,info,connData,curData,connSrc,curSrc)
			else:
				###3.insert album, insert music
				#album_info = get_new_album(album_id,connData,curData)
				album_info = {}
				from_aid,priority = insert_AlbumSrc(album_id,album_info,timing_online,artistTop,config,connData,curData,connSrc,curSrc)
				if from_aid == -1:
					logging.info("no album info %s" % (album_id))
					###insert fail album
					insert_editor_album(album_id,connSrc,curSrc)
					continue
				priority = pri_manual
				for track_id,info in infos.items():
					###check Artist and insert Artist
					if info["singer_name"] != "" and info["singer_id"] != "":
						names = info["singer_name"].strip().split("###")
						artids = info["singer_id"].strip().split("###")
						if len(names) != len(artids):
							logging.error("singername track %s: names %s - artids %s" % (track_id,str(names),str(artids)))
						else:
							for i in range(len(names)):
								artinfo = {}
								artinfo["singer_name"] = names[i]
								artinfo["singer_id"] = artids[i]
								tmeid,m_artist_id = checkSrcArtist(artinfo["singer_id"],connSrc,curSrc)
								if tmeid > -1:
									continue
								else:
									insert_ArtistSrc(config,artinfo,connData,curData,connSrc,curSrc)
					mid,m_status = checkSrcMusic(info["track_id"],connSrc,curSrc)
					if mid > 0 and m_status in [0,2,6,9,12,4]:
						logging.info("mid : %s exists" % (info["track_id"]))
						logging.info("mid : %s update" % (info["track_id"]))
						if m_status in [6,9,12]:
							update_MusicSrc(config,info["track_id"],info,0,connData,curData,connSrc,curSrc)
						else:
							update_MusicSrc(config,info["track_id"],info,m_status,connData,curData,connSrc,curSrc)
						continue
					elif mid > 0:
						logging.info("mid : %s exists" % (info["track_id"]))
						continue
					insert_MusicSrc(config,from_aid,priority,info,connData,curData,connSrc,curSrc)
		else:
			from_aid = 0
			priority = 6
			priority = pri_manual
			for track_id,info in infos.items():
				#if info.has_key("album_name") and info["album_name"] == "":
				#	continue
				###check Artist and insert Artist
				if info["singer_name"] != "" and info["singer_id"] != "":
					names = info["singer_name"].strip().split("###")
					artids = info["singer_id"].strip().split("###")
					if len(names) != len(artids):
						logging.error("singername track %s: names %s - artids %s" % (track_id,str(names),str(artids)))
					else:
						for i in range(len(names)):
							artinfo = {}
							artinfo["singer_name"] = names[i]
							artinfo["singer_id"] = artids[i]
							tmeid,m_artist_id = checkSrcArtist(artinfo["singer_id"],connSrc,curSrc)
							if tmeid > -1:
								continue
							else:
								insert_ArtistSrc(config,artinfo,connData,curData,connSrc,curSrc)

				if int(album_id) == 8623:
					pass
				elif int(album_id) == 0:
					album_info = {}
					if info["album_name"] != "" and info["singer_name"] != "":
						album_info["album_name"] = info["album_name"]
						album_info["singer_name"] = info["singer_name"]
						from_aid,priority = checkSrcAlbum(0,album_info["album_name"],album_info["singer_name"],connSrc,curSrc)
					if from_aid == 0:
						from_aid,priority = insert_AlbumSrc(album_id,album_info,timing_online,artistTop,config,connData,curData,connSrc,curSrc)

				mid,m_status = checkSrcMusic(info["track_id"],connSrc,curSrc)
				if mid > 0 and m_status in [0,2,6,9,12,4]:
					logging.info("mid : %s exists" % (info["track_id"]))
					logging.info("mid : %s update" % (info["track_id"]))
					if m_status in [6,9,12]:
						update_MusicSrc(config,info["track_id"],info,0,connData,curData,connSrc,curSrc)
					else:
						update_MusicSrc(config,info["track_id"],info,m_status,connData,curData,connSrc,curSrc)
					continue
				elif mid > 0:
					logging.info("mid : %s exists" % (info["track_id"]))
					continue
				insert_MusicSrc(config,from_aid,priority,info,connData,curData,connSrc,curSrc)
		#break
		#logging.info("sleep 180s")
		#time.sleep(180)

def data_get_pool_makeup(config,pri_manual,connData,curData,connSrc,curSrc,album_dict,limit):
	###1.check music, return dict:albumid-trackids
	artistTop = loadArtistTop("./artistTop3k.txt")
	#while True:
	from_aid = 0
	for album_id,infos in album_dict.items():
		###2.check album exists
		#print album_id
		if len(infos) == 0:
			continue
		timing_online = ""
		for track_id,info in infos.items():
			timing_online = info["ctif_scheduled_release_time"]
			break
		if int(album_id) > 0 and int(album_id) != 8623:
			album_info = get_new_album(album_id,connData,curData)
			###check Artist and insert Artist
			if album_info["singer_name"] != "" and album_info["singer_id"] != "":
				names = album_info["singer_name"].strip().split("###")
				artids = album_info["singer_id"].strip().split("###")
				if len(names) != len(artids):
					logging.error("singername track %s: names %s - artids %s" % (track_id,str(names),str(artids)))
				else:
					for i in range(len(names)):
						artinfo = {}
						artinfo["singer_name"] = names[i]
						artinfo["singer_id"] = artids[i]
						tmeid,m_artist_id = checkSrcArtist(artinfo["singer_id"],connSrc,curSrc)
						if tmeid > -1:
							continue
						else:
							insert_ArtistSrc(config,artinfo,connData,curData,connSrc,curSrc)
			from_aid,priority = checkSrcAlbum(album_id,"","",connSrc,curSrc)
			if priority == 0:
				priority = pri_manual
			priority = pri_manual
			if from_aid > 0:
				###3.album exists, insert music
				for track_id,info in infos.items():
					###check Artist and insert Artist
					if info["singer_name"] != "" and info["singer_id"] != "":
						names = info["singer_name"].strip().split("###")
						artids = info["singer_id"].strip().split("###")
						if len(names) != len(artids):
							logging.error("singername track %s: names %s - artids %s" % (track_id,str(names),str(artids)))
						else:
							for i in range(len(names)):
								artinfo = {}
								artinfo["singer_name"] = names[i]
								artinfo["singer_id"] = artids[i]
								tmeid,m_artist_id = checkSrcArtist(artinfo["singer_id"],connSrc,curSrc)
								if tmeid > -1:
									continue
								else:
									insert_ArtistSrc(config,artinfo,connData,curData,connSrc,curSrc)
					mid,m_status = checkSrcMusic(info["track_id"],connSrc,curSrc)
					if mid > 0 and m_status in [0,2,6,9,12,4]:
						logging.info("mid : %s exists" % (info["track_id"]))
						logging.info("mid : %s update" % (info["track_id"]))
						if m_status in [6,9,12]:
							update_MusicSrc2(config,info["track_id"],info,0,connData,curData,connSrc,curSrc)
						else:
							update_MusicSrc2(config,info["track_id"],info,m_status,connData,curData,connSrc,curSrc)
						continue
					elif mid > 0:
						logging.info("mid : %s exists" % (info["track_id"]))
						continue
					insert_MusicSrc(config,from_aid,priority,info,connData,curData,connSrc,curSrc)
			else:
				###3.insert album, insert music
				#album_info = get_new_album(album_id,connData,curData)
				album_info = {}
				from_aid,priority = insert_AlbumSrc(album_id,album_info,timing_online,artistTop,config,connData,curData,connSrc,curSrc)
				if from_aid == -1:
					logging.info("no album info %s" % (album_id))
					###insert fail album
					insert_editor_album(album_id,connSrc,curSrc)
					continue
				priority = pri_manual
				for track_id,info in infos.items():
					###check Artist and insert Artist
					if info["singer_name"] != "" and info["singer_id"] != "":
						names = info["singer_name"].strip().split("###")
						artids = info["singer_id"].strip().split("###")
						if len(names) != len(artids):
							logging.error("singername track %s: names %s - artids %s" % (track_id,str(names),str(artids)))
						else:
							for i in range(len(names)):
								artinfo = {}
								artinfo["singer_name"] = names[i]
								artinfo["singer_id"] = artids[i]
								tmeid,m_artist_id = checkSrcArtist(artinfo["singer_id"],connSrc,curSrc)
								if tmeid > -1:
									continue
								else:
									insert_ArtistSrc(config,artinfo,connData,curData,connSrc,curSrc)
					mid,m_status = checkSrcMusic(info["track_id"],connSrc,curSrc)
					if mid > 0 and m_status in [0,2,6,9,12,4]:
						logging.info("mid : %s exists" % (info["track_id"]))
						logging.info("mid : %s update" % (info["track_id"]))
						if m_status in [6,9,12]:
							update_MusicSrc2(config,info["track_id"],info,0,connData,curData,connSrc,curSrc)
						else:
							update_MusicSrc2(config,info["track_id"],info,m_status,connData,curData,connSrc,curSrc)
						continue
					elif mid > 0:
						logging.info("mid : %s exists" % (info["track_id"]))
						continue
					insert_MusicSrc(config,from_aid,priority,info,connData,curData,connSrc,curSrc)
		else:
			from_aid = 0
			priority = 6
			priority = pri_manual
			for track_id,info in infos.items():
				#if info.has_key("album_name") and info["album_name"] == "":
				#	continue
				###check Artist and insert Artist
				if info["singer_name"] != "" and info["singer_id"] != "":
					names = info["singer_name"].strip().split("###")
					artids = info["singer_id"].strip().split("###")
					if len(names) != len(artids):
						logging.error("singername track %s: names %s - artids %s" % (track_id,str(names),str(artids)))
					else:
						for i in range(len(names)):
							artinfo = {}
							artinfo["singer_name"] = names[i]
							artinfo["singer_id"] = artids[i]
							tmeid,m_artist_id = checkSrcArtist(artinfo["singer_id"],connSrc,curSrc)
							if tmeid > -1:
								continue
							else:
								insert_ArtistSrc(config,artinfo,connData,curData,connSrc,curSrc)

				if int(album_id) == 8623:
					pass
				elif int(album_id) == 0:
					album_info = {}
					if info["album_name"] != "" and info["singer_name"] != "":
						album_info["album_name"] = info["album_name"]
						album_info["singer_name"] = info["singer_name"]
						from_aid,priority = checkSrcAlbum(0,album_info["album_name"],album_info["singer_name"],connSrc,curSrc)
					if from_aid == 0:
						from_aid,priority = insert_AlbumSrc(album_id,album_info,timing_online,artistTop,config,connData,curData,connSrc,curSrc)

				mid,m_status = checkSrcMusic(info["track_id"],connSrc,curSrc)
				if mid > 0 and m_status in [0,2,6,9,12,4]:
					logging.info("mid : %s exists" % (info["track_id"]))
					logging.info("mid : %s update" % (info["track_id"]))
					if m_status in [6,9,12]:
						update_MusicSrc2(config,info["track_id"],info,0,connData,curData,connSrc,curSrc)
					else:
						update_MusicSrc2(config,info["track_id"],info,m_status,connData,curData,connSrc,curSrc)
					continue
				elif mid > 0:
					logging.info("mid : %s exists" % (info["track_id"]))
					continue
				insert_MusicSrc(config,from_aid,priority,info,connData,curData,connSrc,curSrc)
