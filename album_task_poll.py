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
from resource_request import *

reload(sys)
sys.setdefaultencoding('utf-8')

###global###
G_CONFIG_FILE = "./config.conf.2"
g_config = utils.loadConfig(G_CONFIG_FILE)
g_sqlConfig = g_config["dbconfig"]
g_connSrc = MySQLdb.connect(host=g_sqlConfig["src_dbhost"],user=g_sqlConfig["src_dbuser"],passwd=g_sqlConfig["src_dbpwd"],db=g_sqlConfig["src_dbname"],charset=g_sqlConfig["src_dbcharset"],port=g_sqlConfig["src_dbport"],cursorclass=MySQLdb.cursors.DictCursor)
g_curSrc = g_connSrc.cursor()
g_connRes = MySQLdb.connect(host=g_sqlConfig["res_dbhost"],user=g_sqlConfig["res_dbuser"],passwd=g_sqlConfig["res_dbpwd"],db=g_sqlConfig["res_dbname"],charset=g_sqlConfig["res_dbcharset"],port=g_sqlConfig["res_dbport"])
g_curRes = g_connRes.cursor()
g_connRun = MySQLdb.connect(host=g_sqlConfig["task_dbhost"],user=g_sqlConfig["task_dbuser"],passwd=g_sqlConfig["task_dbpwd"],db=g_sqlConfig["task_dbname"],charset=g_sqlConfig["task_dbcharset"],port=g_sqlConfig["task_dbport"])
g_curRun = g_connRun.cursor()

g_status = {"default":0,"dispatch":1,"sig_ok":2,"artist_ok":3,"task_send":4,"artist_no":5,"task_fail":6,"task_suc":7,"sig_fail":8}

def initlog():
        logger = logging.getLogger()
        LOG_FILE = "log/" + sys.argv[0].split("/")[-1].replace(".py","") + '.log'
        hdlr = TimedRotatingFileHandler(LOG_FILE,when='H',backupCount=24)
        formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(filename)s[line:%(lineno)d]:%(message)s')
        hdlr.setFormatter(formatter)
        logger.addHandler(hdlr)
        logger.setLevel(logging.NOTSET)
        return logger

logging = initlog()

def update_AlbumSrc(fields_dict,where,connSrc,curSrc):
	sql_field = ""
	where_field = ""
	for k,v in fields_dict.items():
		sql_field += "%s=%s," % (k,v)
	for k,v in where.items():
		where_field += "%s=%s and" % (k,v)
	sql = "update AlbumSrc set %s where %s" % (sql_field.rstrip(","),where_field.rstrip(" and"))
	cnt = curSrc.execute(sql)
	if cnt > 0:
		connSrc.commit()

def update_MusicSrc(field_dict,where,connSrc,curSrc):
	sql_field = ""
	where_field = ""
	for k,v in field_dict.items():
		sql_field += "%s=%s," % (k,v)
	for k,v in where.items():
		where_field += "%s=%s and" % (k,v)
	sql = "update MusicSrc set %s where %s" % (sql_field.rstrip(","),where_field.rstrip(" and"))
	cnt = curSrc.execute(sql)
	if cnt > 0:
		connSrc.commit()

def select_MusicSrc(fields,where,connSrc,curSrc):
	sql_field = ""
	where_field = ""
	for k in fields:
		sql_field += "%s," % (k)
	for k,v in where.items():
		where_field += "%s=%s and" % (k,v)
	sql = "select %s MusicSrc where %s" % (sql_field.rstrip(","),where_field.rstrip(" and"))
	cnt = curSrc.execute(sql)
	if cnt > 0:
		return

def get_id_AlbumSrc_from_taskid(id,connSrc,curSrc):
	aid = 0
	sql = '''select id from AlbumSrc where taskid=%s''' % (id)
	cnt = curSrc.execute(sql)
	if cnt > 0:
		ret = curSrc.fetchone()
		aid = ret["id"]
		connSrc.commit()
	return aid

def get_taskids(conn,cur):
	ids = {}
	sql = '''select taskid,tx_albumid,source_type from AlbumSrc where m_status=%s limit 5000''' % (g_status["task_send"])
	cnt = cur.execute(sql)
	if cnt > 0:
		ret = cur.fetchall()
		conn.commit()
		for r in ret:
			ids[r["taskid"]] = [r["tx_albumid"],r["source_type"]]
	return ids

def get_realId_from_task(taskid,conn,cur):
	table_id = 0
	status = "none"
	sql = '''select table_id,status from DMSTask.Action where task_id=%s and `table`="%s" limit 1''' % (taskid,"Album")
	cnt = cur.execute(sql)
	print sql
	if cnt > 0:
		ret = cur.fetchone()
		table_id = ret[0]
		status = ret[1]
		conn.commit()
	return table_id,status

def get_music_no_albumid(conn,cur):
	albumid = {}
	sql = '''select b.id,a.m_album_id from AlbumSrc as a,MusicSrc as b where b.m_album_id=0 and a.id=b.from_aid'''
	cnt = cur.execute(sql)
	if cnt > 0:
		ret = cur.fetchall()
		conn.commit()
		for r in ret:
			print r
			albumid[int(r["id"])] = int(r["m_album_id"])
	return albumid

def update_music_albumId_poll():
	albumidmap = get_music_no_albumid(g_connSrc,g_curSrc)
	for from_aid,m_album_id in albumidmap.items():
		###check task if success and get table_id
		logging.info("id: %s  m_album_id: %s" % (from_aid,m_album_id))
		###update MusicSrc
		if from_aid > 0 and m_album_id > 0:
			where = {"id":from_aid}
			fields = {"m_album_id":m_album_id}
			update_MusicSrc(fields,where,g_connSrc,g_curSrc)
		#break

def update_albumId_poll():
	###1.select task from AlbumSrc
	taskids = get_taskids(g_connSrc,g_curSrc)
	if len(taskids) == 0:
		logging.info("no task")
	while len(taskids) > 0:
		taskComplete = set()
		for taskid,info in taskids.items():
			tx_albumid = info[0]
			source_type = info[1]
			###check task if success and get table_id
			table_id,status = get_realId_from_task(taskid,g_connRun,g_curRun)
			logging.info("process task %s" % (taskid))
			logging.info("table_id: %s   status: %s" % (table_id,status))
			if status == "none":
				taskComplete.add(taskid)
				continue
			if status == "fail":
				###update AlbumSrc
				where = {"taskid":taskid}
				fields = {"m_status":g_status["task_fail"]}
				update_AlbumSrc(fields,where,g_connSrc,g_curSrc)
				taskComplete.add(taskid)
				continue
			if status == "success":
				###update AlbumSrc
				where = {"taskid":taskid}
				fields = {"m_status":g_status["task_suc"],"m_album_id":table_id}
				update_AlbumSrc(fields,where,g_connSrc,g_curSrc)
				if source_type == 1:
					notify_sync_request(1,tx_albumid,2,table_id,4)
				taskComplete.add(taskid)
				#aid = get_id_AlbumSrc_from_taskid(taskid,g_connSrc,g_curSrc)
				###update MusicSrc
				#if aid > 0:
				#	where = {"from_aid":aid}
				#	fields = {"m_album_id":table_id}
					#update_MusicSrc(fields,where,g_connSrc,g_curSrc)
				continue
		update_music_albumId_poll()
		#logging.info("sleep 300s")
		#time.sleep(300)
		#for key in taskComplete:
                #	taskids.remove(key)
                logging.info("task numbers : %s" % (len(taskids)))
                #taskComplete.clear()
		#if len(taskids) == 0:
		taskids = get_taskids(g_connSrc,g_curSrc)
		break

if __name__ == "__main__":
	update_albumId_poll()
