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
from pooldb import *
from sqlClass import *
from taskClass import *

reload(sys)
sys.setdefaultencoding('utf-8')

def get_taskids(conn,cur):
	ids = {}
	sql = '''select taskid,id,tmeid from ArtistSrc where m_status=%s limit 100''' % (g_status["task_send"])
	cnt = cur.execute(sql)
	if cnt > 0:
		ret = cur.fetchall()
		conn.commit()
		for r in ret:
			ids[r["taskid"]] = [r["tmeid"],r["id"]]
	return ids

def update_ArtistId_poll():
	g_connSrc = g_pool_Src.connection()
	g_curSrc = g_connSrc.cursor()

	g_connRun = g_pool_Run.connection()
	g_curRun = g_connRun.cursor()

	g_connRes = g_pool_Res.connection()
	g_curRes = g_connRes.cursor()

	tableSrc = "ArtistSrc"
	task_type = "Artist"
	config_task = g_config.configinfo["ArtistSrc"]
        sql_class = sqlClass(g_connSrc,g_curSrc)
        task_class = Task(g_connRun,g_curRun,g_connRes,g_curRes)
	#while True:
	#where = "m_status in (%s) and taskid > 0" % (g_status["task_send"])
	#param = ["id","tmeid","taskid"]
	#results = sql_class.mysqlSelect(tableSrc,where,500,param)
	taskinfos = get_taskids(g_connSrc,g_curSrc)
	while len(taskinfos)>0:
		for taskid,ret in taskinfos.items():
			p_id = ret[1]
			tmeid = ret[0]
			table_id,status = task_class.getRealIdfromTask(taskid,task_type)
			if status == False:
				logging.info("select dmsruntime error")
			elif table_id > 0 and status == "success":
				logging.info("tableid:%s status:%s" % (table_id,status))
				where = "id=%s" % (p_id)
				param_dict = {}
				param_dict["m_artist_id"] = table_id
				param_dict["m_status"] = g_status["task_suc"]
				cnt = sql_class.mysqlUpdate(tableSrc,where,param_dict)
				if cnt <= 0:
					logging.info("update src table error")
			elif status == "fail":
				where = "id=%s" % (p_id)
				param_dict = {}
				param_dict["m_artist_id"] = table_id
				param_dict["m_status"] = g_status["task_fail"]
				cnt = sql_class.mysqlUpdate(tableSrc,where,param_dict)
				if cnt <= 0:
					logging.info("update src table error")
			#break
		break
	g_curSrc.close()
	g_connSrc.close()
	g_curRun.close()
	g_connRun.close()
	g_curRes.close()
	g_connRes.close()

if __name__ == '__main__':
	update_ArtistId_poll()
