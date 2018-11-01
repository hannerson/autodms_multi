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
import urllib2
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
g_connRelation = MySQLdb.connect(host=g_sqlConfig["relation_dbhost"],user=g_sqlConfig["relation_dbuser"],passwd=g_sqlConfig["relation_dbpwd"],db=g_sqlConfig["relation_dbname"],charset=g_sqlConfig["relation_dbcharset"],port=g_sqlConfig["relation_dbport"])
g_curRelation = g_connRelation.cursor()

g_connTMApi = MySQLdb.connect(host=g_sqlConfig["TMApi_dbhost"],user=g_sqlConfig["TMApi_dbuser"],passwd=g_sqlConfig["TMApi_dbpwd"],db=g_sqlConfig["TMApi_dbname"],charset=g_sqlConfig["TMApi_dbcharset"],port=g_sqlConfig["TMApi_dbport"],cursorclass=MySQLdb.cursors.DictCursor)
g_curTMApi = g_connTMApi.cursor()

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

def checkTMApiStatus2(mid,conn,cur):
	show_status = 0
	sql = '''select ctif_available_status from CenterApi where track_id="%s"''' % (mid)
	cnt = cur.execute(sql)
	if cnt > 0:
		ret = cur.fetchone()
		show_status = ret["ctif_available_status"]
	return show_status

def sendTaskOnline(mid):
    try:
        task_id = 0
        post_dict = {}
        Music = {}
        Music["id"] = "%s" % (mid)
        Music["c_show_type"] = "%s" % (0)
        info = []
        info.append({"Music":Music})
        post_dict["count"] = "%s" % (len(info))
        post_dict["info"] = info
        post_dict["priority"] = "%s" % (6)
        post_dict["editor_id"] = "%s" % (254)
        post_json = json.dumps(post_dict)
        print post_json
        #'''
        f = urllib2.urlopen(
            #url     = "http://103.79.26.21:33327/add_task",
            url     = "http://centerserver.kuwo.cn/add_task",
            data    = post_json
            )

        result = f.read()
        print result
        js_ret = json.loads(result)
        if result.find("OK") >= 0:
            task_id = int(js_ret["taskid"])
        #'''
        return task_id
    except Exception,e:
        traceback.print_exc()
        pass

def update_AlbumSrc(fields_dict,where,connSrc,curSrc):
	sql_field = ""
	where_field = ""
	for k,v in field_dict.items():
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

def checkMusictm(tm_id,connRun,curRun):
	mid1 = 0
	mid2 = 0
	sql = '''select id from Music where from_id = \"tm_%s\" and version_editor > 1 and version_pub > 0''' % (tm_id)
	cnt1 = curRun.execute(sql)
	if cnt1 > 0:
		ret = curRun.fetchone()
		mid1 = ret[0]
	sql = '''select id from Music where from_id = \"tx_%s\" and version_editor > 1 and version_pub > 0''' % (tm_id)
	cnt2 = curRun.execute(sql)
	if cnt2 > 0:
		ret = curRun.fetchone()
		mid2 = ret[0]
	if cnt1 + cnt2 > 0:
		if mid2 != 0:
			return mid2
		return mid1
	return 0

def checkMusictmFail(tm_id,connRun,curRun):
	mid1 = 0
	mid2 = 0
	sql = '''select id from Music where from_id = \"tm_%s\" and version_editor > 1''' % (tm_id)
	cnt1 = curRun.execute(sql)
	if cnt1 > 0:
		ret = curRun.fetchone()
		return 0
	sql = '''select id from Music where from_id = \"tm_%s\" and version_editor = 1 and version_pub = 0''' % (tm_id)
	cnt2 = curRun.execute(sql)
	if cnt2 > 0:
		ret = curRun.fetchone()
		return ret[0]
	if cnt1 + cnt2 == 0:
		return -1
	return 0

def checkMusicSrcSuc(conn, cur):
	mids = set()
	taskids = {}
	sql = '''select mid,taskid,source_type from MusicSrc where m_status=%s and mid>0 and is_tme<2''' % (g_status["task_send"])
	cnt = cur.execute(sql)
	if cnt > 0:
		ret = cur.fetchall()
		conn.commit()
		for r in ret:
			if not taskids.has_key(r["taskid"]):
				taskids[r["taskid"]] = []
			taskids[r["taskid"]].append([r["mid"],r["source_type"]])
	return taskids

def get_task_status(taskid,conn,cur):
	tsk_status = "none"
	sql = '''select status from DMSTask.Task where id=%s''' % (taskid)
	cnt = cur.execute(sql)
	if cnt > 0:
		ret = cur.fetchone()
		conn.commit()
		return ret[0]
	return tsk_status

def insert_KWRelation(kw_id,tx_id,conn,cur):
	try:
		sql = '''insert into KuwoTencentMusicRelation (rid,mid,level) values (%s,%s,1)''' % (kw_id,tx_id)
		cnt = cur.execute(sql)
		if cnt > 0:
			conn.commit()
			logging.info("insert relation (%s,%s)" % (kw_id,tx_id))
		else:
			logging.info("insert relation failed (%s,%s)" % (kw_id,tx_id))
	except Exception,e:
		logging.info(str(e))
		pass

def update_musicId_poll():
	###1.select task from MusicSrc
	taskids = checkMusicSrcSuc(g_connSrc,g_curSrc)
	if len(taskids) == 0:
		logging.info("no match task")
	while len(taskids) > 0:
		taskComplete = set()
		for taskid,mids in taskids.items():
			status = get_task_status(taskid, g_connRun, g_curRun)
			logging.info("%s status %s" % (taskid,status))
			if status != "success" and status != "fail":
				continue
			for id,source_type in mids:
				###check if id in dms
				kuwo_id = checkMusictm(id, g_connRun, g_curRun)
				if kuwo_id > 0:
					logging.info("process mid: %s success" % (id))
					###update MusicSrc
					fields = {"kw_id":kuwo_id,"m_status":g_status["task_suc"]}
					where = {"mid":id}
					update_MusicSrc(fields, where, g_connSrc, g_curSrc)
					if source_type == 1:
						notify_sync_request(1,id,1,kuwo_id,4)
					taskComplete.add(id)
					###need update KWRelation
					insert_KWRelation(kuwo_id,id,g_connRelation,g_curRelation)
					####check tx status and send task
					tx_show = checkTMApiStatus2(id,g_connTMApi,g_curTMApi)
					if tx_show == 2:
						if source_type == 1:
							notify_sync_request(1,id,1,kuwo_id,5)
						sendTaskOnline(kuwo_id)
						logging.info("online %s" % (kuwo_id))
			for id,source_type in mids:
				if id in taskComplete:
					continue
				###check if id in dms
				kuwo_id = checkMusictmFail(id, g_connRun, g_curRun)
				logging.info("kuwo id is %s" % (kuwo_id))
				if kuwo_id > 0:
					logging.info("process mid: %s fail" % (id))
					###update MusicSrc
					fields = {"kw_id":kuwo_id,"m_status":g_status["task_fail"]}
					where = {"mid":id}
					update_MusicSrc(fields, where, g_connSrc, g_curSrc)
				elif kuwo_id == -1:
					logging.info("process mid: %s fail" % (id))
					###update MusicSrc
					fields = {"m_status":g_status["task_fail"]}
					where = {"mid":id}
					update_MusicSrc(fields, where, g_connSrc, g_curSrc)
		break			
		#taskids = checkMusicSrcSuc(g_connSrc,g_curSrc)
		#logging.info("task nums %s" % (len(taskids)))
		#time.sleep(300)

if __name__ == "__main__":
	update_musicId_poll()
