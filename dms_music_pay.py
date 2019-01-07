#!/usr/local/bin/python
# -*- coding=utf-8 -*-

import os,sys
import MySQLdb
import traceback
import utils
import json
import MySQLdb.cursors
import time
from sqlClass import *
#from taskClass import *
from pooldb import *
from logger import *

reload(sys)
sys.setdefaultencoding('utf-8')

def check_pay(aid,pay_type,policy,conn,cur):
	sql = '''select rid from MusicPay where type=%s and rid=%s and policy="%s"''' % (pay_type,aid,policy)
	cnt = cur.execute(sql)
	if cnt > 0:
		conn.commit()
	return cnt

###album pay:type=1,rid,audioPlayBR(试听)=0(免费),1(付费),audioDownBR(下载)=0(免费),1(付费),policy=album
###专辑付费,从第4首开始付费,设置专辑付费,设置专辑下歌曲前3首试听免费
def pay_album(albumid,policy,price,play,down,conn,cur):
	if check_pay(albumid,1,policy,conn,cur) > 0:
		logging.info("album:%s payed. continue." % (albumid))
		#sql = '''update MusicPay set policy="none" where type=1 and rid=%s''' % (albumid)
		#cnt = cur.execute(sql)
		#if cnt > 0:
		#	conn.commit()
		return 0
	sql = '''insert into MusicPay (type, rid, audioPlayBR, audioDownBR, policy, price) VALUES (1, '%s','%s','%s','%s','%s')''' % (albumid,play,down,policy,price)
	cnt = cur.execute(sql)
	if cnt > 0:
		conn.commit()
	logging.info("album:%s paying" % (albumid))
	return cnt

###music pay:type=0,rid,audioPlayBR(试听)=0(免费),1(付费),audioDownBR(下载)=0(免费),1(付费),policy=album,song,none,vip
def pay_music(musicid,policy,price,play,down,conn,cur):
	if check_pay(musicid,0,policy,conn,cur) > 0:
		#sql = '''update MusicPay set policy="none" where type=0 and rid=%s''' % (musicid)
		#cnt = cur.execute(sql)
		#if cnt > 0:
		#	conn.commit()
		logging.info("music:%s payed.continue." % (musicid))
		return 0
	sql = '''insert into MusicPay (type, rid, audioPlayBR, audioDownBR, policy, price) VALUES (0, '%s','%s','%s','%s','%s')''' % (musicid,play,down,policy,price)
	cnt = cur.execute(sql)
	if cnt > 0:
		conn.commit()
	logging.info("music:%s paying" % (musicid))
	return cnt

def check_cachePay(rid,conn,cur):
	sql = '''select rid from tencent_pay where rid=%s''' % (rid)
	cnt = cur.execute(sql)
	if cnt > 0:
		conn.commit()
	return cnt

def insert_cachePay(rid,conn,cur):
	if check_cachePay(rid,conn,cur) > 0:
		logging.info("cache pay exists %s" % (rid))
		return 0
	sql = '''insert into tencent_pay (rid) values(%s)''' % (rid)
	cnt = cur.execute(sql)
	if cnt > 0:
		conn.commit()
	logging.info("pay cache:%s" % (rid))
	return cnt

def check_knowledgePay(rid,p_type,conn,cur):
	sql = '''select * from KnowledgePay where rid=%s and type=%s''' % (rid,p_type)
	cnt = cur.execute(sql)
	if cnt > 0:
		#logging.info("knowledge pay exists %s %s" % (rid,p_type))
		conn.commit()
	return cnt

def insert_knowledgePay(rid,p_type,conn,cur):
	if check_knowledgePay(rid,p_type,conn,cur) > 0:
		logging.info("knowledge pay exists %s %s" % (rid,p_type))
		return 0
	sql = '''insert into KnowledgePay(rid,type) values(%s,%s)''' % (rid,p_type)
	cnt = cur.execute(sql)
	if cnt > 0:
		conn.commit()
	logging.info("pay knowledge:%s,%s" % (rid,p_type))
	return cnt

def sendTaskPay(mid,pay_action,t_type):
    try:
        task_id = 0
        post_dict = {}
        Task = {}
        Task["id"] = "%s" % (mid)
	Task["c_show_type"] = "%s" % (0)
        Task["pay_flag"] = "%s" % (pay_action)
        info = []
        info.append({"%s" % (t_type):Task})
        post_dict["count"] = "%s" % (len(info))
        post_dict["info"] = info
        post_dict["priority"] = "%s" % (8)
        post_dict["editor_id"] = "%s" % (274)
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

def sendTaskOnline(mid,t_type):
    try:
        task_id = 0
        post_dict = {}
        Task = {}
        Task["id"] = "%s" % (mid)
	Task["c_show_type"] = "%s" % (0)
        info = []
        info.append({"%s" % (t_type):Task})
        post_dict["count"] = "%s" % (len(info))
        post_dict["info"] = info
        post_dict["priority"] = "%s" % (8)
        post_dict["editor_id"] = "%s" % (274)
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

