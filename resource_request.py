# -*- coding=utf-8 -*-

import os,sys
import MySQLdb
import traceback
import json
import logging
from logging.handlers import TimedRotatingFileHandler
import MySQLdb.cursors
import time
import urllib2
import re
import hashlib

reload(sys)
sys.setdefaultencoding('utf-8')

def resource_request(tid,ctype):
	try:
		APPID = "1000002"
		KEYS = "kw2018"
		KEYWORDS = "tme_music.MusicSyncServer.MusicSyncObj"
		METHOD = "GetResDownloadLink"
		timestamp = int(time.time())
		post_data = {}
		post_data["comm"] = {}
		post_data["comm"]["appid"] = APPID
		post_data["comm"]["timestamp"] = timestamp

		hl = hashlib.md5()
		hl.update("%s%s+%s%s" % (APPID,APPID,KEYS,timestamp))
		post_data["comm"]["sign"] = hl.hexdigest()
		post_data["download_url"] = {}
		post_data["download_url"]["module"] = KEYWORDS
		post_data["download_url"]["method"] = METHOD
		post_data["download_url"]["param"] = {}
		post_data["download_url"]["param"]["res_id"] =  int(tid)
		post_data["download_url"]["param"]["res_type"] = ctype
		
		url = "https://u.y.qq.com/cgi-bin/musicu.fcg"
		post_json = json.dumps(post_data)
		print post_json
		f = urllib2.urlopen(
			url = url,
			data = post_json
			)

		result = f.read()
		print result
		logging.info(str(result))
		ret = json.loads(result)

		return ret
		#if ret["code"] == 0 and ret["download_url"]["code"] == 0:
		#	return ret["download_url"]["data"]
		#else:
		#	return {}
	except Exception,e:
		traceback.print_exc()
		pass

def notify_sync_request(act,rid,ctype,pid,status,code=0):
	try:
		if code == 0:
			url = "http://vang.kuwo.cn/rsync2music/DataRsyncInterplace101.php?act=%s&rid=%s&rtype=%s&pid=%s&status=%s" % (act,rid,ctype,pid,status)
		else:
			url = "http://vang.kuwo.cn/rsync2music/DataRsyncInterplace101.php?act=%s&rid=%s&rtype=%s&pid=%s&status=%s&msg=%s" % (act,rid,ctype,pid,status,code)
		logging.info(url)	
		f = urllib2.urlopen(
			url = url
			)

		result = f.read()
		#print result
		logging.info(str(result))
		ret = json.loads(result)
		return ret["code"]
	except Exception,e:
		traceback.print_exc()
		pass
	

if __name__ == "__main__":
	resource_request(226944567,1)
	#resource_request(10000004,2)
	#notify_sync_request(1,219402003,1,0,3)
