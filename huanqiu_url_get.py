#!/bin/python
# -*- coding:utf-8 -*-

import os,sys
import MySQLdb
from pooldb import *
from logger import *
import time

reload(sys)
sys.setdefaultencoding('utf-8')


def get_data_huanqiu(conn,cur):
	huan_qiu = {}
	sql = '''select id,isrc from MusicSrc where m_status=9 and source_type=1 and isrc like \"US%\"'''
	cnt = cur.execute(sql)
	if cnt > 0:
		rets = cur.fetchall()
		conn.commit()
		for ret in rets:
			huan_qiu[ret["id"]] = ret["isrc"]
	return huan_qiu

def get_url_huanqiu(isrc,conn,cur):
	url = ""
	sql = "select url from umgisrc2path where isrc=\"%s\"" % (isrc)
	cnt = cur.execute(sql)
	if cnt > 0:
		ret = cur.fetchone()
		url = ret["url"]
	return url

def update_url_huanqiu(id,url,conn,cur):
	sql = '''update MusicSrc set m_audio_url="%s",m_status=0,sig_count=0,retry_count=0 where id=%s''' % (url,id)
	cnt = cur.execute(sql)
	if cnt > 0:
		conn.commit()

def main():
	while True:
		conn = g_pool_Src.connection()
		cur = conn.cursor()
		###1.data huanqiu
		huan_qiu = get_data_huanqiu(conn,cur)
		for id,isrc in huan_qiu.items():
			logging.info("%s -- %s" % (id,isrc))
			if isrc == "":
				logging.info("isrc empty......")
				continue
			###2.get huanqiu url
			url = get_url_huanqiu(isrc,conn,cur)
			if url != "":
				logging.info("update --- %s" % (url))
				update_url_huanqiu(id,url,conn,cur)

		logging.info("sleep 300s")
		time.sleep(300)
		cur.close()
		conn.close()
		#break

if __name__ == '__main__':
	main()
