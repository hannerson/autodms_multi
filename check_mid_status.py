# -*- coding=utf-8 -*-

import os,sys
import MySQLdb
import traceback
import json
import MySQLdb.cursors
import time
from resource_request import *
from logger import *
from pooldb import *
from utils import *

reload(sys)
sys.setdefaultencoding('utf-8')

def checkMusictmFail(tm_id,connRun,curRun):
	mid2 = 0
	sql = '''select id from Music where from_id = \"tm_%s\" and version_editor = 1 and version_pub = 0''' % (tm_id)
	cnt2 = curRun.execute(sql)
	if cnt2 > 0:
		ret = curRun.fetchone()
		mid2 = ret[0]
	sql = '''select id from Music where from_id = \"tx_%s\" and version_editor = 1 and version_pub = 0''' % (tm_id)
	cnt2 = curRun.execute(sql)
	if cnt2 > 0:
		ret = curRun.fetchone()
		mid2 = ret[0]
	return mid2

def checkMusicSrc(tm_id,connRun,curRun):
	mid1 = 0
	status = -1
	sql = '''select mid,m_status from MusicSrc where mid=%s''' % (tm_id)
	cnt1 = curRun.execute(sql)
	if cnt1 > 0:
		ret = curRun.fetchone()
		mid1 = ret["mid"]
		status = ret["m_status"]
	return mid1,status


connSrc = g_pool_Src.connection()
curSrc = connSrc.cursor()
connRun = g_pool_Run.connection()
curRun = connRun.cursor()
connRelation = g_pool_Relation.connection()
curRelation = connRelation.cursor()

def main():
	f = open(sys.argv[1],"r")
	fout = open(sys.argv[1]+".result","w+")
	for line in f:
		arr = line.strip().split("\t")
		mid = arr[0].strip()
		m_status = ""
		###check relation
		kw_id = checkMusicRelation(mid,connRelation,curRelation)
		if kw_id > 0:
			m_status = "成功"
		###check fail
		elif kw_id == 0:
			kw_id = checkMusictmFail(mid,connRun,curRun)
			if kw_id > 0:
				m_status = "失败"
				kw_id = 0
			###check in ruku
			elif kw_id == 0:
				tx_id,status = checkMusicSrc(mid,connSrc,curSrc)
				if tx_id > 0:
					if status == 9:
						m_status = "失败"
					else:
						m_status = "入库中"
				else:###no record
					m_status = "未入库"

		fout.write("%s\t%s\t%s\n" % (line.strip(),m_status,kw_id))
	f.close()
	fout.close()


main()

curSrc.close()
connSrc.close()
curRun.close()
connRun.close()
curRelation.close()
connRelation.close()
