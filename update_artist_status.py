#!/bin/python
# -*- coding = utf-8 -*-

import os,sys
import MySQLdb
import logging
from logging.handlers import TimedRotatingFileHandler
from pooldb import *

reload(sys)                                             
sys.setdefaultencoding('utf-8')

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

def update_artist_status(path,u_type,conn,cur):
	table_map = {"Album":"AlbumSrc","Music":"MusicSrc"}
	f = open(path,"r")
	for line in f:
		arr = line.strip().split("\t")
		if len(arr) < 2:
			continue
		id = arr[1].strip()
		sql = '''update %s set m_status_art=%s where id=%s''' % (table_map[u_type],g_status["artist_ok"],id)
		cnt = cur.execute(sql)
		if cnt > 0:
			logging.info(sql)
			conn.commit()

if len(sys.argv) < 3:
	sys.exit(0)
connSrc = g_pool_Src.connection()
curSrc = connSrc.cursor()
update_artist_status(sys.argv[1],sys.argv[2],connSrc,curSrc)
curSrc.close()
connSrc.close()

