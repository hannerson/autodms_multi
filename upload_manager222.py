#!/bin/python
# -*- coding=utf-8 -*-

import os,sys
import jsonrpc
import MySQLdb
import traceback
import logging
from logging.handlers import TimedRotatingFileHandler
from Queue import Queue
import utils
import multiprocessing
from multiprocessing.managers import BaseManager
import time

reload(sys)
sys.setdefaultencoding('utf-8')

####All Config
g_Server_Ip = "111.206.73.111"
g_Server_Port = 31666

g_Config = utils.loadConfig("./config.conf.2")["dbconfig"]

g_conn = MySQLdb.connect(host=g_Config["src_dbhost"],user=g_Config["src_dbuser"],passwd=g_Config["src_dbpwd"],db=g_Config["src_dbname"],charset=g_Config["src_dbcharset"],port=g_Config["src_dbport"])
g_cur = g_conn.cursor()

g_status = {"default":0,"dispatch":1,"sig_ok":2,"artist_ok":3,"task_send":4,"artist_no":5,"task_fail":6,"task_suc":7,"sig_fail":8,"retry_fail":9}

def initlog():
        logger = logging.getLogger()
        LOG_FILE = "log/" + sys.argv[0].split("/")[-1].replace(".py","") + '.log'
        hdlr = TimedRotatingFileHandler(LOG_FILE,when='H',backupCount=24)
        formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(filename)s[line:%(lineno)d]:%(message)s')
        hdlr.setFormatter(formatter)
        logger.addHandler(hdlr)
        logger.setLevel(logging.NOTSET)
        return logger

logging =  initlog()

def update_music_status(id,status,conn,cur):
	sql = '''update MusicSrc set m_status=%s where id=%s''' % (status,id)
	cnt = cur.execute(sql)
	if cnt > 0:
		conn.commit()
	else:
		logging.error("update status MusicSrc id:%s failed" % (id))

def get_next_batch_jobs(q,conn,cur):
	sql = "select id from MusicSrc where m_status=0 limit 50"
	cnt = cur.execute(sql)
	conn.commit()
	if cnt > 0:
		ret = cur.fetchall()
		for r in ret:
			q.put(r[0])
			logging.info("job dispath %s" % r[0])

class Master:
	def __init__(self,conn,cur):
		self.job_queue = Queue(maxsize = 50)

	def get_dispatched_jobs(self):
		return self.job_queue

	def start(self):
		BaseManager.register('get_dispatched_jobs', callable=self.get_dispatched_jobs)
		manager = BaseManager(address=('0.0.0.0', g_Server_Port), authkey='jobs')
		manager.start()
		dispatched_jobs = manager.get_dispatched_jobs()

		while True:
			while not dispatched_jobs.empty():
				time.sleep(2)
				logging.info("job queue has jobs. sleep 2s")
			sql = "select id from MusicSrc where m_status in (%s,%s) order by priority desc,is_tme desc,id,sig_count limit 500" % (g_status["default"],g_status["sig_fail"])
			cnt = g_cur.execute(sql)
			g_conn.commit()
			if cnt > 0:
				ret = g_cur.fetchall()
				for r in ret:
					dispatched_jobs.put(r[0])
					logging.info("job dispath %s" % r[0])
					#time.sleep(0.1)
			else:
				sql = "select id from MusicSrc where m_status in (%s) order by priority desc,is_tme desc,id,sig_count limit 500" % (g_status["dispatch"])
				cnt = g_cur.execute(sql)
				g_conn.commit()
				if cnt > 0:
					ret = g_cur.fetchall()
					for r in ret:
						dispatched_jobs.put(r[0])
						logging.info("job dispath %s" % r[0])
				else:
					logging.info("no match jobs")
					time.sleep(10)
				time.sleep(10)
			time.sleep(30)
			#break

		manager.shutdown()

if __name__ == "__main__":
	master = Master(g_conn,g_cur)
	master.start()
