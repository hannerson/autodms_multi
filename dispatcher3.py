# -*- coding=utf-8 -*-
import threading
import Queue
from taskProcessor import *
from pooldb import *
from sqlClass import *

reload(sys)
sys.setdefaultencoding('utf-8')


class dispatcher(object):
	def __init__(self,config,logging):
		self.config = config
		self.logging = logging
		self.q_data = Queue.Queue(20)
		#self.taskprocessor = task
		self.has_data = True
		self.data_lock = threading.Lock()
		pass

	def check_music_count(self,sql,conn,cur):
		cnt = cur.execute(sql)
		conn.commit()
		if cnt > 0:
			ret = cur.fetchone()
			return ret['count(*)']
		else:
			return 1

	def send_task(self,type,tableSrc,relaTable,checkInDMS,ret_sql,connSrc,curSrc,connRun,curRun,connRes,curRes):
		try:
			task_id = 0
			ids_task = set()
			taskprocessor = Task(self.config,connRun,curRun,connRes,curRes)
			post_info,ids_task = taskprocessor.genTaskInfo(connSrc,curSrc,type,tableSrc,relaTable,checkInDMS,self.config.configinfo[tableSrc],ret_sql)
			task_id = taskprocessor.sendTask(post_info)
			src_sql_class = sqlClass(connSrc,curSrc)
			if type == "Music":
				if task_id > 0:
					field_dict = {}
					field_dict["m_status"] = g_status["task_send"]
					field_dict["taskid"] = task_id
					for id in ids_task:
						where = "id=%s" % (id)
						src_sql_class.mysqlUpdate(tableSrc,where,field_dict)
						update_MusicSrc_try_count(id,connSrc,curSrc)
				else:
					#time.sleep(10)
					self.logging.info("something error or no task")
			elif type == "Album":
				if task_id > 0:
					field_dict = {}
					field_dict["m_status"] = g_status["task_send"]
					field_dict["taskid"] = task_id
					for id in ids_task:
						where = "id=%s" % (id)
						src_sql_class.mysqlUpdate(tableSrc,where,field_dict)
				else:
					#time.sleep(1)
					self.logging.info("something error or no task")
		except Exception,e:
			self.logging.error(str(e))
			self.has_data = False
			pass

	def thread_worker(self,type,tableSrc,relaTable,checkInDMS,i):
		while self.has_data or not self.q_data.empty():
			self.logging.info("g_has_data : %s" % (self.has_data))
			ret_sql = []
			if self.q_data.empty():
				time.sleep(1)
				self.logging.info("sleep 1s")
			else:
				self.data_lock.acquire()
				if not self.q_data.empty():
					ret_sql = self.q_data.get()
					self.data_lock.release()
				else:
					self.data_lock.release()
				self.logging.info("-------worker %s---------" % (i))
				if len(ret_sql) == 0:
					continue
				connSrc = g_pool_Src.connection()
				curSrc = connSrc.cursor()

				connRun = g_pool_Run.connection()
				curRun = connRun.cursor()

				connRes = g_pool_Res.connection()
				curRes = connRes.cursor()

				self.send_task(type,tableSrc,relaTable,checkInDMS,ret_sql,connSrc,curSrc,connRun,curRun,connRes,curRes)

				curSrc.close()
				connSrc.close()
				curRun.close()
				connRun.close()
				curRes.close()
				connRes.close()

	def get_data(self,type,conn,cur,table,limit,numbyone):
		config_info = self.config.configinfo
		if not config_info.has_key(table):
			self.logging.error("error no config : %s" % (table))
			return

		tsk_status = set()
		if type == "Album":
			tsk_status.add(g_status["default"])
			tsk_status.add(g_status["task_fail"])
		elif type == "Music":
			tsk_status.add(g_status["sig_ok"])
			tsk_status.add(g_status["task_fail"])
			#tsk_status.add(g_status["task_suc"])

		sql_status = ""
		for s in tsk_status:
			sql_status += "%s," % (s)
			
		sql_fields = ""
		for k,v in config_info[table].items():
			#if k in ["m_audio_id"]:
			#	continue
			sql_fields += "%s," % (v)
		#print sql_fields
		if table == "AlbumSrc":
			sql = '''select %s from %s where m_status in (%s) and m_status_art=%s order by priority limit %s''' % (sql_fields.rstrip(","), table, sql_status.rstrip(","), g_status["artist_ok"], limit)
		elif table == "MusicSrc":
			sql = '''select %s from %s where m_status in (%s) and m_status_art=%s and m_artists!="" and m_name!="" and m_album_id=7392290 and file_sig1>0 order by priority desc limit %s''' % (sql_fields.rstrip(","), table, sql_status.rstrip(","), g_status["artist_ok"],limit)
		self.logging.info(sql)

		cnt = cur.execute(sql)
		conn.commit()
		if cnt < 1:
			self.logging.info("no data need to dispatch")
		
		ret_sql = cur.fetchall()
		i = 0
		while i < len(ret_sql):
			self.q_data.put(ret_sql[i:i+numbyone])
			i += numbyone

	def get_data2(self,type,conn,cur,table,limit,numbyone):
		config_info = self.config.configinfo
		if not config_info.has_key(table):
			self.logging.error("error no config : %s" % (table))
			return

		tsk_status = set()
		if type == "Album":
			tsk_status.add(g_status["default"])
			tsk_status.add(g_status["task_fail"])
		elif type == "Music":
			tsk_status.add(g_status["sig_ok"])
			tsk_status.add(g_status["task_fail"])
			tsk_status.add(g_status["editor_album"])

		sql_status = ""
		for s in tsk_status:
			sql_status += "%s," % (s)
			
		sql_fields = ""
		for k,v in config_info[table].items():
			#if k in ["m_audio_id"]:
			#	continue
			sql_fields += "%s," % (v)
		#print sql_fields
		if table == "AlbumSrc":
			sql = '''select %s from %s where m_status in (%s) and m_status_art=%s order by priority limit %s''' % (sql_fields.rstrip(","), table, sql_status.rstrip(","), g_status["artist_ok"], limit)
		elif table == "MusicSrc":
			sql = '''select %s from %s where m_status in (%s) and m_status_art=%s and m_artists!="" and m_name!="" and batch_name="zhuanjipipei" and file_sig1>0 order by priority desc limit %s''' % (sql_fields.rstrip(","), table, "11", g_status["artist_ok"],limit)
		self.logging.info(sql)

		cnt = cur.execute(sql)
		conn.commit()
		if cnt < 1:
			self.logging.info("no data need to dispatch")
		
		ret_sql = cur.fetchall()
		i = 0
		while i < len(ret_sql):
			self.q_data.put(ret_sql[i:i+numbyone])
			i += numbyone

	def create_workers(self,type,tableSrc,relaTable,checkInDMS):
		config_info = self.config.configinfo
		thread_num = 3
		if config_info.has_key("common") and config_info["common"].has_key("thread_num"):
			thread_num = int(config_info["common"]["thread_num"])
		for i in range(thread_num):
			t1 = threading.Thread(target=self.thread_worker, args=(type,tableSrc,relaTable,checkInDMS,i))
			t1.start()

	def dispatch(self,type,table,limit,numbyone,tasklimit,pool_Src,pool_Run,pool_Res):
		connSrc = pool_Src.connection()
		curSrc = connSrc.cursor()
		connRun = pool_Run.connection()
		curRun = connRun.cursor()
		editor_id = self.config.configinfo["common"]["editor_id"]
		task_sum = 0
		if type == "Album":
			count_sql = 'select count(*) from %s where m_status in (0,6) and m_status_art=3' % table
		elif type == "Music":
			count_sql = 'select count(*) from %s where m_status in (11) and m_status_art=3 and batch_name="zhuanjipipei"' % table
		count = self.check_music_count(count_sql,connSrc,curSrc)
		#while True:
		while count > 0:
			self.get_data2(type,connSrc,curSrc,table,limit,numbyone)
			while not self.q_data.empty():
				time.sleep(2)
				self.logging.info("sleep 2s")
			time.sleep(10)
			task_sum += 1
			if task_sum % 10 == 0:
				task_count = get_num_task_running(editor_id, connRun, curRun)
				while task_count > tasklimit:
					self.logging.info("task count %s sleep 10s" % (task_count))
					time.sleep(10)
					task_count = get_num_task_running(editor_id, connRun, curRun)
			count = self.check_music_count(count_sql,connSrc,curSrc)
			if count == 0:
				self.logging.info("sleep 60s")
				time.sleep(60)
			#break
		self.has_data = False
		curSrc.close()
		connSrc.close()
		curRun.close()
		connRun.close()

		self.logging.info("no match task")

