#!/bin/python
# -*- coding:utf-8 -*-

import os,sys
import MySQLdb
import logging
import json
import traceback
import urllib2
import random
import string
from PIL import Image
import datetime
import time
from sqlClass import *
from utils import *
from logger import *
from pooldb import *
from resource_request import *

reload(sys)
sys.setdefaultencoding('utf-8')

g_validFields = ["id","id2","m_name","m_aliasname","m_album_id","m_subalbum","m_track","m_audio_id","m_copyright","basic_version","basic_releasedate","basic_company","basic_intro","tags_category","tags_genre","tags_region","tags_lang","c_show_type","m_artists"]

class Task(object):
	def __init__(self,config,connRun,curRun,connRes,curRes,taskurl="http://centerserver.kuwo.cn/add_task"):
		self.taskurl = taskurl
		self.connRun = connRun
		self.curRun = curRun
		self.connRes = connRes
		self.curRes = curRes
		self.config = config

	def getPicSource(self,src_url,tmp_local_dir):
		try:
			ret = download_url(tmp_local_dir,src_url)
			if ret != 0:
				return 0
			logging.info("downloaded picture path:" + tmp_local_dir)
			file_size = os.path.getsize(tmp_local_dir)
			if file_size <= 0:
				return 0
			dms_sig1,dms_sig2 = getSig(tmp_local_dir).strip().split(",")
			if not (dms_sig1 and dms_sig2):
				return 0
			sql = "select id from PicSource where file_sig1=%s and file_sig2=%s and version_editor>1 limit 1" % (dms_sig1,dms_sig2)
			cnt = self.curRun.execute(sql)
			if cnt > 0:
				ret = self.curRun.fetchone()
				self.connRun.commit()
				return ret[0]
			return 0
		except Exception,e:
			logging.info(str(e))
			traceback.print_exc()
			#pass

	def getAudioSource(self,src_url,tmp_local_dir):
		try:
			ret = download_url(tmp_local_dir,src_url)
			if ret != 0:
				return 0
			dms_sig1,dms_sig2 = getSig(tmp_local_dir).strip().split(",")
			if not (dms_sig1 and dms_sig2):
				return 0
			sql = "select id from AudioSource where file_sig1=%s and file_sig2=%s and version_editor>1 limit 1" % (dms_sig1,dms_sig2)
			cnt = self.curRun.execute(sql)
			if cnt > 0:
				ret = self.curRun.fethone()
				return ret[0]
			return 0
		except Exception,e:
			logging.info(str(e))
			#pass

	def getAudioSource2(self,file_sig1,file_sig2):
		try:
			sql = "select id from AudioSource where file_sig1=%s and file_sig2=%s and version_editor>1 limit 1" % (file_sig1,file_sig2)
			cnt = self.curRun.execute(sql)
			if cnt > 0:
				ret = self.curRun.fetchone()
				return ret[0]
			return 0
		except Exception,e:
			logging.info(str(e))
			#pass

	def genAudioSource(self,src_url,tmp_local_dir,tsk_count):
		try:
			AudioSource = {}
			###download
			ret = download_url(tmp_local_dir,src_url)
			if ret != 0:
				return AudioSource
			file_size = os.path.getsize(tmp_local_dir)
			dms_sig1,dms_sig2 = getSig(tmp_local_dir).strip().split(",")
			if not (dms_sig1 and dms_sig2):
				return AudioSource
			#dms_path = songFileProcess(dms_sig1,dms_sig2,self.config.configinfo["common"]["path_pre"],tmp_local_dir)
			AudioSource["file_size"] = ("%s") % file_size
			AudioSource["file_path"] = src_url.encode('utf-8')
			AudioSource["file_sig1"] = ("%s") % dms_sig1
			AudioSource["file_sig2"] = ("%s") % dms_sig2
			AudioSource["file_type"] = "audio_mass"
			if self.config.configinfo["MusicConst"].has_key("orig_ip") and self.config.configinfo["MusicConst"].has_key("targ_ip"):
				AudioSource["c_extparams"] = "orig_ip=%s&targ_ip=%s" % (self.config.configinfo["MusicConst"]["orig_ip"],self.config.configinfo["MusicConst"]["targ_ip"])
			AudioSource["file_format"] = src_url.split('.')[-1]
			audio_id = ("NEW_AudioSource_%s") % (tsk_count)
			AudioSource["id"] = audio_id
			return AudioSource
		except Exception,e:
			logging.info(str(e))
			#pass

	def update_album_txid_task(self,id,tx_id):
	    try:
		logging.info(("update tencent relation id = %s.") % id)
		task_info = []
		# 生成Music描述
		Album = {}
		Album["id"] = "%s" % id
		Album["tx_albumid"] = "tm_%s" % tx_id
		task_info.append({"Album":Album})

		post_dict = {}
		post_dict["count"] = str(len(task_info))
		post_dict["priority"] = "8"
		post_dict["editor_id"] = "254"
		post_dict["timely"] = "0"
		post_dict["info"] = task_info

		post_json = json.dumps(post_dict)
		#print post_json
		logging.debug(post_json)
		#return False
		#'''
		f = urllib2.urlopen(
			url     = self.taskurl,
			data    = post_json
			)

		result = f.read()
		logging.info(str(result))
		js_ret = json.loads(result)
		logging.debug(result)
		task_id = 0
		if result.find("OK") >= 0:
		   task_id = js_ret["taskid"]

		if task_id == 0:
		    logging.error( ("fail when we modify tx_albumid, album id:%s, tx_albumid:%s.") % (id,tx_id) )
		    return task_id
		#print task_id
		#'''
		return task_id
	    except:
		traceback.print_exc()
		#pass

	def update_music_from_id_task(self,id,from_id):
	    try:
		logging.info(("update music relation id = %s.") % id)
		task_info = []
		# 生成Music描述
		Music = {}
		Music["id"] = "%s" % id
		Music["from_id"] = "tm_%s" % from_id
		task_info.append({"Music":Music})

		post_dict = {}
		post_dict["count"] = str(len(task_info))
		post_dict["priority"] = "8"
		post_dict["editor_id"] = "254"
		post_dict["timely"] = "0"
		post_dict["info"] = task_info

		post_json = json.dumps(post_dict)
		logging.debug(post_json)
		#return False
		#'''
		f = urllib2.urlopen(
			url     = self.taskurl,
			data    = post_json
			)

		result = f.read()
		logging.info(str(result))
		js_ret = json.loads(result)
		logging.debug(result)
		task_id = 0
		if result.find("OK") >= 0:
		   task_id = js_ret["taskid"]

		if task_id == 0:
		    logging.error( ("fail when we modify batch,music id:%s,music fromid:%s.") % (id,from_id) )
		    return task_id
		#print task_id
		#'''
		return task_id
	    except:
		traceback.print_exc()
		#pass

	def checkDMSExists(self,connSrc,curSrc,result,type,tableSrc,relaTable):
		try:
			matched = False
			src_sql_class = sqlClass(connSrc,curSrc)
			###TODO:check if exists in database,return dict
			if type == "Album":
				if not result.has_key("m_name") or result["m_name"].strip() == "":
					where = "id=%s" % (result["id"])
					param = {}
					param["m_status"] = "%s" % g_status["retry_fail"]
					src_sql_class.mysqlUpdate(tableSrc,where,param)

					where = "from_aid=%s" % (result["id"])
					param = {}
					param["m_status"] = "%s" % g_status["retry_fail"]
					src_sql_class.mysqlUpdate(relaTable,where,param)
					matched = True
					#continue
				if not result.has_key("m_artists") or result["m_artists"].strip() == "":
					where = "id=%s" % (result["id"])
					param = {}
					param["m_status"] = "%s" % g_status["retry_fail"]
					src_sql_class.mysqlUpdate(tableSrc,where,param)

					where = "from_aid=%s" % (result["id"])
					param = {}
					param["m_status"] = "%s" % g_status["retry_fail"]
					src_sql_class.mysqlUpdate(relaTable,where,param)
					matched = True
					#continue

				is_manual = 100
				tx_albumid = ""
				tm_aid = 0
				if result.has_key("tx_albumid") and int(result["tx_albumid"]) > 0:
					tm_aid,is_manual = checkAlbumtm(result["tx_albumid"], self.connRun, self.curRun)
				if tm_aid == 0:
					tm_aid,tx_albumid = checkAlbumExists(result["m_name"],result["m_artists"],self.connRun,self.curRun,self.connRes,self.curRes)
					logging.info("match from name: albumid %s, tx_albumid %s " % (tm_aid, tx_albumid))
					if tm_aid > 0 and tx_albumid == "":
						logging.info("album exists skip: %s" % (tm_aid))
						###update albumid and skip: MusicSrc,AlbumSrc
						where = "id=%s" % (result["id"])
						param = {}
						param["m_status"] = "%s" % g_status["task_suc"]
						param["m_album_id"] = "%s" % tm_aid
						src_sql_class.mysqlUpdate(tableSrc,where,param)
						'''
						where = "from_aid=%s" % (result["id"])
						param = {}
						param["m_album_id"] = "%s" % tm_aid
						src_sql_class.mysqlUpdate(relaTable,where,param)
						'''
						###: send task modify tx_albumid
						self.update_album_txid_task(tm_aid,result["tx_albumid"])
						matched = True
						#continue
					elif tm_aid > 0 and tx_albumid != "":
						where = "id=%s" % (result["id"])
						param = {}
						param["m_status"] = "%s" % g_status["has_matched"]
						param["m_album_id"] = "%s" % tm_aid
						src_sql_class.mysqlUpdate(tableSrc,where,param)
						'''
						where = "from_aid=%s" % (result["id"])
						param = {}
						param["m_album_id"] = "%s" % tm_aid
						src_sql_class.mysqlUpdate(relaTable,where,param)
						'''
						logging.info("match album: m_album_id-%s m_name-%s m_artists-%s   matched:%s" % (tm_aid,result["m_name"],result["m_artists"],tx_albumid))
						matched = True
						#continue
				elif tm_aid > 0:
					logging.info("album exists skip: %s" % (tm_aid))
					###TODO:update albumid and skip: MusicSrc,AlbumSrc
					where = "id=%s" % (result["id"])
					param = {}
					param["m_status"] = "%s" % g_status["task_suc"]
					param["m_album_id"] = "%s" % tm_aid
					src_sql_class.mysqlUpdate(tableSrc,where,param)
					'''
					where = "from_aid=%s" % (result["id"])
					param = {}
					param["m_album_id"] = "%s" % tm_aid
					src_sql_class.mysqlUpdate(relaTable,where,param)
					if is_manual == 0:
						where = "from_aid=%s" % (result["id"])
						param = {}
						param["m_status"] = "%s" % g_status["editor_album"]
						src_sql_class.mysqlUpdate(relaTable,where,param)
					'''
					matched = True
					#continue
			elif type == "Music":
				###:check name,m_artist
				if not result.has_key("m_name") or result["m_name"].strip() == "":
					where = "id=%s" % (result["id"])
					param = {}
					param["m_status"] = "%s" % g_status["retry_fail"]
					src_sql_class.mysqlUpdate(tableSrc,where,param)
					matched = True
					#continue
				if not result.has_key("m_artists") or result["m_artists"].strip() == "":
					where = "id=%s" % (result["id"])
					param = {}
					param["m_status"] = "%s" % g_status["retry_fail"]
					src_sql_class.mysqlUpdate(tableSrc,where,param)
					matched = True
					#continue
				###:check retry count
				retry_cnt = check_MusicSrc_try_count(result["id"],connSrc,curSrc)
				logging.info("retry cnt : %s - %s" % (result["id"], retry_cnt))
				if retry_cnt > 3:
					matched = True
					#continue
				kw_id = 0
				if result.has_key("mid") and result["mid"] > 0:
					kw_id = checkMusictm(result["mid"], self.connRun, self.curRun)
				if result.has_key("kw_id") and result["kw_id"] > 0:
					logging.info("need update kw_id:%s" % (result["kw_id"]))
				else:
					if kw_id > 0:
						logging.info("Music exists skip: %s" % (kw_id))
						###TODO:update albumid and skip: MusicSrc,AlbumSrc
						where = "id=%s" % (result["id"])
						param = {}
						param["m_status"] = "%s" % g_status["task_suc"]
						param["kw_id"] = "%s" % kw_id
						src_sql_class.mysqlUpdate(tableSrc,where,param)
						matched = True
						#continue
					else:
						if result.has_key("m_album_id"):
							tx_aid,is_editor = checkEditorAlbum(result["m_album_id"],self.connRun,self.curRun)
							kw_id,from_id = checkMusicExists(result["m_album_id"],result["m_name"],result["m_artists"],result["basic_version"],result["version2"],self.connRun,self.curRun,self.connRes,self.curRes)
							logging.info("match from name: mid %s, from_id %s, is_editor %s " % (kw_id, from_id, is_editor))
							if is_editor == 1:
								where = "id=%s" % (result["id"])
								param = {}
								param["m_status"] = "%s" % g_status["editor_album"]
								param["kw_id"] = "%s" % kw_id
								src_sql_class.mysqlUpdate(tableSrc,where,param)
								if kw_id > 0:
									self.update_music_from_id_task(kw_id,result["mid"])
								matched = True
								#continue
							if kw_id > 0:
								if from_id.find("tx_") != -1 or from_id.find("tm_") != -1:
									where = "id=%s" % (result["id"])
									param = {}
									param["m_status"] = "%s" % g_status["has_matched"]
									param["kw_id"] = "%s" % kw_id
									src_sql_class.mysqlUpdate(tableSrc,where,param)
								else:
									where = "id=%s" % (result["id"])
									param = {}
									param["m_status"] = "%s" % g_status["task_suc"]
									param["kw_id"] = "%s" % kw_id
									src_sql_class.mysqlUpdate(tableSrc,where,param)
									self.update_music_from_id_task(kw_id,result["mid"])
								g_connTMApi = g_pool_TMApi.connection()
								g_curTMApi = g_connTMApi.cursor()
								if checkTMApiStatus2(kw_id,g_connTMApi,g_curTMApi) == 2:
									sendTaskOnline(kw_id,"Music")
								g_curTMApi.close()
								g_connTMApi.close()
								logging.info("matched music: m_album_id-%s m_name-%s m_artists-%s kw_id-%s" % (result["m_album_id"],result["m_name"],result["m_artists"],kw_id))
								matched = True
								#continue
			return matched
		except Exception,e:
			traceback.print_exc()


	def genPicSource(self,t_task,src_url,tmp_local_dir,path_pre,http_pre):
		try:
			PicSource = {}
			ret = download_url(tmp_local_dir,src_url)
			if ret != 0:
				return PicSource
			logging.info("downloaded picture path:" + tmp_local_dir)
			if os.path.getsize(tmp_local_dir) == 0:
				return PicSource
			Im = Image.open(tmp_local_dir)
			Im_format = Im.format
			if Im_format == "JPEG":
				Im_format = "jpg"
			elif Im_format == "PNG":
				Im_format = "png"
			elif Im_format == "BMP":
				Im_format = "bmp"
			else:
				Im_format = "jpg"

			dms_sig1,dms_sig2 = getSig(tmp_local_dir).strip().split(",")
			if not (dms_sig1 and dms_sig2):
				return PicSource
			sigpath = ImageFileProcess(dms_sig1,dms_sig2,path_pre,tmp_local_dir).lstrip("/")
			PicSource = {}
			PicSource["file_size"] = str(os.path.getsize(path_pre + sigpath))
			PicSource["file_path"] = http_pre + sigpath
			PicSource["file_sig1"] = ("%s") % dms_sig1
			PicSource["file_sig2"] = ("%s") % dms_sig2
			if t_task == "Album":
				PicSource["file_type"] = "pic_bat_album"
			elif t_task == "Artist":
				PicSource["file_type"] = "pic_bat_artist"
			elif t_task == "Music":
				PicSource["file_type"] = "pic_nor_music"
			PicSource["file_format"] = Im_format
			PicSource["id"] = "NEW_PicSource"
			return PicSource
		except Exception,e:
			traceback.print_exc()
			logging.info(str(e))
			#pass

	def genTaskInfo(self,connSrc,curSrc,task_type,tableSrc,relaTable,checkInDMS,config,info_arr):
		try:
			task_info = []
			tsk_count = 0
			ids_task = set()
			priority = 6
			for info in info_arr:
				taskSingle = {}
				#print info
				if checkInDMS and self.checkDMSExists(connSrc,curSrc,info,task_type,tableSrc,relaTable):
					continue
				if task_type == "Music" and info.has_key("m_album_id") and info["m_album_id"] > 0:
					if not check_DMS_Album_status(info["m_album_id"],self.connRun,self.curRun):
						continue
				for k,v in config.items():
					if k in ["file_size","file_sig1","file_sig2","file_path","file_type","c_extparams","file_format"]:
						continue
					if k == "m_artists":###: split ';','###',',','&'
						if config.has_key("m_artist_ids") and info[config["m_artist_ids"]].strip() != "":
							continue
						taskSingle[k] = list(getArtistIdsFromName(info[v].strip(),"/",self.connRun,self.curRun,connSrc,curSrc))
						if len(taskSingle[k]) < 1:
							taskSingle[k] = list(getArtistIdsFromName(info[v].strip(),";",self.connRun,self.curRun,connSrc,curSrc))
						if len(taskSingle[k]) < 1:
							taskSingle[k] = list(getArtistIdsFromName(info[v].strip(),"###",self.connRun,self.curRun,connSrc,curSrc))
						if len(taskSingle[k]) < 1:
							taskSingle[k] = list(getArtistIdsFromName(info[v].strip(),",",self.connRun,self.curRun,connSrc,curSrc))
						if len(taskSingle[k]) < 1:
							taskSingle[k] = list(getArtistIdsFromName(info[v].strip(),"、",self.connRun,self.curRun,connSrc,curSrc))
						if len(taskSingle[k]) < 1:
							taskSingle[k] = list(getArtistIdsFromName(info[v].strip(),"|",self.connRun,self.curRun,connSrc,curSrc))
						continue
					if k == "m_artist_ids": ###default sepa=;
						#taskSingle["m_artists"] = info[v].strip().split(";")
						if info[v].strip() != "":
							taskSingle["m_artists"] = info[v].strip().split("&")
						continue
					if k == "m_record_pic_id" or k == "m_pic_id":
						###tmp dir,genarate the file name by rand or use source file name
						tmp_local_dir = self.config.configinfo["common"]["pre_path"] + "pic/"
						if not os.path.exists(tmp_local_dir):
							os.makedirs(tmp_local_dir)
						source_url = ""
						source_fmt = ""
						if info.has_key("tx_albumid"):
							if int(info["tx_albumid"]) > 0:
								ret_url = resource_request(int(info["tx_albumid"]),2)
								if ret_url is not None and ret_url["code"] == 0 and ret_url["download_url"]["code"] == 0:
									source_url = ret_url["download_url"]["data"]["url"]
									source_fmt = ret_url["download_url"]["data"]["file_type"]
						if source_url == "":
							if info[v] == "":
								taskSingle[k] = "%s" % (1594440)
								continue
							if self.config.configinfo["common"].has_key("http_peer_pre") and info[v].find("http") == -1:
								source_url = self.config.configinfo["common"]["http_peer_pre"] + info[v]
						
						tmp_local_dir += "%s_%s" % (task_type,''.join(random.sample(string.ascii_letters + string.digits, 8)))
						pic_id = self.getPicSource(source_url,tmp_local_dir)
						if pic_id == 0:
							taskSingle[k] = "NEW_PicSource"
							PicSource = self.genPicSource(task_type,source_url,tmp_local_dir,self.config.configinfo["common"]["pre_path"] + "pic/",self.config.configinfo["common"]["http_local_pre"])
							if len(PicSource) < 7:
								logging.info("pic source error.continue")
								taskSingle[k] = "1594440"
								#return []
							else:
								taskSingle[k] = "NEW_PicSource"
								task_info.append({"PicSource":PicSource})
						else:
							taskSingle[k] = "%s" % (pic_id)
					elif k == "m_audio_id":
						if v == "m_audio_url" and info.has_key("file_sig1") and info.has_key("file_sig2"):
							audio_id = self.getAudioSource2(info["file_sig1"],info["file_sig2"])
							if audio_id > 0:
								taskSingle[k] = "%s" % (audio_id)
							else:
								AudioSource = {}
								AudioSource["file_size"] = ("%s") % info["file_size"]
								AudioSource["file_path"] = info["file_path"].strip("/")
								AudioSource["file_sig1"] = ("%s") % info["file_sig1"]
								AudioSource["file_sig2"] = ("%s") % info["file_sig2"]
								AudioSource["file_type"] = "audio_mass"
								AudioSource["c_extparams"] = ("%s") % info["c_extparams"]
								AudioSource["file_format"] = ("%s") % info["file_format"]
								audio_id = ("NEW_AudioSource_%s") % (tsk_count)
								AudioSource["id"] = "%s" % (audio_id)
								task_info.append({"AudioSource":AudioSource})
								taskSingle[k] = "%s" % (audio_id)
						elif v == "m_audio_url" and not info.has_key("file_sig1"):
							###tmp dir,genarate the file name by rand or use source file name
							tmp_local_dir = self.config.configinfo["common"]["pre_path"] + "audio/"
							if not os.path.exists(tmp_local_dir):
								os.makedirs(tmp_local_dir)
							tmp_local_dir += info[v].split("/")[-1]
							audio_id = self.getAudioSource(info[v],tmp_local_dir)
							if audio_id == 0:
								taskSingle[k] = "NEW_AudioSource_%s" % (tsk_count)
								AudioSource = {}
								AudioSource = self.genAudioSource(info[v],tmp_local_dir,tsk_count)
								if len(AudioSource) < 7:
									logging.info("audio source error")
									return False
								task_info.append({"AudioSource":AudioSource})
							else:
								taskSingle[k] = "%s" % (audio_id)
					elif k == "basic_releasedate":
						print info[v]
						if info[v].isdigit():
							taskSingle[k] = time.strftime("%Y-%m-%d",time.localtime(int(info[v])))
						elif info[v] != "":
							taskSingle[k] = "%s" % info[v]
						continue
					elif k == "id":
						if config.has_key("id2") and info.has_key(config["id2"]) and info[config["id2"]] != "" and info[config["id2"]] != "0" and info[config["id2"]] != 0 and info[config["id2"]] is not None and info[config["id2"]] != "None":
							taskSingle[k] = "%s" % info[config["id2"]]
						else:
							taskSingle[k] = "NEW_%s_%s" % (task_type,tsk_count)
					elif k == "c_show_type":
						#if int(info[v]) == 1:
						#	taskSingle[k] = "%s" % (0)
						#else:
						#	taskSingle[k] = "%s" % (11)
						#taskSingle[k] = "%s" % info[v]
						#taskSingle[k] = "%s" % 10
						continue
					elif k in ["id2","c_create_editor","from_aid","m_status"]:
						continue
					elif k == "priority":
						if priority < int(info[v]):
							priority = info[v]
						continue
					elif k == "tx_albumid":
						if int(info[v]) > 0:
							taskSingle[k] = "tm_%s" % info[v]
						continue
					elif k == "from_id":
						if int(info[v]) > 0:
							taskSingle[k] = "tm_%s" % info[v]
						continue
					elif info.has_key(v) and info[v] != "None" and info[v] is not None:
						taskSingle[k] = "%s" % info[v]
				if task_type == "Music":
					for k,v in self.config.configinfo["MusicConst"].items():
						if k in ["c_batch","c_show_type"]:
							taskSingle[k] = "%s" % v
				if task_type == "Artist":
					for k,v in self.config.configinfo["ArtistConst"].items():
						if k in ["c_batch","artisttype"]:
							taskSingle[k] = "%s" % v
				if task_type == "Album":
					for k,v in self.config.configinfo["AlbumConst"].items():
						if k in ["c_batch","c_show_type"]:
							taskSingle[k] = "%s" % v
				if len(taskSingle) > 0:
					task_info.append({"%s" % task_type:taskSingle})
				ids_task.add(info["id"])
				tsk_count += 1

			post_dict = {}
			post_dict["count"] = "%s" % (len(task_info))
			post_dict["info"] = task_info
			#logging.info(str(post_dict))
			for k,v in self.config.configinfo["common"].items():
				if k in ["editor_id","priority"]:
					if k == "priority":
						post_dict[k] = "%s" % priority
					else:
						post_dict[k] = "%s" % v
			return post_dict,ids_task
		except Exception,e:
			traceback.print_exc()
			logging.info(str(e))
			#pass

	def getRealIdfromTask(self,taskid,table_type):
		sql = "select table_id,status from DMSTask.Action where task_id=%s and `table`=\"%s\" limit 1" %(taskid,table_type)
		cnt = self.curRun.execute(sql)
		if cnt > 0:
			ret = self.curRun.fetchone()
			self.connRun.commit()
			table_id = ret["table_id"]
			status = ret["status"]
			logging.info("table id : %s, status : %s" % (table_id,status))
			return table_id,status
		else:
			return 0,False

	def disonlineTask(self, task_type, configDict, ids):
		###c_show_type 11
		task_info = []
		for id in ids:
			info = '{"%s":{"c_show_type":"11","id":"%s"}}' % (task_type,id)
			task_info.append(info)
		post_dict = {}
		post_dict["count"] = "%s" % (len(task_info))
		post_dict["info"] = task_info
		for k,v in configDict["common"].items():
			if k in ["editor_id","priority"]:
				post_dict[k] = "%s" % v
		return post_dict

	def sendTask(self, info):
		try:
			task_id = 0
			#task_id = 30884833 #Music Album
			#task_id = 30916210 #Artist
			if len(info["info"]) == 0:
				return task_id
			post_json = json.dumps(info)
			logging.info(str(post_json))
			#'''
			f = urllib2.urlopen(
				url     = self.taskurl,
				data    = post_json
				)

			result = f.read()
			logging.info(str(result))
			js_ret = json.loads(result)
			if result.find("OK") >= 0:
				task_id = int(js_ret["taskid"])
			#'''
			return task_id
		except Exception,e:
			logging.info(str(e))
			#pass

