#!/bin/python
# -*- coding = utf-8 -*-

import os,sys
import MySQLdb
import logging
import json
import utils
import traceback
import urllib2
import random
import string
from PIL import Image
import datetime
import time
from commonConfig import *
from sqlClass import *

g_validFields = ["id","id2","m_name","m_aliasname","m_album_id","m_subalbum","m_track","m_audio_id","m_copyright","basic_version","basic_releasedate","basic_company","basic_intro","tags_category","tags_genre","tags_region","tags_lang","c_show_type","m_artists"]

class Task(object):
	def __init__(self,connRun,curRun,connRes,curRes,taskurl="http://centerserver.kuwo.cn/add_task"):
		self.taskurl = taskurl
		self.connRun = connRun
		self.curRun = curRun
		self.connRes = connRes
		self.curRes = curRes
		self.sqlRes = sqlClass(connRes,curRes)
		self.sqlRun = sqlClass(connRun,curRun)

	def getPicSource(self,src_url,tmp_local_dir):
		try:
			ret = utils.download_url(tmp_local_dir,src_url)
			if ret != 0:
				return 0
			logging.info("downloaded picture path:" + tmp_local_dir)
			dms_sig1,dms_sig2 = utils.getSig(tmp_local_dir).strip().split(",")
			if not (dms_sig1 and dms_sig2):
				return 0
			where = "file_sig1=%s and file_sig2=%s and version_editor>1" % (dms_sig1,dms_sig2)
			ret = self.sqlRun.mysqlSelect("PicSource",where,1,["id"])
			if ret:
				return ret[0]["id"]
			return 0
		except Exception,e:
			logging.info(str(e))
			pass

	def getAudioSource(self,src_url,tmp_local_dir):
		try:
			ret = utils.download_url(tmp_local_dir,src_url)
			if ret != 0:
				return 0
			dms_sig1,dms_sig2 = utils.getSig(tmp_local_dir).strip().split(",")
			if not (dms_sig1 and dms_sig2):
				return 0
			where = "file_sig1=%s and file_sig2=%s and version_editor>1" % (dms_sig1,dms_sig2)
			ret = self.sqlRun.mysqlSelect("AudioSource",where,1,["id"])
			if ret:
				return ret[0]["id"]
			return 0
		except Exception,e:
			logging.info(str(e))
			pass

	def getAudioSource2(self,file_sig1,file_sig2):
		try:
			where = "file_sig1=%s and file_sig2=%s and version_editor>1" % (file_sig1,file_sig2)
			ret = self.sqlRun.mysqlSelect("AudioSource",where,1,["id"])
			if ret:
				return ret[0]["id"]
			return 0
		except Exception,e:
			logging.info(str(e))
			pass

	def genAudioSource(self,src_url,tmp_local_dir):
		try:
			AudioSource = {}
			###download
			ret = utils.download_url(tmp_local_dir,src_url)
			if ret != 0:
				return AudioSource
			file_size = os.path.getsize(tmp_local_dir)
			dms_sig1,dms_sig2 = utils.getSig(tmp_local_dir).strip().split(",")
			if not (dms_sig1 and dms_sig2):
				return AudioSource
			#dms_path = utils.songFileProcess(dms_sig1,dms_sig2,g_config["common"]["path_pre"],tmp_local_dir)
			AudioSource["file_size"] = ("%s") % file_size
			AudioSource["file_path"] = src_url.encode('utf-8')
			AudioSource["file_sig1"] = ("%s") % dms_sig1
			AudioSource["file_sig2"] = ("%s") % dms_sig2
			#AudioSource["file_type"] = "audio_mass"
			AudioSource["file_type"] = "audio_mass2"
			if g_config["MusicConst"].has_key("orig_ip") and g_config["MusicConst"].has_key("targ_ip"):
				AudioSource["c_extparams"] = "orig_ip=%s&targ_ip=%s" % (g_config["MusicConst"]["orig_ip"],g_config["MusicConst"]["targ_ip"])
			AudioSource["file_format"] = src_url.split('.')[-1]
			audio_id = ("NEW_AudioSource")
			AudioSource["id"] = audio_id
			return AudioSource
		except Exception,e:
			logging.info(str(e))
			pass

	def genPicSource(self,t_task,src_url,tmp_local_dir,path_pre,http_pre):
		try:
			PicSource = {}
			ret = utils.download_url(tmp_local_dir,src_url)
			if ret != 0:
				return PicSource
			logging.info("downloaded picture path:" + tmp_local_dir)
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

			dms_sig1,dms_sig2 = utils.getSig(tmp_local_dir).strip().split(",")
			if not (dms_sig1 and dms_sig2):
				return PicSource
			sigpath = utils.ImageFileProcess(dms_sig1,dms_sig2,path_pre,tmp_local_dir).lstrip("/")
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
			logging.info(str(e))
			pass

	def genTaskSingle(self,task_type,config,info):
		try:
			task_info = []
			taskSingle = {}
			print info
			for k,v in config.items():
				if k in ["file_size","file_sig1","file_sig2","file_path","file_type","c_extparams","file_format"]:
					continue
				if k == "m_artists":###: split ';','###',',','&'
					taskSingle[k] = list(utils.getArtistIdsFromName(info[v].strip(),"|",g_connRun,g_curRun))
					continue
				if k == "m_artists_id": ###default sepa=;
					#taskSingle["m_artists"] = info[v].strip().split(";")
					taskSingle["m_artists"] = info[v]
					continue
				if k == "m_record_pic_id" or k == "m_pic_id":
					###tmp dir,genarate the file name by rand or use source file name
					tmp_local_dir = g_config["common"]["pre_path"] + "pic/"
					if not os.path.exists(tmp_local_dir):
						os.makedirs(tmp_local_dir)
					logging.info(str(info[v]))
					if info[v] == "":
						continue
					if g_config["common"].has_key("http_peer_pre") and info[v].find("http") == -1:
						info[v] = g_config["common"]["http_peer_pre"] + info[v]
					#tmp_local_dir += info[v].split("/")[-1]
					###task_type + "_" + id
					tmp_local_dir += "%s_%s" % (task_type,''.join(random.sample(string.ascii_letters + string.digits, 8)))
					pic_id = self.getPicSource(info[v],tmp_local_dir)
					if pic_id == 0:
						taskSingle[k] = "NEW_PicSource"
						PicSource = self.genPicSource(task_type,info[v],tmp_local_dir,g_config["common"]["pre_path"] + "pic/",g_config["common"]["http_local_pre"])
						if len(PicSource) < 7:
							logging.info("pic source error")
							return []
						task_info.append({"PicSource":PicSource})
					else:
						taskSingle[k] = "%s" % (pic_id)
				elif k == "m_audio_id":
					if v == "NEW_AudioSource":
						audio_id = self.getAudioSource2(info["file_sig1"],info["file_sig2"])
						if audio_id > 0:
							taskSingle[k] = "%s" % (audio_id)
						else:
							AudioSource = {}
							AudioSource["file_size"] = ("%s") % info["file_size"]
							AudioSource["file_path"] = info["file_path"].strip("/")
							#AudioSource["file_path"] = "http://111.206.73.125" + "/source_data/audio/" + info["file_path"].lstrip("/")
							AudioSource["file_sig1"] = ("%s") % info["file_sig1"]
							AudioSource["file_sig2"] = ("%s") % info["file_sig2"]
							AudioSource["file_type"] = "audio_mass2"
							AudioSource["c_extparams"] = ("%s") % info["c_extparams"]
							#if g_config["MusicConst"].has_key("orig_ip") and g_config["MusicConst"].has_key("targ_ip"):
							#	AudioSource["c_extparams"] = "orig_ip=%s&targ_ip=%s" % (g_config["MusicConst"]["orig_ip"],g_config["MusicConst"]["targ_ip"])
							AudioSource["file_format"] = ("%s") % info["file_format"]
							audio_id = ("NEW_AudioSource")
							AudioSource["id"] = "%s" % (v)
							task_info.append({"AudioSource":AudioSource})
							taskSingle[k] = "%s" % (v)
					elif v == "m_audio_url":
						###tmp dir,genarate the file name by rand or use source file name
						tmp_local_dir = g_config["common"]["pre_path"] + "audio/"
						if not os.path.exists(tmp_local_dir):
							os.makedirs(tmp_local_dir)
						tmp_local_dir += info[v].split("/")[-1]
						audio_id = self.getAudioSource(info[v],tmp_local_dir)
						if audio_id == 0:
							taskSingle[k] = "NEW_AudioSource"
							AudioSource = {}
							AudioSource = self.genAudioSource(info[v],tmp_local_dir)
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
				elif k == "id" and v.find("NEW") == 0:
					if config.has_key("id2") and info.has_key(config["id2"]) and info[config["id2"]] != "" and info[config["id2"]] != "0" and info[config["id2"]] != 0 and info[config["id2"]] is not None and info[config["id2"]] != "None":
						taskSingle[k] = "%s" % info[config["id2"]]
					else:
						taskSingle[k] = "%s" % v
				elif k == "c_show_type":
					#if int(info[v]) == 1:
					#	taskSingle[k] = "%s" % (0)
					#else:
					#	taskSingle[k] = "%s" % (11)
					taskSingle[k] = "%s" % info[v]
					continue
				elif k in ["id2"]:
					continue
				elif info.has_key(v) and info[v] != "None" and info[v] is not None:
					taskSingle[k] = "%s" % info[v]
			if task_type == "Music":
				for k,v in g_config["MusicConst"].items():
					#if k in ["c_batch","c_show_type"]:
					if k in ["c_batch"]:
						taskSingle[k] = "%s" % v
			if task_type == "Artist":
				for k,v in g_config["ArtistConst"].items():
					if k in ["c_batch","artisttype"]:
						taskSingle[k] = "%s" % v
			if task_type == "Album":
				for k,v in g_config["AlbumConst"].items():
					if k in ["c_batch"]:
						taskSingle[k] = "%s" % v
			if len(taskSingle) > 0:
				task_info.append({"%s" % task_type:taskSingle})
			return task_info
		except Exception,e:
			traceback.print_exc()
			logging.info(str(e))
			pass

	def getInfoFromFile(self,line,configDict,task_type):
		try:
			task_info = []
			arr = line.strip("\r\n").split("\t")
			if configDict.has_key("MusicFile") and "Music" in task_type:
				infoConfig = configDict["MusicFile"]
				taskSingle = self.genTaskSingle("Music",infoConfig,arr)
				for task in taskSingle:
					task_info.append(task)

			if configDict.has_key("AlbumFile") and "Album" in task_type:
				infoConfig = configDict["AlbumFile"]
				taskSingle = self.genTaskSingle("Album",infoConfig,arr)
				for task in taskSingle:
					task_info.append(task)

			if configDict.has_key("ArtistFile") and "Artist" in task_type:
				infoConfig = configDict["ArtistFile"]
				taskSingle = self.genTaskSingle("Artist",infoConfig,arr)
				for task in taskSingle:
					task_info.append(task)

			post_dict = {}
			post_dict["count"] = "%s" % (len(task_info))
			post_dict["info"] = task_info
			for k,v in configDict["common"].items():
				if k in ["editor_id","priority"]:
					post_dict[k] = v
			return post_dict
		except Exception,e:
			logging.info(str(e))
			pass

	def getInfoFromSql(self,sqlinfo,configDict,task_type):
		try:
			task_info = []
			if configDict.has_key("MusicSql") and "Music" in task_type:
				infoConfig = configDict["MusicSql"]
				taskSingle = self.genTaskSingle("Music",infoConfig,sqlinfo)
				for task in taskSingle:
					task_info.append(task)

			if configDict.has_key("AlbumSql") and "Album" in task_type:
				infoConfig = configDict["AlbumSql"]
				taskSingle = self.genTaskSingle("Album",infoConfig,sqlinfo)
				for task in taskSingle:
					task_info.append(task)

			if configDict.has_key("ArtistSql") and "Artist" in task_type:
				infoConfig = configDict["ArtistSql"]
				taskSingle = self.genTaskSingle("Artist",infoConfig,sqlinfo)
				for task in taskSingle:
					task_info.append(task)

			post_dict = {}
			post_dict["count"] = "%s" % (len(task_info))
			post_dict["info"] = task_info
			for k,v in configDict["common"].items():
				if k in ["editor_id","priority"]:
					post_dict[k] = "%s" % v
			return post_dict
		except Exception,e:
			logging.info(str(e))
			#pass

	def getRealIdfromTask(self,taskid,table_type):
		where = "task_id=%s and `table`=\"%s\"" %(taskid,table_type)
		ret = self.sqlRun.mysqlSelect("DMSTask.Action",where,1,["table_id","status"])
		if ret:
			table_id = ret[0]["table_id"]
			status = ret[0]["status"]
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
			pass

