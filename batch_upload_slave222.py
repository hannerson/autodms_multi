#! /usr/bin/env python
#coding=utf-8

import MySQLdb
import sys
import os
import traceback
import json
import os.path
import time
from multiprocessing import Process
import logging
from logging.handlers import TimedRotatingFileHandler
from Queue import Queue 
import utils                    
from multiprocessing.managers import BaseManager
import time
import re
import random
import urllib2
import hashlib
from resource_request import *
import string

reload(sys)
sys.setdefaultencoding('utf-8')

###config --- global###
g_Server_Ip = "111.206.73.111"
g_Server_Port = 31415

###ip source: local ip address
g_source_ip = "192.168.74.86"
#g_server_ip = "192.168.73.108"
g_server_ips = []

ftp_user = "ftserver"
ftp_pwd = "yeelion"
upload_dir = "/data/ftserver/"
local_dir = "/data/ftserver/tmp/"

g_Config = utils.loadConfig("./config.conf")

g_status = {"default":0,"dispatch":1,"sig_ok":2,"artist_ok":3,"task_send":4,"artist_no":5,"task_fail":6,"task_suc":7,"sig_fail":8,"retry_fail":9,"has_matched":10,"editor_album":11}

g_sqlConfig = g_Config["dbconfig"]
g_connSrc = MySQLdb.connect(host=g_sqlConfig["src_dbhost"],user=g_sqlConfig["src_dbuser"],passwd=g_sqlConfig["src_dbpwd"],db=g_sqlConfig["src_dbname"],charset=g_sqlConfig["src_dbcharset"],port=g_sqlConfig["src_dbport"])
g_curSrc = g_connSrc.cursor()
#g_connRes = MySQLdb.connect(host=g_sqlConfig["res_dbhost"],user=g_sqlConfig["res_dbuser"],passwd=g_sqlConfig["res_dbpwd"],db=g_sqlConfig["res_dbname"],charset=g_sqlConfig["res_dbcharset"],port=g_sqlConfig["res_dbport"])
#g_curRes = g_connRes.cursor()
#g_connRun = MySQLdb.connect(host=g_sqlConfig["task_dbhost"],user=g_sqlConfig["task_dbuser"],passwd=g_sqlConfig["task_dbpwd"],db=g_sqlConfig["task_dbname"],charset=g_sqlConfig["task_dbcharset"],port=g_sqlConfig["task_dbport"])
#g_curRun = g_connRun.cursor()

def initlog():
        logger = logging.getLogger()
        LOG_FILE = sys.argv[0].split("/")[-1].replace(".py","") + '.log'
        hdlr = TimedRotatingFileHandler(LOG_FILE,when='H',backupCount=24)
        formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(filename)s[line:%(lineno)d]:%(message)s')
        hdlr.setFormatter(formatter)
        logger.addHandler(hdlr)
        logger.setLevel(logging.NOTSET)
        return logger

logging =  initlog()

def get_all_cdns_availible(conn,cur):
	sql = '''select ip_cdn,weight from IP_CDN where percent<95 and m_status=0'''
	cnt = cur.execute(sql)
	if cnt > 0:
		rets = cur.fetchall()
		conn.commit()
		for ret in rets:
			for i in range(0,ret[1]):
				g_server_ips.append(ret[0].strip())

def upload_ftp_audio(file_path):
	ret = True
	audiovideo_prefix = {'mp3':'n','wma':'m','mkv':'v','wmv':'v','ape':'s','flac':'s',\
	                                                         'aac':'a','jpg':'p','f4v':'v','mp4':'m'}
	str_sigs = get_file_sig(file_path)
	if str_sigs == "":
		ret = False
		return ret
	ssigs = str_sigs.split(",")
	str_path_name = file_path.split("/")[-1]
	fmt = str_path_name.split(".")[-1]
	sig1 = ssigs[0]
	sig2 = ssigs[1]
	d1 = str(long(sig2)%100)
	d2 = str((long(sig2)/100)%100)
	dv = str((long(sig1)^long(sig2))%3 + 1)
	path = "%s%s/%s/%s/%s.%s"%(audiovideo_prefix.get(fmt,"mp3"),dv,d1,d2,sig1,fmt)
	if path == "":
		ret = False
		return ret
	subdirs = path.split("/")
	filename = subdirs[-1].strip()
	del(subdirs[-1])
	MaxPutfileRetry = 10
	PutfileOK = False
	for retry in range(0, MaxPutfileRetry):
		dp = ftpuse.putfile(server_ip, ftp_user, ftp_pwd,upload_dir, subdirs, filename, file_path)
		if dp:
			PutfileOK = True
			break
		else:
			time.sleep(5)
	if not PutfileOK:
		ret = False
		return ret
	return ret,sig1,sig2,path

def Mvfile2Source(src, dst, ismove=True):
    ret = True
    try:
        if os.path.exists(os.path.dirname(dst)) == False:
            os.makedirs(os.path.dirname(dst))
        cmd = 'mv -f %s %s ' % (src, dst)
        if ismove == False:
            cmd = 'cp -f %s %s ' % (src, dst)
        os.system(cmd)
    except Exception as e :
        print('''Exception in Mvfile2Srouce , src=%s , dst  = %s , reason = %s ''' % (src, dst, str(e)))
        ret = False
    return ret

def upload_local_audio(file_path,ismove=True):
	ret = True
	sig1 = ""
	sig2 = ""
	path = ""
	audiovideo_prefix = {'mp3':'n','wma':'m','mkv':'v','wmv':'v','ape':'s','flac':'s',\
	                                                         'aac':'a','jpg':'p','f4v':'v','mp4':'m'}
	str_sigs = get_file_sig(file_path)
	if str_sigs == "":
		ret = False
		return ret,sig1,sig2,path
	ssigs = str_sigs.split(",")
	str_path_name = file_path.split("/")[-1]
	fmt = str_path_name.split(".")[-1]
	sig1 = ssigs[0]
	sig2 = ssigs[1]
	d1 = str(long(sig2)%100)
	d2 = str((long(sig2)/100)%100)
	dv = str((long(sig1)^long(sig2))%3 + 1)
	path = "%s%s/%s/%s/%s.%s"%(audiovideo_prefix.get(fmt,"mp3"),dv,d1,d2,sig1,fmt)
	if path == "":
		ret = False
		return ret,sig1,sig2,path
	dst = upload_dir + path
	ret = Mvfile2Source(file_path, dst,ismove)
	return ret,sig1,sig2,path

def get_has_upload(f):
	has_music = set()
	for line in f.readlines():
		mid = line.strip()
		if mid != "":
			has_music.add(mid)
	return has_music

# 下载文件
def downurl(url, topath, cookie=""):
    cmd = "mkdir -p \"%s\" 2>/dev/null"%os.path.dirname(topath)
    os.system(cmd)

    cmd = "curl -m 400 --connect-timeout 60 --location -s \"%s\" -o \"%s\""%(url, topath)
    print cmd
    if len(cookie)>0:
        cmd += " -b \"" + cookie + "\""

    # 进行一次重试
    if os.system(cmd)!=0 and os.system(cmd)!=0:
        os.system("rm -f \""+topath+"\"")
        time.sleep(60)

    # 一分钟后进行最后一次尝试
    if os.system(cmd)!=0:
        os.system("rm -f \""+topath+"\"")
        return False
    return True

#下载文件
def download_url(file, url):
    if os.path.exists(file) and os.path.getsize(file):
        return 0

    cmd    = "wget --limit-rate=%dk --tries=20  --read-timeout=300 -q -O \"%s\" \"%s\" " % (5000, file, url)
    print cmd
    res    = os.system(cmd)
    print res
    if res != 0:
        cmd    = "wget --limit-rate=%dk --tries=10  --read-timeout=500 -q -O \"%s\" \"%s\" " % (5000, file, url)
        res    = os.system(cmd)
    else:
        res = 0
    return res

def get_audio_file(strUrl,strLocalFile):
	ret = download_url(strLocalFile,strUrl)
	if ret != 0:
		logging.error("down load file %s failed." % strUrl)
		return False
	return True

def get_file_size(path):
    try:
        return os.path.getsize(path)
    except Exception as e:
        print("get_file_size for file %s fail:%s " % (path, str(e)))
        return ""

__mkylnewsig_bin__ = "/home/ftserver/mkylnewsig"
def get_file_sig(path):
    cmd = "%s \"%s\""%(__mkylnewsig_bin__, path)
    try:
        return os.popen(cmd).readline().strip()
    except Exception as e:
        print("get_file_sig for file %s fail:%s " % (path, str(e)))
	logging.error("get_file_sig for file %s fail:%s " % (path, str(e)))
        return ""

def update_music_sig_info(id,file_size,file_sig1,file_sig2,file_path,file_type,c_extparams,file_format,status):
        sql = '''update MusicSrc set file_size=%s,file_sig1=%s,file_sig2=%s,file_path=\"%s\",file_type=\"%s\",c_extparams=\"%s\",file_format=\"%s\",m_status=%s,sig_count=sig_count+1 where id=%s''' % (file_size,file_sig1,file_sig2,file_path,file_type,c_extparams,file_format,status,id)
        cnt = g_curSrc.execute(sql)
        if cnt > 0:
		g_connSrc.commit()
                return True
        else:
                return False

def update_music_status(id,status,connSrc,curSrc):
	sql = '''update MusicSrc set m_status=%s where id=%s''' % (status,id)
	cnt = curSrc.execute(sql)
	if cnt > 0:
		connSrc.commit()
	else:
		logging.info("MusicSrc id:%s failed" % (id))

def update_music_sig_count(id,connSrc,curSrc):
	sql = '''update MusicSrc set sig_count=sig_count+1 where id=%s''' % (id)
	cnt = curSrc.execute(sql)
	if cnt > 0:
		connSrc.commit()
		return True
	else:
		return False

def check_MusicSrc_sig_count(id,connSrc,curSrc):
        retry_cnt = 0
        sql = '''select sig_count from MusicSrc where id=%s''' % (id)
        cnt = curSrc.execute(sql)
        if cnt > 0:
                ret = curSrc.fetchone()
                retry_cnt = ret[0]
                if retry_cnt > 3:
                        sql = '''update MusicSrc set m_status=%s where id=%s''' % (g_status["retry_fail"],id)
                        cnt = curSrc.execute(sql)
                        if cnt > 0:
                                connSrc.commit()
        return retry_cnt

def get_music_server_ip(id,connSrc,curSrc):
	sql = '''select server_ip from MusicSrc where id=%s''' % (id)
	cnt = curSrc.execute(sql)
	if cnt > 0:
		ret = curSrc.fetchone()
		return ret[0]
	else:
		return ""

def upload_music_into_sql(id,connSrc,curSrc):
	sql ='''select source_ip,m_audio_url,source_type,mid from MusicSrc where id=%s''' % (id)
	cnt = curSrc.execute(sql)
	###just 
	if cnt < 1:
		logging.info("MusicSrc id: %s not exists" % (id))
		return

	ret = curSrc.fetchone()
	source_ip = ret[0].strip()
	m_audio_url = ret[1].strip()
	source_type = ret[2]
	qq_track_id = ret[3]
	connSrc.commit()
	###check cdn space
	count_upload = 0
	global g_server_ips
	g_server_ips = []
	get_all_cdns_availible(connSrc,curSrc)
	logging.info("cdn space availible : %s" % (len(g_server_ips)))
	if len(g_server_ips) == 0:
		logging.info("cdn space not availible : %s" % (len(g_server_ips)))
		return
	g_server_ip = g_server_ips[random.randint(0,len(g_server_ips)-1)]
	logging.info("cdn select : %s" % (g_server_ip))
	if source_type == 0:###normal
		digit_replace = re.compile(r'/data\d*/')
		qq_find = re.compile(r'/data\d*/qqtemp')
		kg_find = re.compile(r'/data\d*/kugoutemp')
		ftproot_find = re.compile(r'/data\d*/ftproot')
		print m_audio_url
		if len(qq_find.findall(m_audio_url)) > 0:
			#http_audio_url = "http://%s:8888/%s" % (source_ip,m_audio_url.replace("/data/qqtemp","qqtemp"))
			http_audio_url = "http://%s:8888/%s" % (source_ip,qq_find.sub("qqtemp",m_audio_url))
		elif len(ftproot_find.findall(m_audio_url)) > 0:
			#http_audio_url = "http://%s:8888/%s" % (source_ip,m_audio_url.replace("/data1/ftproot",""))
			http_audio_url = "http://%s:8888/%s" % (source_ip,ftproot_find.sub("",m_audio_url))
		elif len(kg_find.findall(m_audio_url)) > 0:
			#http_audio_url = "http://%s:8888/%s" % (source_ip,m_audio_url.replace("/data/qqtemp","qqtemp"))
			http_audio_url = "http://%s:8888/%s" % (source_ip,kg_find.sub("kugoutemp",m_audio_url))
		else:
			http_audio_url = "http://%s:8888/%s/%s" % (source_ip,"source_data/audio",m_audio_url)
			
		tmp_path = local_dir + os.path.basename(m_audio_url)
	elif source_type == 1:###quku 2.0
		if qq_track_id == 0:
			update_music_status(id,g_status["retry_fail"],connSrc,curSrc)
			return
		ret_url = resource_request(qq_track_id,1)
		http_audio_url = ret_url["url"]
		tmp_path = local_dir + ''.join(random.sample(string.ascii_letters + string.digits, 16)
		
	logging.info(http_audio_url)
	logging.info(tmp_path)
	ret = get_audio_file(http_audio_url, tmp_path)
	if ret == False:
		logging.info("download failed")
		update_music_status(id,g_status["sig_fail"],connSrc,curSrc)
		update_music_sig_count(id,connSrc,curSrc)
		#return
	if check_MusicSrc_sig_count(id,connSrc,curSrc) > 3:
		logging.info("MusicSrc id %s retry sig failed" % (id))
		return
	if not ret:
		###sig fail
		update_music_status(id,g_status["sig_fail"],connSrc,curSrc)
		update_music_sig_count(id,connSrc,curSrc)
		return

	file_fmt = m_audio_url.split(".")[-1]

	server_ip = get_music_server_ip(id,connSrc,curSrc)
	logging.info("server_ip %s" % (server_ip))
	if server_ip == "":
		logging.info("MusicSrc id %s server_ip not exists" % (id))
		update_music_status(id,g_status["sig_fail"],connSrc,curSrc)
		update_music_sig_count(id,connSrc,curSrc)
		return
	filesize = get_file_size(tmp_path)
	logging.info("filesize %s" % (filesize))
	if filesize == 0:
		update_music_status(id,g_status["sig_fail"],connSrc,curSrc)
		update_music_sig_count(id,connSrc,curSrc)
		return
	ret,sig1,sig2,dms_path = upload_local_audio(tmp_path,True)
	logging.info("%s %s %s" % (ret,sig1,sig2))
	if ret:
		c_extparams = "orig_ip=%s&targ_ip=%s" % (g_source_ip,g_server_ip)
		file_type = "audio_mass"
		update_music_sig_info(id,filesize,sig1,sig2,dms_path,file_type,c_extparams,file_fmt,g_status["sig_ok"])
		update_music_sig_count(id,connSrc,curSrc)
		count_upload += 1
		logging.info("sig ok")
	else:
		update_music_status(id,g_status["sig_fail"],connSrc,curSrc)
		update_music_sig_count(id,connSrc,curSrc)
		logging.info("sig failed")

class Slave:
	def __init__(self):
		self.job_queue = Queue(maxsize = 50)

	def start(self):
		BaseManager.register('get_dispatched_jobs')
		manager = BaseManager(address=(g_Server_Ip, g_Server_Port), authkey='jobs')
		manager.connect()
		
		dispatched_jobs = manager.get_dispatched_jobs()

		while True:
			while dispatched_jobs.empty():
				logging.info("no dispatched jobs sleep 2s")
				time.sleep(2)
			job = dispatched_jobs.get(timeout=60)
			logging.info('get jobs %s' % job)
			###mark has dispatched
			update_music_status(job,g_status["dispatch"],g_connSrc,g_curSrc)
			###upload process
			upload_music_into_sql(job,g_connSrc,g_curSrc)
			break

if __name__ == "__main__":
	slave = Slave()
	slave.start()
