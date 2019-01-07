#! /usr/bin/env python
#coding=utf-8

import logging
import os
import sys
import time
import urllib
import urllib2
import json
import MySQLdb
import traceback
import datetime
import logging
from logging.handlers import TimedRotatingFileHandler
import re

reload(sys)
sys.setdefaultencoding('utf-8')

def initlog():
        logger = logging.getLogger()
        LOG_FILE = sys.argv[0].split("/")[-1].replace(".py","") + '.log'
        hdlr = TimedRotatingFileHandler(LOG_FILE,when='H',backupCount=24)
        formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(filename)s[line:%(lineno)d]:%(message)s')
        hdlr.setFormatter(formatter)
        logger.addHandler(hdlr)
        logger.setLevel(logging.NOTSET)
        return logger

dlog = initlog()

c_batch = "1"
editor_id = "252"

def replace_html(s):
    s = s.replace('&quot;','"')
    s = s.replace('&amp;','&')
    s = s.replace('&lt;','<')
    s = s.replace('&gt;','>')
    s = s.replace('&nbsp;',' ')
    return s

def insert_dms_task(task_id,net_album_id,kw_artist_id,album,cur,conn):
    #now = datetime.datetime.now()
    #strDate = now.strftime('%Y-%m-%d %H:%M:%S')
    sql = (''' insert into netease_dms_task(task_id,net_album_id,artist_id,album) values("%s","%s","%s","%s") ''') % (task_id,net_album_id,kw_artist_id,album)
    try:
       cnt = cur.execute(sql)
       conn.commit()
       if cnt > 0:
            return True
    except Exception, e:
        dlog.error( "insert_dms_task,execute sql (%s) failed." % sql )
    return False

def new_add_album(art,cur,conn,fw):
    ret_add_album = [True,0]
    try:
        task_list = []
        strArtId = "NEW_Artist"
        ArtistJson = {}
        ArtistJson["id"] = strArtId
        ArtistJson["m_name"] = art.encode('utf-8')
        task_list.append({"Artist":ArtistJson})
        post_dict = {}
        post_dict["count"] = str(len(task_list))
        post_dict["priority"] = "9"   #任务优先级
        post_dict["editor_id"] = ("%s") % editor_id    #batch专用用户
        post_dict["timely"] = "0"           #即时上线
        post_dict["info"] = task_list

        post_json = json.dumps(post_dict)
        dlog.debug(post_json)
	#'''
        f = urllib2.urlopen(
                url     = 'http://centerserver.kuwo.cn/add_task',
                data    = post_json
                )
        result = f.read()
        js_ret = json.loads(result)
        dlog.debug(result)
	#'''
        task_id = 0
        if result.find("OK") >= 0:
           task_id = js_ret["taskid"]
        if task_id == 0:
            dlog.error( ("add album fail when we new_add_artist,artist:%s.") % (art) )
            ret_add_album[0] = False
            return ret_add_album
        else:
            strLine = ("%s\t%s\n") % (art,task_id)
            fw.write(strLine)
            #ret_insert = insert_dms_task(task_id,net_album_id,kw_artist_id,album,cur,conn)
            #if ret_insert == False:
            #    dlog.error( "new_add_album,insert album_id:%s,artist id:%s failed." % (net_album_id,kw_artist_id) )
    except Exception, e:
        traceback.print_exc()
        dlog.error("add album fail:%s" % str(e))
        ret_add_album[0] = False
        ret_add_album[1] = -1
        return ret_add_album
    return ret_add_album

def get_has_create_albumid(f):
	has_set = set()
	for line in f.readlines():
		line = line.strip()
		if line == "":
			continue
		has_set.add(line)
	return has_set

def artist_worker(cur,conn,artist_set,fw):
    f_has = open("music_artist_10_30_has_create.id3","ab+")
    #task_cnt = get_dms_has_running_task_cnt()
    #if task_cnt >= task_limit:
    #    dlog.info("current task number more than task limit %s." % task_limit)
    #    return True
    has_set = get_has_create_albumid(f_has)
    count = 0
    art_id = 0
    for art in artist_set:
	#if len(art) > 96:
	    #art = art[:96]
	art_id = get_artist_dms_id(cur,art)
	if art_id > 0:
	    continue
        new_add_album(art,cur,conn,fw)
	count += 1
        strLine = ("%s\n") % art
        f_has.write(strLine)
	if count % 30 == 0:
	    time.sleep(1)
        #break
    f_has.close()
    pass

def get_dms_albumid(curRun,connRun,task_id):
    sql = "select status,`table`,`table_id` from Action where task_id = %s and `table` = 'Album' " % task_id
    ret = [False,""]
    try:
        cnt = curRun.execute(sql)
        connRun.commit()
        if cnt > 0:
            album_id = 0
            ret_arr = curRun.fetchone()
            album_id = ret_arr[2]
            ret[1] = album_id
            ret[0] = True
        else:
            ret[0] = False    
    except:
        traceback.print_exc()
    return ret

def get_artist_dms_id(cur,artist):
	art_id = 0
	name = MySQLdb.escape_string(artist) 
	try:
		#sql = '''select id from DMSRuntime.Artist where m_name = "%s" ''' % name
		sql = '''select id from DMSRuntime.Artist where m_name = "%s" or m_name1="%s" or m_name2="%s" or m_name3="%s" or m_name4="%s" or m_name5="%s"''' % (name,name,name,name,name,name)
		cnt = cur.execute(sql)
		if cnt > 0:
			ret = cur.fetchone()
			art_id = ret[0]
	except:
		traceback.print_exc()
	return art_id

def get_create_artist_info(f,cur,f_art):
    artist_set = set()
    try:
        for line in f.readlines():
            artists = line.strip().split("\t")[0]
            if artists == "":
                print("bad line:%s,artist is empty.") % line.strip()
                continue
	    if artists.find("/") != -1:
                artists = artists.split("/")
	    elif artists.find(";") != -1:
                artist_arr = artists.split(";")
	    else:
                artist_arr = artists.split("###")
            for artist_name in artist_arr:
		#if len(artist_name) > 96:
			#artist_name = artist_name[:96]
		#p2 = re.compile("\(.*?\)|\[.*?\]|（.*?）|【.*?】|\{.*?\}")
		#artist_name = p2.sub("",artist_name).strip()
                art_id = get_artist_dms_id(cur,artist_name)
                if art_id != 0:
                    strLine = ("%s\t%s\n") % (artist_name,art_id)
                    f_art.write(strLine)
                    continue
                if artist_name not in artist_set:
                    artist_set.add(artist_name)
    except Exception as e:
        traceback.print_exc()
    return artist_set

def get_num_task_running(conn,cur):
        count = 0
        try:
                sql = "select count(*) from DMSTask.Task where status != \"fail\" and status != \"success\" and editor_id = %s" % editor_id
                cnt = cur.execute(sql)
                conn.commit()
                if cnt > 0:
                        ret = cur.fetchone()
                        count = ret[0]
        except Exception,e:
                traceback.print_exc()
        return count

if __name__ == "__main__":
    try:
        limit = 100
        #conn    = MySQLdb.connect(host="192.168.73.111",user="dmsbatch",passwd="dmsbatch",db="Res",charset='utf8',port=3306)
        #connRes = MySQLdb.connect(host="192.168.73.111",user="autodms",passwd="yeelion",db="Resource",charset='utf8',port=13306)
        #connRun = MySQLdb.connect(host="60.28.199.25",user="hanxin",passwd="lI52pq8a",db="DMSTask",charset='utf8',port=3306)
        connRun = MySQLdb.connect(host="10.0.23.5",user="autodms",passwd="yeelion",db="DMSTask",charset='utf8',port=3306)
        #cur     = conn.cursor()
        #curRes  = connRes.cursor()
	curRun = connRun.cursor()
        f = open("artist/artist.create." + sys.argv[1],"r")
        fw = open("artist/artist.create.ret." + sys.argv[1],"ab+")
        f_art = open("artist/artist.create.artistid." + sys.argv[1],"w")	
        artist_set = get_create_artist_info(f,curRun,f_art)
        artist_worker(curRun,connRun,artist_set,fw)
	###check whether task is over
	count = get_num_task_running(connRun,curRun)
	while count > 0:
		time.sleep(10)
		dlog.info("wait task over. sleep 10s")
		count = get_num_task_running(connRun,curRun)

        #cur.close()
        #conn.close()
        f.close()
        fw.close()
        f_art.close()
        #curRes.close()
        #connRes.close()
	curRun.close()
	connRun.close()
    except Exception, e:
        traceback.print_exc()
        exit(1)

