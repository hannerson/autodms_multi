# -*- coding=utf-8 -*-
import os,sys
import MySQLdb
import traceback
from pooldb import *
from logger import *
from taskProcessor import *
from dispatcher_match import *

reload(sys)
sys.setdefaultencoding('utf-8')

if __name__ == '__main__':
	#print g_config.configinfo
	dispatcher_album = dispatcher_match(g_config,logging)
	dispatcher_album.create_workers("Album","AlbumSrc","MusicSrc",True)
	dispatcher_album.dispatch("Album","AlbumSrc",1,10,500,g_pool_Src,g_pool_Run,g_pool_Res)
