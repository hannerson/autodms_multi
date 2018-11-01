# -*- coding=utf-8 -*-
import os,sys
import MySQLdb
import traceback
from pooldb import *
from logger import *
from taskProcessor import *
from dispatcher import *

reload(sys)
sys.setdefaultencoding('utf-8')

if __name__ == '__main__':
	#print g_config.configinfo
	dispatcher_music = dispatcher(g_config,logging)
	dispatcher_music.create_workers("Music","MusicSrc","MusicSrc",True)
	dispatcher_music.dispatch("Music","MusicSrc",300,5,500,g_pool_Src,g_pool_Run,g_pool_Res)
