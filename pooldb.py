# -*- coding=utf-8 -*-

import os,sys
import MySQLdb
import utils
import MySQLdb.cursors
from DBUtils.PooledDB import PooledDB
from config import *

reload(sys)
sys.setdefaultencoding('utf-8')

###global###
#G_CONFIG_FILE = "./config.conf"
g_config = config("./config.conf.2")
g_sqlConfig = g_config.configinfo["dbconfig"]
g_pool_Src = PooledDB(MySQLdb,7,host=g_sqlConfig["src_dbhost"],user=g_sqlConfig["src_dbuser"],passwd=g_sqlConfig["src_dbpwd"],db=g_sqlConfig["src_dbname"],charset=g_sqlConfig["src_dbcharset"],port=g_sqlConfig["src_dbport"],cursorclass=MySQLdb.cursors.DictCursor)
g_pool_Res = PooledDB(MySQLdb,5,host=g_sqlConfig["res_dbhost"],user=g_sqlConfig["res_dbuser"],passwd=g_sqlConfig["res_dbpwd"],db=g_sqlConfig["res_dbname"],charset=g_sqlConfig["res_dbcharset"],port=g_sqlConfig["res_dbport"])
g_pool_Run = PooledDB(MySQLdb,5,host=g_sqlConfig["task_dbhost"],user=g_sqlConfig["task_dbuser"],passwd=g_sqlConfig["task_dbpwd"],db=g_sqlConfig["task_dbname"],charset=g_sqlConfig["task_dbcharset"],port=g_sqlConfig["task_dbport"])
g_pool_TMApi = PooledDB(MySQLdb,5,host=g_sqlConfig["TMApi_dbhost"],user=g_sqlConfig["TMApi_dbuser"],passwd=g_sqlConfig["TMApi_dbpwd"],db=g_sqlConfig["TMApi_dbname"],charset=g_sqlConfig["TMApi_dbcharset"],port=g_sqlConfig["TMApi_dbport"],cursorclass=MySQLdb.cursors.DictCursor)

g_status = {"default":0,"dispatch":1,"sig_ok":2,"artist_ok":3,"task_send":4,"artist_no":5,"task_fail":6,"task_suc":7,"sig_fail":8,"retry_fail":9,"has_matched":10,"editor_album":11}

