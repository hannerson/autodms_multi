# -*- coding=utf-8 -*-

import logging
from logging.handlers import TimedRotatingFileHandler
import sys
import threading

reload(sys)
sys.setdefaultencoding('utf-8')

def get_logger():
	logger = logging.getLogger()
	LOG_FILE = "log/" + sys.argv[0].split("/")[-1].replace(".py","") + '.log'
	hdlr = TimedRotatingFileHandler(LOG_FILE,when='H',backupCount=24)
	formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(filename)s[line:%(lineno)d]:%(message)s')
	hdlr.setFormatter(formatter)
	logger.addHandler(hdlr)
	logger.setLevel(logging.NOTSET)
	return logger

logging = get_logger()
