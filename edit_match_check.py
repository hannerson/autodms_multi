# -*- coding=utf-8 -*-

import os,sys
import MySQLdb
import traceback
import utils
import json
import logging
from logging.handlers import TimedRotatingFileHandler
import MySQLdb.cursors
import time
from sqlClass import *
from pooldb import *
from logger import *





def process_edit_musicsrc(path):
	musicsrc = {}
	f = open(path,"r")
	for line in f:
		arr = line.strip().split("\t")
		arr
		
