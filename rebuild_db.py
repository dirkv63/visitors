"""
This procedure will rebuild the sqlite database
"""

import logging
from lib import my_env
from lib import sqlstore

cfg = my_env.init_env("visitors", __file__)
logging.info("Start application")
db_obj = sqlstore.DirectConn(cfg)
db_obj.rebuild()
logging.info("End application")
