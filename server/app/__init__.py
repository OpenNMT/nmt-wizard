from flask import Flask
from flask_ini import FlaskIni
import os
from nmtwizard.redis_database import RedisDatabase
from nmtwizard import common

import logging
import time
from redis.exceptions import ConnectionError

VERSION = "1.2.1"
def append_version(v):
    global VERSION
    VERSION += ":" + v
def get_version():
    return VERSION

app = Flask(__name__)
app._requestid = 1

ch = logging.StreamHandler()
app.logger.addHandler(ch)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)

common.add_log_handler(ch)

config_file = os.getenv('LAUNCHER_CONFIG', 'settings.ini')
assert config_file is not None and os.path.isfile(config_file), "invalid LAUNCHER_CONFIG"

app.iniconfig = FlaskIni()
with app.app_context():
    app.iniconfig.read(config_file)

app.logger.setLevel(logging.getLevelName(
                    app.iniconfig.get('default', 'log_level', fallback='ERROR')))

redis = RedisDatabase(app.iniconfig.get('redis','host'),
                      app.iniconfig.get('redis','port',fallback=6379),
                      app.iniconfig.get('redis','db',fallback=0),
                      app.iniconfig.get('redis', 'password',fallback=None))

assert app.iniconfig.get('default', 'taskfile_dir'), "missing taskfile_dir from settings.ini"
taskfile_dir = app.iniconfig.get('default', 'taskfile_dir')
assert os.path.isdir(taskfile_dir), "taskfile_dir (%s) must be a directory" % taskfile_dir

retry = 0
while retry < 10:
    try:
        storages_list = redis.get("admin:storages")
        assert storages_list, "ERROR: cannot get storages from worker db"
        break
    except (ConnectionError, AssertionError) as e:
        retry += 1
        time.sleep(1)

assert retry < 10, "Cannot connect to redis DB - aborting"

from app import routes
