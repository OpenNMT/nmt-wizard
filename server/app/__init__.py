import os
import time
import logging
import json

from flask import Flask
from flask_ini import FlaskIni
# from redis.exceptions import ConnectionError

from nmtwizard.redis_database import RedisDatabase
from nmtwizard import common

VERSION = "1.10.1"

app = Flask(__name__)
app._requestid = 1

ch = logging.StreamHandler()
app.logger.addHandler(ch)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)

common.add_log_handler(ch)

config_file = os.getenv('LAUNCHER_CONFIG', 'settings.ini')
assert config_file is not None and os.path.isfile(config_file), "invalid LAUNCHER_CONFIG"

default_file = os.path.join(os.path.dirname(config_file), "default.json")
assert os.path.isfile(default_file), "Cannot find default.json: %s" % default_file

with open(default_file) as default_fh:
    default_config = default_fh.read()
    base_config = json.loads(default_config)
    assert 'storages' in base_config, "incomplete configuration - missing " \
                                      "`storages` in %s" % default_file

app.iniconfig = FlaskIni()
with app.app_context():
    app.iniconfig.read(config_file)

app.logger.setLevel(logging.getLevelName(
    app.iniconfig.get('default', 'log_level', fallback='ERROR')))

redis_db = RedisDatabase(app.iniconfig.get('redis', 'host'),
                         app.iniconfig.get('redis', 'port', fallback=6379),
                         app.iniconfig.get('redis', 'db', fallback=0),
                         app.iniconfig.get('redis', 'password', fallback=None))

redis_db_without_decode = RedisDatabase(app.iniconfig.get('redis', 'host'),
                                        app.iniconfig.get('redis', 'port', fallback=6379),
                                        app.iniconfig.get('redis', 'db', fallback=0),
                                        app.iniconfig.get('redis', 'password', fallback=None), False)

assert app.iniconfig.get('default', 'taskfile_dir'), "missing taskfile_dir from settings.ini"
taskfile_dir = app.iniconfig.get('default', 'taskfile_dir')
assert os.path.isdir(taskfile_dir), "taskfile_dir (%s) must be a directory" % taskfile_dir

retry = 0
while retry < 10:
    try:
        current_default_config = redis_db.exists("default") \
                                 and redis_db.hget("default", "configuration")
        break
    except (ConnectionError, AssertionError) as e:
        retry += 1
        time.sleep(1)

assert retry < 10, "Cannot connect to redis DB - aborting"

if current_default_config != default_config:
    redis_db.hset("default", "configuration", default_config)
    redis_db.hset("default", "timestamp", time.time())


def append_version(v):
    global VERSION
    VERSION += ":" + v


def get_version():
    return VERSION


from app import routes
