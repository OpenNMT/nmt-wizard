import os
import time
import logging
import json

from flask import Flask
from flask_ini import FlaskIni
# from redis.exceptions import ConnectionError

from nmtwizard.redis_database import RedisDatabase
from nmtwizard import common
from lib.pn9databases import MongodbClient
from nmtwizard import configuration as config

CONFIG_LOAD_METHOD = {
    "from_mongo": "1",
    "from_file_upsert_to_mongo": "2",
    "from_file_check_duplicate_mongo": "3"
}

VERSION = "1.11.1"

app = Flask(__name__)
app._requestid = 1

ch = logging.StreamHandler()
app.logger.addHandler(ch)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)

common.add_log_handler(ch)

config_file = os.getenv('LAUNCHER_CONFIG', 'settings.ini')
assert os.path.isfile(config_file), "invalid LAUNCHER_CONFIG"
default_config_location = os.getenv('DEFAULT_CONFIG', CONFIG_LOAD_METHOD["from_mongo"])
assert default_config_location in CONFIG_LOAD_METHOD.values(), "invalid DEFAULT_CONFIG"

default_config_path = os.path.join(os.path.dirname(config_file), "default.json")
mongo_config_file = os.path.join(os.path.dirname(config_file), "mongo_config.json")
assert os.path.isfile(mongo_config_file), "Cannot find mongo_config_file.json: %s" % mongo_config_file

with open(mongo_config_file) as mongo_config_file_fh:
    mongo_config = mongo_config_file_fh.read()
    mongo_config = json.loads(mongo_config)

mongo_client = MongodbClient(mongo_config)
base_config = {}

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


def process_config():
    config_from_mongo = config.get_default_config(mongo_client)
    if default_config_location == CONFIG_LOAD_METHOD["from_mongo"]:
        assert config_from_mongo is not None, "Config not found"
        base_config.update(config_from_mongo)
    else:
        conflict_config = config.is_conflict_default_config(mongo_client, default_config_path)
        if default_config_location == CONFIG_LOAD_METHOD["from_file_upsert_to_mongo"]:
            load_config_from_file_to_mongo()
        else:
            assert not conflict_config, "Conflict config"
            load_config_from_file_to_mongo()
        if conflict_config:
            redis_db.set("last_change_default_config_ts", time.time())


def load_config_from_file_to_mongo():
    config_from_file = config.get_default_config_from_file(default_config_path)
    config.upsert_default_config(mongo_client, config_from_file)
    base_config.update(config_from_file)


process_config()

base_config["database"] = mongo_config


def append_version(v):
    global VERSION
    VERSION += ":" + v


def get_version():
    return VERSION


from app import routes
