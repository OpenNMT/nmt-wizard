import os
import time
import logging
import json

from flask import Flask
from nmtwizard.redis_database import RedisDatabase
from nmtwizard import common
from utils.config_utils import ConfigUtils

VERSION = "1.11.1"

app = Flask(__name__)
app.request_id = 1

system_config_file = os.getenv('LAUNCHER_CONFIG', 'settings.yaml')


def get_system_config():
    assert system_config_file is not None and os.path.isfile(
        system_config_file), f"missing `{system_config_file}` file in current directory"
    config = ConfigUtils.read_file(system_config_file)

    return config


def get_log_handler():
    log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler = logging.StreamHandler()
    handler.setFormatter(log_formatter)

    return handler


def get_base_config():
    base_config_file = os.path.join(os.path.dirname(system_config_file), "default.json")
    assert os.path.isfile(base_config_file), "Cannot find default.json: %s" % base_config_file

    with open(base_config_file) as base_config_binary:
        default_config = base_config_binary.read()
        config = json.loads(default_config)
        config["database"] = system_config["database"]
        assert 'storages' in config, "incomplete configuration - missing " \
                                     "`storages` in %s" % base_config_file
        return config, default_config


def get_redis_client(decode_response=True):
    redis_password = None
    assert "redis" in system_config, f"Can't get redis config from {system_config_file}"
    redis_config = system_config["redis"]
    if "password" in redis_config:
        redis_password = redis_config["password"]

    redis_client = RedisDatabase(system_config["redis"]["host"],
                                 system_config["redis"]["port"],
                                 system_config["redis"]["db"],
                                 redis_password, decode_response)

    return redis_client


def get_other_config_flask(self, keys=None, fallback=None):
    if keys is None:
        keys = []
    if not isinstance(keys, list):
        return fallback
    result = self.other_config.copy()
    if not keys or len(keys) == 0:
        return result
    for key in keys:
        if key not in result:
            return fallback
        result = result[key]

    return result


def append_version(version):
    global VERSION
    VERSION += ":" + version


def get_version():
    return VERSION


system_config = get_system_config()
base_config, default_config = get_base_config()

log_handler = get_log_handler()
app.logger.addHandler(log_handler)
common.add_log_handler(log_handler)

flask_config = system_config["flask"]
app.config.update(flask_config)
app.other_config = system_config

app.__class__.get_other_config = get_other_config_flask

app.logger.setLevel(logging.getLevelName(app.get_other_config(["default", "log_level"], fallback='ERROR')))

redis_db = get_redis_client()
redis_db_without_decode = get_redis_client(decode_response=False)

assert system_config["default"]["taskfile_dir"], "missing taskfile_dir from settings.ini"
taskfile_dir = system_config["default"]["taskfile_dir"]
assert os.path.isdir(taskfile_dir), "taskfile_dir (%s) must be a directory" % taskfile_dir

retry = 0
while retry < 10:
    try:
        current_default_config = redis_db.exists("default") \
                                 and redis_db.hget("default", "configuration")
        if current_default_config != default_config:
            redis_db.hset("default", "configuration", default_config)
            redis_db.hset("default", "timestamp", time.time())
        break
    except (ConnectionError, AssertionError) as e:
        retry += 1
        time.sleep(1)

assert retry < 10, "Cannot connect to redis DB - aborting"

from app import routes
