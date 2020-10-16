import os
import logging.config

from flask import Flask
from nmtwizard import common
from nmtwizard import configuration as config
from utils.database_utils import DatabaseUtils

from flask_cors import CORS

VERSION = "1.12.0"

app = Flask(__name__)
app.request_id = 1

CORS(app)
def get_log_handler():
    log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler = logging.StreamHandler()
    handler.setFormatter(log_formatter)

    return handler


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


system_config = config.get_system_config()
mongo_client = DatabaseUtils.get_mongo_client(system_config)
redis_db = DatabaseUtils.get_redis_client(system_config)
redis_db_without_decode = DatabaseUtils.get_redis_client(system_config, decode_response=False)

base_config = config.process_base_config(mongo_client)

log_handler = get_log_handler()
app.logger.addHandler(log_handler)
common.add_log_handler(log_handler)

flask_config = system_config["flask"]
app.config.update(flask_config)
app.other_config = system_config

app.__class__.get_other_config = get_other_config_flask

app.logger.setLevel(logging.getLevelName(app.get_other_config(["default", "log_level"], fallback='ERROR')))

assert system_config["default"]["taskfile_dir"], "missing taskfile_dir from settings.yaml"
taskfile_dir = system_config["default"]["taskfile_dir"]
assert os.path.isdir(taskfile_dir), "taskfile_dir (%s) must be a directory" % taskfile_dir

from app import routes
