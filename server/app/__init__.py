import os
import logging.config
import redis

from flask import Flask
from nmtwizard import common
from nmtwizard import configuration as config
from utils.database_utils import DatabaseUtils
from flask_session import Session

VERSION = "1.12.0"
app = Flask(__name__)
app.request_id = 1


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
redis_uri = DatabaseUtils.get_redis_uri(system_config)
flask_config['SESSION_REDIS'] = redis.from_url(redis_uri)
app.config.update(flask_config)
app.other_config = system_config

app.__class__.get_other_config = get_other_config_flask

app.logger.setLevel(logging.getLevelName(app.get_other_config(["default", "log_level"], fallback='ERROR')))

sess = Session()
sess.init_app(app)

assert system_config["default"]["taskfile_dir"], "missing taskfile_dir from settings.yaml"
taskfile_dir = system_config["default"]["taskfile_dir"]
assert os.path.isdir(taskfile_dir), "taskfile_dir (%s) must be a directory" % taskfile_dir

input_dir = app.get_other_config(["push_model", "inputDir"], fallback=None)
if input_dir is not None:
    if not os.path.isabs(input_dir):
        input_dir = os.path.join(os.path.dirname(config.system_config_file), input_dir)
        app.other_config['push_model']['inputDir'] = input_dir
    assert os.path.isdir(input_dir), "Invalid input directory used for deploying model: %s" % input_dir

from app import routes
