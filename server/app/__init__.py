from flask import Flask
from six.moves import configparser
import os
from nmtwizard.redis_database import RedisDatabase
from nmtwizard import config, common
import logging

app = Flask(__name__)

ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)

config.add_log_handler(ch)
common.add_log_handler(ch)

app.config.from_envvar('LAUNCHER_CONFIG')

redis_password = None
if 'redis_password' in app.config:
    redis_password = app.config['REDIS_PASSWORD']

redis = RedisDatabase(app.config['REDIS_HOST'],
                      app.config['REDIS_PORT'],
                      app.config['REDIS_DB'],
                      redis_password)
services = config.load_services(app.config['CONFIG_DIR'])

from app import routes
