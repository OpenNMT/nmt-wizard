from flask import Flask
from flask_ini import FlaskIni
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

config_file = os.getenv('LAUNCHER_CONFIG')
assert config_file is not None and os.path.isfile(config_file), "invalid LAUNCHER_CONFIG"

app.iniconfig = FlaskIni()
with app.app_context():
    app.iniconfig.read(config_file)

redis = RedisDatabase(app.iniconfig.get('redis','host'),
                      app.iniconfig.get('redis','port',fallback=6379),
                      app.iniconfig.get('redis','db',fallback=0),
                      app.iniconfig.get('redis', 'password',fallback=None))
services, base_config = config.load_services(app.iniconfig.get('default','config_dir'))

from app import routes
