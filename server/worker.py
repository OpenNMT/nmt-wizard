import logging
import os
import sys

from six.moves import configparser

from nmtwizard import config, task
from nmtwizard.redis_database import RedisDatabase
from nmtwizard.worker import Worker

cfg = configparser.ConfigParser()
cfg.read('settings.ini')
MODE = os.getenv('LAUNCHER_MODE', 'Production')

logging.basicConfig(stream=sys.stdout, level=cfg.get(MODE, 'log_level'))
logger = logging.getLogger()

redis_password = None
if cfg.has_option(MODE, 'redis_password'):
    redis_password = cfg.get(MODE, 'redis_password')

redis = RedisDatabase(cfg.get(MODE, 'redis_host'),
                      cfg.getint(MODE, 'redis_port'),
                      cfg.get(MODE, 'redis_db'),
                      redis_password)


services = config.load_services(cfg.get(MODE, 'config_dir'))

# On startup, add all active tasks in the work queue.
for task_id in task.list_active(redis):
    task.queue(redis, task_id)

# TODO: start multiple workers here?
worker = Worker(redis, services)
worker.run()
