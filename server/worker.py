import logging
import time
import os
import sys
import six
from redis.exceptions import ConnectionError

from six.moves import configparser

from nmtwizard import config, task
from nmtwizard.redis_database import RedisDatabase
from nmtwizard.worker import Worker

cfg = configparser.ConfigParser()
cfg.read('settings.ini')

logging.basicConfig(stream=sys.stdout, level=cfg.get('default', 'log_level'))
logger = logging.getLogger()

redis_password = None
if cfg.has_option('redis', 'password'):
    redis_password = cfg.get('redis', 'password')

redis = RedisDatabase(cfg.get('redis', 'host'),
                      cfg.getint('redis', 'port'),
                      cfg.get('redis', 'db'),
                      redis_password)

retry = 0
while retry < 10:
    try:
        # make sure notify events are set
        redis.config_set('notify-keyspace-events', 'Klgx')
        break
    except ConnectionError as e:
        retry += 1
        logger.warn("cannot connect to redis DB - retrying (%d)" % retry)
        time.sleep(1)

assert retry < 10, "Cannot connect to redis DB - aborting"

services, base_config = config.load_services(cfg.get('default', 'config_dir'))

# remove busy state from resources
for key in redis.keys('busy:*'):
    redis.delete(key)
# remove reserved state from resources
for key in redis.keys('reserved:*'):
    redis.delete(key)

# On startup, add all active tasks in the work queue.
for task_id in task.list_active(redis):
    with redis.acquire_lock(task_id):
        status = redis.hget('task:'+task_id, 'status')
        if status == 'queued' or status == 'allocating' or status == 'allocated':
            task.service_queue(redis, task_id, redis.hget('task:'+task_id, 'service'))
            task.set_status(redis, 'task:'+task_id, 'queued')
        else:
            task.work_queue(redis, task_id)

# Desallocate all resources that are not anymore associated to a running task
for service in services:
    resources = services[service].list_resources()
    for resource in resources:
        keyr = 'resource:%s:%s' % (service, resource)
        running_tasks = redis.hgetall(keyr)
        for g, task_id in six.iteritems(running_tasks):
            with redis.acquire_lock(task_id):
                status = redis.hget('task:'+task_id, 'status')
                if not(status == 'running' or status == 'terminating'):
                    redis.hdel(keyr, g)

# TODO: start multiple workers here?
worker = Worker(redis, services,
                cfg.getint('default', 'refresh_counter'),
                cfg.getint('default', 'quarantine_time'))
worker.run()
