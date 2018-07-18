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

# Migration for tasks on 'work/active' queue to 'work/active:service' queue
while True:
    task_id = redis.rpop('work')
    if task_id is None:
        break
    redis.lpush('work:'+redis.hget('task:'+task_id, 'service'), task_id)
while True:
    task_id = redis.rpop('active')
    if task_id is None:
        break
    redis.lpush('active:'+redis.hget('task:'+task_id, 'service'), task_id)

services, base_config = config.load_services(cfg.get('default', 'config_dir'))

for service in services:

    # remove busy state from resources
    for key in redis.keys('busy:%s:*' % service):
        redis.delete(key)
    # remove reserved state from resources
    for key in redis.keys('reserved:%s:*' % service):
        redis.delete(key)
    # remove queued tasks on service
    for key in redis.keys('queued:%s' % service):
        redis.delete(key)

    # On startup, add all active tasks in the work queue or service queue
    for task_id in task.list_active(redis, service):
        with redis.acquire_lock(task_id):
            status = redis.hget('task:'+task_id, 'status')
            if status == 'queued' or status == 'allocating' or status == 'allocated':
                task.service_queue(redis, task_id, redis.hget('task:'+task_id, 'service'))
                task.set_status(redis, 'task:'+task_id, 'queued')
            else:
                task.work_queue(redis, task_id, service)
        # check integrity of tasks
        if redis.hget(task_id, 'priority') is None:
            redis.hset(task_id, 'priority', 0)
        if redis.hget(task_id, 'queued_time') is None:
            redis.hset(task_id, 'queued_time', time.time())

    # Desallocate all resources that are not anymore associated to a running task
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
