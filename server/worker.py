import logging
import logging.config
import time
import json
import pickle
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

logging.config.fileConfig('logging.conf')
logger = logging.getLogger('worker')

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

redis.set('admin:storages', json.dumps(base_config['storages']))

pid = os.getpid()
redis.set('admin:worker:%d' % pid, time.time())

for service in services:
    keys = 'admin:service:%s' % service
    redis.hset(keys, 'worker_pid', pid)
    redis.hset(keys, "launch_time", time.time())

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
    servers = services[service].list_servers()

    redis.delete('admin:resources:'+service)
    redis.hset(keys, 'def', pickle.dumps(services[service]))
    for resource in resources:
        redis.lpush('admin:resources:'+service, resource)
        keyc = 'ncpus:%s:%s' % (service, resource)
        redis.set(keyc, servers[resource]['ncpus'])
        keyr = 'gpu_resource:%s:%s' % (service, resource)
        running_tasks = redis.hgetall(keyr)
        for g, task_id in six.iteritems(running_tasks):
            with redis.acquire_lock(task_id):
                status = redis.hget('task:'+task_id, 'status')
                if not(status == 'running' or status == 'terminating'):
                    redis.hdel(keyr, g)
                else:
                    redis.decr(keyc, int(redis.hget('task:'+task_id, 'ncpus')))
        keyr = 'cpu_resource:%s:%s' % (service, resource)
        tasks = redis.lrange(keyr, 0, -1)
        redis.ltrim(keyr, 0, -1)
        for task_id in tasks:
            with redis.acquire_lock(task_id):
                status = redis.hget('task:'+task_id, 'status')
                if status == 'running' or status == 'terminating':
                    redis.decr(keyc, int(redis.hget('task:'+task_id, 'ncpus')))
                    redis.rpush(task_id)            

# define ttl policy for a task
def ttl_policy(task_map):
    s = task_map['service']
    if s is not None and s in services and 'ttl_policy' in services[s]._config:
        for ttl_rule in services[s]._config['ttl_policy']:
            match = True
            for p, v in six.iteritems(ttl_rule['pattern']):
                match = p in task_map and task_map[p] == v
                if not match:
                    break
            if match:
                return ttl_rule['ttl']
    return 0

# TODO: start multiple workers here?
worker = Worker(redis, services,
                ttl_policy,
                cfg.getint('default', 'refresh_counter'),
                cfg.getint('default', 'quarantine_time'),
                'admin:worker:%d' % pid,
                cfg.get('default', 'taskfile_dir'))
worker.run()
