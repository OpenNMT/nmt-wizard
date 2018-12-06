import logging
import logging.config
import hashlib
import time
import json
import pickle
import os
import sys
import six
import argparse
import signal

from redis.exceptions import ConnectionError

from six.moves import configparser

from nmtwizard import config, task
from nmtwizard.redis_database import RedisDatabase
from nmtwizard.worker import Worker

parser = argparse.ArgumentParser()
parser.add_argument('config', type=str,
                    help="path to config file for the service")
args = parser.parse_args()

assert os.path.isfile(args.config) and args.config.endswith(".json"), \
    "`config` must be path to JSON service configuration file"
assert os.path.isfile('settings.ini'), "missing `settings.ini` file in current directory"
assert os.path.isfile('logging.conf'), "missing `logging.conf` file in current directory"

services, base_config = config.load_service_config(args.config)
assert len(services) == 1, "workers are now dedicated to one single service"
service = next(iter(services))


def md5file(fp):
    """Returns the MD5 of the file fp."""
    m = hashlib.md5()
    with open(fp, 'rb') as f:
        for l in f.readlines():
            m.update(l)
    return m.hexdigest()


current_configuration = None
configurations = {}

if os.path.isdir("configurations"):
    configurations = {}
    config_service_md5 = {}
    for filename in os.listdir("configurations"):
        if filename.startswith(service+"_") and filename.endswith(".json"):
            file_path = os.path.join("configurations", filename)
            with open(file_path) as f:
                configurations[filename[len(service)+1:-5]] = (os.path.getmtime(file_path),
                                                               f.read())
            config_service_md5[md5file(file_path)] = filename[len(service)+1:-5]
    current_configuration_md5 = md5file(args.config)
    if current_configuration_md5 in config_service_md5:
        current_configuration = config_service_md5[current_configuration_md5]

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


redis.set('admin:storages', json.dumps(base_config['storages']))

pid = os.getpid()

logger.info('Running worker for %s - PID = %d' % (service, pid))

keyw = 'admin:worker:%s:%d' % (service, pid)
redis.hset(keyw, "launch_time", time.time())
redis.hset(keyw, "beat_time", time.time())
redis.expire(keyw, 600)

keys = 'admin:service:%s' % service
redis.hset(keys, "current_configuration", current_configuration)
redis.hset(keys, "configurations", json.dumps(configurations))
redis.hset(keys, "def", pickle.dumps(services[service]))

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
    if redis.hget('task:'+task_id, 'priority') is None:
        redis.hset('task:'+task_id, 'priority', 0)
    if redis.hget('task:'+task_id, 'queued_time') is None:
        redis.hset('task:'+task_id, 'queued_time', time.time())

# Desallocate all resources that are not anymore associated to a running task
resources = services[service].list_resources()
servers = services[service].list_servers()

# TODO:
# if multiple workers are for same service with different configurations
# or storage definition change - restart all workers


redis.delete('admin:resources:'+service)
for resource in resources:
    redis.lpush('admin:resources:'+service, resource)
    keycr = 'cpu_resource:%s:%s' % (service, resource)
    keygr = 'gpu_resource:%s:%s' % (service, resource)
    running_tasks = redis.hgetall(keygr)
    for g, task_id in six.iteritems(running_tasks):
        with redis.acquire_lock(task_id):
            status = redis.hget('task:'+task_id, 'status')
            if not(status == 'running' or status == 'terminating'):
                redis.hdel(keygr, g)
    running_tasks = redis.hgetall(keygr)
    for c, task_id in six.iteritems(running_tasks):
        with redis.acquire_lock(task_id):
            status = redis.hget('task:'+task_id, 'status')
            if status == 'running' or status == 'terminating':
                redis.hdel(keycr, c)


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


def graceful_exit(signum, frame):
    logger.info('received interrupt - stopping')
    redis.delete(keyw)
    sys.exit(0)


signal.signal(signal.SIGTERM, graceful_exit)
signal.signal(signal.SIGINT, graceful_exit)

# TODO: start multiple workers here?
worker = Worker(redis, services,
                ttl_policy,
                cfg.getint('default', 'refresh_counter'),
                cfg.getint('default', 'quarantine_time'),
                keyw,
                cfg.get('default', 'taskfile_dir'))
worker.run()
