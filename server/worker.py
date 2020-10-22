import logging.config
import hashlib
import time
import json
import pickle
import os
import sys
import argparse
import signal
from multiprocessing import Process

import six
from six.moves import configparser
from nmtwizard import configuration as config, task
from nmtwizard.redis_database import RedisDatabase
from nmtwizard.worker import Worker
from nmtwizard import workeradmin

parser = argparse.ArgumentParser()
parser.add_argument('config', type=str,
                    help="path to config file for the service")

args = parser.parse_args()

assert os.path.isfile(args.config) and args.config.endswith(".json"), \
    "`config` must be path to JSON service configuration file"
assert os.path.isfile('settings.ini'), "missing `settings.ini` file in current directory"
assert os.path.isfile('logging.conf'), "missing `logging.conf` file in current directory"


def md5file(fp):
    """Returns the MD5 of the file fp."""
    m = hashlib.md5()
    with open(fp, 'rb') as f:
        for l in f.readlines():
            m.update(l)
    return m.hexdigest()


cfg = configparser.ConfigParser()
cfg.read('settings.ini')

logging.config.fileConfig('logging.conf')
logger = logging.getLogger('worker')

process_count = 1
if cfg.has_option('worker', 'process_count'):
    process_count_config = cfg.get('worker', 'process_count')
    assert process_count_config.isnumeric() and int(
        process_count_config) > 0, "process_count must be numeric and greater than 0"
    process_count = int(process_count_config)

redis_password = None
if cfg.has_option('redis', 'password'):
    redis_password = cfg.get('redis', 'password')

redis = RedisDatabase(cfg.get('redis', 'host'),
                      cfg.getint('redis', 'port'),
                      cfg.get('redis', 'db'),
                      redis_password)

redis2 = RedisDatabase(cfg.get('redis', 'host'),
                       cfg.getint('redis', 'port'),
                       cfg.get('redis', 'db'),
                       redis_password, False)

retry = 0
while retry < 10:
    try:
        # make sure notify events are set
        redis.config_set('notify-keyspace-events', 'Klgx')
        break
    except ConnectionError as e:
        retry += 1
        logger.warning("cannot connect to redis DB - retrying (%d)", retry)
        time.sleep(1)

assert retry < 10, "Cannot connect to redis DB - aborting"

# load default configuration from database
retry = 0
while retry < 10:
    default_config = redis.hget('default', 'configuration')
    default_config_timestamp = redis.hget('default', 'timestamp')
    if default_config:
        break
    time.sleep(5)

assert retry < 10, "Cannot retrieve default config from redis DB - aborting"

base_config = json.loads(default_config)

services, merged_config = config.load_service_config(args.config, base_config)
assert len(services) == 1, "workers are now dedicated to one single service"
service = next(iter(services))

current_configuration = None
configurations = {}

if os.path.isdir("configurations"):
    configurations = {}
    config_service_md5 = {}
    for filename in os.listdir("configurations"):
        if filename.startswith(service + "_") and filename.endswith(".json"):
            file_path = os.path.join("configurations", filename)
            with open(file_path) as f:
                configurations[filename[len(service) + 1:-5]] = (os.path.getmtime(file_path),
                                                                 f.read())
            config_service_md5[md5file(file_path)] = filename[len(service) + 1:-5]
    current_configuration_md5 = md5file(args.config)
    if current_configuration_md5 in config_service_md5:
        current_configuration = config_service_md5[current_configuration_md5]

pid = os.getpid()
logger.info('Running worker for %s - PID = %d' % (service, pid))

instance_id = 'admin:worker:%s:%d' % (service, pid)
redis.hset(instance_id, "launch_time", time.time())
redis.hset(instance_id, "beat_time", time.time())
redis.expire(instance_id, 600)

keys = 'admin:service:%s' % service
redis.hset(keys, "current_configuration", current_configuration)
redis.hset(keys, "configurations", json.dumps(configurations))
redis2.hset(keys, "def", pickle.dumps(services[service]))


def graceful_exit(signum, frame):
    logger.info('received interrupt - stopping')
    redis.delete(instance_id)
    sys.exit(0)


signal.signal(signal.SIGTERM, graceful_exit)
signal.signal(signal.SIGINT, graceful_exit)


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


def reorganize_data():
    logger.debug(f"[{service}-{pid}]: Reorganizing data")
    remove_queued_tasks()
    reorganize_tasks()
    reorganize_resources()


def remove_queued_tasks():
    logger.debug(f"[{service}-{pid}]: Removing queued tasks")
    for key in redis.keys('queued:%s' % service):
        redis.delete(key)


def reorganize_tasks():
    logger.debug(f"[{service}-{pid}]: Reorganizing tasks")
    # On startup, add all active tasks in the work queue or service queue
    for task_id in task.list_active(redis, service):
        task_key = f'task:{task_id}'
        with redis.acquire_lock(task_id):
            status = redis.hget(task_key, 'status')
            if status in ['queued', 'allocated']:
                task.service_queue(redis, task_id, service)
                task.set_status(redis, 'task:' + task_id, 'queued')
            else:
                task.work_queue(redis, task_id, service)
        # check integrity of tasks
        if redis.hget(task_key, 'priority') is None:
            redis.hset(task_key, 'priority', 0)
        if redis.hget(task_key, 'queued_time') is None:
            redis.hset(task_key, 'queued_time', time.time())


def reorganize_resources():
    logger.debug(f"[{service}-{pid}]: Reorganizing resources")
    if services[service].valid:
        # Deallocate all resources that are not anymore associated to a running task
        resources = services[service].list_resources()

        # TODO:
        # if multiple workers are for same service with different configurations
        # or storage definition change - restart all workers`
        cleanup_list_resource()
        for resource in resources:
            declare_resource(resource)
            reorganize_reserved_resource(resource)


def cleanup_list_resource():
    logger.debug(f"[{service}-{pid}]: Cleaning up list resource")
    redis.delete('admin:resources:' + service)


def declare_resource(resource):
    logger.debug(f"[{service}-{pid}]: Declaring resource {resource}")
    redis.lpush('admin:resources:' + service, resource)


def reorganize_reserved_resource(resource):
    logger.debug(f"[{service}-{pid}]: Reorganizing reserved resource {resource}")
    reorganize_reserved_gpu_resource(resource)
    reorganize_reserved_cpu_resource(resource)


def reorganize_reserved_gpu_resource(resource):
    key = 'gpu_resource:%s:%s' % (service, resource)
    reorganize_reserved_resource_by_key(key)


def reorganize_reserved_cpu_resource(resource):
    key = 'cpu_resource:%s:%s' % (service, resource)
    reorganize_reserved_resource_by_key(key)


def reorganize_reserved_resource_by_key(key):
    running_tasks = redis.hgetall(key)
    for reserved_resource, task_id in six.iteritems(running_tasks):
        with redis.acquire_lock(task_id):
            status = redis.hget('task:' + task_id, 'status')
            if status not in ['running', 'terminating']:
                redis.hdel(key, reserved_resource)


reorganize_data()

worker_processes = []

PROCESS_CHECK_INTERVAL = 3
WORKER_ADMIN_CHECK_INTERVAL = 1
HEART_BEAT_CHECK_INTERVAL = 10


def start():
    start_all_worker()
    count = 0
    while True:
        count += 1
        if count % PROCESS_CHECK_INTERVAL == 0 and is_any_process_stopped():
            logger.debug(f"[{service}-{pid}]: Any process has stopped")
            restart_all_worker()
            continue
        # Currently, WORKER_ADMIN_CHECK_INTERVAL = 1, can ignore this condition
        if count % WORKER_ADMIN_CHECK_INTERVAL == 0:
            process_worker_admin_command()
        if count % HEART_BEAT_CHECK_INTERVAL == 0:
            process_heart_beat()
        if count % (PROCESS_CHECK_INTERVAL * WORKER_ADMIN_CHECK_INTERVAL * HEART_BEAT_CHECK_INTERVAL) == 0:
            count = 0
        time.sleep(1)


def is_any_process_stopped():
    return any(list(map(lambda worker_process: not worker_process.is_alive(), worker_processes)))


def restart_all_worker():
    kill_all_worker()
    worker_processes.clear()
    reorganize_data()
    start_all_worker()


def kill_all_worker():
    for worker_process in worker_processes:
        if worker_process.is_alive():
            logger.debug(f"[{service}-{pid}]: Killing { worker_process.pid}")
            worker_process.terminate()


def start_all_worker():
    logger.debug(f"[{service}-{pid}]: Starting {process_count} workers")
    for i in range(0, process_count):
        worker_process = Process(target=start_worker, args=(redis, services,
                                                            ttl_policy,
                                                            cfg.getint('default', 'refresh_counter'),
                                                            cfg.getint('default', 'quarantine_time'),
                                                            pid,
                                                            cfg.get('default', 'taskfile_dir'),
                                                            default_config_timestamp))
        worker_process.daemon = True
        worker_process.start()
        worker_processes.append(worker_process)


def start_worker(redis, services,
                 ttl_policy,
                 refresh_counter,
                 quarantine_time,
                 instance_id,
                 taskfile_dir,
                 default_config_timestamp=default_config_timestamp):

    worker = Worker(redis, services,
                    ttl_policy,
                    refresh_counter,
                    quarantine_time,
                    instance_id,
                    taskfile_dir,
                    default_config_timestamp)
    worker.run()


def process_worker_admin_command():
    workeradmin.process(logger, redis, service)
    if default_config_timestamp and redis.hget('default', 'timestamp') != default_config_timestamp:
        logger.info('stopped by default configuration change')
        sys.exit(0)


def process_heart_beat():
    if not is_exists_heart_beat():
        logger.info('stopped by key expiration/removal')
        sys.exit(0)
    set_heart_beat_is_current_time()
    set_expire_time_of_instance(1200)


def is_exists_heart_beat():
    return redis.exists(instance_id)


def set_heart_beat_is_current_time():
    redis.hset(instance_id, "beat_time", time.time())


def set_expire_time_of_instance(time_in_sec):
    redis.expire(instance_id, time_in_sec)


start()
