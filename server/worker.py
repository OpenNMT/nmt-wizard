import logging.config
import time
import os
import sys
import argparse
import signal
from multiprocessing import Process

import six
from nmtwizard import configuration as config, task, workeradmin
from nmtwizard.worker import Worker
from nmtwizard.worker_butler import WorkerButler
from utils.database_utils import DatabaseUtils

parser = argparse.ArgumentParser()
parser.add_argument('service_name', type=str, help="Name of service")
args = parser.parse_args()

service_name = args.service_name
assert service_name, "Name of service mustn't None"


def get_logger(logger_config):
    logger_config["version"] = 1
    logging.config.dictConfig(logger_config)
    return logging.getLogger("worker")


system_config = config.get_system_config()
mongo_client = DatabaseUtils.get_mongo_client(system_config)
redis_db = DatabaseUtils.get_redis_client(system_config)

base_config = config.process_base_config(mongo_client)
service_config = config.get_service_config(mongo_client, service_name)

assert "default" in system_config, "Can't read default config from settings.yaml"
system_config_default = system_config["default"]

assert 'logging' in system_config, "Can't read logging config from settings.yaml"
logger = get_logger(system_config["logging"])

process_count = 1
worker_cycle = 0.05
worker_butler_cycle = 0.5


if "worker" in service_config and "process_count" in service_config["worker"]:
    process_count_config = service_config["worker"]["process_count"]
    assert isinstance(process_count_config, int), "number_of_workers config must be integer"
    process_count = process_count_config

if "worker" in service_config and "worker_cycle" in service_config["worker"]:
    worker_cycle_config = service_config["worker"]["worker_cycle"]
    assert isinstance(worker_cycle_config,
                      float) and worker_cycle_config > 0, "worker/worker_cycle must be numeric and greater than 0"
    worker_cycle = worker_cycle_config

if "worker" in service_config and "worker_butler_cycle" in service_config["worker"]:
    worker_butler_cycle_config = service_config["worker"]["worker_butler_cycle"]
    assert isinstance(worker_butler_cycle_config,
                      float) and worker_butler_cycle_config > 0,\
        "worker/worker_butler_cycle must be numeric and greater than 0"
    worker_butler_cycle = worker_butler_cycle_config


retry = 0
while retry < 10:
    try:
        # make sure notify events are set
        redis_db.config_set('notify-keyspace-events', 'Klgx')
        break
    except ConnectionError as e:
        retry += 1
        logger.warning("cannot connect to redis DB - retrying (%d)", retry)
        time.sleep(1)

assert retry < 10, "Cannot connect to redis DB - aborting"

services, merged_config = config.load_service_config(service_config, base_config)
assert len(services) == 1, "workers are now dedicated to one single service"
service = next(iter(services))

pid = os.getpid()
logger.info('Running worker for %s - PID = %d', service, pid)

instance_id = 'admin:worker:%s:%d' % (service, pid)
redis_db.hset(instance_id, "launch_time", time.time())
redis_db.hset(instance_id, "beat_time", time.time())
redis_db.expire(instance_id, 600)

keys = 'admin:services'
redis_db.sadd(keys, service)


def graceful_exit(signum, frame):   # pylint: disable=unused-argument
    logger.info('received interrupt - stopping')
    redis_db.delete(instance_id)
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
    logger.debug("[%s-%s]: Reorganizing data", service, pid)
    remove_queued_tasks()
    reorganize_tasks()
    reorganize_resources()


def remove_queued_tasks():
    logger.debug("[%s-%s]: Removing queued tasks", service, pid)
    for key in redis_db.keys('queued:%s' % service):
        redis_db.delete(key)


def reorganize_tasks():
    logger.debug("[%s-%s]: Reorganizing tasks", service, pid)
    # On startup, add all active tasks in the work queue or service queue
    for task_id in task.list_active(redis_db, service):
        task_key = f'task:{task_id}'
        with redis_db.acquire_lock(task_id):
            status = redis_db.hget(task_key, 'status')
            if status in ['queued', 'allocated']:
                task.service_queue(redis_db, task_id, service)
                task.set_status(redis_db, 'task:' + task_id, 'queued')
            else:
                task.work_queue(redis_db, task_id, service)
        # check integrity of tasks
        if redis_db.hget(task_key, 'priority') is None:
            redis_db.hset(task_key, 'priority', 0)
        if redis_db.hget(task_key, 'queued_time') is None:
            redis_db.hset(task_key, 'queued_time', time.time())


def reorganize_resources():
    logger.debug("[%s-%s]: Reorganizing resources", service, pid)
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
    logger.debug("[%s-%s]: Cleaning up list resource", service, pid)
    redis_db.delete('admin:resources:' + service)


def declare_resource(resource):
    logger.debug("[%s-%s]: Declaring resource {resource}", service, pid)
    redis_db.lpush('admin:resources:' + service, resource)


def reorganize_reserved_resource(resource):
    logger.debug("[%s-%s]: Reorganizing reserved resource {resource}", service, pid)
    reorganize_reserved_gpu_resource(resource)
    reorganize_reserved_cpu_resource(resource)


def reorganize_reserved_gpu_resource(resource):
    key = 'gpu_resource:%s:%s' % (service, resource)
    reorganize_reserved_resource_by_key(key)


def reorganize_reserved_cpu_resource(resource):
    key = 'cpu_resource:%s:%s' % (service, resource)
    reorganize_reserved_resource_by_key(key)


def reorganize_reserved_resource_by_key(key):
    running_tasks = redis_db.hgetall(key)
    for reserved_resource, task_id in six.iteritems(running_tasks):
        with redis_db.acquire_lock(task_id):
            status = redis_db.hget('task:' + task_id, 'status')
            if status not in ['running', 'terminating']:
                redis_db.hdel(key, reserved_resource)


reorganize_data()

worker_processes = []
worker_butler_process = Process()

PROCESS_CHECK_INTERVAL = 3
WORKER_ADMIN_CHECK_INTERVAL = 1
HEART_BEAT_CHECK_INTERVAL = 10


def start():
    start_all()
    count = 0
    while True:
        count += 1
        if count % PROCESS_CHECK_INTERVAL == 0 and is_any_process_stopped():
            logger.debug("[%s-%s]: Any process has stopped", service, pid)
            restart_all()
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
    for worker_process in worker_processes:
        if not worker_process.is_alive():
            logger.debug("Worker {worker_process.pid} has stopped")
            return True
    if not worker_butler_process.is_alive():
        logger.debug("Worker butler has stopped")
        return True
    return False


def restart_all():
    kill_all()
    worker_processes.clear()
    reorganize_data()
    start_all()


def kill_all():
    for worker_process in worker_processes:
        if worker_process.is_alive():
            logger.debug("[%s-%s]: Killing worker {worker_process.pid}", service, pid)
            worker_process.terminate()
    if worker_butler_process.is_alive():
        logger.debug("[%s-%s]: Killing worker butler", service, pid)
        worker_butler_process.terminate()


def start_all():
    logger.debug("[%s-%s]: Starting worker butler", service, pid)
    global worker_butler_process
    worker_butler_process = Process(target=start_worker_butler, args=(redis_db, services, pid, worker_butler_cycle))
    worker_butler_process.daemon = True
    worker_butler_process.start()

    logger.debug("[%s-%s]: Starting {process_count} workers", service, pid)
    for _ in range(0, process_count):
        mongodb_client = DatabaseUtils.get_mongo_client(system_config)
        worker_process = Process(target=start_worker, args=(redis_db, mongodb_client, services,
                                                            ttl_policy,
                                                            system_config_default["refresh_counter"],
                                                            system_config_default["quarantine_time"],
                                                            instance_id,
                                                            system_config_default["taskfile_dir"],
                                                            worker_cycle))

        worker_process.daemon = True
        worker_process.start()
        worker_processes.append(worker_process)


def start_worker(current_redis_db, mongodb_client, current_services,
                 current_ttl_policy,
                 refresh_counter,
                 quarantine_time,
                 current_instance_id,
                 taskfile_dir,
                 current_worker_cycle):

    worker = Worker(current_redis_db, mongodb_client, current_services,
                    current_ttl_policy,
                    refresh_counter,
                    quarantine_time,
                    current_instance_id,
                    taskfile_dir,
                    current_worker_cycle)

    worker.run()


def start_worker_butler(current_redis_db, current_services, current_instance_id, current_worker_butler_cycle):
    worker_butler = WorkerButler(current_redis_db, current_services, current_instance_id, current_worker_butler_cycle)
    worker_butler.run()


def process_worker_admin_command():
    workeradmin.process(logger, redis_db, service, instance_id)


def process_heart_beat():
    if not is_exists_heart_beat():
        logger.info('stopped by key expiration/removal')
        sys.exit(0)
    set_heart_beat_is_current_time()
    set_expire_time_of_instance(1200)


def is_exists_heart_beat():
    return redis_db.exists(instance_id)


def set_heart_beat_is_current_time():
    redis_db.hset(instance_id, "beat_time", time.time())


def set_expire_time_of_instance(time_in_sec):
    redis_db.expire(instance_id, time_in_sec)


start()
