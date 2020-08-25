#!/usr/bin/python
import subprocess
import hashlib
import os
import shutil
import sys
import time
import argparse
import signal
import logging

from six.moves import configparser
from nmtwizard.redis_database import RedisDatabase
from app import mongo_client
from nmtwizard import configuration as config

CONFIG_LOAD_METHOD = {
    "from_mongo": 1,
    "from_file_upsert_to_mongo": 2,
    "from_file_check_duplicate_mongo": 3
}

# connecting to redis to monitor the worker process
cfg = configparser.ConfigParser()
cfg.read('settings.ini')
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
        redis.get('notify-keyspace-events')
        break
    except ConnectionError as e:
        retry += 1
        logging.warning("cannot connect to redis DB - retrying (%d)", retry)
        time.sleep(1)
assert retry < 10, "Cannot connect to redis DB - aborting"


def md5file(fp):
    """Returns the MD5 of the file fp."""
    m = hashlib.md5()
    with open(fp, 'rb') as f:
        for l in f.readlines():
            m.update(l)
    return m.hexdigest()


parser = argparse.ArgumentParser()
parser.add_argument('service', type=str,
                    help="name of the service to launch")
parser.add_argument('--config_load_method', type=int,
                    help="how to download data", default=CONFIG_LOAD_METHOD["from_mongo"])
parser.add_argument('--config_name', type=str,
                    help="config name", default="base")
parser.add_argument('--fast_restart_delay', type=int, default=30,
                    help="time interval analysed as fast_restart - default 30")
args = parser.parse_args()

assert os.path.isdir("configurations"), "missing `configurations` directory"

service = args.service
config_load_method = args.config_load_method
config_name = args.config_name

current_config = {}


def process_config():
    config_from_mongo = config.get_service_config_from_mongo(mongo_client, service, config_name)
    if config_load_method == CONFIG_LOAD_METHOD["from_mongo"]:
        assert config_from_mongo is not None, "Config not found"
        config.deactivate_current_config(mongo_client, service)
        config.active_config(mongo_client, service, config_name)
        current_config.update(config_from_mongo)
    elif config_load_method == CONFIG_LOAD_METHOD["from_file_upsert_to_mongo"]:
        load_config_from_file_to_mongo(service, config_name)
    else:
        conflict_config = config.is_conflict_config(mongo_client, service, config_name)
        assert not conflict_config, "Conflict config"
        load_config_from_file_to_mongo(service, config_name)


def load_config_from_file_to_mongo(service, config_name):
    config_from_file = config.get_service_config_from_file(service, config_name)
    config_from_file["activated"] = True
    config_from_file["config_name"] = config_name
    config.deactivate_current_config(mongo_client, service)
    config.upsert_config(mongo_client, service, config_name, config_from_file)
    current_config.update(config_from_file)


process_config()

assert sys.argv[0].find("runworker") != -1

worker_arg = [sys.executable, sys.argv[0].replace("runworker", "worker"), service]

count_fast_fail = 0
counter = 0

current_pid = None


def graceful_exit(signum, frame):
    if current_pid:
        os.kill(current_pid, signal.SIGTERM)
    sys.exit(0)


signal.signal(signal.SIGTERM, graceful_exit)
signal.signal(signal.SIGINT, graceful_exit)

while True:
    start = time.time()
    date = time.strftime('%Y-%m-%d_%H%M%S', time.localtime(start))
    logfile = "logs/log-%s-%s:%d" % (service, date, counter)
    log_fh = open(logfile, "w")
    counter += 1
    cmdline = "%s '%s'" % (worker_arg[0], "','".join(worker_arg[1:]))
    log_fh.write("%s - RUN %s\n" % (date, cmdline))
    log_fh.write("CONFIG: %s\n" % current_config)
    log_fh.write("-" * 80)
    log_fh.write("\n")
    log_fh.flush()

    print("[%s] ** launching: %s - log %s" % (service, cmdline, logfile))
    sys.stdout.flush()
    p1 = subprocess.Popen(worker_arg, stdout=log_fh, stderr=subprocess.STDOUT, close_fds=True)
    current_pid = p1.pid
    print("[%s] ** launched with pid: %d" % (service, p1.pid))
    sys.stdout.flush()

    try:
        while True:
            poll = p1.poll()
            if poll is not None:
                break
            time.sleep(5)
            if time.time() - start > 30:
                # check if worker still there
                w = redis.exists("admin:worker:%s:%d" % (service, p1.pid))
                if not w:
                    p1.kill()
    except Exception as e:
        log_fh.write("-" * 80)
        log_fh.write("\n")
        log_fh.write("INTERRUPTED: "+str(e))
        log_fh.write("\n")

    # whatever happened, we remove trace of the worker
    redis.delete("admin:worker:%s:%d" % (service, p1.pid))

    stop = time.time()

    print("[%s] ** process stopped: %d" % (service, p1.pid))
    sys.stdout.flush()

    log_fh.flush()
    log_fh.write("-" * 80)
    log_fh.write("\n")
    log_fh.write("RUNNING TIME: %f\n" % (stop-start))

    if p1.returncode == 55:
        break

    if stop - start < args.fast_restart_delay:
        count_fast_fail += 1
    else:
        count_fast_fail = 0

    if count_fast_fail > 10:
        if md5file("%s.json" % service) != md5file(os.path.join("configurations",
                                                                "%s_base.json" % service)):
            shutil.copyfile("configurations/%s_base.json" % service, "%s.json" % service)
            count_fast_fail = 0
            print("[%s] ** 10 fast fails in a row - switching to base configuration..." % service)
            sys.stdout.flush()
        else:
            print("[%s] ** 10 fast fails in a row - aborting..." % service)
            sys.stdout.flush()
            log_fh.write("...10 fast fails in a row - aborting...\n")
            break
