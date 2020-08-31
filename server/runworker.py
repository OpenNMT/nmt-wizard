#!/usr/bin/python
import subprocess
import os
import sys
import time
import argparse
import signal
import logging

from nmtwizard import configuration as config
from utils.database_utils import DatabaseUtils

system_config = config.get_system_config()
mongo_client = DatabaseUtils.get_mongo_client(system_config)
redis_db = DatabaseUtils.get_redis_client(system_config)

retry = 0
while retry < 10:
    try:
        redis_db.get('notify-keyspace-events')
        break
    except ConnectionError as e:
        retry += 1
        logging.warning("cannot connect to redis DB - retrying (%d)", retry)
        time.sleep(1)
assert retry < 10, "Cannot connect to redis DB - aborting"

parser = argparse.ArgumentParser()
parser.add_argument('service_name', type=str,
                    help="name of the service to launch")
parser.add_argument('--fast_restart_delay', type=int, default=30,
                    help="time interval analysed as fast_restart - default 30")

args = parser.parse_args()
service_name = args.service_name

worker_arg = [sys.executable, sys.argv[0].replace("runworker", "worker"), service_name]

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
    service_config = config.process_service_config(mongo_client, service_name)
    start = time.time()
    date = time.strftime('%Y-%m-%d_%H%M%S', time.localtime(start))
    logfile = "logs/log-%s-%s:%d" % (service_name, date, counter)
    log_fh = open(logfile, "w")
    counter += 1
    cmdline = "%s '%s'" % (worker_arg[0], "','".join(worker_arg[1:]))
    log_fh.write("%s - RUN %s\n" % (date, cmdline))
    log_fh.write("CONFIG: %s\n" % service_config)
    log_fh.write("-" * 80)
    log_fh.write("\n")
    log_fh.flush()

    print("[%s] ** launching: %s - log %s" % (service_name, cmdline, logfile))
    sys.stdout.flush()
    p1 = subprocess.Popen(worker_arg, stdout=log_fh, stderr=subprocess.STDOUT, close_fds=True)
    current_pid = p1.pid
    print("[%s] ** launched with pid: %d" % (service_name, p1.pid))
    sys.stdout.flush()

    try:
        while True:
            poll = p1.poll()
            if poll is not None:
                break
            time.sleep(5)
            if time.time() - start > 30:
                # check if worker still there
                w = redis_db.exists("admin:worker:%s:%d" % (service_name, p1.pid))
                if not w:
                    p1.kill()
    except Exception as e:
        log_fh.write("-" * 80)
        log_fh.write("\n")
        log_fh.write("INTERRUPTED: " + str(e))
        log_fh.write("\n")

    # whatever happened, we remove trace of the worker
    redis_db.delete("admin:worker:%s:%d" % (service_name, p1.pid))

    stop = time.time()

    print("[%s] ** process stopped: %d" % (service_name, p1.pid))
    sys.stdout.flush()

    log_fh.flush()
    log_fh.write("-" * 80)
    log_fh.write("\n")
    log_fh.write("RUNNING TIME: %f\n" % (stop - start))

    if p1.returncode == 55:
        break

    if stop - start < args.fast_restart_delay:
        count_fast_fail += 1
    else:
        count_fast_fail = 0

    if count_fast_fail > 10:
        if config.is_db_service_config_outdated(mongo_client, service_name):
            config_from_file = config.get_service_config_from_file(service_name)
            config.save_service_config_to_mongo(mongo_client, service_name, config_from_file)
            count_fast_fail = 0
            print("[%s] ** 10 fast fails in a row - switching to base configuration..." % service_name)
            sys.stdout.flush()
        else:
            print("[%s] ** 10 fast fails in a row - aborting..." % service_name)
            sys.stdout.flush()
            log_fh.write("...10 fast fails in a row - aborting...\n")
            break
