#!/usr/bin/python
import subprocess
import hashlib
import os
import shutil
import sys
import time
import argparse
import json

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
parser.add_argument('--fast_restart_delay', type=int, default=30, help="time interval analysed as fast_restart - default 30")
args = parser.parse_args()

assert os.path.isdir("configurations"), "missing `configurations` directory"

service = args.service

config_service = {}
config_service_md5 = {}
for filename in os.listdir("configurations"):
    if filename.startswith(service+"_") and filename.endswith(".json"):
        config_service[filename] = md5file(os.path.join("configurations", filename))
        config_service_md5[config_service[filename]] = filename

assert "%s_base.json" % service in config_service, "missing base configuration for "+service

if os.path.isfile("%s.json" % service):
    current_config_md5 = md5file("%s.json" % service)
    assert current_config_md5 in config_service_md5, "current configuration file not in `configurations`"
    print "** current configuration is: %s" % config_service_md5[current_config_md5]
else:
    shutil.copyfile("configurations/%s_base.json" % service, "%s.json" % service)
    print "** using base configuration: configurations/%s_base.json" % service

assert sys.argv[0].find("runworker") != -1

config_file = "%s.json" % service
worker_arg = [sys.executable, sys.argv[0].replace("runworker", "worker"), config_file]

count_fast_fail = 0
counter = 0

while True:
    with open(config_file) as f:
        config = f.read()


    start = time.time()
    date = time.strftime('%Y-%m-%d_%H%M%S', time.localtime(start))
    logfile = "logs/log-%s-%s:%d" % (service, date, counter)
    log_fh = open(logfile, "wb")
    counter += 1
    cmdline = "%s '%s'" % (worker_arg[0], "','".join(worker_arg[1:]))
    log_fh.write("%s - RUN %s\n" % (date, cmdline))
    log_fh.write("CONFIG: %s\n" % config)
    log_fh.write("-" * 80)
    log_fh.write("\n")
    log_fh.flush()
 
    print "** [%s] launching: %s - log %s" % (service, cmdline, logfile)
    p1 = subprocess.Popen(worker_arg, stdout=log_fh, stderr=subprocess.STDOUT, close_fds=True) 
    print "** [%s] launched with pid: %d" % (service, p1.pid)

    try:
        p1.wait()
    except Exception as e:
    	print str(e)

    stop = time.time()

    print "** [%s] process stopped: %d" % (service, p1.pid)

    log_fh.flush()
    log_fh.write("-" * 80)
    log_fh.write("\n")
    log_fh.write("RUNNING TIME: %f\n" % (stop-start))

    if stop-start < args.fast_restart_delay:
        count_fast_fail += 1
    else:
        count_fast_fail = 0

    if count_fast_fail > 10:
        if md5file("%s.json" % service) != md5file(os.path.join("configurations", "%s_base.json" % service)):
            shutil.copyfile("configurations/%s_base.json" % service, "%s.json" % service)
            count_fast_fail = 0
            print "** [%s] 10 fast fails in a row - switching to base configuration..." % service
        else:
            print "** [%s] 10 fast fails in a row - aborting..." % service
            log_fh.write("...10 fast fails in a row - aborting...\n")
            break