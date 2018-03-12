from __future__ import print_function

import argparse
import json
import sys
import os
import logging
import requests
from datetime import datetime
import math

def getjson(config):
    if config is None:
        return None
    if not config.startswith('@'):
        return json.loads(config)
    with open(config[1:]) as data:
        return json.load(data)

def find_files_parameters(config, files):
    for k in config:
        v = config[k]
        if isinstance(v, unicode) and v.startswith('/') and os.path.exists(v):
            basename = os.path.basename(v)
            files[basename] = (basename, open(v, 'rb'))
            config[k] = "${TMP_DIR}/%s" % basename
            logger.debug('found local file: %s -> ${TMP_DIR}/%s', v, basename)
        elif isinstance(v, dict):
            find_files_parameters(v, files)

def confirm(prompt=None, resp=False):
    """prompts for yes or no response from the user. Returns True for yes and
    False for no.
    """

    if prompt is None:
        prompt = 'Confirm'

    if resp:
        prompt = '%s [%s]|%s: ' % (prompt, 'y', 'n')
    else:
        prompt = '%s [%s]|%s: ' % (prompt, 'n', 'y')

    while True:
        ans = raw_input(prompt)
        if not ans:
            return resp
        if ans not in ['y', 'Y', 'n', 'N']:
            print('please enter y or n.')
            continue
        if ans == 'y' or ans == 'Y':
            return True
        if ans == 'n' or ans == 'N':
            return False

parser = argparse.ArgumentParser()
parser.add_argument('-u', '--url',
                    help="url to the launcher")
parser.add_argument('-l', '--log-level', default='INFO',
                    help="log-level (INFO|WARN|DEBUG|FATAL|ERROR)")
parser.add_argument('-j', '--json', action='store_true',
                    help="display output in json format from rest server (default text)")
subparsers = parser.add_subparsers(help='command help', dest='cmd')
parser_list_services = subparsers.add_parser('ls',
                                             help='list available services')
parser_describe = subparsers.add_parser('describe',
                                        help='list available options for the service')
parser_describe.add_argument('-s', '--service', help="service name")
parser_check = subparsers.add_parser('check',
                                     help='check that service associated to provided options is operational')
parser_check.add_argument('-s', '--service', help="service name")
parser_check.add_argument('-o', '--options', default='{}',
                          help="options selected to run the service")
parser_launch = subparsers.add_parser('launch',
                                      help='launch a task on the service associated to provided options')
parser_launch.add_argument('-s', '--service', help="service name")
parser_launch.add_argument('-o', '--options', default='{}',
                           help="options selected to run the service")
parser_launch.add_argument('-w', '--wait_after_launch', default=2, type=int,
                           help=('if not 0, wait for this number of seconds after launch '
                                 'to check that launch is ok - by default wait for 2 seconds'))
parser_launch.add_argument('-r', '--docker_registry', default='dockerhub',
                           help='docker registry (as configured on server side) - default is `dockerhub`')
parser_launch.add_argument('-i', '--docker_image', default=os.getenv('LAUNCHER_IMAGE', None),
                           help='Docker image')
parser_launch.add_argument('-t', '--docker_tag', default="latest",
                           help='Docker image tag (default is latest)')
parser_launch.add_argument('-n', '--name',
                           help='Friendly name for the model, for retraining, inherits from previous')
parser_launch.add_argument('-T', '--trainer_id', default=os.getenv('LAUNCHER_TID', None),
                           help='trainer id, used as a prefix to generated models (default ENV[LAUNCHER_TID])')
parser_launch.add_argument('-I', '--iterations', type=int, default=1,
                           help='for training tasks, iterate several trainings in a row')
parser_launch.add_argument('docker_command', type=str, nargs='*',
                           help='Docker command')
parser_list_tasks = subparsers.add_parser('lt',
                                          help='list tasks matching prefix pattern')
parser_list_tasks.add_argument('-p', '--prefix', default=os.getenv('LAUNCHER_TID', ''),
                               help='prefix for the tasks to list (default ENV[LAUNCHER_TID])')
parser_del_tasks = subparsers.add_parser('dt',
                                         help='delete tasks matching prefix pattern')
parser_del_tasks.add_argument('-p', '--prefix', required=True,
                              help='prefix for the tasks to delete')
parser_status = subparsers.add_parser('status', help='get status of a task')
parser_status.add_argument('-k', '--task_id',
                              help="task identifier", required=True)
parser_terminate = subparsers.add_parser('terminate', help='terminate a task')
parser_terminate.add_argument('-k', '--task_id',
                              help="task identifier", required=True)
parser_log = subparsers.add_parser('log', help='get log associated to a task')
parser_log.add_argument('-k', '--task_id',
                              help="task identifier", required=True)
parser_file = subparsers.add_parser('file', help='get file associated to a task')
parser_file.add_argument('-k', '--task_id',
                              help="task identifier", required=True)
parser_file.add_argument('-f', '--filename',
                              help="filename to retrieve - for instance log", required=True)

args = parser.parse_args()

logging.basicConfig(stream=sys.stdout, level=args.log_level)
logger = logging.getLogger()

if args.url is None:
    args.url = os.getenv('LAUNCHER_URL')
    if args.url is None:
        logger.error('missing launcher_url')
        sys.exit(1)

r = requests.get(os.path.join(args.url, "list_services"))
if r.status_code != 200:
    logger.error('incorrect result from \'list_services\' service: %s', r.text)
serviceList = r.json()

if args.cmd == "ls":
    result = serviceList
    if not args.json:
        print("%-20s\t%s" % ("SERVICE NAME", "DESCRIPTION"))
        for k in result:
            print("%-20s\t%s" % (k, result[k]))
        sys.exit(0)
elif args.cmd == "lt":
    r = requests.get(os.path.join(args.url, "list_tasks", args.prefix + '*'))
    if r.status_code != 200:
        logger.error('incorrect result from \'list_tasks\' service: %s', r.text)
        sys.exit(1)
    result = r.json()
    if not args.json:
        print("%-5s %-42s %-20s %-30s %-10s %s" %
              ("TYPE", "TASK_ID", "LAUNCH DATE", "IMAGE", "STATUS", "MESSAGE"))
        for k in sorted(result, key=lambda k: float(k["queued_time"])):
            date = datetime.fromtimestamp(math.ceil(float(k["queued_time"]))).isoformat(' ')
            print("%-4s %-42s %-20s %-30s %-10s %s" %
                  (k["type"], k["task_id"], date, k["image"], k["status"], k.get("message")))
        sys.exit(0)
elif args.cmd == "describe":
    if args.service not in serviceList:
        logger.fatal("ERROR: service '%s' not defined", args.service)
        sys.exit(1)
    r = requests.get(os.path.join(args.url, "describe", args.service))
    if r.status_code != 200:
        logger.error('incorrect result from \'describe\' service: %s', r.text)
        sys.exit(1)
    result = r.json()
elif args.cmd == "check":
    if args.service not in serviceList:
        logger.fatal("ERROR: service '%s' not defined", args.service)
        sys.exit(1)
    r = requests.get(os.path.join(args.url, "check", args.service), json=getjson(args.options))
    if r.status_code != 200:
        logger.error('incorrect result from \'check\' service: %s', r.text)
        sys.exit(1)
    result = r.json()
    if not args.json:
        print(result["message"])
        sys.exit(0)
elif args.cmd == "launch":
    if args.trainer_id is None:
        logger.error('missing trainer_id (you can set LAUNCHER_TID)')
        sys.exit(1)

    if args.docker_image is None:
        logger.error('missing docker image (you can set LAUNCHER_IMAGE)')
        sys.exit(1)
    if args.service not in serviceList:
        logger.fatal("ERROR: service '%s' not defined", args.service)
        sys.exit(1)

    # for multi-part file sending
    files = {}
    docker_command = []

    for c in args.docker_command:
        orgc = c
        if c.startswith("@"):
            with open(c[1:], "rt") as f:
                c = f.read()
        if os.path.exists(c):
            basename = os.path.basename(c)
            files[basename] = (basename, open(c, 'rb'))
            c = "${TMP_DIR}/%s" % basename
        # if json, explore for values to check local path values
        if c.startswith('{'):
            try:
                cjson = json.loads(c)
            except ValueError as err:
                logger.fatal("Invalid JSON parameter in %s: %s", orgc, str(err))
                sys.exit(1)
            find_files_parameters(cjson, files)
            c = json.dumps(cjson)
        docker_command.append(c)

    if args.service not in serviceList:
        logger.fatal("ERROR: service '%s' not defined", args.service)
        sys.exit(1)

    content = {
        "docker": {
            "registry": args.docker_registry,
            "image": args.docker_image,
            "tag": args.docker_tag,
            "command": docker_command
        },
        "wait_after_launch": args.wait_after_launch,
        "trainer_id": args.trainer_id,
        "options": getjson(args.options),
        "name": args.name,
        "iterations": args.iterations
    }

    launch_url = os.path.join(args.url, "launch", args.service)
    r = None
    if len(files) > 0:
        r = requests.post(launch_url, files=files, data = {'content': json.dumps(content) })
    else:
        r = requests.post(launch_url, json=content)
    if r.status_code != 200:
        logger.error('incorrect result from \'launch\' service: %s', r.text)
        sys.exit(1)
    result = r.json()
    if not args.json:
        if isinstance(result, list):
            print("\n".join(result))
        else:
            print(result)
        sys.exit(0)
elif args.cmd == "status":
    r = requests.get(os.path.join(args.url, "status", args.task_id))
    if r.status_code != 200:
        logger.error('incorrect result from \'status\' service: %s', r.text)
        sys.exit(1)
    result = r.json()
    if not args.json:
        times = []
        current_time = float(result["current_time"])
        result.pop("current_time", None)
        for k in result:
            if k.endswith('_time'):
                times.append(k)
        sorted_times = sorted(times, key=lambda k: float(result[k]))
        last_update = ''
        if sorted_times:
            upd = current_time - float(result[sorted_times[-1]])
            last_update = " - updated %d seconds ago" % upd
        print("TASK %s - TYPE %s - status %s (%s)%s" % (
                args.task_id, result.get('type'),
                result.get('status'), result.get('message'), last_update))
        if "service" in result:
            print("SERVICE %s - RESOURCE %s - CONTAINER %s" % (
                    result['service'], result.get('resource'), result.get('container_id')))
        print("ATTACHED FILES: %s" % ', '.join(result['files']))
        print("TIMELINE:")
        last = -1
        delay = []
        for k in sorted_times:
            if k != "updated_time":
                current = float(result[k])
                delta = current-last if last != -1 else 0
                delay.append("(%ds)" % delta)
                last = current
        delay.append('')
        idx = 1
        for k in sorted_times:
            if k != "updated_time":
                current = float(result[k])
                date = datetime.fromtimestamp(math.ceil(current)).isoformat(' ')
                print("\t%-12s\t%s\t%s" % (k[:-5], date, delay[idx]))
                idx += 1
        content = result["content"]
        content = json.loads(content)
        print("CONTENT")
        print(json.dumps(content, indent=True))
        sys.exit(0)
elif args.cmd == "dt":
    r = requests.get(os.path.join(args.url, "list_tasks", args.prefix + '*'))
    if r.status_code != 200:
        logger.error('incorrect result from \'list_tasks\' service: %s', r.text)
        sys.exit(1)
    result = r.json()
    if not args.json:
        print('Delete %d tasks:' % len(result))
        print("\t%-32s\t%-20s\t%-30s\t%-10s\t%s" % ("TASK_ID", "LAUNCH DATE", "IMAGE", "STATUS", "MESSAGE"))
        for k in sorted(result, key=lambda k: float(k["queued_time"])):
            date = datetime.fromtimestamp(math.ceil(float(k["queued_time"]))).isoformat(' ')
            print("\t%-32s\t%-20s\t%-30s\t%-10s\t%s" % (
                k["task_id"], date, k["image"], k["status"], k.get("message")))
        if confirm():
            for k in result:
                r = requests.get(os.path.join(args.url, "del", k["task_id"]))
                if r.status_code != 200:
                    logger.error('incorrect result from \'delete_task\' service: %s', r.text)
                    sys.exit(1)
        sys.exit(0)
elif args.cmd == "terminate":
    r = requests.get(os.path.join(args.url, "terminate", args.task_id))
    if r.status_code != 200:
        logger.error('incorrect result from \'terminate\' service: %s', r.text)
        sys.exit(1)
    result = r.json()
    if not args.json:
        print(result["message"])
        sys.exit(0)
elif args.cmd == "file":
    r = requests.get(os.path.join(args.url, "file", args.task_id, args.filename))
    if r.status_code != 200:
        logger.error('incorrect result from \'file\' service: %s', r.text)
        sys.exit(1)
    print(r.text.encode("utf-8"))
    sys.exit(0)
elif args.cmd == "log":
    r = requests.get(os.path.join(args.url, "log", args.task_id))
    if r.status_code != 200:
        logger.error('incorrect result from \'log\' service: %s', r.text)
        sys.exit(1)
    print(r.text.encode("utf-8"))
    sys.exit(0)

print(json.dumps(result))
