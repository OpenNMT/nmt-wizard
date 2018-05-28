from __future__ import print_function

import argparse
import json
import sys
import os
import requests
import regex as re
from datetime import datetime
import math

reimage = re.compile(r"(([-A-Za-z_.0-9]+):|)([-A-Za-z_.0-9]+/[-A-Za-z_.0-9]+)(:([-A-Za-z_.0-9]+)|)$")
logger = None

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
parser_launch.add_argument('-r', '--docker_registry', default='auto',
                           help='docker registry (as configured on server side) - default is `auto`')
parser_launch.add_argument('-i', '--docker_image', default=os.getenv('LAUNCHER_IMAGE', None),
                           help='Docker image (can be prefixed by docker_registry:)')
parser_launch.add_argument('-t', '--docker_tag', default="latest",
                           help='Docker image tag (default is latest)')
parser_launch.add_argument('-n', '--name',
                           help='Friendly name for the model, for subsequent tasks, inherits from previous')
parser_launch.add_argument('-T', '--trainer_id', default=os.getenv('LAUNCHER_TID', None),
                           help='trainer id, used as a prefix to generated models (default ENV[LAUNCHER_TID])')
parser_launch.add_argument('-I', '--iterations', type=int, default=1,
                           help='for training tasks, iterate several tasks in a row')
parser_launch.add_argument('-p', '--priority', type=int, default=0,
                           help='task priority - highest better')
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
parser_status.add_argument('task_id',
                              help="task identifier")
parser_terminate = subparsers.add_parser('terminate', help='terminate a task')
parser_terminate.add_argument('task_id',
                              help="task identifier")
parser_log = subparsers.add_parser('log', help='get log associated to a task')
parser_log.add_argument('task_id',
                              help="task identifier")
parser_file = subparsers.add_parser('file', help='get file associated to a task')
parser_file.add_argument('task_id',
                              help="task identifier")
parser_file.add_argument('-f', '--filename',
                              help="filename to retrieve - for instance log", required=True)

def process_request(serviceList, cmd, is_json, args, auth=None):
    res = None
    result = None
    if cmd == "ls":
        result = serviceList
        if not is_json:
            busymsg = []
            res = "%-20s\t%10s\t%10s\t%10s\t%10s\t%s\n" % ("SERVICE NAME", "USAGE", "QUEUED",
                                                   "CAPACITY", "BUSY", "DESCRIPTION")
            for k in result:
                res += ("%-20s\t%10d\t%10d\t%10d\t%10d\t%s\n" % (k,
                                                 result[k]['usage'],
                                                 result[k]['queued'],
                                                 result[k]['capacity'],
                                                 len(result[k]['busy']),
                                                 result[k]['name']))
                if len(result[k]['busy']):
                    for r in result[k]['busy']:
                        busymsg.append("%-20s\t%s" % (r, result[k]['busy'][r]))

            if len(busymsg):
                res += "\n" + "\n".join(busymsg)
    elif cmd == "lt":
        r = requests.get(os.path.join(args.url, "task/list", args.prefix + '*'), auth=auth)
        if r.status_code != 200:
            raise RuntimeError('incorrect result from \'task/list\' service: %s' % r.text)
        result = r.json()
        if not is_json:
            res = ("%-5s %-42s %-12s %-8s %-20s %-22s %-9s %s\n" %
                    ("TYPE", "TASK_ID", "RESOURCE", "PRIORITY", "LAUNCH DATE", "IMAGE", "STATUS", "MESSAGE"))
            for k in sorted(result, key=lambda k: float(k["queued_time"] or 0)):
                date = datetime.fromtimestamp(math.ceil(float(k["queued_time"] or 0))).isoformat(' ')
                res += ("%-4s %-42s %-12s %6d   %-20s %-22s %-9s %s\n" %
                          (k["type"], k["task_id"], k["alloc_resource"] or k["resource"], int(k["priority"] or 0), 
                           date, k["image"], k["status"], k.get("message")))
    elif cmd == "describe":
        if args.service not in serviceList:
            raise ValueError("ERROR: service '%s' not defined" % args.service)
        r = requests.get(os.path.join(args.url, "service/describe", args.service), auth=auth)
        if r.status_code != 200:
            raise RuntimeError('incorrect result from \'service/describe\' service: %s' % r.text)
        result = r.json()
    elif cmd == "check":
        if args.service not in serviceList:
            raise ValueError("ERROR: service '%s' not defined" % args.service)
        r = requests.get(os.path.join(args.url, "service/check", args.service),
                         json=getjson(args.options), auth=auth)
        if r.status_code != 200:
            raise RuntimeError('incorrect result from \'service/check\' service: %s' % r.text)
        result = r.json()
        if not is_json:
            res = result["message"]
    elif cmd == "launch":
        if args.trainer_id is None:
            raise RuntimeError('missing trainer_id (you can set LAUNCHER_TID)')

        if args.docker_image is None:
            raise RuntimeError('missing docker image (you can set LAUNCHER_IMAGE)')
        m = reimage.match(args.docker_image)
        if not m:
            raise ValueError('incorrect docker image syntax (%s) - should be [registry:]organization/image[:tag]' %
                             args.docker_image)

        if m.group(2):
            args.docker_registry = m.group(2)
        if m.group(5):
            args.docker_tag = m.group(5)
        args.docker_image = m.group(3)

        if args.service not in serviceList:
            raise ValueError("ERROR: service '%s' not defined" % args.service)

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
                cjson = json.loads(c)
                find_files_parameters(cjson, files)
                c = json.dumps(cjson)
            docker_command.append(c)

        if args.service not in serviceList:
            raise ValueError("ERROR: service '%s' not defined" % args.service)

        content = {
            "docker": {
                "registry": args.docker_registry,
                "image": args.docker_image,
                "tag": args.docker_tag,
                "command": docker_command
            },
            "wait_after_launch": args.wait_after_launch,
            "trainer_id": args.trainer_id,
            "options": getjson(args.options)
        }

        if args.name:
            content["name"] = args.name
        if args.iterations:
            content["iterations"] = args.iterations
        if args.priority:
            content["priority"] = args.priority

        logger.debug("sending request: %s", json.dumps(content))

        launch_url = os.path.join(args.url, "task/launch", args.service)
        r = requests.post(launch_url,
                          files=files,
                          data = {'content': json.dumps(content) },
                          auth=auth)
        if r.status_code != 200:
            raise RuntimeError('incorrect result from \'task/launch\' service: %s' % r.text)
        result = r.json()
        if not is_json:
            if isinstance(result, list):
                res=("\n".join(result))
            else:
                res=result
    elif cmd == "status":
        r = requests.get(os.path.join(args.url, "task/status", args.task_id), auth=auth)
        if r.status_code != 200:
            raise RuntimeError('incorrect result from \'task/status\' service: %s' % r.text)
        result = r.json()
        if not is_json:
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
            res = ("TASK %s - TYPE %s - status %s (%s)%s\n" % (
                       args.task_id, result.get('type'),
                       result.get('status'), result.get('message'), last_update))
            if "service" in result:
                res += ("SERVICE %s - RESOURCE %s - CONTAINER %s\n" % (
                            result['service'], result.get('resource'), result.get('container_id')))
            res += "ATTACHED FILES: %s\n" % ', '.join(result['files'])
            res =+ "TIMELINE:\n"
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
                    res += "\t%-12s\t%s\t%s\n" % (k[:-5], date, delay[idx])
                    idx += 1
            content = result["content"]
            content = json.loads(content)
            res += "CONTENT"
            res += json.dumps(content, indent=True)+"\n"
    elif cmd == "dt":
        r = requests.get(os.path.join(args.url, "task/list", args.prefix + '*'), auth=auth)
        if r.status_code != 200:
            raise RuntimeError('incorrect result from \'task/list\' service: %s' % r.text)
        result = r.json()
        if not is_json:
            print('Delete %d tasks:' % len(result))
            print("\t%-32s\t%-20s\t%-30s\t%-10s\t%s" % ("TASK_ID", "LAUNCH DATE", "IMAGE", "STATUS", "MESSAGE"))
            for k in sorted(result, key=lambda k: float(k["queued_time"] or 0)):
                date = datetime.fromtimestamp(math.ceil(float(k["queued_time"] or 0))).isoformat(' ')
                print("\t%-32s\t%-20s\t%-30s\t%-10s\t%s" % (
                    k["task_id"], date, k["image"], k["status"], k.get("message")))
            if confirm():
                for k in result:
                    r = requests.delete(os.path.join(args.url, "task", k["task_id"]), auth=auth)
                    if r.status_code != 200:
                        raise RuntimeError('incorrect result from \'delete_task\' service: %s' % r.text)
    elif cmd == "terminate":
        r = requests.get(os.path.join(args.url, "task/terminate", args.task_id), auth=auth)
        if r.status_code != 200:
            raise RuntimeError('incorrect result from \'task/terminate\' service: %s' % r.text)
        result = r.json()
        if not is_json:
            res = result["message"]
    elif cmd == "file":
        r = requests.get(os.path.join(args.url, "task/file", args.task_id, args.filename), auth=auth)
        if r.status_code != 200:
            raise RuntimeError('incorrect result from \'task/file\' service: %s' % r.text)
        res = r.text.encode("utf-8")
    elif cmd == "log":
        r = requests.get(os.path.join(args.url, "task/log", args.task_id), auth=auth)
        if r.status_code != 200:
            raise RuntimeError('incorrect result from \'task/log\' service: %s' % r.text)
        res = r.text.encode("utf-8")
    if res is not None:
        return res
    else:
        return result

if __name__ == "__main__":
    import logging

    args = parser.parse_args()

    logging.basicConfig(stream=sys.stdout, level=args.log_level)
    logger = logging.getLogger()

    if args.url is None:
        args.url = os.getenv('LAUNCHER_URL')
        if args.url is None:
            logger.error('missing launcher_url')
            sys.exit(1)

    r = requests.get(os.path.join(args.url, "service/list"))
    if r.status_code != 200:
        logger.error('incorrect result from \'service/list\' service: %s', r.text)
        sys.exit(1)

    serviceList = r.json()

    try:
        res = process_request(serviceList, args.cmd, args.json, args)
    except RuntimeError as err:
        logger.error(err)
        sys.exit(1)
    except ValueError as err:
        logger.error(err)
        sys.exit(1)

    if args.json:
        print(json.dumps(res))
    else:
        print(res.strip())
