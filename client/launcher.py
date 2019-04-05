from __future__ import print_function

import argparse
import json
import sys
import os
import six
import requests
import regex as re
from prettytable import PrettyTable, PLAIN_COLUMNS
from datetime import datetime
import math

VERSION = "1.7.2"

try:
    # for Python 3
    from http.client import HTTPConnection
except ImportError:
    from httplib import HTTPConnection


def append_version(v):
    global VERSION
    VERSION += ":" + v


def get_version():
    return VERSION


class VersionAction(argparse.Action):
    def __init__(self, nargs=0, **kw):
        super(VersionAction, self).__init__(nargs=nargs, **kw)

    def __call__(self, parser, namespace, values, option_string=None):
        print("Client version: %s" % VERSION)
        r = requests.get(os.path.join(os.getenv('LAUNCHER_URL'), "version"))
        if r.status_code != 200:
            raise RuntimeError('incorrect result from \'version\' service: %s' % r.text)
        print("Server version: %s" % r.text)
        sys.exit(1)


reimage = re.compile(r"(([-A-Za-z_.0-9]+):|)"
                     "([-A-Za-z_.0-9]+/[-A-Za-z_.0-9]+)(:([-A-Za-z_.0-9]+)|)$")
logger = None


def getjson(config):
    if config is None or type(config) == dict:
        return None
    if not config.startswith('@'):
        return json.loads(config)
    with open(config[1:]) as data:
        return json.load(data)


def _truncate_string(s, n=25):
    if len(s) > n:
        return s[:22]+"..."
    return s


def find_files_parameters(v, files):
    if isinstance(v, six.string_types) and v.startswith('/') and os.path.exists(v):
        global_basename = os.path.basename(v)
        if os.path.isdir(v):
            allfiles = [(os.path.join(v, f), os.path.join(global_basename, f))
                        for f in os.listdir(v) if os.path.isfile(os.path.join(v, f))]
        else:
            allfiles = [(v, global_basename)]
        for f in allfiles:
            files[f[1]] = (f[1], open(f[0], 'rb'))
            logger.info('transferring local file: %s -> ${TMP_DIR}/%s', f[0], f[1])
        return "${TMP_DIR}/%s" % global_basename
    elif isinstance(v, list):
        for idx in xrange(len(v)):
            v[idx] = find_files_parameters(v[idx], files)
    elif isinstance(v, dict):
        for k in v:
            v[k] = find_files_parameters(v[k], files)
    return v


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
parser.add_argument('-d', '--display', default='TABLE',
                    help="display mode (TABLE, JSON, HTML, RAW)")
parser.add_argument('-v', '--version', action=VersionAction,
                    help='Version information')

subparsers = parser.add_subparsers(help='command help', dest='cmd')
subparsers.required = True

subparsers_map = {
    "service": subparsers.add_parser(u'service'),
    "task": subparsers.add_parser(u'task'),
}
shortcut_map = {}

subparsers_service = subparsers_map["service"].add_subparsers(help='sub-command help', dest='subcmd')
subparsers_service.required = True
parser_list_services = subparsers_service.add_parser('list',
                                                     help='{ls} list available services')
shortcut_map["ls"] = ["service", "list"]
parser_list_services.add_argument('-v', '--verbose', help='detail resource name, and running tasks',
                                  action='store_true')
parser_list_services.add_argument('-a', '--all', action='store_true',
                                  help='show all visible pools, default show only user pool')

parser_describe = subparsers_service.add_parser('describe',
                                                help='list available options for the service')
parser_describe.add_argument('-s', '--service', help="service name")

parser_check = subparsers_service.add_parser('check',
                                             help='check that service associated to provided'
                                                  'options is operational')
parser_check.add_argument('-s', '--service',
                          help='service name')
parser_check.add_argument('-o', '--options', default='{}',
                          help="options selected to run the service")
parser_check.add_argument('-r', '--resource',
                          help="alternatively to `options`, resource name to check")

exec_arguments = [
    ['-s', '--service', {"help": 'service name'}],
    ['-o', '--options', {"default": '{}',
     "help": 'options selected to run the service'}],
    ['-r', '--resource',
     {"help": "alternatively to `options`, resource name to use"}],
    ['-g', '--gpus', {"type": int, "default": 1, "help": 'number of gpus'}],
    ['-c', '--cpus', {"type": int,
     "help": 'number of cpus - if not provided, will be obtained from pool config'}],
    ['-w', '--wait_after_launch', {
     "default": 2, "type": int,
     "help": 'if not 0, wait for this number of seconds after launch '
             'to check that launch is ok - by default wait for 2 seconds'}],
    ['--docker_registry', {"default": 'auto',
     "help": 'docker registry (as configured on server side), default is `auto`'}],
    ['-i', '--docker_image', {"default": os.getenv('LAUNCHER_IMAGE', None),
     "help": 'Docker image (can be prefixed by docker_registry:)'}],
    ['-t', '--docker_tag',
     {"help": 'Docker image tag (is infered from docker_image if missing)'}],
    ['-n', '--name',
     {"help": 'Friendly name for the model, for subsequent tasks, inherits from previous'}],
    ['-T', '--trainer_id', {"default": os.getenv('LAUNCHER_TID', None),
     "help": 'trainer id, used as a prefix to generated models (default ENV[LAUNCHER_TID])'}],
    ['-P', '--priority', {"type": int, "default": 0, "help": 'task priority - highest better'}],
]


parser_exec = subparsers.add_parser('exec',
                                    help='execute a generic docker-utility task on the service associated'
                                         ' to provided options')
for arg in exec_arguments:
    parser_exec.add_argument(*arg[:-1], **arg[-1])
parser_exec.add_argument('docker_command', type=str, nargs='*', help='Docker command')

parser_launch = subparsers.add_parser('launch',
                                      help='launch a task on the service associated'
                                           ' to provided options')
for arg in exec_arguments:
    parser_launch.add_argument(*arg[:-1], **arg[-1])

subparsers_tasks = subparsers_map["task"].add_subparsers(help='sub-command help', dest='subcmd')
subparsers_tasks.required = True
parser_launch = subparsers_tasks.add_parser('launch',
                                            help='launch a task on the service associated'
                                                 ' to provided options')
shortcut_map["launch"] = ["task", "launch"]
parser_launch.add_argument('-s', '--service',
                           help='service name')
parser_launch.add_argument('-o', '--options', default='{}',
                           help='options selected to run the service')
parser_launch.add_argument('-r', '--resource',
                           help="alternatively to `options`, resource name to use")
parser_launch.add_argument('-g', '--gpus', type=int, default=1,
                           help='number of gpus')
parser_launch.add_argument('-c', '--cpus', type=int,
                           help='number of cpus - if not provided, will be obtained from pool config')
parser_launch.add_argument('-w', '--wait_after_launch', default=2, type=int,
                           help='if not 0, wait for this number of seconds after launch '
                                'to check that launch is ok - by default wait for 2 seconds')
parser_launch.add_argument('--docker_registry', default='auto',
                           help='docker registry (as configured on server side), default is `auto`')
parser_launch.add_argument('-i', '--docker_image', default=os.getenv('LAUNCHER_IMAGE', None),
                           help='Docker image (can be prefixed by docker_registry:)')
parser_launch.add_argument('-t', '--docker_tag',
                           help='Docker image tag (is infered from docker_image if missing)')
parser_launch.add_argument('-n', '--name',
                           help='Friendly name for the model, for subsequent tasks, inherits'
                                ' from previous')
parser_launch.add_argument('-T', '--trainer_id', default=os.getenv('LAUNCHER_TID', None),
                           help='trainer id, used as a prefix to generated models (default '
                                'ENV[LAUNCHER_TID])')
parser_launch.add_argument('-I', '--iterations', type=int, default=1,
                           help='for training tasks, iterate several tasks in a row')
parser_launch.add_argument('-P', '--priority', type=int, default=0,
                           help='task priority - highest better')
parser_launch.add_argument('--nochainprepr', action='store_true',
                           help='don\'t split prepr and train (image >= 1.4.0)')
parser_launch.add_argument('--notransasrelease', action='store_true',
                           help='don\'t run translate as release (image >= 1.8.0)')
parser_launch.add_argument('docker_command', type=str, nargs='*', help='Docker command')

parser_list_tasks = subparsers_tasks.add_parser('list',
                                                help='{lt} list tasks matching prefix pattern')
shortcut_map["lt"] = ["task", "list"]
parser_list_tasks.add_argument('-p', '--prefix', default=os.getenv('LAUNCHER_TID', ''),
                               help='prefix for the tasks to list (default ENV[LAUNCHER_TID])')
parser_list_tasks.add_argument('-q', '--quiet', action='store_true',
                               help='only display task_id')
parser_list_tasks.add_argument('-S', '--status',
                               help='filter on status value')

parser_del_tasks = subparsers_tasks.add_parser('delete',
                                               help='{dt} delete tasks matching prefix pattern')
shortcut_map["dt"] = ["task", "delete"]
parser_del_tasks.add_argument('-p', '--prefix', required=True,
                              help='prefix for the tasks to delete')

parser_status = subparsers_tasks.add_parser('status',
                                            help='{status} get status of a task')
shortcut_map["status"] = ["task", "status"]
parser_status.add_argument('task_id', help='task identifier')

parser_terminate = subparsers_tasks.add_parser('terminate',
                                               help='{terminate} terminate a task')
shortcut_map["terminate"] = ["task", "terminate"]
parser_terminate.add_argument('task_id', help='task identifier')

parser_log = subparsers_tasks.add_parser('log', help='{log} get log associated to a task')
shortcut_map["log"] = ["task", "log"]
parser_log.add_argument('task_id', help='task identifier')

parser_file = subparsers_tasks.add_parser('file', help='{file} get file associated to a task')
shortcut_map["file"] = ["task", "file"]
parser_file.add_argument('task_id', help='task identifier')
parser_file.add_argument('-f', '--filename', required=True,
                         help='filename to retrieve - for instance log')


def _format_message(msg, length=40):
    msg = msg.replace("\n", "\\n")
    if len(msg) >= length:
        return msg[:length-3]+"..."
    return msg


def _parse_local_filename(arg, files):
    if os.path.isabs(arg):
        if not os.path.exists(arg):
            raise ValueError("file '%s' does not exist" % arg)
    elif arg.find('/') != -1 and arg.find(':') == -1:
        print("==", os.path.exists(arg))
        if not os.path.exists(arg):
            logger.warning("parameter %s could be a filename but does not exists, considering it is not" % arg)
            return arg
        logger.warning("parameter %s could be a filename and exists, considering it is" % arg)
    else:
        return arg

    logger.debug("considering %s is a file" % arg)

    basename = os.path.basename(arg)
    if basename not in files:
        files[basename] = (basename, open(arg, 'rb'))
    arg = "${TMP_DIR}/%s" % basename

    return arg


def argparse_preprocess():
    skip = False
    for idx, v in enumerate(sys.argv[1:], 1):
        if skip:
            skip = False
            continue
        if "--display".startswith(v) or v == "-d" or "--url".startswith(v) or v == "-u" or \
           "--log-level".startswith(v) or v == "-l":
            skip = True
        elif v == "-v" or "--version".startswith(v):
            continue
        else:
            if v in shortcut_map:
                sys.argv = sys.argv[:idx] + shortcut_map[v] + sys.argv[idx+1:]
            return


def process_request(serviceList, cmd, subcmd, is_json, args, auth=None):
    res = None
    result = None
    if cmd == "service" and subcmd == "list":
        params = {'all': args.all}
        r = requests.get(os.path.join(args.url, "service/list"), auth=auth, params=params)
        if r.status_code != 200:
            logger.error('incorrect result from \'service/list\' service: %s', r.text)
            sys.exit(1)
        result = r.json()

        if not is_json:
            busymsg = []
            res = PrettyTable(["Service Name", "Usage", "Queued",
                               "Capacity", "Busy", "Description"])
            res.align["Service Name"] = "l"
            res.align["Description"] = "l"
            for k in result:
                name = k
                if 'pid' in result[k]:
                    name += " [%s]" % result[k]['pid']
                res.add_row([name,
                             result[k]['usage'],
                             result[k]['queued'],
                             result[k]['capacity'],
                             result[k]['busy'],
                             _format_message(result[k]['name'])])
                if args.verbose:
                    for r in result[k]['detail']:
                        if result[k]['detail'][r]['busy'] != '':
                            err = '**' + _format_message(result[k]['detail'][r]['busy'])
                        else:
                            err = ''
                        res.add_row(["  +-- "+r,
                                     "\n".join(result[k]['detail'][r]['usage']),
                                     result[k]['detail'][r]['reserved'],
                                     '(%d,%d)' % tuple(result[k]['detail'][r]['capacity']),
                                     'yes' if result[k]['detail'][r]['busy'] else '',
                                     err])
            if len(busymsg):
                res += "\n" + "\n".join(busymsg)
    elif cmd == "task" and subcmd == "list":
        r = requests.get(os.path.join(args.url, "task/list", args.prefix + '*'), auth=auth)
        if r.status_code != 200:
            raise RuntimeError('incorrect result from \'task/list\' service: %s' % r.text)
        result = r.json()
        if args.status:
            result = [r for r in result if r["status"] == args.status]
        if args.quiet:
            res = []
            for k in result:
                res.append(k["task_id"])
        elif not is_json:
            res = PrettyTable(["Task ID", "Resource", "Priority",
                               "Launch Date", "Image", "Status", "Message"])
            res.align["Task ID"] = "l"
            for k in sorted(result, key=lambda k: float(k.get("launched_time") or 0)):
                date = datetime.fromtimestamp(
                    math.ceil(float(k.get("launched_time") or 0))).isoformat(' ')
                resource = _truncate_string(k["alloc_resource"] or k["resource"])
                if k.get("alloc_lgpu") is not None and k.get("alloc_lcpu") is not None:
                    resource += ':(%d,%d)' % (len(k.get("alloc_lgpu", [])),
                                              len(k.get("alloc_lcpu", [])))
                p = k["image"].find('/')
                if p != -1:
                    k["image"] = k["image"][p+1:]
                task_id = k["task_id"]
                res.add_row([task_id, resource, int(k["priority"] or 0),
                             date, k["image"], k["status"], k.get("message")])
        else:
            res = r.json()
    elif cmd == "service" and subcmd == "describe":
        if args.service not in serviceList:
            raise ValueError("ERROR: service '%s' not defined" % args.service)
        r = requests.get(os.path.join(args.url, "service/describe", args.service), auth=auth)
        if r.status_code != 200:
            raise RuntimeError('incorrect result from \'service/describe\' service: %s' % r.text)
        res = r.json()
    elif cmd == "service" and subcmd == "check":
        if args.service not in serviceList:
            raise ValueError("ERROR: service '%s' not defined" % args.service)
        if args.options == '{}' and args.resource is not None:
            options = {"server": args.resource}
        else:
            options = getjson(args.options)
        r = requests.get(os.path.join(args.url, "service/check", args.service),
                         json=options, auth=auth)
        if r.status_code != 200:
            raise RuntimeError('incorrect result from \'service/check\' service: %s' % r.text)
        res = r.json()
    elif cmd == "exec" or cmd == "task" and subcmd == "launch":
        if args.trainer_id is None:
            raise RuntimeError('missing trainer_id (you can set LAUNCHER_TID)')

        if args.docker_image is None:
            raise RuntimeError('missing docker image (you can set LAUNCHER_IMAGE)')
        m = reimage.match(args.docker_image)
        if not m:
            raise ValueError('incorrect docker image syntax (%s) -'
                             ' should be [registry:]organization/image[:tag]' %
                             args.docker_image)

        if m.group(2):
            args.docker_registry = m.group(2)
        if m.group(5):
            args.docker_tag = m.group(5)
        args.docker_image = m.group(3)

        if args.service not in serviceList:
            raise ValueError("service '%s' not defined" % args.service)

        # for multi-part file sending
        files = {}
        docker_command = []

        for c in args.docker_command:
            orgc = c
            if c.startswith("@"):
                with open(c[1:], "rt") as f:
                    c = f.read()
            c = _parse_local_filename(c, files)
            # if json, explore for values to check local path values
            if c.startswith('{'):
                cjson = json.loads(c)
                find_files_parameters(cjson, files)
                c = json.dumps(cjson)
            docker_command.append(c)

        if args.service not in serviceList:
            raise ValueError("ERROR: service '%s' not defined" % args.service)

        if args.gpus < 0:
            raise ValueError("ERROR: ngpus must be >= 0")

        if args.options == '{}' and args.resource is not None:
            options = {"server": args.resource}
        else:
            options = getjson(args.options)

        content = {
            "docker": {
                "registry": args.docker_registry,
                "image": args.docker_image,
                "tag": args.docker_tag,
                "command": docker_command
            },
            "wait_after_launch": args.wait_after_launch,
            "trainer_id": args.trainer_id,
            "options": options,
            "ngpus": args.gpus,
            "ncpus": args.cpus
        }

        if cmd == "exec":
            content["exec_mode"] = True
        if args.name:
            content["name"] = args.name
        if args.priority:
            content["priority"] = args.priority

        if cmd == "task" and subcmd == "launch":
            if args.iterations:
                content["iterations"] = args.iterations
            if args.nochainprepr:
                content["nochainprepr"] = True
            if args.notransasrelease:
                content["notransasrelease"] = True
            if 'totranslate' in args and args.totranslate:
                content["totranslate"] = [(_parse_local_filename(i, files),
                                           o) for (i, o) in args.totranslate]
            if 'toscore' in args and args.toscore:
                content["toscore"] = [(o,
                                       _parse_local_filename(r, files)) for (o, r) in args.toscore]

        logger.debug("sending request: %s", json.dumps(content))

        launch_url = os.path.join(args.url, "task/launch", args.service)
        r = requests.post(launch_url,
                          files=files,
                          data={'content': json.dumps(content)},
                          auth=auth)
        if r.status_code != 200:
            raise RuntimeError('incorrect result from \'task/launch\' service: %s' % r.text)
        result = r.json()
        if not is_json:
            if isinstance(result, list):
                res = ("\n".join(result))
            else:
                res = result
    elif cmd == "task" and subcmd == "status":
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
            res = ("TASK %s - TYPE %s - status %s (%s)%s\nPARENT %s\n" % (
                       args.task_id, result.get('type'),
                       result.get('status'), result.get('message'), last_update,
                       result.get('parent', '')))
            if "service" in result:
                res += ("SERVICE %s - RESOURCE %s - CONTAINER %s\n" % (
                            result['service'], result.get('resource'), result.get('container_id')))
            res += "USING GPUs: %s\n" % ", ".join(result.get('alloc_lgpu', []))
            res += "USING CPUs: %s\n" % ", ".join(result.get('alloc_lcpu', []))
            res += "ATTACHED FILES: %s\n" % ', '.join(result['files'])
            res += "TIMELINE:\n"
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
    elif cmd == "task" and subcmd == "delete":
        r = requests.get(os.path.join(args.url, "task/list", args.prefix + '*'), auth=auth)
        if r.status_code != 200:
            raise RuntimeError('incorrect result from \'task/list\' service: %s' % r.text)
        result = r.json()
        if not is_json:
            print('Delete %d tasks:' % len(result))
            print("\t%-32s\t%-20s\t%-30s\t%-10s\t%s" % (
                "TASK_ID", "LAUNCH DATE", "IMAGE", "STATUS", "MESSAGE"))
            for k in sorted(result, key=lambda k: float(k["launched_time"] or 0)):
                date = datetime.fromtimestamp(
                    math.ceil(float(k["launched_time"] or 0))).isoformat(' ')
                print("\t%-32s\t%-20s\t%-30s\t%-10s\t%s" % (
                    k["task_id"], date, k["image"], k["status"], k.get("message")))
            if confirm():
                for k in result:
                    r = requests.delete(os.path.join(args.url, "task", k["task_id"]), auth=auth)
                    if r.status_code != 200:
                        raise RuntimeError(
                            'incorrect result from \'delete_task\' service: %s' % r.text)
    elif cmd == "task" and subcmd == "terminate":
        r = requests.get(os.path.join(args.url, "task/terminate", args.task_id), auth=auth)
        if r.status_code != 200:
            raise RuntimeError('incorrect result from \'task/terminate\' service: %s' % r.text)
        result = r.json()
        if not is_json:
            res = result["message"]
    elif cmd == "task" and subcmd == "file":
        r = requests.get(os.path.join(args.url, "task/file", args.task_id, args.filename),
                         auth=auth)
        if r.status_code != 200:
            raise RuntimeError('incorrect result from \'task/file\' service: %s' % r.text)
        res = r.content
    elif cmd == "task" and subcmd == "log":
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

    argparse_preprocess()
    args = parser.parse_args()

    logging.basicConfig(stream=sys.stdout, level=args.log_level)
    logger = logging.getLogger()

    if args.log_level == "DEBUG":
        requests_log = logging.getLogger("urllib3")
        requests_log.setLevel(logging.DEBUG)
        requests_log.propagate = True
        HTTPConnection.debuglevel = 1

    if args.url is None:
        args.url = os.getenv('LAUNCHER_URL')
        if args.url is None:
            logger.error('missing launcher_url')
            sys.exit(1)

    r = requests.get(os.path.join(args.url, "service/list", params={"minimal": True}))
    if r.status_code != 200:
        logger.error('incorrect result from \'service/list\' service: %s', r.text)
        sys.exit(1)

    serviceList = r.json()

    try:
        res = process_request(serviceList, args.cmd, args.subcmd, args.display == "JSON", args)
    except RuntimeError as err:
        logger.error(err)
        sys.exit(1)
    except ValueError as err:
        logger.error(err)
        sys.exit(1)

    if args.display == "JSON" or isinstance(res, dict):
        print(json.dumps(res))
    elif isinstance(res, PrettyTable):
        if args.display == "TABLE":
            print(res)
        elif args.display == "RAW":
            res.set_style(PLAIN_COLUMNS)
            print(res)
        else:
            print(res.get_html_string())
    else:
        print(res.strip())
