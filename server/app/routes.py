import flask
import io
import pickle
import json
import logging
import os
import time
from copy import deepcopy
from functools import wraps

from app import app, redis, get_version, ch, taskfile_dir
from nmtwizard import common, task
from nmtwizard.helper import build_task_id, shallow_command_analysis, change_parent_task, remove_config_option

logger = logging.getLogger(__name__)
logger.addHandler(ch)

def get_service(service):
    """Wrapper to fail on invalid service."""
    def_string = redis.hget("admin:service:"+service, "def")
    if def_string is None:
        response = flask.jsonify(message="invalid service name: %s" % service)
        flask.abort(flask.make_response(response, 404))
    return pickle.loads(def_string)

def _usagecapacity(service):
    """calculate the current usage of the service."""
    usage = 0
    usage_cpu = 0
    capacity = 0
    capacity_cpus = 0
    busy = 0
    detail = {}
    servers = service.list_servers()
    for resource in service.list_resources():
        detail[resource] = { 'busy': '', 'reserved': '' }
        r_capacity = service.list_resources()[resource]
        detail[resource]['capacity'] = r_capacity
        capacity += r_capacity
        reserved = redis.get("reserved:%s:%s" % (service.name, resource))
        if reserved:
            detail[resource]['reserved'] = reserved
        r_usage = redis.hgetall("gpu_resource:%s:%s" % (service.name, resource)).values()
        count_usage = {}
        ncpus = {}
        for t in r_usage:
            if t in count_usage:
                count_usage[t] += 1
            else:
                count_usage[t] = 1
                ncpus[t] = int(redis.hget("task:%s" % t, "ncpus"))
        r_usage = redis.lrange("cpu_resource:%s:%s" % (service.name, resource), 0, -1)
        for t in r_usage:
            ncpus[t] = int(redis.hget("task:%s" % t, "ncpus"))
        detail[resource]['usage'] = [ "%s: %d (%d)" % (k, count_usage[k], ncpus[k]) for k in count_usage ]
        detail[resource]['ncpus'] = servers[resource]['ncpus']
        detail[resource]['avail_cpus'] = int(redis.get("ncpus:%s:%s" % (service.name, resource)))
        usage += len(r_usage)
        capacity_cpus += servers[resource]['ncpus']
        usage_cpu += sum([ncpus[k] for k in count_usage])
        err = redis.get("busy:%s:%s" % (service.name, resource))
        if err:
            detail[resource]['busy'] = err
            busy = busy + 1
    queued = redis.llen("queued:"+service.name)
    return "%d (%d)" % (usage, usage_cpu), queued, "%d (%d)" % (capacity, capacity_cpus), busy, detail

def _count_maxgpu(service):
    aggr_resource = {}
    max_gpu = -1
    for resource in service.list_resources():
        capacity = service.list_resources()[resource]
        p = resource.find(':')
        if p != -1:
            resource = resource[0:p]
        if resource not in aggr_resource:
            aggr_resource[resource] = 0
        aggr_resource[resource] += capacity
        if aggr_resource[resource] > max_gpu:
            max_gpu = aggr_resource[resource]
    return max_gpu    

def task_request(func):
    """minimal check on the request to check that tasks exists"""
    @wraps(func)
    def func_wrapper(*args, **kwargs):
        if not task.exists(redis, kwargs['task_id']):
            flask.abort(flask.make_response(flask.jsonify(message="task %s unknown" % kwargs['task_id']), 404))
        return func(*args, **kwargs)
    return func_wrapper

filter_routes = []
def filter_request(route, ability=None):
    def wrapper(func):
        """generic request filter system for customization"""
        @wraps(func)
        def func_wrapper(*args, **kwargs):
            if len(filter_routes):
                return filter_routes[0](route, ability, func, *args, **kwargs)
            # if no filter defined, just pass through
            return func(*args, **kwargs)
        return func_wrapper
    return wrapper

@app.route("/service/list", methods=["GET"])
@filter_request("GET/service/list")
def list_services():
    res = {}
    for keys in redis.scan_iter("admin:service:*"):
        service = keys[14:]
        service_def = get_service(service)                
        usage, queued, capacity, busy, detail = _usagecapacity(service_def)
        pids = []
        for keyw in redis.scan_iter("admin:worker:%s:*" % service):
            pids.append(keyw[len("admin:worker:%s:" % service):])
        pid = ",".join(pids)
        name = service_def.display_name
        if len(pids) == 0:
            busy = "yes"
            pid = "**NO WORKER**"
        res[service] = { 'name': name, 'pid': pid,
                         'usage': usage, 'queued': queued,
                         'capacity': capacity, 'busy': busy,
                         'detail': detail }
    return flask.jsonify(res)

@app.route("/service/describe/<string:service>", methods=["GET"])
@filter_request("GET/service/describe")
def describe(service):
    service_module = get_service(service)
    return flask.jsonify(service_module.describe())

@app.route("/service/listconfig/<string:service>", methods=["GET"])
@filter_request("GET/service/listconfig", "edit:config")
def server_listconfig(service):
    current_configuration = redis.hget("admin:service:%s" % service, "current_configuration")
    configurations = redis.hget("admin:service:%s" % service, "configurations")
    return flask.jsonify({
                            'current': current_configuration,
                            'configurations': json.loads(configurations)
                         })


def post_adminrequest(app, service, action, configname, value=True):
    identifier = "%d.%d" % (os.getpid(), app._requestid)
    app._requestid += 1
    redis.set("admin:config:%s:%s:%s:%s" % (service, action, configname, identifier), value)
    wait = 0
    while wait < 360:
        configresult = redis.get("admin:configresult:%s:%s:%s:%s" % (service, action, configname, identifier))
        if configresult:
            break
        wait += 1
        time.sleep(1)
    if configresult is None:
        redis.delete("admin:configresult:%s:%s:%s:%s" % (service, action, configname, identifier))
        flask.abort(flask.make_response(flask.jsonify(message="request time-out"), 408))
    elif configresult != "ok":
        flask.abort(flask.make_response(flask.jsonify(message=configresult), 400))
    return configresult

@app.route("/service/selectconfig/<string:service>/<string:configname>", methods=["GET"])
@filter_request("GET/service/selectconfig", "edit:config")
def server_selectconfig(service, configname):
    configresult = post_adminrequest(app, service, "select", configname)
    return flask.jsonify(configresult)

@app.route("/service/setconfig/<string:service>/<string:configname>", methods=["POST"])
@filter_request("GET/service/setconfig", "edit:config")
def server_setconfig(service, configname):
    config = flask.request.form.get('config')
    configresult = post_adminrequest(app, service, "set", configname, config)
    return flask.jsonify(configresult)

@app.route("/service/delconfig/<string:service>/<string:configname>", methods=["GET"])
@filter_request("GET/service/delconfig", "edit:config")
def server_delconfig(service, configname):
    configresult = post_adminrequest(app, service, "del", configname)
    return flask.jsonify(configresult)

@app.route("/service/check/<string:service>", methods=["GET"])
@filter_request("GET/service/check")
def check(service):
    service_options = flask.request.get_json() if flask.request.is_json else None
    if service_options is None:
        service_options = {}
    service_module = get_service(service)
    try:
        details = service_module.check(service_options)
    except ValueError as e:
        flask.abort(flask.make_response(flask.jsonify(message=str(e)), 400))
    except Exception as e:
        flask.abort(flask.make_response(flask.jsonify(message=str(e)), 500))
    else:
        return flask.jsonify(details)

@app.route("/task/launch/<string:service>", methods=["POST"])
@filter_request("POST/task/launch")
def launch(service):
    content = flask.request.form.get('content')
    if content is not None:
        content = json.loads(content)
    else:
        flask.abort(flask.make_response(flask.jsonify(message="missing content in request"), 400))

    files = {}
    for k in flask.request.files:
        files[k] = flask.request.files[k].read()

    service_module = get_service(service)
    content["service"] = service

    task_type = '????'
    if "train" in content["docker"]["command"]: task_type = "train"
    elif "trans" in content["docker"]["command"]: task_type = "trans"
    elif "preprocess" in content["docker"]["command"]: task_type = "prepr"
    elif "release" in content["docker"]["command"]: task_type = "relea"
    elif "buildvocab" in content["docker"]["command"]: task_type = "vocab"

    if task_type == '????':
        flask.abort(flask.make_response(flask.jsonify(message="incorrect task definition"), 400))

    # Sanity check on content.
    if 'options' not in content or not isinstance(content['options'], dict):
        flask.abort(flask.make_response(flask.jsonify(message="invalid options field"), 400))
    if 'docker' not in content:
        flask.abort(flask.make_response(flask.jsonify(message="missing docker field"), 400))
    if ('image' not in content['docker']
        or 'registry' not in content['docker']
        or 'tag' not in content['docker']
        or 'command' not in content['docker']):
        flask.abort(flask.make_response(flask.jsonify(message="incomplete docker field"), 400))
    if content['docker']['registry'] == 'auto':
        repository = content['docker']['image']
        p = repository.find("/")
        if p == -1:
            flask.abort(flask.make_response(flask.jsonify(message="image should be repository/name"), 400))
        repository = repository[:p]
        registry = None
        for r in service_module._config['docker']['registries']:
            v = service_module._config['docker']['registries'][r]
            if "default_for" in v and repository in v['default_for']:
                registry = r
                break
        if registry is None:
            flask.abort(flask.make_response(
                flask.jsonify(message="cannot find registry for repository %s" % repository), 400))
        content['docker']['registry'] = registry
    elif content['docker']['registry'] not in service_module._config['docker']['registries']:
        flask.abort(flask.make_response(flask.jsonify(message="unknown docker registry"), 400))
    resource = service_module.get_resource_from_options(content["options"])

    iterations = 1
    if "iterations" in content:
        iterations = content["iterations"]
        if (task_type != "train" and iterations != 1) or iterations < 1:
            flask.abort(flask.make_response(flask.jsonify(message="invalid value for iterations"), 400))

    ngpus = 1
    if "ngpus" in content:
        ngpus = content["ngpus"]
    # check that we have a resource able to run such a request
    if _count_maxgpu(service_module) < ngpus:
        flask.abort(flask.make_response(flask.jsonify(message="no resource available on %s for %d gpus"
                                            % (service, ngpus)), 400))

    if "totranslate" in content:
        totranslate = content["totranslate"]
        del content["totranslate"]
    else:
        totranslate = None

    priority = content.get("priority", 0)

    (xxyy, parent_task_id) = shallow_command_analysis(content["docker"]["command"])

    task_ids = []

    while iterations > 0:
        task_id = build_task_id(content, xxyy, parent_task_id)
        task.create(redis, taskfile_dir,
                    task_id, task_type, parent_task_id, resource, service, content, files, priority, ngpus)
        task_ids.append(task_id)
        remove_config_option(content["docker"]["command"])
        if totranslate:
            content_translate = deepcopy(content)
            content_translate["priority"] = priority+1
            content_translate["ngpus"] = min(ngpus, 1)
            content_translate["docker"]["command"] = ["trans"]
            content_translate["docker"]["command"].append('-i')
            for f in totranslate:
                content_translate["docker"]["command"].append(f[0])
            content_translate["docker"]["command"].append('-o')
            for f in totranslate:
                content_translate["docker"]["command"].append(f[1].replace('<MODEL>', task_id))
            change_parent_task(content_translate["docker"]["command"], task_id)
            trans_task_id = build_task_id(content_translate, xxyy, task_id)
            task.create(redis, taskfile_dir,
                        trans_task_id, "trans", task_id, resource, service, content_translate, (),
                        content_translate["priority"], content_translate["ngpus"])
            task_ids.append(trans_task_id)
        iterations -= 1
        if iterations > 0:
            parent_task_id = task_id
            change_parent_task(content["docker"]["command"], parent_task_id)

    if len(task_ids) == 1:
        task_ids = task_ids[0]

    return flask.jsonify(task_ids)

@app.route("/task/status/<string:task_id>", methods=["GET"])
@filter_request("GET/task/status")
@task_request
def status(task_id):
    response = task.info(redis, taskfile_dir, task_id, [])
    return flask.jsonify(response)

@app.route("/task/<string:task_id>", methods=["DELETE"])
@filter_request("DELETE/task")
@task_request
def del_task(task_id):
    response = task.delete(redis, taskfile_dir, task_id)
    if isinstance(response, list) and not response[0]:
        flask.abort(flask.make_response(flask.jsonify(message=response[1]), 400))        
    return flask.jsonify(message="deleted %s" % task_id)

@app.route("/task/list/<string:pattern>", methods=["GET"])
@filter_request("GET/task/list")
def list_tasks(pattern):
    ltask = []
    for task_key in task.scan_iter(redis, pattern):
        task_id = task.id(task_key)
        info = task.info(redis, taskfile_dir, task_id,
                ["queued_time", "alloc_resource", "alloc_lgpu", "resource", "content",
                 "status", "message", "type", "iterations", "priority"])
        if info["content"] is not None and info["content"] != "":
            content = json.loads(info["content"])
            info["image"] = content['docker']['image']
            del info['content']
        else:
            info["image"] = '-'
        info['task_id'] = task_id
        ltask.append(info)
    return flask.jsonify(ltask)

@app.route("/task/terminate/<string:task_id>", methods=["GET"])
@task_request
def terminate(task_id):
    with redis.acquire_lock(task_id):
        current_status = task.info(redis, taskfile_dir, task_id, "status")
        if current_status is None:
            flask.abort(flask.make_response(flask.jsonify(message="task %s unknown" % task_id), 404))
        elif current_status == "stopped":
            return flask.jsonify(message="%s already stopped" % task_id)
        phase = flask.request.args.get('phase')
        task.terminate(redis, task_id, phase=phase)
    return flask.jsonify(message="terminating %s" % task_id)

@app.route("/task/beat/<string:task_id>", methods=["PUT", "GET"])
@task_request
def task_beat(task_id):
    duration = flask.request.args.get('duration')
    try:
        if duration is not None:
            duration = int(duration)
    except ValueError:
        flask.abort(flask.make_response(flask.jsonify(message="invalid duration value"), 400))
    container_id = flask.request.args.get('container_id')
    task.beat(redis, task_id, duration, container_id)
    return flask.jsonify(200)

@app.route("/task/file/<string:task_id>/<path:filename>", methods=["GET"])
@task_request
def get_file(task_id, filename):
    content = task.get_file(redis, taskfile_dir, task_id, filename)
    if content is None:
        flask.abort(flask.make_response(
            flask.jsonify(message="cannot find file %s for task %s" % (filename, task_id)), 404))
    return flask.send_file(io.BytesIO(content), attachment_filename=filename, mimetype="application/octet-stream")

@app.route("/file/<string:task_id>/<path:filename>", methods=["POST"])
@app.route("/task/file/<string:task_id>/<path:filename>", methods=["POST"])
@task_request
def post_file(task_id, filename):
    content = flask.request.get_data()
    task.set_file(redis, taskfile_dir, task_id, content, filename)
    return flask.jsonify(200)

@app.route("/task/log/<string:task_id>", methods=["GET"])
@filter_request("GET/task/log")
@task_request
def get_log(task_id):
    content = task.get_log(redis, taskfile_dir, task_id)
    if content is None:
        flask.abort(flask.make_response(
            flask.jsonify(message="cannot find log for task %s" % task_id), 404))
    response = flask.make_response(content)
    return response

@app.route("/task/log/<string:task_id>", methods=["PATCH"])
@task_request
def append_log(task_id):
    content = flask.request.get_data()
    task.append_log(redis, taskfile_dir, task_id, content)
    return flask.jsonify(200)

@app.route("/task/log/<string:task_id>", methods=["POST"])
@task_request
def post_log(task_id):
    content = flask.request.get_data()
    task.set_log(redis, taskfile_dir, task_id, content)
    return flask.jsonify(200)

@app.route("/status", methods=["GET"])
def get_status():
    return flask.jsonify(200)

@app.route("/version", methods=["GET"])
def get_version_request():
    return flask.make_response(get_version())