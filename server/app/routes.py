import flask
import io
import pickle
import json
import logging
import os
import time
import semver
from copy import deepcopy
from functools import wraps

from flask import abort, make_response, jsonify
from app import app, redis, get_version, ch, taskfile_dir
from nmtwizard import common, task
from nmtwizard.helper import build_task_id, shallow_command_analysis
from nmtwizard.helper import change_parent_task, remove_config_option, model_name_analysis

logger = logging.getLogger(__name__)
logger.addHandler(app.logger)

def get_service(service):
    """Wrapper to fail on invalid service."""
    def_string = redis.hget("admin:service:"+service, "def")
    if def_string is None:
        response = flask.jsonify(message="invalid service name: %s" % service)
        abort(flask.make_response(response, 404))
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
        task_type = {}
        ncpus = {}
        for t in r_usage:
            task_type[t] = redis.hget("task:%s" % t, "type")
            if t in count_usage:
                count_usage[t] += 1
            else:
                count_usage[t] = 1
                ncpus[t] = int(redis.hget("task:%s" % t, "ncpus"))
        r_usage = redis.lrange("cpu_resource:%s:%s" % (service.name, resource), 0, -1)
        for t in r_usage:
            if t not in task_type:
                task_type[t] = redis.hget("task:%s" % t, "type")
            count_usage[t] = 0
            ncpus[t] = int(redis.hget("task:%s" % t, "ncpus"))
        detail[resource]['usage'] = [ "%s %s: %d (%d)" % (task_type[k], k, count_usage[k], ncpus[k]) for k in count_usage ]
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
            abort(flask.make_response(flask.jsonify(message="task %s unknown" % kwargs['task_id']), 404))
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

has_ability_funcs = []
def has_ability(g, ability, entity, accessall=True):
    for f in has_ability_funcs:
        if not f(g, ability, entity, accessall):
            return False
    return True

# extension functions
post_functions = {}
def post_function(method, *args):
    if method in post_functions:
        return post_functions[method](*args)
    return args

@app.route("/service/list", methods=["GET"])
@filter_request("GET/service/list")
def list_services():
    minimal = flask.request.args.get('minimal', False)
    minimal = not(minimal is False or minimal == "" or minimal == "0" or minimal == "False")
    showall = flask.request.args.get('all', False)
    showall = not(showall is False or showall == "" or showall == "0" or showall == "False")
    res = {}
    for keys in redis.scan_iter("admin:service:*"):
        service = keys[14:]
        pool_entity = service[0:2].upper()
        if has_ability(flask.g, "", pool_entity, showall):
            service_def = get_service(service)                
            name = service_def.display_name
            if minimal:
                res[service] = { 'name': name }
            else:
                usage, queued, capacity, busy, detail = _usagecapacity(service_def)
                pids = []
                for keyw in redis.scan_iter("admin:worker:%s:*" % service):
                    pids.append(keyw[len("admin:worker:%s:" % service):])
                pid = ",".join(pids)
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
    pool_entity = service[0:2].upper()
    if not has_ability(flask.g, "edit:config", pool_entity):
        abort(make_response(jsonify(message="insufficient credentials for edit:config (entity %s)" % pool_entity), 403))
    current_configuration = redis.hget("admin:service:%s" % service, "current_configuration")
    configurations = redis.hget("admin:service:%s" % service, "configurations")
    return flask.jsonify({
                            'current': current_configuration,
                            'configurations': json.loads(configurations)
                         })


def post_adminrequest(app, service, action, configname="base", value=True):
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
        abort(flask.make_response(flask.jsonify(message="request time-out"), 408))
    elif configresult != "ok":
        abort(flask.make_response(flask.jsonify(message=configresult), 400))
    return configresult

@app.route("/service/selectconfig/<string:service>/<string:configname>", methods=["GET"])
@filter_request("GET/service/selectconfig", "edit:config")
def server_selectconfig(service, configname):
    pool_entity = service[0:2].upper()
    if not has_ability(flask.g, "edit:config", pool_entity):
        abort(make_response(jsonify(message="insufficient credentials for edit:config (entity %s)" % pool_entity), 403))
    configresult = post_adminrequest(app, service, "select", configname)
    return flask.jsonify(configresult)

@app.route("/service/setconfig/<string:service>/<string:configname>", methods=["POST"])
@filter_request("GET/service/setconfig", "edit:config")
def server_setconfig(service, configname):
    pool_entity = service[0:2].upper()
    if not has_ability(flask.g, "edit:config", pool_entity):
        abort(make_response(jsonify(message="insufficient credentials for edit:config (entity %s)" % pool_entity), 403))
    config = flask.request.form.get('config')
    configresult = post_adminrequest(app, service, "set", configname, config)
    return flask.jsonify(configresult)

@app.route("/service/delconfig/<string:service>/<string:configname>", methods=["GET"])
@filter_request("GET/service/delconfig", "edit:config")
def server_delconfig(service, configname):
    pool_entity = service[0:2].upper()
    if not has_ability(flask.g, "edit:config", pool_entity):
        abort(make_response(jsonify(message="insufficient credentials for edit:config (entity %s)" % pool_entity), 403))
    configresult = post_adminrequest(app, service, "del", configname)
    return flask.jsonify(configresult)

@app.route("/service/restart/<string:service>", methods=["GET"])
@filter_request("GET/service/restart", "edit:config")
def server_restart(service):
    pool_entity = service[0:2].upper()
    if not has_ability(flask.g, "edit:config", pool_entity):
        abort(make_response(jsonify(message="insufficient credentials for edit:config (entity %s)" % pool_entity), 403))
    configresult = post_adminrequest(app, service, "restart")
    return flask.jsonify(configresult)

@app.route("/service/stop/<string:service>", methods=["GET"])
@filter_request("GET/service/stop", "stop:config")
def server_stop(service):
    pool_entity = service[0:2].upper()
    if not has_ability(flask.g, "edit:config", pool_entity):
        abort(make_response(jsonify(message="insufficient credentials for edit:config (entity %s)" % pool_entity), 403))
    configresult = post_adminrequest(app, service, "stop")
    return flask.jsonify(configresult)

@app.route("/service/enable/<string:service>/<string:resource>", methods=["GET"])
@filter_request("GET/service/enable", "edit:config")
def server_enable(service, resource):
    pool_entity = service[0:2].upper()
    if not has_ability(flask.g, "edit:config", pool_entity):
        abort(make_response(jsonify(message="insufficient credentials for edit:config (entity %s)" % pool_entity), 403))
    keyr = "busy:%s:%s" % (service, resource)
    if redis.exists(keyr):
        redis.delete("busy:%s:%s" % (service, resource))
        return flask.jsonify("ok")
    else:
        abort(flask.make_response(flask.jsonify(message="resource was not disabled"), 400))

@app.route("/service/disable/<string:service>/<string:resource>", methods=["GET"])
@filter_request("GET/service/disable", "edit:config")
def server_disable(service, resource):
    pool_entity = service[0:2].upper()
    if not has_ability(flask.g, "edit:config", pool_entity):
        abort(make_response(jsonify(message="insufficient credentials for edit:config (entity %s)" % pool_entity), 403))
    message = flask.request.args.get('message')
    if message is None:
        message = "DISABLED"
    redis.set("busy:%s:%s" % (service, resource), message)
    return flask.jsonify("ok")

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
        abort(flask.make_response(flask.jsonify(message=str(e)), 400))
    except Exception as e:
        abort(flask.make_response(flask.jsonify(message=str(e)), 500))
    else:
        return flask.jsonify(details)

@app.route("/task/launch/<string:service>", methods=["POST"])
@filter_request("POST/task/launch", "train")
def launch(service):
    pool_entity = service[0:2].upper()
    if not has_ability(flask.g, "train", pool_entity):
        abort(make_response(jsonify(message="insufficient credentials for train (entity %s)" % pool_entity), 403))

    content = flask.request.form.get('content')
    if content is not None:
        content = json.loads(content)
    else:
        abort(flask.make_response(flask.jsonify(message="missing content in request"), 400))

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
        abort(flask.make_response(flask.jsonify(message="incorrect task definition"), 400))

    # Sanity check on content.
    if 'options' not in content or not isinstance(content['options'], dict):
        abort(flask.make_response(flask.jsonify(message="invalid options field"), 400))
    if 'docker' not in content:
        abort(flask.make_response(flask.jsonify(message="missing docker field"), 400))
    if ('image' not in content['docker']
        or 'registry' not in content['docker']
        or 'tag' not in content['docker']
        or 'command' not in content['docker']):
        abort(flask.make_response(flask.jsonify(message="incomplete docker field"), 400))
    if content['docker']['registry'] == 'auto':
        repository = content['docker']['image']
        p = repository.find("/")
        if p == -1:
            abort(flask.make_response(flask.jsonify(message="image should be repository/name"), 400))
        repository = repository[:p]
        registry = None
        for r in service_module._config['docker']['registries']:
            v = service_module._config['docker']['registries'][r]
            if "default_for" in v and repository in v['default_for']:
                registry = r
                break
        if registry is None:
            abort(flask.make_response(
                flask.jsonify(message="cannot find registry for repository %s" % repository), 400))
        content['docker']['registry'] = registry
    elif content['docker']['registry'] not in service_module._config['docker']['registries']:
        abort(flask.make_response(flask.jsonify(message="unknown docker registry"), 400))
    resource = service_module.get_resource_from_options(content["options"])

    iterations = 1
    if "iterations" in content:
        iterations = content["iterations"]
        if (task_type != "train" and iterations != 1) or iterations < 1:
            abort(flask.make_response(flask.jsonify(message="invalid value for iterations"), 400))

    ngpus = 1
    if "ngpus" in content:
        ngpus = content["ngpus"]
    # check that we have a resource able to run such a request
    if _count_maxgpu(service_module) < ngpus:
        abort(flask.make_response(flask.jsonify(message="no resource available on %s for %d gpus"
                                            % (service, ngpus)), 400))

    if "totranslate" in content:
        totranslate = content["totranslate"]
        del content["totranslate"]
    else:
        totranslate = None

    docker_version = content['docker']['tag']
    if docker_version.startswith('v'):
        docker_version = docker_version[1:]
    try:
        chain_prepr_train = (not content.get("nochainprepr", False) and
                             task_type == "train" and
                             semver.match(docker_version, ">=1.4.0"))
    except ValueError as err:
        # could not match docker_version - not valid semver
        chain_prepr_train = False

    priority = content.get("priority", 0)

    (xxyy, parent_task_id) = shallow_command_analysis(content["docker"]["command"])
    parent_task_type = None
    if parent_task_id:
        (parent_struct, parent_task_type) = model_name_analysis(parent_task_id)

    # check that parent model type matches current command
    if parent_task_type:
        if (parent_task_type == "trans" or parent_task_type == "relea" or
            (task_type == "prepr" and parent_task_type != "train" and parent_task_type != "vocab")):
            abort(flask.make_response(flask.jsonify(message="invalid parent task type: %s"
                                    % (parent_task_type)), 400))

    task_ids = []

    while iterations > 0:
        if (chain_prepr_train and parent_task_type != "prepr") or task_type == "prepr":
            prepr_task_id = build_task_id(content, xxyy, "prepr", parent_task_id)

            idx = 0
            prepr_command = []
            train_command = content["docker"]["command"]
            while train_command[idx] != 'train' and train_command[idx] != 'preprocess':
                prepr_command.append(train_command[idx])
                idx += 1

            # create preprocess command, don't push the model on the catalog, and generate a pseudo model
            prepr_command.append("--no_push")
            prepr_command.append("preprocess")
            prepr_command.append("--build_model")

            content["docker"]["command"] = prepr_command
            # launch preprocess task on cpus only
            task.create(redis, taskfile_dir,
                        prepr_task_id, "prepr", parent_task_id, resource, service, content, files, priority, 0)
            task_ids.append("%s\t%s\tngpus: %d" % ("prepr", prepr_task_id, 0))
            remove_config_option(train_command)
            change_parent_task(train_command, prepr_task_id)
            parent_task_id = prepr_task_id
            content["docker"]["command"] = train_command

        if task_type != "prepr":
            task_id = build_task_id(content, xxyy, task_type, parent_task_id)
            task.create(redis, taskfile_dir,
                        task_id, task_type, parent_task_id, resource, service, content, files, priority, ngpus)
            task_ids.append("%s\t%s\tngpus: %d" % (task_type, task_id, ngpus))
            parent_task_type = task_type[:5]
            remove_config_option(content["docker"]["command"])
            if totranslate:
                content_translate = deepcopy(content)
                content_translate["priority"] = priority+1
                content_translate["ngpus"] = min(ngpus, 1)
                if ngpus == 0:
                    file_per_gpu = len(totranslate)
                else:
                    file_per_gpu = (len(totranslate)+ngpus-1) / ngpus
                subset_idx = 0
                while subset_idx * file_per_gpu < len(totranslate):
                    content_translate["docker"]["command"] = ["trans"]
                    content_translate["docker"]["command"].append('-i')
                    subset_totranslate = totranslate[subset_idx*file_per_gpu:
                                                     (subset_idx+1)*file_per_gpu]
                    for f in subset_totranslate:
                        content_translate["docker"]["command"].append(f[0])
                    content_translate["docker"]["command"].append('-o')
                    for f in subset_totranslate:
                        content_translate["docker"]["command"].append(f[1].replace('<MODEL>', task_id))
                    change_parent_task(content_translate["docker"]["command"], task_id)
                    trans_task_id = build_task_id(content_translate, xxyy, "trans", task_id)
                    task.create(redis, taskfile_dir,
                                trans_task_id, "trans", task_id, resource, service, content_translate, (),
                                content_translate["priority"], content_translate["ngpus"])
                    task_ids.append("%s\t%s\tngpus: %d" % ("trans", trans_task_id, content_translate["ngpus"]))
                    subset_idx += 1
        iterations -= 1
        if iterations > 0:
            parent_task_id = task_id
            change_parent_task(content["docker"]["command"], parent_task_id)

    if len(task_ids) == 1:
        task_ids = task_ids[0]

    (task_ids,) = post_function('POST/task/launch', task_ids)

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
        abort(flask.make_response(flask.jsonify(message=response[1]), 400))        
    return flask.jsonify(message="deleted %s" % task_id)

@app.route("/task/list/<string:pattern>", methods=["GET"])
@filter_request("GET/task/list")
def list_tasks(pattern):
    ltask = []
    prefix = pattern
    suffix = ''
    if prefix.endswith('*'):
        prefix = prefix[:-1]
        suffix = '*'
    p = has_ability(flask.g, '', prefix)
    if p == False:
        abort(make_response(jsonify(message="insufficient credentials for list tasks starting with %s" % prefix[0:2]), 403))
    elif p != True:
        prefix = p
    for task_key in task.scan_iter(redis, prefix + suffix):
        task_id = task.id(task_key)
        info = task.info(redis, taskfile_dir, task_id,
                ["launched_time", "alloc_resource", "alloc_lgpu", "resource", "content",
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
            abort(flask.make_response(flask.jsonify(message="task %s unknown" % task_id), 404))
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
        abort(flask.make_response(flask.jsonify(message="invalid duration value"), 400))
    container_id = flask.request.args.get('container_id')
    task.beat(redis, task_id, duration, container_id)
    return flask.jsonify(200)

@app.route("/task/file/<string:task_id>/<path:filename>", methods=["GET"])
@task_request
def get_file(task_id, filename):
    content = task.get_file(redis, taskfile_dir, task_id, filename)
    if content is None:
        abort(flask.make_response(
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
def get_log(task_id):
    content = task.get_log(redis, taskfile_dir, task_id)
    (task_id, content) = post_function('GET/task/log', task_id, content)
    if content is None:
        abort(flask.make_response(
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
    (task_id, content) = post_function('POST/task/log', task_id, content)
    return flask.jsonify(200)

@app.route("/status", methods=["GET"])
def get_status():
    return flask.jsonify(200)

@app.route("/version", methods=["GET"])
def get_version_request():
    return flask.make_response(get_version())