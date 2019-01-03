import flask
import io
import pickle
import json
import logging
import os
import time
import semver
from collections import Counter
from copy import deepcopy
from functools import wraps

from flask import abort, make_response, jsonify
from app import app, redis, get_version, ch, taskfile_dir
from nmtwizard import common, task
from nmtwizard.helper import build_task_id, shallow_command_analysis, boolean_param
from nmtwizard.helper import change_parent_task, remove_config_option, model_name_analysis
from nmtwizard.helper import get_cpu_count
from nmtwizard.capacity import Capacity

logger = logging.getLogger(__name__)
logger.addHandler(app.logger)
# get maximum log size from configuration
max_log_size = app.iniconfig.get('default', 'max_log_size', fallback=None)
if max_log_size is not None:
    max_log_size = int(max_log_size)


def get_service(service):
    """Wrapper to fail on invalid service."""
    def_string = redis.hget("admin:service:"+service, "def")
    if def_string is None:
        response = flask.jsonify(message="invalid service name: %s" % service)
        abort(flask.make_response(response, 404))
    return pickle.loads(def_string)


def _duplicate_adapt(service, content):
    """Duplicate content and apply service-specification modifications
    """
    dup_content = deepcopy(content)
    # command = dup_content["docker"]["command"]
    # if "--no_push" in command and service.temporary_ms:
    #     new_command = ['-ms', service.temporary_ms+":"]
    #     idx = 0
    #     while idx < len(command):
    #         c = command[idx]
    #         if c == '-ms' or c == '--model_storage':
    #             idx += 1
    #         elif c != '--no_push':
    #             new_command.append(c)
    #         idx += 1
    #     dup_content["docker"]["command"] = new_command
    return dup_content


def _usagecapacity(service):
    """calculate the current usage of the service."""
    usage_xpu = Capacity()
    capacity_xpus = Capacity()
    busy = 0
    detail = {}
    for resource in service.list_resources():
        detail[resource] = {'busy': '', 'reserved': ''}
        r_capacity = service.list_resources()[resource]
        detail[resource]['capacity'] = r_capacity
        capacity_xpus += r_capacity
        reserved = redis.get("reserved:%s:%s" % (service.name, resource))
        if reserved:
            detail[resource]['reserved'] = reserved

        count_map_gpu = Counter()
        count_map_cpu = Counter()
        task_type = {}
        count_used_xpus = Capacity()

        r_usage_gpu = redis.hgetall("gpu_resource:%s:%s" % (service.name, resource)).values()
        for t in r_usage_gpu:
            if t not in task_type:
                task_type[t] = redis.hget("task:%s" % t, "type")
            count_map_gpu[t] += 1
            count_used_xpus.incr_ngpus(1)

        r_usage_cpu = redis.hgetall("cpu_resource:%s:%s" % (service.name, resource)).values()
        for t in r_usage_cpu:
            if t not in task_type:
                task_type[t] = redis.hget("task:%s" % t, "type")
            count_map_cpu[t] += 1
            count_used_xpus.incr_ncpus(1)

        detail[resource]['usage'] = ["%s %s: %d (%d)" % (task_type[t],
                                                         t,
                                                         count_map_gpu[t],
                                                         count_map_cpu[t]) for t in task_type]
        detail[resource]['avail_gpus'] = r_capacity.ngpus - count_used_xpus.ngpus
        detail[resource]['avail_cpus'] = r_capacity.ncpus - count_used_xpus.ncpus
        err = redis.get("busy:%s:%s" % (service.name, resource))
        if err:
            detail[resource]['busy'] = err
            busy = busy + 1
        usage_xpu += count_used_xpus
    queued = redis.llen("queued:"+service.name)
    return ("%d (%d)" % (usage_xpu.ngpus, usage_xpu.ncpus), queued,
            "%d (%d)" % (capacity_xpus.ngpus, capacity_xpus.ncpus),
            busy, detail)


def _find_compatible_resource(service, ngpus, ncpus, request_resource):
    for resource in service.list_resources():
        if request_resource == 'auto' or resource == request_resource or \
                (isinstance(request_resource, list) and resource in request_resource):
            capacity = service.list_resources()[resource]
            if ngpus <= capacity.ngpus and (ncpus is None or ncpus <= capacity.ncpus):
                return True
    return False


def task_request(func):
    """minimal check on the request to check that tasks exists"""
    @wraps(func)
    def func_wrapper(*args, **kwargs):
        if not task.exists(redis, kwargs['task_id']):
            abort(flask.make_response(flask.jsonify(message="task %s unknown" % kwargs['task_id']),
                                      404))
        return func(*args, **kwargs)
    return func_wrapper


# global variable to contains all filters on the routes
filter_routes = []
# global variable to contains all possible filters on ability
has_ability_funcs = []
# extension functions
post_functions = {}


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


def has_ability(g, ability, entity, accessall=True):
    for f in has_ability_funcs:
        if not f(g, ability, entity, accessall):
            return False
    return True


def post_function(method, *args):
    if method in post_functions:
        return post_functions[method](*args)
    return args


@app.route("/service/list", methods=["GET"])
@filter_request("GET/service/list")
def list_services():
    minimal = boolean_param(flask.request.args.get('minimal'))
    showall = boolean_param(flask.request.args.get('all'))
    res = {}
    for keys in redis.scan_iter("admin:service:*"):
        service = keys[14:]
        pool_entity = service[0:2].upper()
        if has_ability(flask.g, "", pool_entity, showall):
            service_def = get_service(service)
            name = service_def.display_name
            if minimal:
                res[service] = {'name': name}
            else:
                usage, queued, capacity, busy, detail = _usagecapacity(service_def)
                pids = []
                for keyw in redis.scan_iter("admin:worker:%s:*" % service):
                    pids.append(keyw[len("admin:worker:%s:" % service):])
                pid = ",".join(pids)
                if len(pids) == 0:
                    busy = "yes"
                    pid = "**NO WORKER**"
                res[service] = {'name': name, 'pid': pid,
                                'usage': usage, 'queued': queued,
                                'capacity': capacity, 'busy': busy,
                                'detail': detail}
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
        abort(make_response(jsonify(message="insufficient credentials for edit:config "
                                            "(entity %s)" % pool_entity), 403))
    current_configuration = redis.hget("admin:service:%s" % service, "current_configuration")
    configurations = redis.hget("admin:service:%s" % service, "configurations")
    return flask.jsonify({
                            'current': current_configuration,
                            'configurations': json.loads(configurations)
                         })


def post_adminrequest(app, service, action, configname="base", value="1"):
    identifier = "%d.%d" % (os.getpid(), app._requestid)
    app._requestid += 1
    redis.set("admin:config:%s:%s:%s:%s" % (service, action, configname, identifier), value)
    wait = 0
    while wait < 360:
        configresult = redis.get("admin:configresult:%s:%s:%s:%s" % (service, action,
                                                                     configname, identifier))
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
        abort(make_response(jsonify(message="insufficient credentials for edit:config "
                                            "(entity %s)" % pool_entity), 403))
    configresult = post_adminrequest(app, service, "select", configname)
    return flask.jsonify(configresult)


@app.route("/service/setconfig/<string:service>/<string:configname>", methods=["POST"])
@filter_request("GET/service/setconfig", "edit:config")
def server_setconfig(service, configname):
    pool_entity = service[0:2].upper()
    if not has_ability(flask.g, "edit:config", pool_entity):
        abort(make_response(jsonify(message="insufficient credentials for edit:config "
                                            "(entity %s)" % pool_entity), 403))
    config = flask.request.form.get('config')
    configresult = post_adminrequest(app, service, "set", configname, config)
    return flask.jsonify(configresult)


@app.route("/service/delconfig/<string:service>/<string:configname>", methods=["GET"])
@filter_request("GET/service/delconfig", "edit:config")
def server_delconfig(service, configname):
    pool_entity = service[0:2].upper()
    if not has_ability(flask.g, "edit:config", pool_entity):
        abort(make_response(jsonify(message="insufficient credentials for edit:config "
                                            "(entity %s)" % pool_entity), 403))
    configresult = post_adminrequest(app, service, "del", configname)
    return flask.jsonify(configresult)


@app.route("/service/restart/<string:service>", methods=["GET"])
@filter_request("GET/service/restart", "edit:config")
def server_restart(service):
    pool_entity = service[0:2].upper()
    if not has_ability(flask.g, "edit:config", pool_entity):
        abort(make_response(jsonify(message="insufficient credentials for edit:config "
                                            "(entity %s)" % pool_entity), 403))
    configresult = post_adminrequest(app, service, "restart")
    return flask.jsonify(configresult)


@app.route("/service/stop/<string:service>", methods=["GET"])
@filter_request("GET/service/stop", "stop:config")
def server_stop(service):
    pool_entity = service[0:2].upper()
    if not has_ability(flask.g, "edit:config", pool_entity):
        abort(make_response(jsonify(message="insufficient credentials for edit:config "
                                            "(entity %s)" % pool_entity), 403))
    configresult = post_adminrequest(app, service, "stop")
    return flask.jsonify(configresult)


@app.route("/service/enable/<string:service>/<string:resource>", methods=["GET"])
@filter_request("GET/service/enable", "edit:config")
def server_enable(service, resource):
    pool_entity = service[0:2].upper()
    if not has_ability(flask.g, "edit:config", pool_entity):
        abort(make_response(jsonify(message="insufficient credentials for edit:config "
                                            "(entity %s)" % pool_entity), 403))
    service_module = get_service(service)
    if resource not in service_module.list_resources():
        abort(make_response(jsonify(message="unknown resource '%s' in '%s'" % (resource, service)),
                            400))
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
        abort(make_response(jsonify(message="insufficient credentials for edit:config "
                                            "(entity %s)" % pool_entity), 403))
    message = flask.request.args.get('message')
    if message is None:
        message = "DISABLED"
    service_module = get_service(service)
    if resource not in service_module.list_resources():
        abort(make_response(jsonify(message="unknown resource '%s' in '%s'" % (resource, service)),
                            400))
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
        abort(make_response(jsonify(message="insufficient credentials for train "
                                            "(entity %s)" % pool_entity), 403))

    current_configuration_name = redis.hget("admin:service:%s" % service, "current_configuration")
    configurations = json.loads(redis.hget("admin:service:%s" % service, "configurations"))
    current_configuration = json.loads(configurations[current_configuration_name][1])

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
    if "train" in content["docker"]["command"]:
        task_type = "train"
    elif "trans" in content["docker"]["command"]:
        task_type = "trans"
    elif "preprocess" in content["docker"]["command"]:
        task_type = "prepr"
    elif "release" in content["docker"]["command"]:
        task_type = "relea"
    elif "buildvocab" in content["docker"]["command"]:
        task_type = "vocab"

    if task_type == '????':
        abort(flask.make_response(flask.jsonify(message="incorrect task definition"), 400))

    # Sanity check on content.
    if 'options' not in content or not isinstance(content['options'], dict):
        abort(flask.make_response(flask.jsonify(message="invalid options field"), 400))
    if 'docker' not in content:
        abort(flask.make_response(flask.jsonify(message="missing docker field"), 400))
    if ('image' not in content['docker'] or 'registry' not in content['docker'] or
       'tag' not in content['docker'] or 'command' not in content['docker']):
        abort(flask.make_response(flask.jsonify(message="incomplete docker field"), 400))
    if content['docker']['registry'] == 'auto':
        repository = content['docker']['image']
        p = repository.find("/")
        if p == -1:
            abort(flask.make_response(flask.jsonify(message="image should be repository/name"),
                                      400))
        repository = repository[:p]
        registry = None
        for r in service_module._config['docker']['registries']:
            v = service_module._config['docker']['registries'][r]
            if "default_for" in v and repository in v['default_for']:
                registry = r
                break
        if registry is None:
            abort(flask.make_response(
                    flask.jsonify(message="cannot find registry for repository %s" % repository),
                    400))
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
    ncpus = content.get("ncpus")

    # check that we have a resource able to run such a request
    if not _find_compatible_resource(service_module, ngpus, ncpus, resource):
        abort(flask.make_response(
                    flask.jsonify(message="no resource available on %s for %d gpus (%s cpus)" %
                                  (service, ngpus, ncpus and str(ncpus) or "-")), 400))

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
        can_trans_as_release = semver.match(docker_version, ">=1.8.0")
        trans_as_release = (not content.get("notransasrelease", False) and
                            semver.match(docker_version, ">=1.8.0"))
    except ValueError as err:
        # could not match docker_version - not valid semver
        chain_prepr_train = False
        trans_as_release = False

    priority = content.get("priority", 0)

    (xxyy, parent_task_id) = shallow_command_analysis(content["docker"]["command"])
    parent_task_type = None
    if parent_task_id:
        (parent_struct, parent_task_type) = model_name_analysis(parent_task_id)

    # check that parent model type matches current command
    if parent_task_type:
        if (parent_task_type == "trans" or parent_task_type == "relea" or
           (task_type == "prepr" and parent_task_type != "train" and parent_task_type != "vocab")):
            abort(flask.make_response(flask.jsonify(message="invalid parent task type: %s" %
                                      (parent_task_type)), 400))

    task_ids = []
    task_create = []

    while iterations > 0:
        if (chain_prepr_train and parent_task_type != "prepr") or task_type == "prepr":
            prepr_task_id = build_task_id(content, xxyy, "prepr", parent_task_id)

            idx = 0
            prepr_command = []
            train_command = content["docker"]["command"]
            while train_command[idx] != 'train' and train_command[idx] != 'preprocess':
                prepr_command.append(train_command[idx])
                idx += 1

            # create preprocess command, don't push the model on the catalog,
            # and generate a pseudo model
            prepr_command.append("--no_push")
            prepr_command.append("preprocess")
            prepr_command.append("--build_model")

            content["docker"]["command"] = prepr_command

            content["ncpus"] = ncpus or \
                get_cpu_count(current_configuration, 0, "preprocess")
            content["ngpus"] = 0

            preprocess_resource = service_module.select_resource_from_capacity(
                                            resource, Capacity(content["ngpus"], content["ncpus"]))

            # launch preprocess task on cpus only
            task_create.append(
                    (redis, taskfile_dir,
                     prepr_task_id, "prepr", parent_task_id, preprocess_resource, service,
                     _duplicate_adapt(service_module, content),
                     files, priority, 0, content["ncpus"], {}))
            task_ids.append("%s\t%s\tngpus: %d, ncpus: %d" % ("prepr", prepr_task_id, 0, content["ncpus"]))
            remove_config_option(train_command)
            change_parent_task(train_command, prepr_task_id)
            parent_task_id = prepr_task_id
            content["docker"]["command"] = train_command

        if task_type != "prepr":
            task_id = build_task_id(content, xxyy, task_type, parent_task_id)

            content["ncpus"] = ncpus or \
                get_cpu_count(current_configuration, ngpus, task_type)
            content["ngpus"] = ngpus

            if task_type == "trans" and can_trans_as_release:
                if "--as_release" not in content["docker"]["command"] and trans_as_release:
                    content["docker"]["command"].append("--as_release")
                    content["ngpus"] = ngpus = 0

            task_resource = service_module.select_resource_from_capacity(
                                            resource, Capacity(content["ngpus"],
                                                               content["ncpus"]))

            task_create.append(
                    (redis, taskfile_dir,
                     task_id, task_type, parent_task_id, task_resource, service,
                     _duplicate_adapt(service_module, content),
                     files, priority,
                     content["ngpus"], content["ncpus"],
                     {}))
            task_ids.append("%s\t%s\tngpus: %d, ncpus: %d" % (
                        task_type, task_id,
                        content["ngpus"], content["ncpus"]))
            parent_task_type = task_type[:5]
            remove_config_option(content["docker"]["command"])

            if totranslate:
                content_translate = deepcopy(content)
                content_translate["priority"] = priority + 1
                if trans_as_release:
                    content_translate["ngpus"] = 0
                else:
                    content_translate["ngpus"] = min(ngpus, 1)

                content_translate["ncpus"] = ncpus or \
                    get_cpu_count(current_configuration,
                                  content_translate["ngpus"], "trans")

                translate_resource = service_module.select_resource_from_capacity(
                                                resource, Capacity(content_translate["ngpus"],
                                                                   content_translate["ncpus"]))

                if ngpus == 0 or trans_as_release:
                    file_per_gpu = len(totranslate)
                else:
                    file_per_gpu = (len(totranslate)+ngpus-1) / ngpus
                subset_idx = 0
                while subset_idx * file_per_gpu < len(totranslate):
                    content_translate["docker"]["command"] = ["trans"]
                    if trans_as_release:
                        content_translate["docker"]["command"].append("--as_release")
                    content_translate["docker"]["command"].append('-i')
                    subset_totranslate = totranslate[subset_idx*file_per_gpu:
                                                     (subset_idx+1)*file_per_gpu]
                    for f in subset_totranslate:
                        content_translate["docker"]["command"].append(f[0])
                    content_translate["docker"]["command"].append('-o')
                    for f in subset_totranslate:
                        content_translate["docker"]["command"].append(
                            f[1].replace('<MODEL>', task_id))
                    change_parent_task(content_translate["docker"]["command"], task_id)
                    trans_task_id = build_task_id(content_translate, xxyy, "trans", task_id)
                    task_create.append(
                            (redis, taskfile_dir,
                             trans_task_id, "trans", task_id, translate_resource, service,
                             _duplicate_adapt(service_module, content_translate),
                             (), content_translate["priority"],
                             content_translate["ngpus"], content_translate["ncpus"],
                             {}))
                    task_ids.append("%s\t%s\tngpus: %d, ncpus: %d" % (
                                           "trans", trans_task_id,
                                           content_translate["ngpus"], content_translate["ncpus"]))
                    subset_idx += 1
        iterations -= 1
        if iterations > 0:
            parent_task_id = task_id
            change_parent_task(content["docker"]["command"], parent_task_id)

    (task_ids, task_create) = post_function('POST/task/launch', task_ids, task_create)

    for tc in task_create:
        task.create(*tc)

    if len(task_ids) == 1:
        task_ids = task_ids[0]

    return flask.jsonify(task_ids)


@app.route("/task/status/<string:task_id>", methods=["GET"])
@filter_request("GET/task/status")
@task_request
def status(task_id):
    fields = flask.request.args.get('fields', None)
    if fields is not None and fields != '':
        fields = fields.split(',')
    else:
        fields = None
    response = task.info(redis, taskfile_dir, task_id, fields)
    if response.get("alloc_lgpu"):
        response["alloc_lgpu"] = response["alloc_lgpu"].split(",")
    if response.get("alloc_lcpu"):
        response["alloc_lcpu"] = response["alloc_lcpu"].split(",")
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
    if p is False:
        abort(make_response(jsonify(message="insufficient credentials for "
                                            "list tasks starting with %s" % prefix[0:2]), 403))
    elif p is not True:
        prefix = p
    for task_key in task.scan_iter(redis, prefix + suffix):
        task_id = task.id(task_key)
        info = task.info(
                redis, taskfile_dir, task_id,
                ["launched_time", "alloc_resource", "alloc_lgpu", "alloc_lcpu", "resource", "content",
                 "status", "message", "type", "iterations", "priority"])
        if info["alloc_lgpu"]:
            info["alloc_lgpu"] = info["alloc_lgpu"].split(",")
        if info["alloc_lcpu"]:
            info["alloc_lcpu"] = info["alloc_lcpu"].split(",")
        info["image"] = '-'
        info["model"] = '-'
        if info["content"]:
            content = json.loads(info["content"])
            info["image"] = content["docker"]["image"] + ':' + content["docker"]["tag"]
            j = 0
            while j < len(content["docker"]["command"]) - 1:
                if content["docker"]["command"][j] == "-m" or content["docker"]["command"][j] == "--model":
                    info["model"] = content["docker"]["command"][j+1]
                    break
                j = j+1
            del info['content']
        info['task_id'] = task_id
        ltask.append(info)
    return flask.jsonify(ltask)


@app.route("/task/terminate/<string:task_id>", methods=["GET"])
@filter_request("GET/task/terminate")
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
@filter_request("PUT/task/beat")
@task_request
def task_beat(task_id):
    duration = flask.request.args.get('duration')
    try:
        if duration is not None:
            duration = int(duration)
    except ValueError:
        abort(flask.make_response(flask.jsonify(message="invalid duration value"), 400))
    container_id = flask.request.args.get('container_id')
    try:
        task.beat(redis, task_id, duration, container_id)
    except Exception as e:
        abort(flask.make_response(flask.jsonify(message=str(e)), 400))
    return flask.jsonify(200)


@app.route("/task/file/<string:task_id>/<path:filename>", methods=["GET"])
@task_request
def get_file(task_id, filename):
    content = task.get_file(redis, taskfile_dir, task_id, filename)
    if content is None:
        abort(flask.make_response(
            flask.jsonify(message="cannot find file %s for task %s" % (filename, task_id)), 404))
    return flask.send_file(io.BytesIO(content), attachment_filename=filename,
                           mimetype="application/octet-stream")


@app.route("/file/<string:task_id>/<path:filename>", methods=["POST"])
@app.route("/task/file/<string:task_id>/<path:filename>", methods=["POST"])
@filter_request("POST/task/file")
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
@filter_request("PATCH/task/log")
@task_request
def append_log(task_id):
    content = flask.request.get_data()
    task.append_log(redis, taskfile_dir, task_id, content, max_log_size)
    duration = flask.request.args.get('duration')
    try:
        if duration is not None:
            duration = int(duration)
    except ValueError:
        abort(flask.make_response(flask.jsonify(message="invalid duration value"), 400))
    try:
        task.beat(redis, task_id, duration, None)
    except Exception as e:
        abort(flask.make_response(flask.jsonify(message=str(e)), 400))
    return flask.jsonify(200)


@app.route("/task/log/<string:task_id>", methods=["POST"])
@filter_request("POST/task/log")
@task_request
def post_log(task_id):
    content = flask.request.get_data()
    content = task.set_log(redis, taskfile_dir, task_id, content, max_log_size)
    (task_id, content) = post_function('POST/task/log', task_id, content)
    return flask.jsonify(200)


@app.route("/status", methods=["GET"])
def get_status():
    return flask.jsonify(200)


@app.route("/version", methods=["GET"])
def get_version_request():
    return flask.make_response(get_version())
