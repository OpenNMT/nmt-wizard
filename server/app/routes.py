import io
import json
import logging
import os
import time
from collections import Counter
from copy import deepcopy
from functools import wraps
import builtins
import re
import traceback
from werkzeug.wsgi import FileWrapper
import semver
import six
from werkzeug.exceptions import HTTPException
import flask
from flask import abort, make_response, jsonify, Response, request, g, render_template
import tempfile
import uuid
from functools import cmp_to_key
from bson import ObjectId
from app import app, redis_db, redis_db_without_decode, mongo_client, get_version, taskfile_dir
from nmtwizard import task, configuration as config
from nmtwizard.helper import build_task_id, shallow_command_analysis, \
    get_docker_action, cust_jsondump, get_cpu_count, get_params, boolean_param
from nmtwizard.helper import change_parent_task, remove_config_option, model_name_analysis
from nmtwizard.capacity import Capacity
from nmtwizard.task import get_task_entity, TaskInfo

from systran_storages import StorageClient
from nmtwizard.common import rmprivate
from nmtwizard.helper import boolean_param


GLOBAL_POOL_NAME = "sa_global_pool"

logger = logging.getLogger(__name__)
logger.addHandler(app.logger)
# get maximum log size from configuration
max_log_size = app.get_other_config(['default', 'max_log_size'], fallback=None)
if max_log_size is not None:
    max_log_size = int(max_log_size)

TASK_RELEASE_TYPE = "relea"
ACCEPT_FORMAT = {"text/html" : "html", "application/json" : "json"}


class StorageId:
    @staticmethod
    def encode_storage_name(name, entity):
        return name + "@" + entity

    @staticmethod
    def decode_storage_name(name):
        decoded_name = name.split("@", 1)
        if len(decoded_name) == 2:
            entity = decoded_name[0]
            storage_id = decoded_name[1]
        else:  # case user not giving the entity, consider it as storage name
            entity = None
            storage_id = decoded_name[0]

        return entity, storage_id

    @staticmethod
    def get_entites(names):
        entities = set({})
        for storage in names:
            entity, storage_id = StorageId.decode_storage_name(storage)
            if entity and entity != config.CONFIG_DEFAULT:
                entities.add(entity)
        return entities


def get_entity_owner(service_entities, service_name):
    trainer_of_entities = get_entities_by_permission("train", flask.g)

    if not trainer_of_entities:
        abort(flask.make_response(flask.jsonify(message="you are not a trainer in any entity"), 403))

    entity_owner = flask.request.form.get('entity_owner')
    if not entity_owner:
        if len(trainer_of_entities) == 1:
            entity_owner = trainer_of_entities[0]
        elif len(service_entities) == 1:
            entity_owner = service_entities[0]

    if not entity_owner:
        abort(flask.make_response(flask.jsonify(
            message="model owner is ambigious between these entities: (%s)" % str(",".join(trainer_of_entities))), 400))
    entity_owner = entity_owner.upper()

    if not has_ability(flask.g, 'train', entity_owner):
        abort(flask.make_response(flask.jsonify(message="you are not a trainer of %s" % entity_owner), 403))
    elif entity_owner not in service_entities:
        abort(flask.make_response(flask.jsonify(
            message="This service '%s' is not reserved to launch the task of the entity %s" % (
                service_name, entity_owner)), 400))

    return entity_owner


def get_entities_by_permission(the_permission, g):
    return [ent_code for ent_code in g.entities if
            isinstance(ent_code, str) and has_ability(g, the_permission, ent_code)]


def check_permission(service, permission):
    services = redis_db.smembers("admin:services")
    if service not in services:
        abort(make_response(jsonify(message="insufficient credentials for edit_config on this service %s" % service),
                            403))
    is_polyentity = config.is_polyentity_service(mongo_client, service)
    if is_polyentity and not has_ability(flask.g, permission, ""):  # super admin
        abort(make_response(jsonify(message="insufficient credentials for edit_config on this service %s" % service),
                            403))
    elif not is_polyentity:
        pool_entity = service[0:2].upper()
        if not has_ability(flask.g, "edit_config", pool_entity):
            abort(make_response(jsonify(message="insufficient credentials for edit_config (entity %s)" % pool_entity),
                                403))


@app.errorhandler(Exception)
def handle_error(e):
    # return a nice message when any exception occured, keeping the orignal Http error
    # https://stackoverflow.com/questions/29332056/global-error-handler-for-any-exception
    if 'user' in flask.g:
        app.logger.error("User:'%s'" % flask.g.user.user_code)
    
    app.logger.error(traceback.format_exc())

    code = 500
    if isinstance(e, HTTPException):
        code = e.code

    accept = accept_for_error()
        
    if accept == 'json':
        return jsonify(error=str(e)), code
    else:
        session = None
        current_user = None
        if g.is_authenticated:
            session = g.session
            current_user = g.session['currentUser']
        return render_template("error.html",infos=session, user=current_user, code=code, title=e.name, message=e.description)

def accept_for_error():
    for mime_type,_ in request.accept_mimetypes:
        if mime_type in ACCEPT_FORMAT:
            return ACCEPT_FORMAT[mime_type]
        
    json = request.full_path.startswith('/api/')
    
    return 'json' if json else 'html'


def cust_jsonify(obj):
    result = cust_jsondump(obj)
    return Response(result, mimetype='application/json')


def get_service(service):
    """Wrapper to fail on invalid service."""
    service_config = config.get_service_config(mongo_client, service)
    base_config = config.get_base_config(mongo_client)
    services, merged_config = config.load_service_config(service_config, base_config)

    return services[service]


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
        reserved = redis_db.get("reserved:%s:%s" % (service.name, resource))
        if reserved:
            detail[resource]['reserved'] = reserved

        count_map_gpu = Counter()
        count_map_cpu = Counter()
        task_type = {}
        count_used_xpus = Capacity()

        r_usage_gpu = redis_db.hgetall("gpu_resource:%s:%s" % (service.name, resource)).values()
        for t in r_usage_gpu:
            if t not in task_type:
                task_type[t] = redis_db.hget("task:%s" % t, "type")
            count_map_gpu[t] += 1
            count_used_xpus.incr_ngpus(1)

        r_usage_cpu = redis_db.hgetall("cpu_resource:%s:%s" % (service.name, resource)).values()
        for t in r_usage_cpu:
            if t not in task_type:
                task_type[t] = redis_db.hget("task:%s" % t, "type")
            count_map_cpu[t] += 1
            count_used_xpus.incr_ncpus(1)

        detail[resource]['usage'] = ["%s %s: %d (%d)" % (task_type[t], t, count_map_gpu[t],
                                                         count_map_cpu[t]) for t in task_type]
        detail[resource]['avail_gpus'] = r_capacity.ngpus - count_used_xpus.ngpus
        detail[resource]['avail_cpus'] = r_capacity.ncpus - count_used_xpus.ncpus
        err = redis_db.get("busy:%s:%s" % (service.name, resource))
        if err:
            detail[resource]['busy'] = err
            busy = busy + 1
        usage_xpu += count_used_xpus
    queued = redis_db.llen("queued:" + service.name)
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


def _get_registry(service_module, image):
    p = image.find("/")
    if p == -1:
        abort(flask.make_response(flask.jsonify(message="image should be repository/name"),
                                  400))
    repository = image[:p]
    registry = None
    docker_registries = config.get_registries(mongo_client, service_module.name)
    for r in docker_registries:
        v = docker_registries[r]
        if "default_for" in v and repository in v['default_for']:
            registry = r
            break
    if registry is None:
        abort(flask.make_response(
            flask.jsonify(message="cannot find registry for repository %s" % repository),
            400))
    return registry


def task_request(func):
    """minimal check on the request to check that tasks exists"""

    @wraps(func)
    def func_wrapper(*args, **kwargs):
        if not task.exists(redis_db, kwargs['task_id']):
            abort(flask.make_response(flask.jsonify(message="task %s unknown" % kwargs['task_id']),
                                      404))
        return func(*args, **kwargs)

    return func_wrapper


def task_write_control(func):
    """minimal check on the request to check that tasks exists"""

    @wraps(func)
    def func_wrapper(*args, **kwargs):
        ok = False
        task_id = kwargs['task_id']

        if task_id is None:
            abort(flask.make_response(flask.jsonify(message="task empty"), 404))

        if not task.exists(redis_db, task_id):
            abort(flask.make_response(flask.jsonify(message="task %s unknown" % task_id), 404))

        entity = get_task_entity(task_id)

        if has_ability(flask.g, 'admin_task', entity):
            ok = True
        elif has_ability(flask.g, 'train', entity) and flask.g.user.user_code == task_id[2:5]:
            ok = True

        if not ok:
            abort(make_response(jsonify(message="insufficient credentials for tasks %s" % task_id),
                                403))

        return func(*args, **kwargs)

    return func_wrapper


def task_readonly_control(task_id):
    if task_id is None:
        abort(flask.make_response(flask.jsonify(message="task empty"), 404))

    if not task.exists(redis_db, task_id):
        abort(flask.make_response(flask.jsonify(message="task %s unknown" % task_id), 404))
    entity = get_task_entity(task_id)
    if not has_ability(flask.g, 'train', entity):
        abort(
            make_response(jsonify(message="insufficient credentials for tasks %s" % task_id), 403))


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


def has_ability(g, ability, entity):
    for f in has_ability_funcs:
        if not f(g, ability, entity):
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
    for service_name in redis_db.smembers("admin:services"):
        service_config = config.get_service_config(mongo_client, service_name)
        if service_config is None:
            abort(make_response(jsonify(message="service configuration %s unknown" % service_name), 404))

        pool_entities = config.get_entities(service_config)

        if not showall and flask.g.user.entity.entity_code not in pool_entities:
            continue

        if any(has_ability(flask.g, "train", pool_entity) for pool_entity in pool_entities):
            service_def = get_service(service_name)
            name = service_def.display_name
            if minimal:
                res[service_name] = {'name': name}
            else:
                usage, queued, capacity, busy, detail = _usagecapacity(service_def)
                pids = get_worker_pids(service_name)
                pid = ",".join(pids)
                if len(pids) == 0:
                    busy = "yes"
                    pid = "**NO WORKER**"
                res[service_name] = {'name': name, 'pid': pid,
                                     'usage': usage, 'queued': queued,
                                     'capacity': capacity, 'busy': busy,
                                     'detail': detail}
    return flask.jsonify(res)


@app.route("/service/describe/<string:service>", methods=["GET"])
@filter_request("GET/service/describe")
def describe(service):
    service_module = get_service(service)
    return flask.jsonify(service_module.describe())


@app.route("/service/configs/_base", methods=["GET"])
@filter_request("GET/service/configs", "is_super")
def get_base_config():
    base_config = config.get_base_config(mongo_client)
    return flask.jsonify(base_config)


@app.route("/service/configs/<string:service>", methods=["GET"])
@filter_request("GET/service/configs", "edit_config")
def get_service_config(service):
    check_permission(service, "edit_config")
    service_config = config.get_service_config(mongo_client, service)
    return flask.jsonify(service_config)


def post_admin_request(app, service, action, value="1"):
    identifier = "%d.%d" % (os.getpid(), app.request_id)
    app.request_id += 1
    redis_db.set("admin:command:%s:%s:%s" % (service, action, identifier), value)
    wait = 0
    while wait < 360:
        command_result = redis_db.get("admin:command_result:%s:%s:%s" % (service, action, identifier))
        if command_result:
            break
        wait += 1
        time.sleep(1)
    if command_result is None:
        redis_db.delete(
            "admin:command_result:%s:%s:%s" % (service, action, identifier))
        abort(flask.make_response(flask.jsonify(message="request time-out"), 408))
    elif command_result != "ok":
        abort(flask.make_response(flask.jsonify(message=command_result), 400))
    return command_result


@app.route("/service/configs/<string:service>", methods=["POST"])
@filter_request("GET/service/configs", "edit_config")
def set_service_config(service):
    check_permission(service, "edit_config")
    request_body = flask.request.form.get('config')
    try:
        update_config = json.loads(request_body)
        update_config["updated_at"] = time.time()
        config.set_service_config(mongo_client, service, update_config)
        worker_pids = get_worker_pids(service)
        if len(worker_pids) == 0:
            return flask.jsonify("ok")
        command_response = post_admin_request(app, service, "restart")
        return flask.jsonify(command_response)
    except Exception as e:
        abort(flask.make_response(flask.jsonify(message=str(e)), 400))


@app.route("/service/restart/<string:service>", methods=["GET"])
@filter_request("GET/service/restart", "edit_config")
def server_restart(service):
    check_permission(service, "edit_config")
    command_response = post_admin_request(app, service, "restart")
    return flask.jsonify(command_response)


@app.route("/service/stop/<string:service>", methods=["GET"])
@filter_request("GET/service/stop", "stop_config")
def server_stop(service):
    check_permission(service, "stop_config")
    command_response = post_admin_request(app, service, "stop")
    return flask.jsonify(command_response)


@app.route("/service/enable/<string:service>/<string:resource>", methods=["GET"])
@filter_request("GET/service/enable", "edit_config")
def server_enable(service, resource):
    check_permission(service, "edit_config")
    service_module = get_service(service)
    if resource not in service_module.list_resources():
        abort(make_response(jsonify(message="unknown resource '%s' in '%s'" % (resource, service)),
                            400))
    keyr = "busy:%s:%s" % (service, resource)
    if redis_db.exists(keyr):
        redis_db.delete("busy:%s:%s" % (service, resource))
        return flask.jsonify("ok")
    abort(flask.make_response(flask.jsonify(message="resource was not disabled"), 400))


@app.route("/service/disable/<string:service>/<string:resource>", methods=["GET"])
@filter_request("GET/service/disable", "edit_config")
def server_disable(service, resource):
    check_permission(service, "edit_config")
    message = flask.request.args.get('message')
    if message is None:
        message = "DISABLED"
    service_module = get_service(service)
    if resource not in service_module.list_resources():
        abort(make_response(jsonify(message="unknown resource '%s' in '%s'" % (resource, service)),
                            400))
    redis_db.set("busy:%s:%s" % (service, resource), message)
    return flask.jsonify("ok")


@app.route("/service/check/<string:service>", methods=["GET"])
@filter_request("GET/service/check")
def check(service):
    service_options = flask.request.get_json() if flask.request.is_json else None
    if service_options is None:
        service_options = {}

    service_module = get_service(service)
    registries = config.get_registries(mongo_client, service)
    try:
        details = service_module.check(service_options, registries)
    except ValueError as e:
        abort(flask.make_response(flask.jsonify(message=str(e)), 400))
    except Exception as e:
        abort(flask.make_response(flask.jsonify(message=str(e)), 500))
    else:
        return flask.jsonify(details)


def patch_config_explicitname(content, explicitname):
    if "docker" in content and content["docker"].get("command"):
        idx = 0
        command = content["docker"].get("command")
        while idx < len(command):
            if command[idx][0] != '-':
                return
            if command[idx] == '-m' or command[idx] == '--model':
                return
            if command[idx] == '--no_push':
                idx += 1
                continue
            if command[idx] == '-c' or command[idx] == '--config':
                config = json.loads(command[idx + 1])
                config["modelname_description"] = explicitname
                command[idx + 1] = json.dumps(config)
                return
            idx += 2


@app.route("/v2/task/launch", methods=["POST"])
@filter_request("POST/v2/task/launch", "train")
def launch_v2():
    service = GLOBAL_POOL_NAME
    entity_code = g.user.entity.entity_code
    user_code = g.user.user_code
    # TODO: Try-catch
    request_data = parse_request_data(request)

    service_config = config.get_service_config(mongo_client, service_name=GLOBAL_POOL_NAME)
    entity_storages_config = get_entity_storages(service_config, entity_code)
    storage_client = get_storage_client(entity_storages_config)
    storage_id = next(iter(entity_storages_config))
    upload_path = f"/{entity_code}/{uuid.uuid4().hex}"

    training_data = request_data.get("training_data")
    testing_data = request_data.get("testing_data")
    tags = request_data.get("tags")

    upload_user_files(storage_client, storage_id, upload_path, "train", training_data)
    upload_user_files(storage_client, storage_id, upload_path, "test", testing_data)
    tag_ids = process_tags(tags, entity_code, user_code)

    service_config = config.get_service_config(mongo_client, service)
    service_module = get_service(service)
    service_entities = config.get_entities(service_config)
    entity_owner = get_entity_owner(service_entities, service)
    trainer_entities = get_entities_by_permission("train", flask.g)
    assert trainer_entities  # Here: almost sure you are trainer

    content = get_training_config(service, request_data, user_code, service_module, entity_owner, upload_path,
                                  storage_id, tag_ids)

    other_task_info = {TaskInfo.ENTITY_OWNER.value: entity_owner,
                       TaskInfo.STORAGE_ENTITIES.value: json.dumps(trainer_entities)}

    task_type = "train"
    task_suffix = task_type
    resource = service_module.get_resource_from_options(content["options"])
    iterations = content.get("iterations", 1)
    files = {}
    ngpus = content.get("ngpus", 1)
    ncpus = content.get("ncpus")

    totranslate = content["totranslate"]
    toscore = content["toscore"]
    totuminer = None

    del content["totranslate"]
    del content["toscore"]

    priority = content.get("priority", 0)

    xxyy = f'{request_data["source"]}{request_data["target"]}'
    parent_task_id = None
    parent_struct = None
    parent_task_type = None

    task_ids = []
    task_create = []

    docker_version = content['docker']['tag']
    if docker_version.startswith('v'):
        docker_version = docker_version[1:]
    try:
        chain_prepr_train = semver.match(docker_version, ">=1.4.0")
        can_trans_as_release = semver.match(docker_version, ">=1.8.0")
        trans_as_release = semver.match(docker_version, ">=1.8.0")
        content["support_statistics"] = semver.match(docker_version, ">=1.17.0")
    except ValueError as err:
        # could not match docker_version - not valid semver
        chain_prepr_train = False
        trans_as_release = False

    while iterations > 0:
        if (chain_prepr_train and parent_task_type != "prepr") or task_type == "prepr":
            prepr_task_id, explicitname = build_task_id(content, xxyy, "prepr", parent_task_id)

            if explicitname:
                patch_config_explicitname(content, explicitname)

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

            content["ncpus"] = ncpus or get_cpu_count(service_config, 0, "preprocess")
            content["ngpus"] = 0

            preprocess_resource = service_module.select_resource_from_capacity(
                resource, Capacity(content["ngpus"], content["ncpus"]))

            # launch preprocess task on cpus only
            task_create.append(
                (redis_db, taskfile_dir,
                 prepr_task_id, "prepr", parent_task_id, preprocess_resource, service,
                 _duplicate_adapt(service_module, content),
                 files, priority, 0, content["ncpus"], other_task_info))
            task_ids.append(
                "%s\t%s\tngpus: %d, ncpus: %d" % ("prepr", prepr_task_id, 0, content["ncpus"]))
            remove_config_option(train_command)
            change_parent_task(train_command, prepr_task_id)
            parent_task_id = prepr_task_id
            content["docker"]["command"] = train_command

        if task_type != "prepr":
            task_id, explicitname = build_task_id(content, xxyy, task_suffix, parent_task_id)

            if explicitname:
                patch_config_explicitname(content, explicitname)

            file_to_transtaskid = {}
            if task_type == "trans":
                try:
                    idx = content["docker"]["command"].index("trans")
                    output_files = get_params(("-o", "--output"), content["docker"]["command"][idx + 1:])
                    for ofile in output_files:
                        file_to_transtaskid[ofile] = task_id
                except Exception:
                    pass

            content["ncpus"] = ncpus or \
                               get_cpu_count(service_config, ngpus, task_type)
            content["ngpus"] = ngpus

            if task_type == "trans" and can_trans_as_release:
                if "--as_release" not in content["docker"]["command"] and trans_as_release:
                    content["docker"]["command"].append("--as_release")
                    content["ngpus"] = ngpus = 0

            task_resource = service_module.select_resource_from_capacity(
                resource, Capacity(content["ngpus"],
                                   content["ncpus"]))

            task_create.append(
                (redis_db, taskfile_dir,
                 task_id, task_type, parent_task_id, task_resource, service,
                 _duplicate_adapt(service_module, content),
                 files, priority,
                 content["ngpus"], content["ncpus"],
                 other_task_info))
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

                content_translate["ncpus"] = ncpus or get_cpu_count(service_config,
                                                                    content_translate["ngpus"],
                                                                    "trans")

                translate_resource = service_module.select_resource_from_capacity(
                    resource, Capacity(content_translate["ngpus"],
                                       content_translate["ncpus"]))

                if ngpus == 0 or trans_as_release:
                    file_per_gpu = len(totranslate)
                else:
                    file_per_gpu = int((len(totranslate) + ngpus - 1) / ngpus)
                subset_idx = 0
                while subset_idx * file_per_gpu < len(totranslate):
                    content_translate["docker"]["command"] = ["trans"]
                    if trans_as_release:
                        content_translate["docker"]["command"].append("--as_release")
                    content_translate["docker"]["command"].append('-i')
                    subset_totranslate = totranslate[subset_idx * file_per_gpu:
                                                     (subset_idx + 1) * file_per_gpu]
                    for f in subset_totranslate:
                        content_translate["docker"]["command"].append(f[0])

                    change_parent_task(content_translate["docker"]["command"], task_id)
                    trans_task_id, explicitname = build_task_id(content_translate, xxyy, "trans",
                                                                task_id)

                    content_translate["docker"]["command"].append('-o')
                    for f in subset_totranslate:
                        ofile = f[1].replace('<MODEL>', task_id)
                        file_to_transtaskid[ofile] = trans_task_id
                        content_translate["docker"]["command"].append(ofile)

                    task_create.append(
                        (redis_db, taskfile_dir,
                         trans_task_id, "trans", task_id, translate_resource, service,
                         _duplicate_adapt(service_module, content_translate),
                         (), content_translate["priority"],
                         content_translate["ngpus"], content_translate["ncpus"],
                         other_task_info))
                    task_ids.append("%s\t%s\tngpus: %d, ncpus: %d" % (
                        "trans", trans_task_id,
                        content_translate["ngpus"], content_translate["ncpus"]))
                    subset_idx += 1

            if toscore:
                toscore_parent = {}
                for (ofile, rfile) in toscore:
                    ofile = ofile.replace('<MODEL>', task_id)
                    parent_task_id = file_to_transtaskid.get(ofile)
                    if parent_task_id:
                        if parent_task_id not in toscore_parent:
                            toscore_parent[parent_task_id] = {"output": [], "ref": []}
                        ofile_split = ofile.split(':')
                        if len(ofile_split) == 2 and ofile_split[0] == 'launcher':
                            ofile = 'launcher:../' + parent_task_id + "/" + ofile_split[1]
                        toscore_parent[parent_task_id]["output"].append(ofile)
                        toscore_parent[parent_task_id]["ref"].append(rfile)
                for parent_task_id, oref in six.iteritems(toscore_parent):
                    content_score = deepcopy(content)
                    content_score["priority"] = priority + 1
                    content_score["ngpus"] = 0
                    content_score["ncpus"] = 1

                    score_resource = service_module.select_resource_from_capacity(resource,
                                                                                  Capacity(0, 1))

                    image_score = "nmtwizard/score"

                    option_lang = []
                    if parent_struct is not None:
                        option_lang.append('-l')
                        option_lang.append(parent_struct['xxyy'][-2:])

                    content_score["docker"] = {
                        "image": image_score,
                        "registry": _get_registry(service_module, image_score),
                        "tag": "latest",
                        "command": ["score", "-o"] + oref["output"] + ["-r"] + oref["ref"] + option_lang + ['-f',
                                                                                                            "launcher:scores"]
                    }

                    score_task_id, explicitname = build_task_id(content_score, xxyy, "score",
                                                                parent_task_id)
                    task_create.append(
                        (redis_db, taskfile_dir,
                         score_task_id, "exec", parent_task_id, score_resource, service,
                         content_score,
                         files, priority + 2,
                         0, 1,
                         other_task_info))
                    task_ids.append("%s\t%s\tngpus: %d, ncpus: %d" % (
                        "score", score_task_id,
                        0, 1))

            if totuminer:
                # tuminer can run in CPU only mode, but it will be very slow for large data
                ngpus_recommend = ngpus
                ncpus_recommend = ncpus or get_cpu_count(service_config, 0, "tuminer")

                totuminer_parent = {}
                for (ifile, ofile) in totuminer:
                    # ofile = ofile.replace('<MODEL>', task_id)
                    parent_task_id = file_to_transtaskid.get(ofile)
                    if parent_task_id:
                        if parent_task_id not in totuminer_parent:
                            totuminer_parent[parent_task_id] = {"infile": [], "outfile": [],
                                                                "scorefile": []}
                        ofile_split = ofile.split(':')
                        if len(ofile_split) == 2 and ofile_split[0] == 'launcher':
                            ofile = 'launcher:../' + parent_task_id + "/" + ofile_split[1]
                        totuminer_parent[parent_task_id]["infile"].append(ifile)
                        totuminer_parent[parent_task_id]["outfile"].append(ofile)
                        scorefile = ofile
                        if scorefile.endswith(".gz"):
                            scorefile = scorefile[:-3]
                        totuminer_parent[parent_task_id]["scorefile"].append(scorefile[:-3])
                for parent_task_id, in_out in six.iteritems(totuminer_parent):
                    content_tuminer = deepcopy(content)
                    content_tuminer["priority"] = priority + 1
                    content_tuminer["ngpus"] = ngpus_recommend
                    content_tuminer["ncpus"] = ncpus_recommend

                    tuminer_resource = service_module.select_resource_from_capacity(resource,
                                                                                    Capacity(
                                                                                        ngpus_recommend,
                                                                                        ncpus_recommend))

                    image_score = "nmtwizard/tuminer"

                    content_tuminer["docker"] = {
                        "image": image_score,
                        "registry": _get_registry(service_module, image_score),
                        "tag": "latest",
                        "command": ["tuminer", "--tumode", "score", "--srcfile"] + in_out["infile"] + ["--tgtfile"] +
                                   in_out["outfile"] + ["--output"] + in_out["scorefile"]
                    }

                    tuminer_task_id, explicitname = build_task_id(content_tuminer, xxyy, "tuminer", parent_task_id)
                    task_create.append(
                        (redis_db, taskfile_dir,
                         tuminer_task_id, "exec", parent_task_id, tuminer_resource, service,
                         content_tuminer,
                         (), priority + 2,
                         ngpus_recommend, ncpus_recommend,
                         other_task_info))
                    task_ids.append("%s\t%s\tngpus: %d, ncpus: %d" % (
                        "tuminer", tuminer_task_id,
                        ngpus_recommend, ncpus_recommend))

            if task_type == TASK_RELEASE_TYPE:
                j = 0
                while j < len(content["docker"]["command"]) - 1:
                    if content["docker"]["command"][j] == "-m" \
                            or content["docker"]["command"][j] == "--model":
                        model_name = content["docker"]["command"][j + 1]
                        builtins.pn9model_db.model_set_release_state(model_name,
                                                                     content.get("trainer_id"),
                                                                     task_id,
                                                                     "in progress")
                        break
                    j = j + 1

        iterations -= 1
        if iterations > 0:
            parent_task_id = task_id
            change_parent_task(content["docker"]["command"], parent_task_id)

    (task_ids, task_create) = post_function('POST/task/launch', task_ids, task_create)

    for tc in task_create:
        task.create(*tc)

    if len(task_ids) == 1:
        task_ids = task_ids[0]

    create_model_catalog(task_id, request_data, content["docker"], entity_code, tag_ids)

    return flask.jsonify(task_ids)


def parse_request_data(request):
    validate_request_data(request)

    request_files = request.files
    request_data = request.form
    tags = request_data.get("tags", [])

    training_data = request_files.getlist("training_data")
    testing_data = request_files.getlist("testing_data")

    return {**request_data, **{"tags": json.loads(tags)}, **{
        "training_data": training_data,
        "testing_data": testing_data
    }}


def validate_request_data(request):
    request_files = request.files
    request_data = request.form

    training_data = request_files.getlist("training_data")
    testing_data = request_files.getlist("testing_data")

    # source = request_data.get("source")
    # target = request_data.get("target")
    tags = request_data.get("tags")
    model_name = request_data.get("model_name")
    # parent_model = request_data.get("parent_model")
    docker_image = request_data.get("docker_image")
    ncpus = request_data.get("ncpus")
    priority = request_data.get("priority")
    num_of_iteration = request_data.get("num_of_iteration")

    base_config = config.get_base_config(mongo_client)
    corpus_config = base_config.get("corpus")

    validate_tags(tags)
    validate_training_data(training_data, corpus_config)
    validate_testing_data(testing_data, corpus_config)
    validate_model_name(model_name)
    validate_docker_image(docker_image)
    validate_ncpus(ncpus)
    validate_priority(priority)
    validate_iteration(num_of_iteration)


def validate_tags(tags):
    try:
        if not tags:
            return
        tags_json = json.loads(tags)
        existed_tags = tags_json.get("existed", [])
        new_tags = tags_json.get("new", [])
        if not isinstance(existed_tags, list):
            raise Exception("new_tags tags must be array")
        if not isinstance(existed_tags, list):
            raise Exception("new tags must be array")
        for tag in existed_tags:
            if not is_valid_object_id(tag):
                raise Exception(f"Invalid id: {tag}")
        for tag in new_tags:
            print(f"Tag: {tag}")
            # TODO: Validate tag
    except Exception as e:
        raise Exception("Invalid tags json")


def validate_training_data(training_data, corpus_config):

    if not isinstance(training_data, list) or len(training_data) == 0:
        raise Exception("training data is required")
    for file in training_data:
        file_name = file.filename
        if not is_valid_corpus_extension(file_name, corpus_config):
            raise Exception(f"Invalid corpus extension: {file_name}")


def validate_testing_data(testing_data, corpus_config):
    if not isinstance(testing_data, list):
        raise Exception("testing data must be Array")
    if len(testing_data) == 0:
        return
    for file in testing_data:
        file_name = file.filename
        if not is_valid_corpus_extension(file_name, corpus_config):
            raise Exception(f"Invalid corpus extension: {file_name}")


def is_valid_corpus_extension(file_name, corpus_config):
    valid_extensions = [".tmx", ".txt"]
    if corpus_config:
        valid_extensions = corpus_config.get("extensions") or valid_extensions
    name, extension = os.path.splitext(file_name)
    return extension in valid_extensions


def validate_model_name(model_name):
    reg = r"(^[a-zA-Z0-9\.]+$)"
    if not re.match(reg, model_name):
        raise Exception("Invalid model_name")
    return


def validate_docker_image(docker_image):
    if docker_image is None:
        return
    # TODO: Validate image
    return


def validate_ncpus(ncpus):
    if ncpus is None:
        return
    if not ncpus.isnumeric():
        raise Exception('ncpus must be numeric')

    # TODO: Validate ncpus


def validate_priority(priority):
    if priority is None:
        return
    if not priority.isnumeric():
        raise Exception('priority must be numeric')

    # TODO: Validate priority


def validate_iteration(num_of_iteration):
    if num_of_iteration is None:
        return
    if not num_of_iteration.isnumeric():
        raise Exception('num_of_iteration must be numeric')

    # TODO: Validate num_of_iteration


def get_entity_storages(service_config, entity_code):
    if config.is_polyentity_config(service_config):
        entity_config = service_config["entities"][entity_code]
        return entity_config["storages"]
    return service_config["storages"]


def get_storage_client(storage_config):
    storage_client = StorageClient(rmprivate(storage_config))
    return storage_client


def upload_user_files(storage_client, storage_id, parent_path, data_type, files):
    path = f"{parent_path}/{data_type}/"
    temp_files = tempfile.mkdtemp()
    for file in files:
        file.save(os.path.join(temp_files, file.filename))
        storage_client.push(os.path.join(temp_files, file.filename), path, storage_id)


def get_to_translate_corpus(testing_data, uploaded_data_path, source, target, storage_id):
    result = []
    test_folder_name = get_test_folder_name(source, target)

    for corpus in testing_data:
        corpus_name = corpus.filename
        result.append([
            f'{storage_id}:{uploaded_data_path}/test/{corpus_name}.{source}',
            f'pn9_testtrans:<MODEL>/{test_folder_name}/{corpus_name}.{target}'
        ])

    return result


def get_to_score_corpus(testing_data, uploaded_data_path, source, target, storage_id):
    result = []
    test_folder_name = get_test_folder_name(source, target)
    for corpus in testing_data:
        corpus_name = corpus.filename
        result.append([
            f'pn9_testtrans:<MODEL>/{test_folder_name}/{corpus_name}.{target}',
            f'{storage_id}:{uploaded_data_path}/test/{corpus_name}.{target}'
        ])

    return result


def get_test_folder_name(source, target):
    return f'{source}_{target}' if source < target else f'{target}_{source}'


def get_training_data_config(uploaded_data_path, parent_model):
    training_data_config = {
        "sample": 50,
        "sample_dist": [{
            "path": "${GLOBAL_DATA}" + uploaded_data_path + "/train/",
            "distribution": [["*", 1]]
        }]
    }
    if not parent_model:
        return training_data_config

    parent_data_config = get_parent_model_training_data_config(parent_model)

    return merge_training_data_config(training_data_config, parent_data_config)


def get_parent_model_training_data_config(parent_model):
    # TODO: Get config from api
    return {
        "sample": 100,
        "sample_dist": [{
            "distribution": ["..."],
            "mode_strict": True,
            "path": "${SHARED_DATA_TRAIN_DIR}/xx_yy/train"
        },
            {
                "distribution": ["..."],
                "mode_strict": True,
                "path": "${SHARED_DATA_TRAIN_DIR}/xx_yy/train_restricted"
            },
            {
                "distribution": ["..."],
                "mode_strict": True,
                "path": "${SHARED_DATA_TRAIN_DIR}/xx_yy/train_synthetic"
            },
            {
                "distribution": ["..."],
                "mode_strict": True,
                "path": "${CLIENT_DATA_TRAIN_DIR}/xx_yy/train"
            }]
    }


def merge_training_data_config(training_data_config, parent_data_config):
    return {
        "sample": training_data_config["sample"] + parent_data_config["sample"],
        "sample_dist": training_data_config["sample_dist"] + parent_data_config["sample_dist"]
    }


def get_docker_image_info(service_module, entity_owner, docker_image):
    if not docker_image:
        return get_docker_image_from_db(service_module)
    return get_docker_image_from_request(service_module, entity_owner, docker_image)


def get_docker_image_from_db(service_module):
    image = "systran/pn9_tf"
    registry = _get_registry(service_module, image)
    tag = "v1.35.1"

    result = {
        "image": image,
        "tag": tag,
        "registry": registry
    }

    latest_docker_image_tag = get_latest_docker_image_tag(image)

    if not latest_docker_image_tag:
        return result
    return {**result, **{"tag": latest_docker_image_tag}}


def get_latest_docker_image_tag(image):
    docker_images = list(mongo_client.get_docker_images(image))
    if len(docker_images) == 0:
        return None
    only_tag_docker_images = list(map(lambda docker_image: get_docker_image_tag(docker_image["image"]), docker_images))
    only_tag_docker_images = list(filter(lambda tag: tag != "latest", only_tag_docker_images))
    if len(only_tag_docker_images) == 0:
        return None
    if len(only_tag_docker_images) == 1:
        return only_tag_docker_images[0]
    sorted_docker_image_tags = sorted(only_tag_docker_images, key=cmp_to_key(
        lambda x, y: semver.compare(x, y)), reverse=True)
    return sorted_docker_image_tags[0]


def get_docker_image_tag(image):
    split_name = image.split(":")
    if len(split_name) < 2:
        return None
    tag = split_name[-1]
    if not tag.startswith("v"):
        return tag
    return tag[1:]


def get_docker_image_from_request(service_module, entity_owner, docker_image):
    result = {**docker_image}
    registry = docker_image["registry"]
    if registry == "auto":
        result["registry"] = _get_registry(service_module, docker_image["image"])
        return result
    if registry not in service_module.get_docker_config(entity_owner)['registries']:
        raise Exception(f"Unknown docker registry: {registry}")
    return result


def get_training_config(service, request_data, user_code, service_module, entity_owner, uploaded_data_path, storage_id, tag_ids):
    model_name = request_data["model_name"]
    parent_model = request_data.get("parent_model", None)
    source = request_data["source"]
    target = request_data["target"]
    ncpus = request_data.get("ncpus", None)
    priority = request_data.get("priority", None)
    iterations = request_data.get("iterations", None)
    docker_image = request_data.get("docker_image", None)
    testing_data = request_data.get("testing_data", None)

    training_data_config = get_training_data_config(uploaded_data_path, parent_model)
    docker_image_info = get_docker_image_info(service_module, entity_owner, docker_image)
    to_translate_corpus = get_to_translate_corpus(testing_data, uploaded_data_path, source, target, storage_id)
    to_score_corpus = get_to_score_corpus(testing_data, uploaded_data_path, source, target, storage_id)

    docker_commands = ["-c", json.dumps({
        "tokenization": {
            "source": {
                "bpe_model": "${SHARED_DATA_DIR}/en_fr/vocab/joint-vocab34k.en_fr",
                "vocabulary": "${SHARED_DATA_DIR}/en_fr/vocab/vocab32k.en",
                "preserve_placeholders": True,
                "mode": "aggressive",
                "preserve_segmented_tokens": True,
                "segment_numbers": True,
                "segment_case": True,
                "joiner_annotate": True
            },
            "target": {
                "bpe_model": "${SHARED_DATA_DIR}/en_fr/vocab/joint-vocab34k.en_fr",
                "vocabulary": "${SHARED_DATA_DIR}/en_fr/vocab/vocab34k.fr",
                "preserve_placeholders": True,
                "mode": "aggressive",
                "preserve_segmented_tokens": True,
                "segment_numbers": True,
                "segment_case": True,
                "joiner_annotate": True
            }
        },
        "description": "apostrophe test",
        "source": source,
        "target": target,
        "data": training_data_config,
        "options": {
            "model_type": "Transformer",
            "config": {
                "train": {
                    "maximum_labels_length": 100,
                    "save_summary_steps": 100,
                    "keep_checkpoint_max": 8,
                    "bucket_width": 1,
                    "maximum_features_length": 100,
                    "batch_size": 9000,
                    "batch_type": "tokens",
                    "sample_buffer_size": 5000000,
                    "save_checkpoints_steps": 5000,
                    "single_pass": True
                },
                "params": {
                    "optimizer": "LazyAdamOptimizer",
                    "decay_steps": 8000,
                    "length_penalty": 0.6,
                    "decay_step_duration": 1,
                    "learning_rate": 2,
                    "average_loss_in_time": True,
                    "decay_rate": 512,
                    "decay_type": "noam_decay",
                    "label_smoothing": 0.1,
                    "beam_width": 5
                },
                "infer": {
                    "batch_size": 32
                }
            }
        }
    }), "train"]

    content = {
        "service": service,
        "name": model_name,
        "docker": {**docker_image_info, **{
            "command": docker_commands
        }},
        "wait_after_launch": 2,
        "trainer_id": f"{entity_owner}{user_code}",
        "options": {},
        "totranslate": to_translate_corpus,
        "toscore": to_score_corpus
    }

    if ncpus:
        content["ncpus"] = ncpus
    if priority:
        content["priority"] = priority
    if iterations:
        content["iterations"] = iterations
    return json.loads(json.dumps(content))


# def build_preprocess_task(content, ):
#     prepr_task_id, explicitname = build_task_id(content, xxyy, "prepr", parent_task_id)
#
#     if explicitname:
#         patch_config_explicitname(content, explicitname)
#
#     idx = 0
#     prepr_command = []
#     train_command = content["docker"]["command"]
#     while train_command[idx] != 'train' and train_command[idx] != 'preprocess':
#         prepr_command.append(train_command[idx])
#         idx += 1
#
#     # create preprocess command, don't push the model on the catalog,
#     # and generate a pseudo model
#     prepr_command.append("--no_push")
#     prepr_command.append("preprocess")
#     prepr_command.append("--build_model")
#
#     content["docker"]["command"] = prepr_command
#
#     content["ncpus"] = ncpus or get_cpu_count(service_config, 0, "preprocess")
#     content["ngpus"] = 0
#
#     preprocess_resource = service_module.select_resource_from_capacity(
#         resource, Capacity(content["ngpus"], content["ncpus"]))
#
#     # launch preprocess task on cpus only
#     task_create.append(
#         (redis_db, taskfile_dir,
#          prepr_task_id, "prepr", parent_task_id, preprocess_resource, service,
#          _duplicate_adapt(service_module, content),
#          files, priority, 0, content["ncpus"], other_task_info))
#     task_ids.append(
#         "%s\t%s\tngpus: %d, ncpus: %d" % ("prepr", prepr_task_id, 0, content["ncpus"]))
#     remove_config_option(train_command)
#     change_parent_task(train_command, prepr_task_id)
#     parent_task_id = prepr_task_id
#     content["docker"]["command"] = train_command


def is_valid_object_id(value):
    return ObjectId.is_valid(value)


def process_tags(tags, entity_code, trainer_id):
    final_tags = []
    existed_tags = tags.get("existed", [])
    new_tags = tags.get("new", [])

    tag_ids = list(map(lambda tag: ObjectId(tag), existed_tags))
    exists_tags_by_id = list(mongo_client.get_tags_by_ids(tag_ids))
    exists_tags_by_value = list(mongo_client.get_tags_by_value(new_tags))
    not_exists_tags = list(
        filter(lambda tag: tag not in list(map(lambda exists_tag: str(exists_tag["tag"]), exists_tags_by_value)),
               new_tags))

    if len(not_exists_tags) > 0:
        insert_tags = list(map(lambda tag: {
            "entity": entity_code,
            "creator": trainer_id,
            "tag": tag
        }, not_exists_tags))

        mongo_client.tags_put(insert_tags)

        inserted_tags = list(mongo_client.get_tags_by_value(not_exists_tags))

        final_tags.extend(list(map(lambda exists_tag: exists_tag["_id"], inserted_tags)))

    final_tags.extend(list(map(lambda exists_tag: exists_tag["_id"], exists_tags_by_id)))
    final_tags.extend(list(map(lambda exists_tag: exists_tag["_id"], exists_tags_by_value)))

    return final_tags


def create_model_catalog(training_task_id, request_data, docker_info, entity_owner, tag_ids, state="creating"):
    source = request_data.get("source")
    target = request_data.get("target")
    parent_model = request_data.get("parent_model")

    config = {
        "source": source,
        "target": target,
        "parent_model": parent_model,
        "imageTag": f'{docker_info["image"]}:{docker_info["tag"]}',
        "tags": tag_ids
    }

    return builtins.pn9model_db.catalog_declare(training_task_id, config,
                                                entity_owner=entity_owner,
                                                lp=None, state=state)


@app.route("/task/launch/<string:service>", methods=["POST"])
@filter_request("POST/task/launch", "train")
def launch(service):
    service_config = config.get_service_config(mongo_client, service)
    pool_entities = config.get_entities(service_config)
    if all(not has_ability(flask.g, "train", entity) for entity in pool_entities):
        abort(make_response(jsonify(message="insufficient credentials for train (entity %s)" % service), 403))

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

    exec_mode = content.get('exec_mode', False)

    if not exec_mode:
        task_type = '????'
        if "train" in content["docker"]["command"]:
            task_type = "train"
        elif "trans" in content["docker"]["command"]:
            task_type = "trans"
        elif "preprocess" in content["docker"]["command"]:
            task_type = "prepr"
        elif "release" in content["docker"]["command"]:
            task_type = TASK_RELEASE_TYPE
        elif "buildvocab" in content["docker"]["command"]:
            task_type = "vocab"
    else:
        task_type = 'exec'

    if task_type == '????':
        abort(flask.make_response(flask.jsonify(message="incorrect task definition"), 400))

    elif task_type != "exec":
        task_suffix = task_type
    else:
        task_suffix = get_docker_action(content["docker"]["command"])
        if task_suffix is None:
            task_suffix = task_type

    service_entities = config.get_entities(service_config)
    entity_owner = get_entity_owner(service_entities, service)
    trainer_entities = get_entities_by_permission("train", flask.g)
    assert trainer_entities  # Here: almost sure you are trainer
    other_task_info = {TaskInfo.ENTITY_OWNER.value: entity_owner,
                       TaskInfo.STORAGE_ENTITIES.value: json.dumps(trainer_entities)}

    # Sanity check on content.
    if 'options' not in content or not isinstance(content['options'], dict):
        abort(flask.make_response(flask.jsonify(message="invalid options field"), 400))
    if 'docker' not in content:
        abort(flask.make_response(flask.jsonify(message="missing docker field"), 400))
    if ('image' not in content['docker'] or 'registry' not in content['docker'] or
            'tag' not in content['docker'] or 'command' not in content['docker']):
        abort(flask.make_response(flask.jsonify(message="incomplete docker field"), 400))
    if content['docker']['registry'] == 'auto':
        content['docker']['registry'] = _get_registry(service_module, content['docker']['image'])
    elif content['docker']['registry'] not in service_module.get_docker_config(entity_owner)['registries']:
        abort(flask.make_response(flask.jsonify(message="unknown docker registry"), 400))

    resource = service_module.get_resource_from_options(content["options"])

    iterations = 1
    if "iterations" in content:
        iterations = content["iterations"]
        if exec_mode:
            abort(flask.make_response(flask.jsonify(message="chain mode unavailable in exec mode"),
                                      400))
        if (task_type != "train" and iterations != 1) or iterations < 1:
            abort(flask.make_response(flask.jsonify(message="invalid value for iterations"), 400))

    ngpus = 1
    if "ngpus" in content:
        ngpus = content["ngpus"]
    ncpus = content.get("ncpus")

    # check that we have a resource able to run such a request
    if not _find_compatible_resource(service_module, ngpus, ncpus, resource):
        abort(flask.make_response(
            flask.jsonify(message="no resource available on %s for %d gpus (%s cpus)" % (
                service, ngpus, ncpus and str(ncpus) or "-")), 400))

    if "totranslate" in content:
        if exec_mode:
            abort(flask.make_response(
                flask.jsonify(message="translate mode unavailable for exec cmd"), 400))
        totranslate = content["totranslate"]
        del content["totranslate"]
    else:
        totranslate = None
    if "toscore" in content:
        if exec_mode:
            abort(flask.make_response(flask.jsonify(message="score mode unavailable for exec cmd"),
                                      400))
        toscore = content["toscore"]
        del content["toscore"]
    else:
        toscore = None
    if "totuminer" in content:
        if exec_mode:
            abort(flask.make_response(
                flask.jsonify(message="tuminer chain mode unavailable for exec cmd"), 400))
        totuminer = content["totuminer"]
        del content["totuminer"]
    else:
        totuminer = None

    docker_version = content['docker']['tag']
    if docker_version.startswith('v'):
        docker_version = docker_version[1:]
    try:
        chain_prepr_train = (not exec_mode and not content.get("nochainprepr", False) and
                             task_type == "train" and
                             semver.match(docker_version, ">=1.4.0"))
        can_trans_as_release = semver.match(docker_version, ">=1.8.0")
        trans_as_release = (not exec_mode and not content.get("notransasrelease", False) and
                            semver.match(docker_version, ">=1.8.0"))
        content["support_statistics"] = semver.match(docker_version, ">=1.17.0")
    except ValueError as err:
        # could not match docker_version - not valid semver
        chain_prepr_train = False
        trans_as_release = False

    priority = content.get("priority", 0)

    (xxyy, parent_task_id) = shallow_command_analysis(content["docker"]["command"])
    parent_struct = None
    parent_task_type = None
    if not exec_mode and parent_task_id:
        (parent_struct, parent_task_type) = model_name_analysis(parent_task_id)

    # check that parent model type matches current command
    if parent_task_type:
        if (parent_task_type == "trans" or parent_task_type == "relea" or
                (task_type == "prepr" and parent_task_type != "train" and parent_task_type != "vocab")):
            abort(flask.make_response(flask.jsonify(message="invalid parent task type: %s" % parent_task_type), 400))

    task_ids = []
    task_create = []

    while iterations > 0:
        if (chain_prepr_train and parent_task_type != "prepr") or task_type == "prepr":
            prepr_task_id, explicitname = build_task_id(content, xxyy, "prepr", parent_task_id)

            if explicitname:
                patch_config_explicitname(content, explicitname)

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

            content["ncpus"] = ncpus or get_cpu_count(service_config, 0, "preprocess")
            content["ngpus"] = 0

            preprocess_resource = service_module.select_resource_from_capacity(
                resource, Capacity(content["ngpus"], content["ncpus"]))

            # launch preprocess task on cpus only
            task_create.append(
                (redis_db, taskfile_dir,
                 prepr_task_id, "prepr", parent_task_id, preprocess_resource, service,
                 _duplicate_adapt(service_module, content),
                 files, priority, 0, content["ncpus"], other_task_info))
            task_ids.append(
                "%s\t%s\tngpus: %d, ncpus: %d" % ("prepr", prepr_task_id, 0, content["ncpus"]))
            remove_config_option(train_command)
            change_parent_task(train_command, prepr_task_id)
            parent_task_id = prepr_task_id
            content["docker"]["command"] = train_command

        if task_type != "prepr":

            task_id, explicitname = build_task_id(content, xxyy, task_suffix, parent_task_id)

            if explicitname:
                patch_config_explicitname(content, explicitname)

            file_to_transtaskid = {}
            if task_type == "trans":
                try:
                    idx = content["docker"]["command"].index("trans")
                    output_files = get_params(("-o", "--output"), content["docker"]["command"][idx + 1:])
                    for ofile in output_files:
                        file_to_transtaskid[ofile] = task_id
                except Exception:
                    pass

            content["ncpus"] = ncpus or \
                               get_cpu_count(service_config, ngpus, task_type)
            content["ngpus"] = ngpus

            if task_type == "trans" and can_trans_as_release:
                if "--as_release" not in content["docker"]["command"] and trans_as_release:
                    content["docker"]["command"].append("--as_release")
                    content["ngpus"] = ngpus = 0

            task_resource = service_module.select_resource_from_capacity(
                resource, Capacity(content["ngpus"],
                                   content["ncpus"]))

            task_create.append(
                (redis_db, taskfile_dir,
                 task_id, task_type, parent_task_id, task_resource, service,
                 _duplicate_adapt(service_module, content),
                 files, priority,
                 content["ngpus"], content["ncpus"],
                 other_task_info))
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

                content_translate["ncpus"] = ncpus or get_cpu_count(service_config,
                                                                    content_translate["ngpus"],
                                                                    "trans")

                translate_resource = service_module.select_resource_from_capacity(
                    resource, Capacity(content_translate["ngpus"],
                                       content_translate["ncpus"]))

                if ngpus == 0 or trans_as_release:
                    file_per_gpu = len(totranslate)
                else:
                    file_per_gpu = int((len(totranslate) + ngpus - 1) / ngpus)
                subset_idx = 0
                while subset_idx * file_per_gpu < len(totranslate):
                    content_translate["docker"]["command"] = ["trans"]
                    if trans_as_release:
                        content_translate["docker"]["command"].append("--as_release")
                    content_translate["docker"]["command"].append('-i')
                    subset_totranslate = totranslate[subset_idx * file_per_gpu:
                                                     (subset_idx + 1) * file_per_gpu]
                    for f in subset_totranslate:
                        content_translate["docker"]["command"].append(f[0])

                    change_parent_task(content_translate["docker"]["command"], task_id)
                    trans_task_id, explicitname = build_task_id(content_translate, xxyy, "trans",
                                                                task_id)

                    content_translate["docker"]["command"].append('-o')
                    for f in subset_totranslate:
                        ofile = f[1].replace('<MODEL>', task_id)
                        file_to_transtaskid[ofile] = trans_task_id
                        content_translate["docker"]["command"].append(ofile)

                    task_create.append(
                        (redis_db, taskfile_dir,
                         trans_task_id, "trans", task_id, translate_resource, service,
                         _duplicate_adapt(service_module, content_translate),
                         (), content_translate["priority"],
                         content_translate["ngpus"], content_translate["ncpus"],
                         other_task_info))
                    task_ids.append("%s\t%s\tngpus: %d, ncpus: %d" % (
                        "trans", trans_task_id,
                        content_translate["ngpus"], content_translate["ncpus"]))
                    subset_idx += 1

            if toscore:
                toscore_parent = {}
                for (ofile, rfile) in toscore:
                    ofile = ofile.replace('<MODEL>', task_id)
                    parent_task_id = file_to_transtaskid.get(ofile)
                    if parent_task_id:
                        if parent_task_id not in toscore_parent:
                            toscore_parent[parent_task_id] = {"output": [], "ref": []}
                        ofile_split = ofile.split(':')
                        if len(ofile_split) == 2 and ofile_split[0] == 'launcher':
                            ofile = 'launcher:../' + parent_task_id + "/" + ofile_split[1]
                        toscore_parent[parent_task_id]["output"].append(ofile)
                        toscore_parent[parent_task_id]["ref"].append(rfile)
                for parent_task_id, oref in six.iteritems(toscore_parent):
                    content_score = deepcopy(content)
                    content_score["priority"] = priority + 1
                    content_score["ngpus"] = 0
                    content_score["ncpus"] = 1

                    score_resource = service_module.select_resource_from_capacity(resource,
                                                                                  Capacity(0, 1))

                    image_score = "nmtwizard/score"

                    option_lang = []
                    if parent_struct is not None:
                        option_lang.append('-l')
                        option_lang.append(parent_struct['xxyy'][-2:])

                    content_score["docker"] = {
                        "image": image_score,
                        "registry": _get_registry(service_module, image_score),
                        "tag": "latest",
                        "command": ["score", "-o"] + oref["output"] + ["-r"] + oref["ref"] + option_lang + ['-f',
                                                                                                            "launcher:scores"]
                    }

                    score_task_id, explicitname = build_task_id(content_score, xxyy, "score",
                                                                parent_task_id)
                    task_create.append(
                        (redis_db, taskfile_dir,
                         score_task_id, "exec", parent_task_id, score_resource, service,
                         content_score,
                         files, priority + 2,
                         0, 1,
                         other_task_info))
                    task_ids.append("%s\t%s\tngpus: %d, ncpus: %d" % (
                        "score", score_task_id,
                        0, 1))

            if totuminer:
                # tuminer can run in CPU only mode, but it will be very slow for large data
                ngpus_recommend = ngpus
                ncpus_recommend = ncpus or get_cpu_count(service_config, 0, "tuminer")

                totuminer_parent = {}
                for (ifile, ofile) in totuminer:
                    # ofile = ofile.replace('<MODEL>', task_id)
                    parent_task_id = file_to_transtaskid.get(ofile)
                    if parent_task_id:
                        if parent_task_id not in totuminer_parent:
                            totuminer_parent[parent_task_id] = {"infile": [], "outfile": [],
                                                                "scorefile": []}
                        ofile_split = ofile.split(':')
                        if len(ofile_split) == 2 and ofile_split[0] == 'launcher':
                            ofile = 'launcher:../' + parent_task_id + "/" + ofile_split[1]
                        totuminer_parent[parent_task_id]["infile"].append(ifile)
                        totuminer_parent[parent_task_id]["outfile"].append(ofile)
                        scorefile = ofile
                        if scorefile.endswith(".gz"):
                            scorefile = scorefile[:-3]
                        totuminer_parent[parent_task_id]["scorefile"].append(scorefile[:-3])
                for parent_task_id, in_out in six.iteritems(totuminer_parent):
                    content_tuminer = deepcopy(content)
                    content_tuminer["priority"] = priority + 1
                    content_tuminer["ngpus"] = ngpus_recommend
                    content_tuminer["ncpus"] = ncpus_recommend

                    tuminer_resource = service_module.select_resource_from_capacity(resource,
                                                                                    Capacity(
                                                                                        ngpus_recommend,
                                                                                        ncpus_recommend))

                    image_score = "nmtwizard/tuminer"

                    content_tuminer["docker"] = {
                        "image": image_score,
                        "registry": _get_registry(service_module, image_score),
                        "tag": "latest",
                        "command": ["tuminer", "--tumode", "score", "--srcfile"] + in_out["infile"] + ["--tgtfile"] +
                                   in_out["outfile"] + ["--output"] + in_out["scorefile"]
                    }

                    tuminer_task_id, explicitname = build_task_id(content_tuminer, xxyy, "tuminer", parent_task_id)
                    task_create.append(
                        (redis_db, taskfile_dir,
                         tuminer_task_id, "exec", parent_task_id, tuminer_resource, service,
                         content_tuminer,
                         (), priority + 2,
                         ngpus_recommend, ncpus_recommend,
                         other_task_info))
                    task_ids.append("%s\t%s\tngpus: %d, ncpus: %d" % (
                        "tuminer", tuminer_task_id,
                        ngpus_recommend, ncpus_recommend))

            if task_type == TASK_RELEASE_TYPE:
                j = 0
                while j < len(content["docker"]["command"]) - 1:
                    if content["docker"]["command"][j] == "-m" \
                            or content["docker"]["command"][j] == "--model":
                        model_name = content["docker"]["command"][j + 1]
                        builtins.pn9model_db.model_set_release_state(model_name,
                                                                     content.get("trainer_id"),
                                                                     task_id,
                                                                     "in progress")
                        break
                    j = j + 1

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
def status(task_id):
    task_readonly_control(task_id)

    fields = flask.request.args.get('fields', None)
    if fields is not None and fields != '':
        fields = fields.split(',')
    else:
        fields = None

    response = task.info(redis_db, taskfile_dir, task_id, fields)
    if response.get("alloc_lgpu"):
        response["alloc_lgpu"] = response["alloc_lgpu"].split(",")
    if response.get("alloc_lcpu"):
        response["alloc_lcpu"] = response["alloc_lcpu"].split(",")
    return flask.jsonify(response)


@app.route("/task/<string:task_id>", methods=["DELETE"])
@filter_request("DELETE/task")
@task_write_control
def del_task(task_id):
    response = task.delete(redis_db, taskfile_dir, task_id)
    if isinstance(response, list) and not response[0]:
        abort(flask.make_response(flask.jsonify(message=response[1]), 400))
    return flask.jsonify(message="deleted %s" % task_id)


def to_regex_format(pattern):
    if not pattern:
        return pattern

    regex_expression = pattern.replace("*", ".*")  # staring by sth. Ex: *B
    if len(pattern) > 1 and pattern[-1:] != "*":  # ending by sth. Ex: A*
        regex_expression += "$"

    return regex_expression


def is_regex_matched(pattern, regex_filter_expression):
    is_matched = isinstance(pattern, six.string_types) and re.match(regex_filter_expression,
                                                                    pattern) is not None
    return is_matched


@app.route("/task/list/<string:pattern>", methods=["GET"])
@filter_request("GET/task/list")
def list_tasks(pattern):
    """
    Goal: return tasks list based on prefix/pattern
    Arguments:
        pattern: if not empty, the first two characters will be used to search the entity.
    """
    with_parent = boolean_param(flask.request.args.get('with_parent'))
    service_filter = flask.request.args.get('service')
    status_filter = flask.request.args.get('status')

    ltask = []
    prefix = "*" if pattern == '-*' else pattern

    suffix = ''
    if prefix.endswith('*'):
        prefix = prefix[:-1]
        suffix = '*'

    task_where_clauses = []
    if has_ability(flask.g, '',
                   ''):  # super admin so no control on the prefix of searching criteria
        task_where_clauses.append(prefix)
    else:
        search_entity_expression = to_regex_format(prefix[:2])  # empty == all entities
        search_user_expression = prefix[2:5]
        search_remaining_expression = prefix[5:]

        filtered_entities = [ent for ent in flask.g.entities if
                             is_regex_matched(ent, search_entity_expression)]

        for entity in filtered_entities:
            if has_ability(flask.g, 'train', entity):
                task_where_clauses.append(
                    entity + search_user_expression + search_remaining_expression)
            else:
                continue

        if not task_where_clauses:
            abort(make_response(jsonify(message="insufficient credentials for tasks %s" % pattern),
                                403))

    for clause in task_where_clauses:
        for task_key in task.scan_iter(redis_db, clause + suffix):
            task_id = task.id(task_key)
            info = task.info(
                redis_db, taskfile_dir, task_id,
                ["launched_time", "alloc_resource", "alloc_lgpu", "alloc_lcpu", "resource",
                 "content",
                 "status", "message", "type", "iterations", "priority", "service", "parent", 'owner'])

            if (service_filter and info["service"] != service_filter) \
                    or (status_filter and info["status"] != status_filter):
                continue

            if info["alloc_lgpu"]:
                info["alloc_lgpu"] = info["alloc_lgpu"].split(",")
            if info["alloc_lcpu"]:
                info["alloc_lcpu"] = info["alloc_lcpu"].split(",")
            info["image"] = '-'
            info["model"] = '-'

            if not info["service"]:
                info["service"] = ""
            if with_parent and not info["parent"]:
                info["parent"] = ""

            if info["content"]:
                content = json.loads(info["content"])
                info["image"] = content["docker"]["image"] + ':' + content["docker"]["tag"]
                j = 0
                while j < len(content["docker"]["command"]) - 1:
                    if content["docker"]["command"][j] == "-m" \
                            or content["docker"]["command"][j] == "--model":
                        info["model"] = content["docker"]["command"][j + 1]
                        break
                    j = j + 1
                del info['content']
            info['task_id'] = task_id

            # bc the parent could make the response more heavy for a http transport.
            if not with_parent:
                del info["parent"]

            ltask.append(info)
    return flask.jsonify(ltask)


@app.route("/task/terminate/<string:task_id>", methods=["GET"])
@filter_request("GET/task/terminate")
@task_write_control
def terminate(task_id):
    with redis_db.acquire_lock(task_id):
        current_status = task.info(redis_db, taskfile_dir, task_id, "status")
        if current_status is None:
            abort(flask.make_response(flask.jsonify(message="task %s unknown" % task_id), 404))
        elif current_status == "stopped":
            return flask.jsonify(message="%s already stopped" % task_id)
        phase = flask.request.args.get('phase')

    res = post_function('GET/task/terminate', task_id, phase)
    if res:
        task.terminate(redis_db, task_id, phase="publish_error")
        return flask.jsonify(message="problem while posting model: %s" % res)

    task.terminate(redis_db, task_id, phase=phase)
    return flask.jsonify(message="terminating %s" % task_id)


@app.route("/task/beat/<string:task_id>", methods=["PUT", "GET"])
@filter_request("PUT/task/beat")
@task_write_control
def task_beat(task_id):
    duration = flask.request.args.get('duration')
    try:
        if duration is not None:
            duration = int(duration)
    except ValueError:
        abort(flask.make_response(flask.jsonify(message="invalid duration value"), 400))
    container_id = flask.request.args.get('container_id')
    try:
        task.beat(redis_db, task_id, duration, container_id)
    except Exception as e:
        abort(flask.make_response(flask.jsonify(message=str(e)), 400))
    return flask.jsonify(200)


@app.route("/file/<string:task_id>/<path:filename>", methods=["GET"])
@app.route("/task/file/<string:task_id>/<path:filename>", methods=["GET"])
@task_request
def get_file(task_id, filename):
    content = task.get_file(redis_db, taskfile_dir, task_id, filename)
    if content is None:
        abort(flask.make_response(
            flask.jsonify(message="cannot find file %s for task %s" % (filename, task_id)), 404))
    # https://www.pythonanywhere.com/forums/topic/13570/
    w = FileWrapper(io.BytesIO(content))
    return Response(w, mimetype="application/octet-stream", direct_passthrough=True,
                    headers={'Content-Disposition': 'attachment; filename="{}"'.format(filename)})


@app.route("/file/<string:task_id>/<path:filename>", methods=["POST"])
@app.route("/task/file/<string:task_id>/<path:filename>", methods=["POST"])
@filter_request("POST/task/file")
@task_request
def post_file(task_id, filename):
    content = flask.request.get_data()
    task.set_file(redis_db, taskfile_dir, task_id, content, filename)
    return flask.jsonify(200)


@app.route("/task/log/<string:task_id>", methods=["GET"])
@filter_request("GET/task/log")
def get_log(task_id):
    task_readonly_control(task_id)

    content = task.get_log(redis_db, taskfile_dir, task_id)

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
    task.append_log(redis_db, taskfile_dir, task_id, content, max_log_size)
    duration = flask.request.args.get('duration')
    try:
        if duration is not None:
            duration = int(duration)
    except ValueError:
        abort(flask.make_response(flask.jsonify(message="invalid duration value"), 400))
    try:
        task.beat(redis_db, task_id, duration, None)
    except Exception as e:
        abort(flask.make_response(flask.jsonify(message=str(e)), 400))
    return flask.jsonify(200)


@app.route("/task/log/<string:task_id>", methods=["POST"])
@filter_request("POST/task/log")
@task_request
def post_log(task_id):
    content = flask.request.get_data()
    content = task.set_log(redis_db, taskfile_dir, task_id, content, max_log_size)
    (task_id, content) = post_function('POST/task/log', task_id, content)
    return flask.jsonify(200)


@app.route("/task/stat/<string:task_id>", methods=["POST"])
@filter_request("POST/task/stat")
@task_request
def post_stat(task_id):
    stats = flask.request.get_json()

    if stats is None:
        abort(flask.make_response(flask.jsonify(message="statistics empty"), 400))

    task_id_check = stats.get('task_id')
    if task_id_check != task_id:
        abort(flask.make_response(flask.jsonify(message="incorrect task_id"), 400))
    start_time = float(stats.get('start_time'))
    end_time = float(stats.get('end_time'))
    statistics = stats.get('statistics')
    task.set_stat(redis_db, task_id, end_time - start_time, statistics)
    return flask.jsonify(200)


@app.route("/status", methods=["GET"])
def get_status():
    version = get_version()
    launcher_version = version.split(":")[1]
    result = {
        "status": "running",
        "name": "Launcher",
        "version": launcher_version
    }
    return flask.jsonify(result)


@app.route("/version", methods=["GET"])
def get_version_request():
    return flask.make_response(get_version())


def get_worker_pids(service_name):
    worker_pids = []
    for keyw in redis_db.scan_iter("admin:worker:%s:*" % service_name):
        worker_pids.append(keyw[len("admin:worker:%s:" % service_name):])
    return worker_pids
