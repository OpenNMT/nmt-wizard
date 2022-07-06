import builtins
import io
import json
import logging
import os
import re
import tempfile
import time
import traceback
import urllib.parse
from collections import Counter
from copy import deepcopy
from functools import wraps

import flask
import semver
import six
from bson import ObjectId
from werkzeug.exceptions import HTTPException
from werkzeug.wsgi import FileWrapper
from flask import abort, make_response, jsonify, Response, request, g
from app import app, redis_db, mongo_client, get_version, taskfile_dir
from nmtwizard import task, configuration as nmtwizard_config
from nmtwizard.capacity import Capacity
from nmtwizard.helper import build_task_id, shallow_command_analysis, get_docker_action, get_registry, cust_jsondump, \
    get_cpu_count, get_params, boolean_param, change_parent_task, remove_config_option, model_name_analysis
# only for launch() maybe deprecated
from nmtwizard.task import TaskBase
from nmtwizard.task import TaskEnum, TaskInfos, TasksCreationInfos, TaskPreprocess, TaskTrain, TaskTranslate, \
    TaskScoring, TASK_RELEASE_TYPE
from utils.common_utils import is_resource_train_restricted, check_permission_access_train_restricted
from utils.storage_utils import StorageUtils

GLOBAL_POOL_NAME = "global_pool"
SYSTRAN_BASE_STORAGE = "shared_testdata"
SYSTRAN = "SA"

logger = logging.getLogger(__name__)
logger.addHandler(app.logger)
# get maximum log size from configuration
max_log_size = app.get_other_config(['default', 'max_log_size'], fallback=None)
if max_log_size is not None:
    max_log_size = int(max_log_size)

CORPUS_TYPE = {
    "USER_UPLOAD": 1,
    "EXISTS_CORPUS": 2
}


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
    def get_entities(names):
        entities = set({})
        for storage in names:
            entity, _ = StorageId.decode_storage_name(storage)
            if entity and entity != nmtwizard_config.CONFIG_DEFAULT:
                entities.add(entity)
        return entities


class RoutesConfiguration:
    def __init__(self, flask_global, service):
        self._service = service
        user = flask_global.user
        self.creator = {
            'user_id': user.id,
            'first_name': user.first_name,
            'last_name': user.last_name,
            'entity_id': user.entity.id,
            'entity_code': user.entity.entity_code,
            'user_code': user.user_code
        }
        self.service_config = nmtwizard_config.get_service_config(mongo_client, service_name=GLOBAL_POOL_NAME)
        self.entity_storages_config = self._get_entity_storages(self.creator['entity_code'])
        self.storage_client, self.global_storage_name = StorageUtils.get_storages(GLOBAL_POOL_NAME,
                                                                                  mongo_client,
                                                                                  redis_db,
                                                                                  has_ability,
                                                                                  g)

        if self._service is not GLOBAL_POOL_NAME:
            self.service_config = nmtwizard_config.get_service_config(mongo_client, self._service)
        self.service_module = get_service(service)
        self.service_entities = nmtwizard_config.get_entities(self.service_config)
        self.entity_owner = self._get_entity_owner()
        self.trainer_entities = RoutesConfiguration.get_entities_by_permission("train")
        assert self.trainer_entities  # Here: almost sure you are trainer

    def _get_entity_storages(self, entity_code):
        if nmtwizard_config.is_polyentity_config(self.service_config):
            entity_config = self.service_config["entities"][entity_code]
            return entity_config["storages"]
        return self.service_config["storages"]

    def _get_entity_owner(self):
        return RoutesConfiguration.get_entity_owner(self.service_entities, self._service)

    @staticmethod
    def get_entity_owner(service_entities, service_name):
        trainer_of_entities = RoutesConfiguration.get_entities_by_permission("train")

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
                message="model owner is ambiguous between these entities: (%s)" % str(
                    ",".join(trainer_of_entities))), 400))
        entity_owner = entity_owner.upper()

        if not has_ability(flask.g, 'train', entity_owner):
            abort(flask.make_response(flask.jsonify(message="you are not a trainer of %s" % entity_owner), 403))
        elif entity_owner not in service_entities:
            abort(flask.make_response(flask.jsonify(
                message="This service '%s' is not reserved to launch the task of the entity %s" % (
                    service_name, entity_owner)), 400))

        return entity_owner

    @staticmethod
    def get_entities_by_permission(the_permission):
        return [ent_code for ent_code in flask.g.entities if
                isinstance(ent_code, str) and has_ability(flask.g, the_permission, ent_code)]


def check_permission(service, permission):
    services = redis_db.smembers("admin:services")
    if service not in services:
        abort(make_response(jsonify(message="insufficient credentials for edit_config on this service %s" % service),
                            403))
    is_poly_entity = nmtwizard_config.is_polyentity_service(mongo_client, service)
    if is_poly_entity and not has_ability(flask.g, permission, ""):  # super admin
        abort(make_response(jsonify(message="insufficient credentials for edit_config on this service %s" % service),
                            403))
    elif not is_poly_entity:
        pool_entity = service[0:2].upper()
        if not has_ability(flask.g, "edit_config", pool_entity):
            abort(make_response(jsonify(message="insufficient credentials for edit_config (entity %s)" % pool_entity),
                                403))


@app.errorhandler(Exception)
def handle_error(e):
    # return a nice message when any exception occurred, keeping the original Http error
    # https://stackoverflow.com/questions/29332056/global-error-handler-for-any-exception
    if 'user' in flask.g:
        app.logger.error("User:'%s'" % flask.g.user.user_code)

    app.logger.error(traceback.format_exc())

    code = 500
    if isinstance(e, HTTPException):
        code = e.code
    return jsonify(error=str(e)), code


def cust_jsonify(obj):
    result = cust_jsondump(obj)
    return Response(result, mimetype='application/json')


def get_service(service):
    """Wrapper to fail on invalid service."""
    service_config = nmtwizard_config.get_service_config(mongo_client, service)
    base_config = nmtwizard_config.get_base_config(mongo_client)
    services, _ = nmtwizard_config.load_service_config(service_config, base_config)

    return services[service]


def _duplicate_adapt(content):
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


def _usage_capacity(service):
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

        detail[resource]['usage'] = ["%s %s: %d (%d)" % (value, t, count_map_gpu[t],
                                                         count_map_cpu[t]) for t, value in task_type.items()]
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


def task_request(func):
    """minimal check on the request to check that tasks exists"""

    @wraps(func)
    def func_wrapper(*args, **kwargs):
        if not task.exists(redis_db, kwargs['task_id']):
            abort(flask.make_response(flask.jsonify(message="task %s unknown" % kwargs['task_id']),
                                      404))
        return func(*args, **kwargs)

    return func_wrapper


def get_task_entity(task_id):
    if task_id is None:
        abort(make_response(jsonify(message="task empty"), 404))

    if not task.exists(redis_db, task_id):
        abort(make_response(jsonify(message="task %s unknown" % task_id), 404))

    entity = task_id[:2] if task_id else ""
    return entity


def task_write_control(func):
    """minimal check on the request to check that tasks exists"""

    @wraps(func)
    def func_wrapper(*args, **kwargs):
        task_id = kwargs['task_id']
        entity = get_task_entity(task_id)

        ok = has_ability(flask.g, 'admin_task', entity) or (has_ability(flask.g, 'train', entity) and
                                                            flask.g.user.user_code == task_id[2:5]) or has_ability(
            flask.g, 'view_all_tasks', SYSTRAN)
        if not ok:
            abort(make_response(jsonify(message="insufficient credentials for tasks %s" % task_id),
                                403))

        return func(*args, **kwargs)

    return func_wrapper


def task_readonly_control(task_id):
    entity = get_task_entity(task_id)
    if not has_ability(flask.g, 'train', entity) and not has_ability(flask.g, 'view_all_tasks', SYSTRAN):
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
            if filter_routes:
                return filter_routes[0](route, ability, func, *args, **kwargs)
            # if no filter defined, just pass through
            return func(*args, **kwargs)

        return func_wrapper

    return wrapper


def filter_mode(required_mode):
    def wrapper(func):
        """generic request filter system for customization"""

        @wraps(func)
        def func_wrapper(*args, **kwargs):
            required_mode.append('admin')

            if g.get('session') is None or g.session['mode'] in required_mode:
                return func(*args, **kwargs)
            abort(make_response(jsonify(message="your account does not allow to access this page"), 403))
            return None

        return func_wrapper

    return wrapper


def has_ability(flask_global, ability, entity):
    for f in has_ability_funcs:
        if not f(flask_global, ability, entity):
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
    show_all = boolean_param(flask.request.args.get('all'))
    res = {}
    for service_name in redis_db.smembers("admin:services"):
        service_config = nmtwizard_config.get_service_config(mongo_client, service_name)
        if service_config is None:
            abort(make_response(jsonify(message="service configuration %s unknown" % service_name), 404))

        pool_entities = nmtwizard_config.get_entities(service_config)

        if not show_all and flask.g.user.entity.entity_code not in pool_entities:
            continue

        if has_ability(flask.g, "view_all_services", SYSTRAN) or \
                any(has_ability(flask.g, "train", pool_entity) for pool_entity in pool_entities):
            service_def = get_service(service_name)
            name = service_def.display_name
            if minimal:
                res[service_name] = {'name': name}
            else:
                usage, queued, capacity, busy, detail = _usage_capacity(service_def)
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
    base_config = nmtwizard_config.get_base_config(mongo_client)
    return flask.jsonify(base_config)


@app.route("/service/configs/<string:service>", methods=["GET"])
@filter_request("GET/service/configs", "edit_config")
def get_service_config(service):
    check_permission(service, "edit_config")
    service_config = nmtwizard_config.get_service_config(mongo_client, service)
    return flask.jsonify(service_config)


def post_admin_request(current_app, service, action, value="1"):
    identifier = "%d.%d" % (os.getpid(), current_app.request_id)
    current_app.request_id += 1
    redis_db.set("admin:command:%s:%s:%s" % (service, action, identifier), value)
    command_result = None
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
@filter_request("POST/service/configs", "edit_config")
def set_service_config(service):
    check_permission(service, "edit_config")
    request_body = flask.request.form.get('config')
    try:
        update_config = json.loads(request_body)
        update_config["updated_at"] = time.time()
        nmtwizard_config.set_service_config(mongo_client, service, update_config)
        worker_pids = get_worker_pids(service)
        if len(worker_pids) == 0:
            return flask.jsonify("ok")
        command_response = post_admin_request(app, service, "restart")
        return flask.jsonify(command_response)
    except Exception as e:
        abort(flask.make_response(flask.jsonify(message=str(e)), 400))
    return None


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
    return None


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


@app.route("/service/check/<string:service>", methods=["GET", "POST"])
@filter_request("GET/service/check")
def check(service):
    service_options = flask.request.get_json() if flask.request.is_json else None
    if service_options is None:
        service_options = {}

    service_module = get_service(service)
    registries = nmtwizard_config.get_registries(mongo_client, service)
    try:
        details = service_module.check(service_options, registries)
    except ValueError as e:
        abort(flask.make_response(flask.jsonify(message=str(e)), 400))
    except Exception as e:
        abort(flask.make_response(flask.jsonify(message=str(e)), 500))
    else:
        return flask.jsonify(details)
    return None


def create_tasks_for_launch_v2(creation_infos):
    task_to_create = []
    tasks_id = []
    # PreprocessTask
    train_command = creation_infos.task_infos.content["docker"]["command"]
    task_preprocess = TaskPreprocess(creation_infos.task_infos)
    task_to_create.append(task_preprocess)
    tasks_id.append(task_preprocess.task_id)
    preprocess_task_id = task_preprocess.task_id
    remove_config_option(train_command)
    change_parent_task(train_command, preprocess_task_id)
    creation_infos.task_infos.content["docker"]["command"] = train_command

    iterations = creation_infos.task_infos.content.get("iterations", 1)
    train_task_id = None
    while iterations > 0:
        iterations -= 1

        task_train = TaskTrain(creation_infos.task_infos, preprocess_task_id)
        task_to_create.append(task_train)
        tasks_id.append(task_train.task_id)
        train_task_id = task_train.task_id
        remove_config_option(creation_infos.task_infos.content["docker"]["command"])
        trans_task_id = None
        if creation_infos.to_translate_corpus:
            task_translate = TaskTranslate(creation_infos.task_infos, train_task_id, creation_infos.to_translate_corpus)
            task_to_create.append(task_translate)
            trans_task_id = task_translate.task_id
            tasks_id.append(trans_task_id)
        if creation_infos.to_score_corpus:
            task_scoring = TaskScoring(creation_infos.task_infos, trans_task_id, train_task_id,
                                       creation_infos.to_score_corpus)
            task_to_create.append(task_scoring)
            tasks_id.append(task_scoring.task_id)

    (tasks_id, task_to_create) = post_function('POST/task/launch_v2', tasks_id, task_to_create)
    for tc in task_to_create:
        tc.other_task_info["model"] = train_task_id
        tc.create(redis_db=redis_db, taskfile_dir=taskfile_dir)

    creation_output = {
        "tasks_id": tasks_id,
        "train_task_id": train_task_id
    }

    # Create tasks for parent model
    to_translate_corpus_for_parent_model = [corpus for corpus in creation_infos.to_translate_corpus if
                                            SYSTRAN_BASE_STORAGE not in corpus[0]]
    to_score_corpus_for_parent_model = [corpus for corpus in creation_infos.to_score_corpus if
                                        SYSTRAN_BASE_STORAGE not in corpus[0]]

    create_trans_score_tasks_for_model(creation_infos.task_infos.request_data.get("parent_model"),
                                       to_translate_corpus_for_parent_model,
                                       to_score_corpus_for_parent_model,
                                       creation_infos.task_infos.request_data)

    return creation_output


def get_only_new_test_corpus(model_tests, to_translate_corpus, to_score_corpus):
    result_to_translate_corpus = []
    result_to_score_corpus = []

    for ttc, tsc in zip(to_translate_corpus, to_score_corpus):
        is_new_corpus = True
        for test in model_tests:
            corpus_path = '/'.join(test.split('/')[1:])
            if any(corpus_path in ttc_path for ttc_path in ttc):
                is_new_corpus = False
                break
        if is_new_corpus:
            result_to_translate_corpus.append(ttc)
            result_to_score_corpus.append(tsc)

    return result_to_translate_corpus, result_to_score_corpus


def create_trans_score_tasks(creation_infos, model, docker_content, parent_model):
    tasks_id = []
    tasks_to_create = []
    ok, model_info = builtins.pn9model_db.catalog_get_info(model, True)
    if not ok:
        abort(flask.make_response(flask.jsonify(message="invalid model %s" % model), 400))

    creation_infos.task_infos.content["name"] = get_input_name(model_info)
    to_translate_corpus, to_score_corpus = creation_infos.to_translate_corpus, creation_infos.to_score_corpus
    if model_info.get('tests'):
        to_translate_corpus, to_score_corpus = get_only_new_test_corpus(model_info.get('tests'), to_translate_corpus,
                                                                        to_score_corpus)

    if not to_translate_corpus:
        return
    creation_infos.task_infos.content["docker"] = docker_content
    task_translate = TaskTranslate(task_infos=creation_infos.task_infos,
                                   parent_task_id=model,
                                   to_translate=to_translate_corpus)
    tasks_to_create.append(task_translate)
    tasks_id.append(task_translate.task_id)

    task_scoring = TaskScoring(task_infos=creation_infos.task_infos,
                               parent_task_id=task_translate.task_id,
                               model=model,
                               to_score=to_score_corpus)
    tasks_to_create.append(task_scoring)
    tasks_id.append(task_scoring.task_id)

    (tasks_id, tasks_to_create) = post_function('POST/task/launch_v2', tasks_id, tasks_to_create)
    tasks_to_create.extend(tasks_to_create)

    for tc in tasks_to_create:
        if not parent_model:
            tc.other_task_info["model"] = model
        tc.create(redis_db=redis_db, taskfile_dir=taskfile_dir)


def get_corpus_info(data_files):
    user_corpus = []
    nb_segments = 0
    for file in data_files:
        nb_segments += int(file["nbSegments"])
        file_name = file["filename"]
        corpus_data = {
            "full_path": file_name,
            "file_id": file["id"],
            "nb_segments": int(file["nbSegments"])
        }
        if file.get("dataset_id"):
            corpus_data["dataset_id"] = file["dataset_id"]
        user_corpus.append(corpus_data)
    return user_corpus, nb_segments


@app.route("/v2/task/launch", methods=["POST"])
@filter_request("POST/v2/task/launch", "train")
def launch_v2():
    service = GLOBAL_POOL_NAME
    try:
        request_data = parse_request_data(request)
    except Exception as e:
        raise Exception("Cannot parse request data in launch_v2") from e

    domain = request_data.get('domain')
    if domain is None:
        abort(make_response(jsonify(message="unknown domain"), 400))
    parent_model = request_data.get('parent_model')
    if parent_model is None:
        abort(make_response(jsonify(message="unknown parent model"), 400))
    routes_config = RoutesConfiguration(flask.g, service)

    if request_data.get("corpus_type") == CORPUS_TYPE["USER_UPLOAD"]:
        dataset_name = request_data.get("dataset_name")

        if not is_valid_dataset_name(dataset_name):
            return make_response(
                jsonify(message="Invalid dataset name. It should only include alphanumeric characters and underscores"),
                400)

        entity_code = routes_config.creator['entity_code']

        exists_dataset = get_dataset_by_name(entity_code, dataset_name)

        if exists_dataset:
            return make_response(jsonify(message=f"Dataset \"{dataset_name}\" already exists"), 400)

        corpus_file_names = []
        if request_data.get('testing_proportion'):
            corpus_file_names.extend([corpus.filename for corpus in request_data.get('model_data')])
        else:
            corpus_file_names.extend([corpus.filename for corpus in request_data.get('training_data')])
            corpus_file_names.extend([corpus.filename for corpus in request_data.get('testing_data')])
        validate_corpus_name(corpus_file_names)

    data_file_info = get_data_file_info(request_data, routes_config)
    if not data_file_info.get("training"):
        return make_response(jsonify(message="Missing training data in dataset. Please check again."), 400)

    content = get_training_config(service, request_data, routes_config, data_file_info)
    content["trainer_email"] = g.user.email
    content["trainer_name"] = g.user.first_name
    if g.get('session'):
        content["application_mode"] = g.session.get('mode')
    image_tag = f'{content["docker"]["image"]}:{content["docker"]["tag"]}'

    to_translate_corpus, to_score_corpus = get_translate_score_corpus(data_file_info["testing"], request_data,
                                                                      routes_config)

    task_infos = TaskInfos(content=content, files={}, request_data=request_data, routes_configuration=routes_config,
                           service=service)

    tasks_creation_infos = TasksCreationInfos(task_infos=task_infos,
                                              to_translate_corpus=to_translate_corpus,
                                              to_score_corpus=to_score_corpus)

    tasks_creation_output = create_tasks_for_launch_v2(tasks_creation_infos)

    tasks_for_model = create_tasks_for_model(tasks_creation_output["tasks_id"])
    tags = process_tags(request_data.get("tags"), g.user.entity.entity_code, g.user.user_code)
    input_name = content["name"] if "name" in content else None

    user_training_corpus, nb_training_segments = get_corpus_info(data_file_info["training"])
    user_corpus = {
        "training": {"corpus": user_training_corpus, "nb_segments": nb_training_segments}
    }
    if "testing" in data_file_info:
        user_testing_corpus, nb_testing_segments = get_corpus_info(data_file_info["testing"])
        user_corpus["testing"] = {"corpus": user_testing_corpus, "nb_segments": nb_testing_segments}

    create_model_catalog(training_task_id=tasks_creation_output["train_task_id"], input_name=input_name,
                         request_data=request_data, image_tag=image_tag, creator=routes_config.creator,
                         tasks=tasks_for_model, tags=tags, domain=domain, user_corpus=user_corpus)

    return flask.jsonify(tasks_creation_output["tasks_id"])


def create_tasks_for_model(tasks_id):
    tasks = []
    for task_id in tasks_id:
        tasks.append(task_id)
    return tasks


def parse_request_data(current_request):
    validate_request_data(current_request)

    request_files = current_request.files
    request_data = current_request.form
    tags = request_data.get("tags", [])

    training_data = request_files.getlist("training_data")
    testing_data = request_files.getlist("testing_data")
    model_data = request_files.getlist("model_data")
    dataset = request_data.getlist("dataset")
    corpus_type = int(request_data.get("corpus_type"))
    dataset_name = request_data.get("dataset_name")
    testing_proportion = request_data.get("testing_proportion")
    if testing_proportion:
        testing_proportion = json.loads(testing_proportion)
        testing_proportion['value'] = int(testing_proportion['value'])

    return {**request_data, **{"tags": json.loads(tags)}, **{
        "training_data": training_data,
        "testing_data": testing_data,
        "model_data": model_data,
        "dataset": dataset,
        "corpus_type": corpus_type,
        "testing_proportion": testing_proportion,
        "dataset_name": dataset_name
    }}


def validate_request_data(current_request):
    request_files = current_request.files
    request_data = current_request.form

    base_config = nmtwizard_config.get_base_config(mongo_client)
    corpus_config = base_config.get("corpus")

    validate_tags(request_data.get("tags"))

    validate_model_name(request_data.get("model_name"))
    validate_docker_image(request_data.get("docker_image"))
    validate_ncpus(request_data.get("ncpus"))
    validate_priority(request_data.get("priority"))
    validate_iteration(request_data.get("num_of_iteration"))

    validate_file(request_data.get("corpus_type"), request_data.get("testing_proportion"), corpus_config,
                  request_files.getlist("training_data"), request_files.getlist("testing_data"),
                  request_files.getlist("model_data"), request_data.getlist("dataset"))


def validate_tags(tags):
    try:
        if not tags:
            return
        tags_json = json.loads(tags)
        existed_tags = tags_json.get("existed", [])
        new_tags = tags_json.get("new", [])
        if not isinstance(existed_tags, list):
            raise Exception("existed_tags tags must be array")
        if not isinstance(new_tags, list):
            raise Exception("new tags must be array")
        for tag in existed_tags:
            if not is_valid_object_id(tag):
                raise Exception(f"Invalid id: {tag}")
        for tag in new_tags:
            print(f"Tag: {tag}")
    except Exception as e:
        raise Exception("Invalid tags json") from e


def validate_training_data(training_data, corpus_config):
    if not isinstance(training_data, list) or len(training_data) == 0:
        raise Exception("training data is required")
    validate_corpus_data(training_data, corpus_config)


def validate_testing_data(testing_data, corpus_config):
    if not isinstance(testing_data, list):
        raise Exception("testing data must be Array")
    if len(testing_data) == 0:
        return
    validate_corpus_data(testing_data, corpus_config)


def validate_corpus_data(files, corpus_config):
    for file in files:
        file_name = file.filename
        if not is_valid_corpus_extension(file_name, corpus_config):
            raise Exception(f"Invalid corpus extension: {file_name}")


def is_valid_corpus_extension(file_name, corpus_config):
    valid_extensions = [".tmx", ".txt"]
    if corpus_config:
        valid_extensions = corpus_config.get("extensions") or valid_extensions
    _, extension = os.path.splitext(file_name)
    return extension in valid_extensions


def validate_model_name(model_name):
    reg = r"(^[a-zA-Z0-9\.\_\-]+$)"
    if not re.match(reg, model_name):
        raise Exception("Invalid model_name")


def validate_docker_image(docker_image):
    if docker_image is None:
        return
    if 'image' not in docker_image:
        raise Exception("no image in docker_image")
    if 'tag' not in docker_image:
        raise Exception("no tag in docker_image")
    if 'registry' not in docker_image:
        raise Exception("no registry in docker_image")
    # TODO check each value separately to check if it is correct
    return


def validate_ncpus(ncpus):
    validate_numeric_value(ncpus, 'ncpus')


def validate_priority(priority):
    validate_numeric_value(priority, 'priority')


def validate_iteration(num_of_iteration):
    validate_numeric_value(num_of_iteration, 'num_of_iteration')


def validate_numeric_value(value, name):
    if value is None:
        return
    if not value.isnumeric():
        raise Exception(str(name + 'must be numeric'))


def is_valid_dataset_name(dataset_name):

    return dataset_name and len(dataset_name) > 5 and re.match("^[a-zA-Z0-9_]*$", dataset_name)


def validate_corpus_name(corpus_names):
    for corpus_name in corpus_names:
        if not re.match("^[a-zA-Z0-9_.-]*$", corpus_name):
            message = "Invalid corpus name: %s. " % corpus_name
            message += "It should only include alphanumeric characters, underscores, dots and dashes."
            abort(make_response(jsonify(message=message), 400))


def upload_user_files(routes_config, path, files):
    temp_files = tempfile.mkdtemp()
    push_infos_list = []
    for file in files:
        tmp_file = os.path.join(temp_files, file.filename)
        file.save(tmp_file)
        if os.stat(tmp_file).st_size == 0:
            abort(flask.make_response(flask.jsonify(message=str("The file %s is empty." % file.filename)), 400))
        try:
            push_infos = routes_config.storage_client.push(os.path.join(temp_files, file.filename), path,
                                                           routes_config.global_storage_name)
        except Exception as e:
            abort(flask.make_response(flask.jsonify(message=str(e)), 400))
        assert push_infos and push_infos['nbSegments']
        push_infos_list.append(push_infos)
    return push_infos_list


def partition_and_upload_user_files(routes_config, training_path, testing_path, files, testing_proportion):
    training_push_infos_list = []
    testing_push_infos_list = []
    temp_files = tempfile.mkdtemp()
    for file in files:
        tmp_file = os.path.join(temp_files, file.filename)
        file.save(tmp_file)
        if os.stat(tmp_file).st_size == 0:
            abort(flask.make_response(flask.jsonify(message=str("The file %s is empty." % file.filename)), 400))
        try:
            push_infos = routes_config.storage_client.partition_auto(tmp_file,
                                                                     training_path,
                                                                     testing_path,
                                                                     remote_path=training_path,
                                                                     storage_id=routes_config.global_storage_name,
                                                                     partition_value=testing_proportion.get('value'),
                                                                     is_percent=testing_proportion.get('isPercentage'))
        except Exception as e:
            abort(flask.make_response(flask.jsonify(message=str(e)), 400))

        assert push_infos and push_infos['files'] and len(push_infos['files']) == 2
        training_file_info = push_infos['files'][0]
        testing_file_info = push_infos['files'][1]
        assert training_file_info['nbSegments'] and testing_file_info['nbSegments']
        training_push_infos_list.append(training_file_info)
        testing_push_infos_list.append(testing_file_info)
    return training_push_infos_list, testing_push_infos_list


def validate_file(corpus_type, testing_proportion, corpus_config, training_data, testing_data, model_data, dataset):
    if not corpus_type or not corpus_type.isnumeric() or int(corpus_type) not in CORPUS_TYPE.values():
        raise Exception('Invalid corpus_type')
    if int(corpus_type) == CORPUS_TYPE["USER_UPLOAD"] and testing_proportion is None:
        validate_training_data(training_data, corpus_config)
        validate_testing_data(testing_data, corpus_config)
    elif int(corpus_type) == CORPUS_TYPE["USER_UPLOAD"] and testing_proportion:
        validate_training_data(model_data, corpus_config)
    else:
        if len(dataset) == 0:
            raise Exception('Num of dataset must greater than 0')
        for dataset_id in dataset:
            if not is_valid_object_id(dataset_id):
                raise Exception(f'Invalid dataset: {dataset_id}')


def get_dataset_by_name(entity, dataset_name):
    return builtins.pn9model_db.get_dataset_by_name(entity, dataset_name)


def build_dataset_path(entity_code, dataset_name, corpus_type):
    return os.path.join(entity_code, dataset_name, corpus_type)


def get_data_file_info(request_data, routes_config):
    corpus_type = request_data.get("corpus_type")
    testing_proportion = request_data.get("testing_proportion")

    if corpus_type == CORPUS_TYPE["USER_UPLOAD"]:
        if testing_proportion:
            training_data = request_data.get("model_data")
        else:
            training_data = request_data.get("training_data")
        testing_data = request_data.get("testing_data")

        return get_user_upload_file_info(routes_config, request_data, training_data, testing_data)

    dataset = request_data.get("dataset")
    dataset_ids = list(map(ObjectId, dataset))

    return get_exists_dataset_file_info(dataset_ids)


def get_user_upload_file_info(routes_config, request_data, training_data, testing_data):
    entity_code = routes_config.creator['entity_code']
    dataset_name = request_data.get('dataset_name')
    testing_proportion = request_data.get("testing_proportion")

    training_data_path = build_dataset_path(entity_code, dataset_name, "train") + os.path.sep
    testing_data_path = build_dataset_path(entity_code, dataset_name, "test") + os.path.sep

    if testing_proportion:
        training_data_path = "/" + build_dataset_path(entity_code, dataset_name, "train") + os.path.sep
        testing_data_path = "/" + build_dataset_path(entity_code, dataset_name, "test") + os.path.sep
        data_training, data_testing = partition_and_upload_user_files(routes_config, training_data_path,
                                                                      testing_data_path, training_data,
                                                                      testing_proportion)
    else:
        data_training = upload_user_files(routes_config, training_data_path, training_data)
        data_testing = upload_user_files(routes_config, testing_data_path, testing_data)
    create_model_dataset(routes_config, request_data, GLOBAL_POOL_NAME)

    dataset = get_dataset_by_name(entity_code, dataset_name)

    return {
        'training': list(map(lambda ele: {**ele, 'dataset_id': str(dataset["_id"])}, data_training)),
        'testing': list(map(lambda ele: {**ele, 'dataset_id': str(dataset["_id"])}, data_testing))
    }


def get_exists_dataset_file_info(dataset_ids):
    result = {
        "training": [],
        "testing": []
    }
    exists_dataset = mongo_client.get_dataset_by_ids(dataset_ids)
    storage_client, global_storage_name = StorageUtils.get_storages(GLOBAL_POOL_NAME, mongo_client, redis_db,
                                                                    has_ability, g)

    for dataset in exists_dataset:
        dataset_name = dataset["name"]
        entity_code = dataset["entity"]
        files = get_all_files_of_dataset(entity_code, dataset_name, global_storage_name, storage_client)

        training_files = files.get("train", [])
        testing_files = files.get("test", [])
        for f in training_files:
            f["dataset_id"] = str(dataset["_id"])

        result["training"].extend(training_files)
        result["testing"].extend(testing_files)

    return result


def get_translate_score_corpus(testing_data_infos, request_data, routes_config, with_default_test=True, output_path=''):
    source = request_data["source"]
    target = request_data["target"]
    default_test_data = get_default_test_data(routes_config.storage_client, source, target) if with_default_test else []

    to_translate_corpus = []
    to_score_corpus = []
    for corpus in testing_data_infos:
        corpus_path = corpus["filename"]
        if corpus_path[0] == '/':
            corpus_path = corpus_path[1:]
        to_translate_corpus.append([
            f'{routes_config.global_storage_name}:{corpus_path}.{source}',
            f'pn9_testtrans:<MODEL>/{output_path + routes_config.global_storage_name}/{corpus_path}.{source}.{target}'
        ])
        to_score_corpus.append([
            f'pn9_testtrans:<MODEL>/{output_path + routes_config.global_storage_name}/{corpus_path}.{source}.{target}',
            f'{routes_config.global_storage_name}:{corpus_path}.{target}'
        ])

    if default_test_data:
        for corpus_name in default_test_data:
            to_translate_corpus.append([
                f'{SYSTRAN_BASE_STORAGE}:{corpus_name}',
                f'pn9_testtrans:<MODEL>/{SYSTRAN_BASE_STORAGE}/{corpus_name}.{target}'
            ])
            target_corpus = corpus_name[:-3] + "." + target
            to_score_corpus.append([
                f'pn9_testtrans:<MODEL>/{SYSTRAN_BASE_STORAGE}/{corpus_name}.{target}',
                f'{SYSTRAN_BASE_STORAGE}:{target_corpus}'
            ])

    return to_translate_corpus, to_score_corpus


def get_test_folder_name(source, target):
    return f'{source}_{target}' if source < target else f'{target}_{source}'


def format_training_folder(training_folder):
    if not training_folder.startswith('/'):
        training_folder = '/' + training_folder
    if not training_folder.endswith('/'):
        training_folder += '/'
    return "${GLOBAL_DATA}" + training_folder


def get_final_training_config(request_data, training_corpus_infos):
    training_corpus_paths = map(lambda corpus: corpus.get("filename"), training_corpus_infos)
    training_corpus_folders = set(map(os.path.dirname, training_corpus_paths))

    sample = 0
    sample_by_path = {}

    for corpus_infos in training_corpus_infos:
        sample += int(corpus_infos["nbSegments"])
        training_folder = os.path.dirname(corpus_infos.get("filename"))
        training_folder_path = format_training_folder(training_folder)

        if sample_by_path.get(training_folder_path) is None:
            sample_by_path[training_folder_path] = int(corpus_infos["nbSegments"])
        else:
            sample_by_path[training_folder_path] += int(corpus_infos["nbSegments"])

    training_data_config = {
        "sample": sample,
        "sample_dist": list(map(lambda training_folder: {
            "path": format_training_folder(training_folder),
            "distribution": [["*", "*"]]
        }, training_corpus_folders))
    }
    parent_model = request_data["parent_model"]
    # add suffix if model type is standalone
    exist_model, model_info = builtins.pn9model_db.catalog_get_info(request_data["parent_model"], True)
    if exist_model and is_standalone_model(model_info):
        parent_model += '_standalone'

    ok, parent_config = builtins.pn9model_db.catalog_get_info(parent_model, boolean_param(request.args.get('short')))
    if ok:
        # Remove build object from parent config
        parent_config.pop('build', None)
        # Change batch size to settings value if specified
        batch_size = app.get_other_config(['training_options', 'batch_size'], fallback=None)
        if batch_size is not None:
            if "config" not in parent_config["options"]:
                parent_config["options"]["config"] = {
                    "train": {}
                }
            parent_config["options"]["config"]["train"]["batch_size"] = batch_size

        # Remove option moving_average_decay from parent config
        if "config" in parent_config["options"] and "train" in parent_config["options"].get("config"):
            parent_config["options"]["config"]["train"].pop("moving_average_decay", None)
        parent_config = delete_nfa_feature_from_config(parent_config)
        sample_size, sample_dist = get_sample_data(training_data_config, parent_config["data"], sample_by_path)

        parent_config["data"] = {
            "sample": sample_size,
            "sample_dist": sample_dist
        }
        prepr_batch_size = app.get_other_config(['training_options', 'prepr_batch_size'], fallback=None)
        if prepr_batch_size is not None:
            parent_config["data"]["batch_size"] = prepr_batch_size

        parent_config["product"] = "SYSTRAN ModelStudio Lite"
        return parent_config

    abort(flask.make_response(flask.jsonify(message="No configuration for parent model %s" % (
        request_data["parent_model"])), 400))
    return None


def is_standalone_model(model_info):
    if model_info.get('standalone'):
        for standalone in model_info['standalone'].values():
            if standalone.get('state') == 'completed':
                return True
    return False


def delete_nfa_feature_from_config(config):
    supported_features = config.get('supported_features')
    if supported_features and supported_features.get('NFA'):
        config['supported_features']['NFA'] = False
    if 'mpreprocess' in config or 'bpreprocess' in config or 'preprocess' not in config:
        config = delete_nfa_v1(config)
    else:
        config = delete_nfa_v2(config)
    return config


def delete_nfa_v1(config):
    def apply(sampling_rule):
        if len(sampling_rule) > 2 and isinstance(sampling_rule[2], dict) and sampling_rule[2].get("bpreprocess"):
            bpreprocess = sampling_rule[2].get("bpreprocess")
            if bpreprocess.get("tm") and len(bpreprocess) > 1:
                del sampling_rule[2]["bpreprocess"]["tm"]
            elif bpreprocess.get("tm"):
                del sampling_rule[2]["bpreprocess"]
                if not sampling_rule[2]:
                    del sampling_rule[2]
        return sampling_rule

    sample_dist = config.get('data').get('sample_dist')

    for storage_block in sample_dist:
        if storage_block.get('distribution'):
            storage_block['distribution'] = list(map(apply, storage_block.get('distribution')))

    return config


def delete_nfa_v2(config):
    if not config.get('preprocess'):
        return config

    operator_to_remove = ['tm', 'tm_noise']
    config['preprocess'] = [operator for operator in config.get('preprocess')
                            if operator.get('op') not in operator_to_remove]

    return config


def adapt_distribution_proportions(distribution, get_new_value, new_val, is_parent=True):
    to_remove = []

    def apply(sampling_rule):
        sampling_rule[1] = get_new_value(sampling_rule[1], new_val) if is_parent else get_new_value(new_val)
        if is_parent and '*' in str(sampling_rule[1]):
            to_remove.append(sampling_rule)
        return sampling_rule

    for storage_block in distribution:
        if storage_block.get('distribution'):
            storage_block['distribution'] = list(map(apply, storage_block.get('distribution')))
        for item_to_remove in to_remove:
            storage_block.get('distribution').remove(item_to_remove)
        to_remove = []
    return distribution


def get_parent_formula_distribution_proportions(old_weight, client_ratio):
    if not isinstance(old_weight, float) and not isinstance(old_weight, int):
        return old_weight

    parent_ratio = float(100 - client_ratio) / 100.0
    return round(old_weight * parent_ratio, 4)


def get_client_formula_distribution_proportions(client_weight):
    return "*{}".format(client_weight)


def get_client_weight(sample_size, client_ratio, client_volume):
    proportion = float(client_ratio) / 100.0
    result = int(round(sample_size * proportion / client_volume, 0))
    return result if result > 0 else 1


def get_client_corpus_info(client_corpus_size):
    client_corpus_object = app.get_other_config(['training_options', 'client_corpus'], fallback=[])
    for operator in client_corpus_object:
        current_func = operator.get('function', 'False')
        values = operator.get('values') if len(operator.get('values')) > 1 else operator.get('values')[0]
        if eval(current_func.format(client_corpus_size, values)):  # pylint: disable=eval-used
            return operator.get('client_ratio'), operator.get('sample_size')
    return 70, 2000000


def get_sample_data(current_data, parent_data, sample_by_path):
    client_ratio, new_sample_size = get_client_corpus_info(current_data.get('sample', 0))
    current_sample_dists = current_data["sample_dist"]
    sample_dists = parent_data["sample_dist"]
    new_sample_dists = []
    client_sample = 0

    for current_sample_dist in current_sample_dists:
        current_sample_dist_path = current_sample_dist['path']

        def equal_path(sample_dist, path=current_sample_dist_path):
            return path == sample_dist['path']

        duplicate = list(filter(equal_path, sample_dists))
        client_sample += int(sample_by_path[current_sample_dist_path])

        if len(duplicate) > 0:
            continue

        new_sample_dists.append(current_sample_dist)

    client_weight = get_client_weight(new_sample_size, client_ratio, client_sample)
    sample_dists = adapt_distribution_proportions(sample_dists, get_parent_formula_distribution_proportions,
                                                  client_ratio)
    new_sample_dists = adapt_distribution_proportions(new_sample_dists, get_client_formula_distribution_proportions,
                                                      client_weight, is_parent=False)

    new_sample_dists.extend(sample_dists)
    return new_sample_size, new_sample_dists


def get_default_test_data(storage_client, source, target):
    result = []
    test_folder_name = get_test_folder_name(source, target)
    listdir = storage_client.listdir(f'{SYSTRAN_BASE_STORAGE}:{test_folder_name}/')
    for corpus_name in listdir:
        if not listdir[corpus_name].get("is_dir", False):
            if corpus_name.endswith(f'.{source}'):
                corresponding_corpus = corpus_name[:-3] + "." + target
            else:
                continue
            if corresponding_corpus in listdir:
                result.append(corpus_name)
    return result


def get_training_config(service, request_data, routes_config, data_file_info):
    final_training_config = get_final_training_config(request_data, data_file_info["training"])
    docker_image_info = TaskBase.get_docker_image_info(routes_config, request_data.get("docker_image"), mongo_client)

    docker_commands = ["-c", json.dumps(final_training_config), "train"]

    content = {
        "service": service,
        "name": request_data["model_name"],
        "docker": {**docker_image_info, **{
            "command": docker_commands
        }},
        "wait_after_launch": 2,
        "trainer_id": f"{routes_config.entity_owner}{routes_config.creator['user_code']}",
        "options": {},
    }

    if request_data.get("ncpus"):
        content["ncpus"] = request_data["ncpus"]
    if request_data.get("priority"):
        content["priority"] = request_data["priority"]
    if request_data.get("iterations"):
        content["iterations"] = request_data["iterations"]
    return json.loads(json.dumps(content))


def is_valid_object_id(value):
    return ObjectId.is_valid(value)


def process_tags(tags, entity_code, user_code):
    final_tags = []
    existed_tags = tags.get("existed", [])
    new_tags = list(set(tags.get("new", [])))  # ensure unique value

    tag_ids = list(map(ObjectId, existed_tags))
    exists_tags_by_id = list(mongo_client.get_tags_by_ids(tag_ids))
    exists_tags_by_value = list(mongo_client.get_tags_by_value(new_tags, entity_code))
    not_exists_tags = list(
        filter(lambda tag: tag not in list(map(lambda exists_tag: str(exists_tag["tag"]), exists_tags_by_value)),
               new_tags))

    if len(not_exists_tags) > 0:
        insert_tags = list(map(lambda tag: {
            "entity": entity_code,
            "creator": entity_code + user_code,
            "tag": tag
        }, not_exists_tags))

        mongo_client.tags_put(insert_tags)
        inserted_tags = list(mongo_client.get_tags_by_value(not_exists_tags, entity_code))
        final_tags.extend(inserted_tags)

    final_tags.extend(exists_tags_by_id)
    final_tags.extend(exists_tags_by_value)

    return final_tags


def create_model_dataset(routes_config, request_data, service):
    source_language = request_data.get("source")
    target_language = request_data.get("target")

    item = {
        "creator": routes_config.creator,
        "entity": routes_config.creator['entity_code'],
        "name": request_data.get('dataset_name'),
        "service": service,
        "source_language": source_language,
        "target_language": target_language,
        "lp": f"{source_language}_{target_language}",
        "created_at": time.time()
    }

    return builtins.pn9model_db.insert_dataset(item)


def create_model_catalog(training_task_id, input_name, request_data, image_tag, creator, tasks, tags, domain,
                         user_corpus, state="creating"):
    source = request_data.get("source")
    target = request_data.get("target")
    parent_model = request_data.get("parent_model")
    tags = [{'entity': tag.get('entity'), 'tag': tag.get('tag')} for tag in tags]
    config = {
        "source": source,
        "target": target,
        "parent_model": parent_model,
        "imageTag": image_tag,
        "tags": tags,
        "tasks": tasks,
        "domain": domain,
        "user_corpus": user_corpus
    }

    return builtins.pn9model_db.catalog_declare(training_task_id, config,
                                                entity_owner=creator['entity_code'],
                                                lp=None, state=state, creator=creator, input_name=input_name)


def create_tasks_for_evaluation(creation_infos, models, evaluation_id, docker_content):
    model_task_map = {}
    tasks_to_create = []
    models_info = []
    google_docker_content = {}
    if 'google_image_info' in docker_content:
        google_docker_content = docker_content['google_image_info']
        docker_content.pop('google_image_info')
        google_docker_content['command'] = []

    for model in models:
        tasks_id_per_model = []
        tasks_to_create_per_model = []
        ok, model_info = builtins.pn9model_db.catalog_get_info(model, True)
        if not ok:
            abort(flask.make_response(flask.jsonify(message="invalid model %s" % model), 400))
        models_info.append(model_info)

        creation_infos.task_infos.other_infos = {
            "evaluation_id": str(evaluation_id),
            "eval_model": model
        }
        creation_infos.task_infos.content["name"] = get_input_name(model_info)
        if check_google_model(model):
            creation_infos.task_infos.content["docker"] = google_docker_content
            task_translate = TaskTranslate(task_infos=creation_infos.task_infos,
                                           parent_task_id=model,
                                           to_translate=creation_infos.to_translate_corpus)
            lang_config = {
                "source": creation_infos.task_infos.request_data['source'],
                "target": creation_infos.task_infos.request_data['target']
            }
            config = ['-c', json.dumps(lang_config)]
            task_translate.update_content_docker_command(config)
        else:
            creation_infos.task_infos.content["docker"] = docker_content
            task_translate = TaskTranslate(task_infos=creation_infos.task_infos,
                                           parent_task_id=model,
                                           to_translate=creation_infos.to_translate_corpus)

        tasks_to_create_per_model.append(task_translate)
        tasks_id_per_model.append(task_translate.task_id)

        task_scoring = TaskScoring(task_infos=creation_infos.task_infos,
                                   parent_task_id=task_translate.task_id,
                                   model=model,
                                   to_score=creation_infos.to_score_corpus)
        tasks_to_create_per_model.append(task_scoring)
        tasks_id_per_model.append(task_scoring.task_id)

        model_task_map[model] = {
            "trans": {
                "id": task_translate.task_id,
                "status": "running"
            },
            "score": {
                "id": task_scoring.task_id,
                "status": "running"
            }
        }

        (tasks_id_per_model, tasks_to_create_per_model) = post_function('POST/task/launch_v2', tasks_id_per_model,
                                                                        tasks_to_create_per_model)
        tasks_to_create.extend(tasks_to_create_per_model)

    for tc in tasks_to_create:
        tc.create(redis_db=redis_db, taskfile_dir=taskfile_dir)

    return model_task_map, models_info


@app.route("/evaluations", methods=["POST"])
@filter_request("POST/evaluations", "train")
def create_evaluation():
    evaluation_id = ObjectId()
    entity_code = g.user.entity.entity_code
    upload_path = f"/{entity_code}/{evaluation_id}"
    output_path = f"evaluation/{evaluation_id}/"

    try:
        request_data = parse_request_data_of_evaluation(request)
    except Exception as e:
        raise Exception("Cannot parse request data in create_evaluation") from e

    service = GLOBAL_POOL_NAME
    routes_config = RoutesConfiguration(flask.g, service)

    models = request_data.get("models")

    corpus_file_names = [corpus.filename for corpus in request_data.get('corpus')]
    validate_corpus_name(corpus_file_names)

    testing_info = upload_user_files(routes_config, f"{upload_path}/test/", request_data.get('corpus'))
    to_translate_corpus, to_score_corpus = get_translate_score_corpus(testing_info, request_data, routes_config, False,
                                                                      output_path)

    docker_image_info = TaskBase.get_docker_image_info(routes_config, request_data.get("docker_image"), mongo_client)
    for model in models:
        if check_google_model(model):
            google_docker_image_info = TaskBase.get_google_docker_image_from_db(routes_config.service_module,
                                                                                mongo_client)
            docker_image_info['google_image_info'] = google_docker_image_info
            break

    docker_content = {**docker_image_info, **{"command": []}}
    content = {
        "docker": {},
        'wait_after_launch': 2,
        'trainer_id': f'{routes_config.creator["entity_code"]}{routes_config.creator["user_code"]}',
        'ngpus': 0,
        'service': service,
        "options": {},
        'support_statistics': True,
        'trainer_email': g.user.email,
        'trainer_name': g.user.first_name,
        'application_mode': g.session.get('mode'),
        'eval_name': request_data["evaluation_name"]
    }

    task_infos = TaskInfos(content=content, files={}, request_data=request_data, routes_configuration=routes_config,
                           service=service, resource="auto")

    tasks_creation_infos = TasksCreationInfos(task_infos=task_infos,
                                              to_translate_corpus=to_translate_corpus,
                                              to_score_corpus=to_score_corpus)

    model_task_map, models_info = create_tasks_for_evaluation(creation_infos=tasks_creation_infos, models=models,
                                                              evaluation_id=evaluation_id,
                                                              docker_content=docker_content)

    create_evaluation_catalog(evaluation_id, request_data, routes_config.creator, models_info, to_translate_corpus,
                              model_task_map)

    return flask.jsonify(model_task_map)


def create_trans_score_tasks_for_model(model, to_translate_corpus, to_score_corpus, request_data, parent_model=True):
    service = GLOBAL_POOL_NAME
    routes_config = RoutesConfiguration(flask.g, service)
    docker_image_info = TaskBase.get_docker_image_info(routes_config, request_data.get("docker_image"), mongo_client)
    docker_content = {**docker_image_info, **{"command": []}}
    content = {
        "docker": {},
        'wait_after_launch': 2,
        'trainer_id': f'{routes_config.creator["entity_code"]}{routes_config.creator["user_code"]}',
        'ngpus': 0,
        'service': service,
        "options": {},
        'support_statistics': True,
        'trainer_email': g.user.email,
        'trainer_name': g.user.first_name,
        'application_mode': g.session.get('mode')
    }

    task_infos = TaskInfos(content=content, files={}, request_data=request_data, routes_configuration=routes_config,
                           service=service, resource="auto")

    tasks_creation_infos = TasksCreationInfos(task_infos=task_infos,
                                              to_translate_corpus=to_translate_corpus,
                                              to_score_corpus=to_score_corpus)

    create_trans_score_tasks(creation_infos=tasks_creation_infos, model=model, docker_content=docker_content,
                             parent_model=parent_model)


def parse_request_data_of_evaluation(current_request):
    validate_request_data_of_evaluation(current_request)
    request_files = current_request.files
    request_data = current_request.form

    models = request_data.getlist("models")
    evaluation_corpus = request_files.getlist("corpus")

    language_pair = request_data.get('lp')
    source_language = language_pair.split("_")[0]
    target_language = language_pair.split("_")[1]

    return {
        **request_data,
        **{"source": source_language, "target": target_language},
        **{"models": models},
        **{"corpus": evaluation_corpus}
    }


def validate_request_data_of_evaluation(current_request):
    # TODO: Validate request data
    # models: exists? training completed? same lp? num of models?
    # corpus: use is_valid_corpus_extension(file_name, corpus_config)
    return current_request


def get_input_name(model):
    if model.get("input_name"):
        return model.get("input_name")
    if model.get("type") == "base":
        name = model["owner"]["entity"] + ' ' + model["domain"]
        if model.get("push") and model.get("push")[-1] and model.get("push")[-1].get("size"):
            size = model.get("push")[-1].get("size")
            if size and size != "M":
                name += " (" + size + ")"
        return name
    return model["model"]


def create_evaluation_catalog(evaluation_id, request_data, creator, models_info, to_translate_corpus, model_task_map):
    source_language = request_data.get("source")
    target_language = request_data.get("target")
    result = {
        "_id": evaluation_id,
        "name": request_data["evaluation_name"],
        "creator": creator,
        "source_language": source_language,
        "target_language": target_language,
        "lp": f"{source_language}_{target_language}",
        "models": [],
        "created_at": int(time.time())
    }

    for model in models_info:
        model_evaluation_info = {
            "input_name": get_input_name(model),
            "name": model["model"],
            "type": model.get("type"),
            "tests": {},
            "tasks": model_task_map[model["model"]]
        }

        for corpus in to_translate_corpus:
            source_corpus = corpus[0]
            result_corpus = corpus[1]
            model_evaluation_info["tests"][source_corpus] = {
                "score": {},
                "output": result_corpus.replace("<MODEL>", model["model"])
            }

        result["models"].append(model_evaluation_info)

    mongo_client.create_evaluation_catalog(result)


@app.route("/evaluations", methods=["GET"])
@filter_request("GET/evaluations", "train")
def get_evaluations():
    visible_entities = [g.user.entity.entity_code]
    evaluation_catalogs = list(mongo_client.get_evaluation_catalogs(visible_entities))
    return cust_jsonify(evaluation_catalogs)


def get_json_config(command):
    idx = 0
    while idx < len(command):
        if (command[idx] == '-c' or command[idx] == '--config'):
            return idx + 1, command[idx + 1]
        idx += 1
    return None, ''


def add_train_restricted_config(json_config, parent_task_id):
    config = json.loads(json_config)

    if not config.get("data") or not config["data"]["sample_dist"]:
        return json_config

    ok, parent_config = builtins.pn9model_db.catalog_get_info(parent_task_id, False)

    if not ok:
        return json_config

    sample_dist = config["data"]["sample_dist"]
    parent_sample_dist = parent_config["data"]["sample_dist"]

    for item in parent_sample_dist:
        # hide train_restricted path
        if not is_resource_train_restricted(item['path']):
            continue
        sample_dist.append(item)

    config["data"]["sample_dist"] = sample_dist
    return json.dumps(config)


def parse_tags(tags):
    result = []
    for tag in tags:
        tag_name = tag.get('tag')
        entity = tag.get('entity', '')
        if not entity:
            entity = flask.g.user.entity.entity_code
        info_tag = builtins.pn9model_db.tag_get(entity, tag_name)
        if info_tag:
            result.append({'tag': tag_name, 'entity': entity})
        else:
            return False, result
    return True, result


@app.route("/task/launch/<string:service>", methods=["POST"])
@filter_request("POST/task/launch", "train")
def launch(service):
    service_config = nmtwizard_config.get_service_config(mongo_client, service)
    pool_entities = nmtwizard_config.get_entities(service_config)
    if all(not has_ability(flask.g, "train", entity) for entity in pool_entities):
        abort(make_response(jsonify(message="insufficient credentials for train (entity %s)" % service), 403))

    content = flask.request.form.get('content')
    if content is not None:
        content = json.loads(content)
        content["trainer_email"] = g.user.email
        content["trainer_name"] = g.user.first_name
        if g.get('session'):
            content["application_mode"] = g.session.get('mode')
    else:
        abort(flask.make_response(flask.jsonify(message="missing content in request"), 400))

    # Parse tags
    if content.get('tags') and isinstance(content.get('tags'), list):
        res, parsed_tags = parse_tags(content.get('tags'))
        if not res:
            abort(flask.make_response(flask.jsonify(message="Invalid tags"), 400))

        content['tags'] = parsed_tags

    files = {}
    for k in flask.request.files:
        files[k] = flask.request.files[k].read()

    service_module = get_service(service)
    content["service"] = service

    exec_mode = content.get('exec_mode', False)
    docker_version = content['docker']['tag']
    if docker_version.startswith('v'):
        docker_version = docker_version[1:]
    is_standalone = False

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
        elif "standalone" in content["docker"]["command"]:
            task_type = "prepr"
            is_standalone = True
    else:
        task_type = 'exec'

    if task_type == '????':
        abort(flask.make_response(flask.jsonify(message="incorrect task definition"), 400))

    if is_standalone:
        if not semver.match(docker_version, ">1.58.1"):
            abort(flask.make_response(flask.jsonify(
                message="This feature is only supported for docker images with tag version greater than 1.58.1"), 400))
        allow_entities = app.get_other_config(['standalone_allow_entities'], fallback=[])
        entity_code = g.user.entity.entity_code

        if entity_code not in allow_entities:
            abort(flask.make_response(
                flask.jsonify(message="insufficient credentials for generate standalone model"), 403))

    elif task_type != "exec":
        task_suffix = task_type
    else:
        task_suffix = get_docker_action(content["docker"]["command"])
        if task_suffix is None:
            task_suffix = task_type

    service_entities = nmtwizard_config.get_entities(service_config)
    entity_owner = RoutesConfiguration.get_entity_owner(service_entities, service)
    trainer_entities = RoutesConfiguration.get_entities_by_permission("train")
    assert trainer_entities  # Here: almost sure you are trainer
    other_task_info = {TaskEnum.ENTITY_OWNER.value: entity_owner,
                       TaskEnum.STORAGE_ENTITIES.value: json.dumps(trainer_entities)}

    # Sanity check on content.
    if 'options' not in content or not isinstance(content['options'], dict):
        abort(flask.make_response(flask.jsonify(message="invalid options field"), 400))
    if 'docker' not in content:
        abort(flask.make_response(flask.jsonify(message="missing docker field"), 400))
    if ('image' not in content['docker'] or 'registry' not in content['docker'] or
            'tag' not in content['docker'] or 'command' not in content['docker']):
        abort(flask.make_response(flask.jsonify(message="incomplete docker field"), 400))
    if content['docker']['registry'] == 'auto':
        content['docker']['registry'] = get_registry(service_module, content['docker']['image'])
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

    ngpus = 1 if task_type == "train" else 0
    if "ngpus" in content:
        ngpus = content["ngpus"]
    ncpus = content.get("ncpus")
    ncpus_prepr = content.get("ncpus_prepr")
    ncpus_train = content.get("ncpus_train")
    ncpus_trans = content.get("ncpus_trans")
    ncpus_max = max((x for x in [ncpus, ncpus_prepr, ncpus_train, ncpus_trans] if x is not None), default=None)

    # check that we have a resource able to run such a request
    if not _find_compatible_resource(service_module, ngpus, ncpus_max, resource):
        abort(flask.make_response(
            flask.jsonify(message="no resource available on %s for %d gpus (%s cpus)" % (
                service, ngpus, ncpus and str(ncpus) or "-")), 400))

    if "totranslate" in content:
        if exec_mode:
            abort(flask.make_response(
                flask.jsonify(message="translate mode unavailable for exec cmd"), 400))
        to_translate = content["totranslate"]
        del content["totranslate"]
    else:
        to_translate = None
    if "toscore" in content:
        if exec_mode:
            abort(flask.make_response(flask.jsonify(message="score mode unavailable for exec cmd"),
                                      400))
        to_score = content["toscore"]
        del content["toscore"]
    else:
        to_score = None
    if "totuminer" in content:
        if exec_mode:
            abort(flask.make_response(
                flask.jsonify(message="tuminer chain mode unavailable for exec cmd"), 400))
        totuminer = content["totuminer"]
        del content["totuminer"]
    else:
        totuminer = None

    try:
        chain_prepr_train = (not exec_mode and not content.get("nochainprepr", False) and
                             task_type == "train" and
                             semver.match(docker_version, ">=1.4.0"))
        can_trans_as_release = semver.match(docker_version, ">=1.8.0")
        trans_as_release = (not exec_mode and not content.get("notransasrelease", False) and
                            semver.match(docker_version, ">=1.8.0"))
        content["support_statistics"] = semver.match(docker_version, ">=1.17.0")
    except ValueError:
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

    if task_type == 'train' and not check_permission_access_train_restricted('read'):
        json_idx, json_config = get_json_config(content["docker"]["command"])

        if json_idx is not None:
            configuration = add_train_restricted_config(json_config, parent_task_id)
            content["docker"]["command"][json_idx] = configuration

    task_ids = []
    task_create = []
    first_of_chain = True
    while iterations > 0:
        if (chain_prepr_train and parent_task_type != "prepr") or task_type == "prepr":
            suffix_name = "standalone" if is_standalone else "prepr"
            prepr_task_id, explicit_name = build_task_id(content, xxyy, suffix_name, parent_task_id)

            if "dependency" in content and first_of_chain:
                parent_task_id = content["dependency"]
                first_of_chain = False

            if explicit_name:
                TaskBase.patch_config_explicit_name(content, explicit_name)

            idx = 0
            prepr_command = []
            train_command = content["docker"]["command"]
            while train_command[idx] != 'train' and train_command[idx] not in ['preprocess', 'standalone']:
                prepr_command.append(train_command[idx])
                idx += 1

            # create preprocess command, don't push the model on the catalog,
            # and generate a pseudo model
            if not is_standalone:
                prepr_command.append("--no_push")
            prepr_command += ["preprocess", "--build_model"]
            if is_standalone:
                prepr_command += ["standalone", "--output_model_name", parent_task_id + "_standalone"]

            content["docker"]["command"] = prepr_command

            content["ncpus"] = ncpus_prepr or ncpus or get_cpu_count(service_config, 0, "preprocess")
            content["ngpus"] = 0

            preprocess_resource = service_module.select_resource_from_capacity(
                resource, Capacity(content["ngpus"], content["ncpus"]))

            # launch preprocess task on cpus only
            task_create.append(
                (redis_db, taskfile_dir,
                 prepr_task_id, "prepr", parent_task_id, preprocess_resource, service,
                 _duplicate_adapt(content),
                 files, priority, 0, content["ncpus"], deepcopy(other_task_info)))
            task_ids.append(
                "%s\t%s\tngpus: %d, ncpus: %d" % ("prepr", prepr_task_id, 0, content["ncpus"]))
            remove_config_option(train_command)
            change_parent_task(train_command, prepr_task_id)
            parent_task_id = prepr_task_id
            content["docker"]["command"] = train_command

        if task_type != "prepr":

            task_id, explicit_name = build_task_id(content, xxyy, task_suffix, parent_task_id)

            if "dependency" in content and first_of_chain:
                parent_task_id = content["dependency"]
                first_of_chain = False

            if explicit_name:
                TaskBase.patch_config_explicit_name(content, explicit_name)

            file_to_transtaskid = {}
            if task_type == "trans":
                try:
                    idx = content["docker"]["command"].index("trans")
                    output_files = get_params(("-o", "--output"), content["docker"]["command"][idx + 1:])
                    for ofile in output_files:
                        file_to_transtaskid[ofile] = task_id
                except Exception:
                    pass

            if task_type == "train" and ncpus_train:
                content["ncpus"] = ncpus_train
            elif task_type == "trans" and ncpus_trans:
                content["ncpus"] = ncpus_trans
            else:
                content["ncpus"] = ncpus or get_cpu_count(service_config, ngpus, task_type)
            content["ngpus"] = ngpus

            if task_type == "trans" and can_trans_as_release:
                if "--as_release" not in content["docker"]["command"] and trans_as_release:
                    content["docker"]["command"].append("--as_release")

            task_resource = service_module.select_resource_from_capacity(
                resource, Capacity(content["ngpus"],
                                   content["ncpus"]))

            task_create.append(
                (redis_db, taskfile_dir,
                 task_id, task_type, parent_task_id, task_resource, service,
                 _duplicate_adapt(content),
                 files, priority,
                 content["ngpus"], content["ncpus"],
                 deepcopy(other_task_info)))
            task_ids.append("%s\t%s\tngpus: %d, ncpus: %d" % (
                task_type, task_id,
                content["ngpus"], content["ncpus"]))
            parent_task_type = task_type[:5]
            remove_config_option(content["docker"]["command"])

            if to_translate:
                content_translate = deepcopy(content)
                content_translate["priority"] = priority + 1
                content_translate["ngpus"] = 0
                content_translate["ncpus"] = ncpus_trans or ncpus or get_cpu_count(service_config,
                                                                                   content_translate["ngpus"],
                                                                                   "trans")

                translate_resource = service_module.select_resource_from_capacity(
                    resource, Capacity(content_translate["ngpus"],
                                       content_translate["ncpus"]))

                if ngpus == 0 or trans_as_release:
                    file_per_gpu = len(to_translate)
                else:
                    file_per_gpu = int((len(to_translate) + ngpus - 1) / ngpus)
                subset_idx = 0
                while subset_idx * file_per_gpu < len(to_translate):
                    content_translate["docker"]["command"] = ["trans"]
                    if trans_as_release:
                        content_translate["docker"]["command"].append("--as_release")
                    content_translate["docker"]["command"].append('-i')
                    subset_to_translate = to_translate[subset_idx * file_per_gpu:
                                                       (subset_idx + 1) * file_per_gpu]
                    for f in subset_to_translate:
                        content_translate["docker"]["command"].append(f[0])

                    change_parent_task(content_translate["docker"]["command"], task_id)
                    trans_task_id, explicit_name = build_task_id(content_translate, xxyy, "trans", task_id)

                    content_translate["docker"]["command"].append('-o')
                    for f in subset_to_translate:
                        ofile = f[1].replace('<MODEL>', task_id)
                        file_to_transtaskid[ofile] = trans_task_id
                        content_translate["docker"]["command"].append(ofile)

                    task_create.append(
                        (redis_db, taskfile_dir,
                         trans_task_id, "trans", task_id, translate_resource, service,
                         _duplicate_adapt(content_translate),
                         (), content_translate["priority"],
                         content_translate["ngpus"], content_translate["ncpus"],
                         deepcopy(other_task_info)))
                    task_ids.append("%s\t%s\tngpus: %d, ncpus: %d" % (
                        "trans", trans_task_id,
                        content_translate["ngpus"], content_translate["ncpus"]))
                    subset_idx += 1

            if to_score:
                toscore_parent = {}
                for (ofile, rfile) in to_score:
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
                        "registry": get_registry(service_module, image_score),
                        "tag": "latest",
                        "command": ["score", "-o"] + oref["output"] + ["-r"] + oref["ref"] +
                                   option_lang + ['-f', "launcher:scores"]
                    }

                    score_task_id, explicit_name = build_task_id(content_score, xxyy, "score", parent_task_id)
                    task_create.append(
                        (redis_db, taskfile_dir,
                         score_task_id, "exec", parent_task_id, score_resource, service,
                         content_score,
                         files, priority + 2,
                         0, 1,
                         deepcopy(other_task_info)))
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
                        "registry": get_registry(service_module, image_score),
                        "tag": "latest",
                        "command": ["tuminer", "--tumode", "score", "--srcfile"] + in_out["infile"] + ["--tgtfile"] +
                                   in_out["outfile"] + ["--output"] + in_out["scorefile"]
                    }

                    tuminer_task_id, explicit_name = build_task_id(content_tuminer, xxyy, "tuminer", parent_task_id)
                    task_create.append(
                        (redis_db, taskfile_dir,
                         tuminer_task_id, "exec", parent_task_id, tuminer_resource, service,
                         content_tuminer,
                         (), priority + 2,
                         ngpus_recommend, ncpus_recommend,
                         deepcopy(other_task_info)))
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
        task.create_internal(*tc)

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
    if isinstance(response, tuple) and not response[0]:
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

    list_task = []
    prefix = "*" if pattern == '-*' else pattern

    suffix = ''
    if prefix.endswith('*'):
        prefix = prefix[:-1]
        suffix = '*'

    task_where_clauses = []
    if has_ability(g, 'view_all_tasks', SYSTRAN):
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
            task_id = task.get_task_id(task_key)
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

            list_task.append(info)
    return flask.jsonify(list_task)


@app.route("/task/terminate/<string:task_id>", methods=["GET"])
@filter_request("GET/task/terminate")
@task_write_control
def terminate(task_id):
    msg, task_status = terminate_internal(task_id)
    if task_status is None:
        abort(flask.make_response(flask.jsonify(message="task %s unknown" % task_id), 404))
    return flask.jsonify(message=msg)


def terminate_internal(task_id):
    with redis_db.acquire_lock(task_id):
        current_status = task.info(redis_db, taskfile_dir, task_id, "status")
        if current_status is None:
            return "task %s unknown" % task_id, current_status
        if current_status == "stopped":
            return "%s already stopped" % task_id, current_status

    phase = flask.request.args.get('phase')
    status_code = flask.request.args.get('status_code')
    res = post_function('GET/task/terminate', task_id, phase)
    if res:
        task.terminate(redis_db, task_id, phase="publish_error", status_code=status_code)
        return "problem while posting model: %s" % res, current_status

    task.terminate(redis_db, task_id, phase=phase, status_code=status_code)
    return "terminating %s" % task_id, current_status


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
    content = task.get_file(taskfile_dir, task_id, filename)
    if content is None:
        abort(flask.make_response(
            flask.jsonify(message="cannot find file %s for task %s" % (filename, task_id)), 404))
    # https://www.pythonanywhere.com/forums/topic/13570/
    w = FileWrapper(io.BytesIO(content))
    return Response(w, mimetype="application/octet-stream", direct_passthrough=True,
                    headers={'Content-Disposition': 'attachment; filename="{}"'.format(urllib.parse.quote(filename))})


@app.route("/file/<string:task_id>/<path:filename>", methods=["POST"])
@app.route("/task/file/<string:task_id>/<path:filename>", methods=["POST"])
@filter_request("POST/task/file")
@task_request
def post_file(task_id, filename):
    content = flask.request.get_data()
    task.set_file(taskfile_dir, task_id, content, filename)
    return flask.jsonify(200)


@app.route("/task/log/<string:task_id>", methods=["GET"])
@filter_request("GET/task/log")
def get_log(task_id):
    task_readonly_control(task_id)

    content = task.get_log(taskfile_dir, task_id)

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
    task.append_log(taskfile_dir, task_id, content, max_log_size)
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
    content = task.set_log(taskfile_dir, task_id, content, max_log_size)
    post_function('POST/task/log', task_id, content)
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
    for key_worker in redis_db.scan_iter("admin:worker:%s:*" % service_name):
        worker_pids.append(key_worker[len("admin:worker:%s:" % service_name):])
    return worker_pids


def get_all_files_of_dataset(entity_code, dataset_name, global_storage_name, storage_client):
    keys = ["train", "test"]
    result = {
        "train": [],
        "test": []
    }

    for key in keys:
        data_path = build_dataset_path(entity_code, dataset_name, key)
        if not storage_client.exists(data_path, storage_id=global_storage_name):
            continue
        directories = storage_client.list(data_path, storage_id=global_storage_name)
        for k, v in directories.items():
            if v.get('status') not in ['error', 'pending']:
                result[key].append(
                    {**v, **{"filename": k if k.startswith('/') else '/' + k, "nbSegments": v.get("entries")}})

    return result


def check_google_model(model):
    return model.split('_')[0].lower() == 'google'
