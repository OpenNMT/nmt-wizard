from app import app, redis, services
import flask
from nmtwizard import common, task
from nmtwizard.helper import build_task_id, shallow_command_analysis, change_parent_task
import json
from functools import wraps

def _get_service(service):
    """Wrapper to fail on invalid service."""
    if service not in services:
        response = flask.jsonify(message="invalid service name: %s" % service)
        flask.abort(flask.make_response(response, 404))
    return services[service]

def _usagecapacity(service):
    """calculate the current usage of the service."""
    usage = 0
    capacity = 0
    for resource in service.list_resources():
        capacity += service.list_resources()[resource]
        usage += redis.llen("resource:%s:%s" % (service.name, resource))
    queued = redis.llen("queued:"+service.name)
    return usage, queued, capacity

def task_request(func):
    """minimal check on the request to check that tasks exists"""
    @wraps(func)
    def func_wrapper(*args, **kwargs):
        if not task.exists(redis, args[0]):
            flask.abort(flask.make_response(flask.jsonify(message="task %s unknown" % args[0]), 404))
        return func(*args, **kwargs)
    return func_wrapper

filter_routes = []
def filter_request(route):
    def wrapper(func):
        """generic request filter system for customization"""
        @wraps(func)
        def func_wrapper(*args, **kwargs):
            if len(filter_routes):
                return filter_routes[0](route, func, *args, **kwargs)
            # if not filter defined, just pass through
            return func(*args, **kwargs)
        return func_wrapper
    return wrapper

@app.route("/service/list", methods=["GET"])
@filter_request("GET/service/list")
def list_services():
    res = {}
    for k in services:
        usage, queued, capacity = _usagecapacity(services[k])
        res[k] = { 'name':services[k].display_name,
                   'usage': usage, 'queued': queued, 'capacity': capacity }
    return flask.jsonify(res)

@app.route("/service/describe/<string:service>", methods=["GET"])
@filter_request("GET/service/describe")
def describe(service):
    service_module = _get_service(service)
    return flask.jsonify(service_module.describe())

@app.route("/service/check/<string:service>", methods=["GET"])
@filter_request("GET/service/check")
def check(service):
    service_options = flask.request.get_json() if flask.request.is_json else None
    if service_options is None:
        service_options = {}
    service_module = _get_service(service)
    try:
        details = service_module.check(service_options)
    except ValueError as e:
        flask.abort(flask.make_response(flask.jsonify(message=str(e)), 400))
    except Exception as e:
        flask.abort(flask.make_response(flask.jsonify(message=str(e)), 500))
    else:
        return flask.jsonify(message=details)

@app.route("/task/launch/<string:service>", methods=["POST"])
@filter_request("POST/task/launch")
def launch(service):
    content = None
    files = {}
    if flask.request.is_json:
        content = flask.request.get_json()
    else:
        content = flask.request.form.get('content')
        if content is not None:
            content = json.loads(content)
        for k in flask.request.files:
            files[k] = flask.request.files[k].read()
    if content is None:
        flask.abort(flask.make_response(flask.jsonify(message="missing content in request"), 400))
    service_module = _get_service(service)
    content["service"] = service

    task_type = '????'
    if "train" in content["docker"]["command"]: task_type = "train"
    elif "trans" in content["docker"]["command"]: task_type = "trans"
    elif "preprocess" in content["docker"]["command"]: task_type = "prepr"

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

    priority = content.get("priority", 0)

    (xxyy, parent_task_id) = shallow_command_analysis(content["docker"]["command"])

    task_ids = []

    while iterations > 0:
        task_id = build_task_id(content, xxyy, parent_task_id)
        task.create(redis, task_id, task_type, parent_task_id, resource, service, content, files, priority)
        task_ids.append(task_id)
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
    response = task.info(redis, task_id, [])
    return flask.jsonify(response)

@app.route("/task/<string:task_id>", methods=["DELETE"])
@filter_request("DELETE/task")
@task_request
def del_task(task_id):
    response = task.delete(redis, task_id)
    if isinstance(response, list) and not response[0]:
        flask.abort(flask.make_response(flask.jsonify(message=response[1]), 400))        
    return flask.jsonify(message="deleted %s" % task_id)

@app.route("/task/list/<string:pattern>", methods=["GET"])
@filter_request("GET/task/list")
def list_tasks(pattern):
    ltask = []
    for task_key in task.scan_iter(redis, pattern):
        task_id = task.id(task_key)
        info = task.info(redis, task_id,
                ["queued_time", "resource", "content", "status", "message", "type", "iterations", "priority"])
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
@filter_request("GET/task/terminate")
@task_request
def terminate(task_id):
    with redis.acquire_lock(task_id):
        current_status = task.info(redis, task_id, "status")
        if current_status is None:
            flask.abort(flask.make_response(flask.jsonify(message="task %s unknown" % task_id), 404))
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
        flask.abort(flask.make_response(flask.jsonify(message="invalid duration value"), 400))
    container_id = flask.request.args.get('container_id')
    task.beat(redis, task_id, duration, container_id)
    return flask.jsonify(200)

@app.route("/task/file/<string:task_id>/<string:filename>", methods=["GET"])
@filter_request("GET/task/file")
@task_request
def get_file(task_id, filename):
    content = task.get_file(redis, task_id, filename)
    if content is None:
        flask.abort(flask.make_response(
            flask.jsonify(message="cannot find file %s for task %s" % (filename, task_id)), 404))
    response = flask.make_response(content)
    return response

@app.route("/task/file/<string:task_id>/<string:filename>", methods=["POST"])
@filter_request("POST/task/file")
@task_request
def post_file(task_id, filename):
    content = flask.request.get_data()
    task.set_file(redis, task_id, content, filename)
    return flask.jsonify(200)

@app.route("/task/log/<string:task_id>", methods=["GET"])
@filter_request("GET/task/log")
@task_request
def get_log(task_id):
    content = task.get_log(redis, task_id)
    if content is None:
        flask.abort(flask.make_response(
            flask.jsonify(message="cannot find log for task %s" % task_id), 404))
    response = flask.make_response(content)
    return response

@app.route("/task/log/<string:task_id>", methods=["PATCH"])
@filter_request("PATH/task/log")
@task_request
def append_log(task_id):
    content = flask.request.get_data()
    task.append_log(redis, task_id, content)
    return flask.jsonify(200)

@app.route("/task/log/<string:task_id>", methods=["POST"])
@filter_request("POST/task/log")
@task_request
def post_log(task_id):
    content = flask.request.get_data()
    task.set_log(redis, task_id, content)
    return flask.jsonify(200)
