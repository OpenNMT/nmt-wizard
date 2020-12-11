import time
import json
import os
import shutil
from app import redis_db, taskfile_dir, mongo_client
from functools import cmp_to_key
import builtins
import semver
from copy import deepcopy
from enum import Enum
from nmtwizard.capacity import Capacity
from nmtwizard.helper import build_task_id, get_cpu_count, get_registry, change_parent_task

import six

TASK_RELEASE_TYPE = "relea"


class TaskEnum(Enum):
    ENTITY_OWNER = "owner"
    STORAGE_ENTITIES = "storage_entities"


ttl_policy_func = None


class TaskInfos:
    def __init__(self, content, files, request_data, routes_configuration, service, other_infos=None, resource=None):
        self.content = content
        self.files = files
        self.request_data = request_data
        self.routes_configuration = routes_configuration
        self.service = service
        self.other_infos = other_infos
        self.resource = resource


class TasksCreationInfos:
    def __init__(self, task_infos, to_translate_corpus, to_score_corpus):
        self.task_infos = task_infos
        self.to_translate_corpus = to_translate_corpus
        self.to_score_corpus = to_score_corpus


class TaskBase:
    def __init__(self, task_infos, must_patch_config_name=True):
        self._content = deepcopy(task_infos.content)
        self._lang_pair = f'{task_infos.request_data["source"]}{task_infos.request_data["target"]}'
        if not self._lang_pair and self._parent_task_id:
            self._lang_pair = self._parent_task_id.split("_")[1]
        self._service = task_infos.service
        self._service_config = task_infos.routes_configuration.service_config
        self._service_module = task_infos.routes_configuration.service_module
        self._files = task_infos.files
        self.other_task_info = {TaskEnum.ENTITY_OWNER.value: task_infos.routes_configuration.entity_owner,
                                TaskEnum.STORAGE_ENTITIES.value: json.dumps(
                                    task_infos.routes_configuration.trainer_entities)}
        if task_infos.other_infos:
            self.update_other_infos(task_infos.other_infos)
        self._priority = self._content.get("priority", 0)
        self._resource = task_infos.resource

        if self._task_suffix:
            self.task_id, explicit_name = build_task_id(self._content, self._lang_pair, self._task_suffix,
                                                        self._parent_task_id)
            self.task_name = "%s\t%s\tngpus: %d, ncpus: %d" % (self._task_suffix, self.task_id,
                                                               self._content["ngpus"], self._content["ncpus"])
            if must_patch_config_name:
                TaskBase.patch_config_explicit_name(self._content, explicit_name)

        if self._resource:
            self._resource = self._service_module.select_resource_from_capacity(
                self._resource, Capacity(self._content["ngpus"], self._content["ncpus"])
            )
        else:
            self._resource = self._service_module.select_resource_from_capacity(
                self._service_module.get_resource_from_options(self._content["options"]),
                Capacity(self._content["ngpus"], self._content["ncpus"])
            )

    def update_other_infos(self, other_infos):
        self.other_task_info.update(other_infos)

    def create(self):
        create_internal(redis_db,
                        taskfile_dir,
                        self.task_id,
                        self._task_type,
                        self._parent_task_id,
                        self._resource,
                        self._service,
                        self._content,
                        self._files,
                        self._priority,
                        self._content["ngpus"],
                        self._content["ncpus"],
                        self.other_task_info)
        self.post_create(self)

    def post_create(self):
        pass

    @staticmethod
    def patch_config_explicit_name(content, explicit_name):
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
                    config["modelname_description"] = explicit_name
                    command[idx + 1] = json.dumps(config)
                    return
                idx += 2

    @staticmethod
    def get_docker_image_info(routes_config, docker_image):
        if not docker_image:
            return TaskBase.get_docker_image_from_db(routes_config.service_module)
        return TaskBase.get_docker_image_from_request(routes_config.service_module, routes_config.entity_owner,
                                                      docker_image)

    @staticmethod
    def get_docker_image_from_db(service_module):
        image = "systran/pn9_tf"
        registry = get_registry(service_module, image)
        tag = "v1.35.2-beta9"

        result = {
            "image": image,
            "tag": tag,
            "registry": registry
        }

        latest_docker_image_tag = TaskBase.get_latest_docker_image_tag(image)

        if not latest_docker_image_tag:
            return result
        return {**result, **{"tag": f'v{latest_docker_image_tag}'}}

    @staticmethod
    def get_latest_docker_image_tag(image):
        docker_images = list(mongo_client.get_docker_images(image))
        if len(docker_images) == 0:
            return None
        only_tag_docker_images = list(
            map(lambda docker_image: TaskBase.get_docker_image_tag(docker_image["image"]), docker_images))
        only_tag_docker_images = list(filter(lambda tag: tag != "latest", only_tag_docker_images))
        if len(only_tag_docker_images) == 0:
            return None
        if len(only_tag_docker_images) == 1:
            return only_tag_docker_images[0]
        sorted_docker_image_tags = sorted(only_tag_docker_images, key=cmp_to_key(
            lambda x, y: semver.compare(x, y)), reverse=True)
        return sorted_docker_image_tags[0]

    @staticmethod
    def get_docker_image_tag(image):
        split_name = image.split(":")
        if len(split_name) < 2:
            return None
        tag = split_name[-1]
        if not tag.startswith("v"):
            return tag
        return tag[1:]

    @staticmethod
    def get_docker_image_from_request(service_module, entity_owner, docker_image):
        result = {**docker_image}
        registry = docker_image["registry"]
        if registry == "auto":
            result["registry"] = get_registry(service_module, docker_image["image"])
            return result
        if registry not in service_module.get_docker_config(entity_owner)['registries']:
            raise Exception(f"Unknown docker registry: {registry}")
        return result


class TaskPreprocess(TaskBase):
    def __init__(self, task_infos):
        self._task_suffix = "prepr"
        self._task_type = "prepr"
        self._parent_task_id = None
        # launch preprocess task on cpus only
        task_infos.content["ngpus"] = 0
        if "ncpus" not in task_infos.content:
            task_infos.content["ncpus"] = get_cpu_count(task_infos.routes_configuration.service_config,
                                                        task_infos.content["ngpus"], "preprocess")

        idx = 0
        preprocess_command = []
        train_command = task_infos.content["docker"]["command"]
        while train_command[idx] != 'train' and train_command[idx] != 'preprocess':
            preprocess_command.append(train_command[idx])
            idx += 1

        # create preprocess command, don't push the model on the catalog,
        # and generate a pseudo model
        preprocess_command.append("--no_push")
        preprocess_command.append("preprocess")
        preprocess_command.append("--build_model")
        task_infos.content["docker"]["command"] = preprocess_command

        TaskBase.__init__(self, task_infos)


class TaskTrain(TaskBase):
    def __init__(self, task_infos, parent_task_id):
        self._task_suffix = "train"
        self._task_type = "train"
        self._parent_task_id = parent_task_id

        if "ncpus" not in task_infos.content:
            if "ngpus" not in task_infos.content:
                task_infos.content["ngpus"] = 0
            task_infos.content["ncpus"] = get_cpu_count(task_infos.routes_configuration.service_config,
                                                        task_infos.content["ngpus"], "train")

        TaskBase.__init__(self, task_infos)


class TaskTranslate(TaskBase):
    def __init__(self, task_infos, parent_task_id, to_translate):
        self._task_suffix = "trans"
        self._task_type = "trans"
        self._parent_task_id = parent_task_id

        task_infos.content["priority"] = task_infos.content.get("priority", 0) + 1
        task_infos.content["ngpus"] = 0
        if "ncpus" not in task_infos.content:
            task_infos.content["ncpus"] = get_cpu_count(task_infos.routes_configuration.service_config,
                                                        task_infos.content["ngpus"], "trans")

        task_infos.content["docker"]["command"] = ["trans"]
        task_infos.content["docker"]["command"].append("--as_release")
        task_infos.content["docker"]["command"].extend('-i')
        for f in to_translate:
            task_infos.content["docker"]["command"].extend(f[0])
        change_parent_task(task_infos.content["docker"]["command"], parent_task_id)
        task_infos.content["docker"]["command"].extend('-o')
        for f in to_translate:
            sub_file = f[1].replace('<MODEL>', parent_task_id)
            task_infos.content["docker"]["command"].extend(sub_file)

        TaskBase.__init__(self, task_infos, must_patch_config_name=False)


class TaskScoring(TaskBase):
    def __init__(self, task_infos, parent_task_id, to_score):
        self._task_suffix = "score"
        self._task_type = "exec"
        self._parent_task_id = parent_task_id

        task_infos.content["priority"] = task_infos.content.get("priority", 0) + 1
        task_infos.content["ngpus"] = 0
        task_infos.content["ncpus"] = 1
        image_score = "nmtwizard/score"
        task_infos.content["docker"] = {
            "image": image_score,
            "registry": get_registry(task_infos.routes_configuration.service_module, image_score),
            "tag": "2.0.0",
            "command": []
        }

        output_corpus = []
        references_corpus = []
        for corpus in to_score:
            corpus[0].replace('<MODEL>', parent_task_id)
            output_corpus.extend(corpus[0])
            references_corpus.extend(corpus[1])

        task_infos.content["docker"]["command"] = ["score", "-o"]
        task_infos.content["docker"]["command"].extend(output_corpus)
        task_infos.content["docker"]["command"].extend("-r")
        task_infos.content["docker"]["command"].extend(references_corpus)
        task_infos.content["docker"]["command"].extend(["-f", "launcher:scores"])

        TaskBase.__init__(self, task_infos, must_patch_config_name=False)


class TaskRelease(TaskBase):
    def __init__(self, task_infos, model, destination):
        self._task_suffix = TASK_RELEASE_TYPE
        self._task_type = TASK_RELEASE_TYPE
        self._parent_task_id = model

        task_infos.content["priority"] = task_infos.content.get("priority", 0) + 10
        task_infos.content["ngpus"] = 0
        task_infos.content["ncpus"] = 2
        task_infos.content["docker"] = TaskBase.get_docker_image_from_db(task_infos.routes_configuration.service_module)
        task_infos.content["docker"]["command"] = ['--model',
                                                   self._parent_task_id,
                                                   'release',
                                                   '--destination',
                                                   destination]
        TaskBase.__init__(self, task_infos, must_patch_config_name=False)

    def post_create(self):
        builtins.pn9model_db.model_set_release_state(self._parent_task_id, self._content["trainer_id"], self.task_id,
                                                     "in progress")


def set_ttl_policy(func):
    global ttl_policy_func
    ttl_policy_func = func


def set_status(redis, keyt, status):
    """Sets the status and save the time of change."""
    global ttl_policy_func
    assert keyt.startswith("task:"), "invalid format of task_id: " + keyt
    redis.hset(keyt, "status", status)
    redis.hset(keyt, status + "_time", time.time())
    if ttl_policy_func is not None and status == "stopped":
        ttl = ttl_policy_func(redis.hgetall(keyt))
        if ttl is not None and ttl != 0:
            print('Apply %d ttl on %s' % (ttl, keyt))
            redis.expire(keyt, ttl)


def exists(redis, task_id):
    """Checks if a task exist."""
    return redis.exists("task:" + task_id)


def create_internal(redis, taskfile_dir, task_id, task_type, parent_task, resource, service, content, files, priority,
                    ngpus, ncpus, generic_map):
    """Creates a new task and enables it."""
    keyt = "task:" + task_id
    redis.hset(keyt, "type", task_type)
    if parent_task:
        redis.hset(keyt, "parent", parent_task)
    if isinstance(resource, list):
        resource = ",".join(resource)
    redis.hset(keyt, "resource", resource)
    redis.hset(keyt, "service", service)
    redis.hset(keyt, "content", json.dumps(content))
    redis.hset(keyt, "priority", priority)
    redis.hset(keyt, "ngpus", ngpus)
    redis.hset(keyt, "ncpus", ncpus)
    for k in generic_map:
        redis.hset(keyt, k, generic_map[k])
    for k in files:
        set_file(redis, taskfile_dir, task_id, files[k], k)
    set_status(redis, keyt, "launched")
    set_status(redis, keyt, "queued")
    enable(redis, task_id, service)
    service_queue(redis, task_id, service)


def terminate(redis, task_id, phase):
    """Requests task termination (assume it is locked)."""
    if phase is None:
        phase = "aborted"
    keyt = "task:" + task_id
    if redis.hget(keyt, "status") in ("terminating", "stopped"):
        return

    # remove from service queue if it was there
    service = redis.hget(keyt, "service")
    if service is not None:
        redis.lrem('queued:'+service, 0, task_id)

    redis.hset(keyt, "message", phase)
    set_status(redis, keyt, "terminating")
    work_queue(redis, task_id)


def work_queue(redis, task_id, service=None, delay=0):
    if service is None:
        service = redis.hget('task:'+task_id, 'service')
    # Queues the task in the work queue with a delay.
    if delay == 0:
        with redis.acquire_lock(f'work_queue:{task_id}', acquire_timeout=1, expire_time=10):
            if task_id not in redis.lrange(f'work:{service}', 0, -1):
                redis.lpush('work:'+service, task_id)
        redis.delete('queue:'+task_id)
    else:
        redis.set('queue:'+task_id, delay)
        redis.expire('queue:'+task_id, int(delay))


def work_unqueue(redis, service):
    """Pop a task from the work queue."""
    return redis.rpop('work:'+service)


def service_queue(redis, task_id, service):
    """Queue the task on the service queue."""
    with redis.acquire_lock('service:'+service):
        redis.lrem('queued:'+service, 0, task_id)
        redis.lpush('queued:'+service, task_id)
        redis.delete('queue:'+task_id)


def enable(redis, task_id, service=None):
    if service is None:
        service = redis.hget('task:'+task_id, 'service')
    # Marks a task as enabled.
    redis.sadd("active:"+service, task_id)


def disable(redis, task_id, service=None):
    if service is None:
        service = redis.hget('task:'+task_id, 'service')
    # Marks a task as disabled.
    redis.srem("active:"+service, task_id)
    redis.delete("beat:"+task_id)


def list_active(redis, service):
    """Returns all active tasks (i.e. non stopped)."""
    return redis.smembers("active:"+service)


def info(redis, taskfile_dir, task_id, fields):
    """Gets information on a task."""
    keyt = "task:" + task_id
    if fields is None:
        # only if we want all information - add a lock on the resource
        with redis.acquire_lock(keyt):
            fields = redis.hkeys(keyt)
            fields.append("ttl")
            r = info(redis, taskfile_dir, task_id, fields)
            r['files'] = file_list(redis, taskfile_dir, task_id)
            return r
    field = None
    if not isinstance(fields, list):
        field = fields
        fields = [field]
    r = {}
    for f in fields:
        if f != "ttl":
            r[f] = redis.hget(keyt, f)
        else:
            r[f] = redis.ttl("beat:" + task_id)
    if field:
        return r[field]
    r["current_time"] = int(time.time())
    return r


def change(redis, task_id, service, priority, ngpus):
    """ move a task to another service or change priority/ngpus, assume service exists
        check task_id is queued """
    keyt = "task:" + task_id
    with redis.acquire_lock(keyt):
        prev_service = redis.hget(keyt, "service")
        status = redis.hget(keyt, "status")
        if status != "queued":
            return False, "cannot move task `%s` - not in queued status" % task_id
        if service:
            if prev_service != service:
                disable(redis, task_id, prev_service)
                enable(redis, task_id, service)
                redis.hset(keyt, "service", service)
                redis.lrem('queued:'+prev_service, 0, task_id)
                redis.lpush('queued:'+service, task_id)
        if priority:
            redis.hset(keyt, "priority", priority)
        if ngpus:
            redis.hset(keyt, "ngpus", ngpus)
    return True, ""


def get_owner_entity(redis, task_id):
    key_task_id = "task:" + task_id
    owner_entity = redis.hget(key_task_id, TaskEnum.ENTITY_OWNER.value)
    if not owner_entity:  # TODO: useful only for the first deployment
        service = redis.hget(key_task_id, "service")
        owner_entity = service[:2]
    return owner_entity.upper()


def get_storages_entity(redis, task_id):
    key_task_id = "task:" + task_id
    task_storage_entities = redis.hget(key_task_id, TaskEnum.STORAGE_ENTITIES.value)
    return json.loads(task_storage_entities) if task_storage_entities else None


def delete(redis, taskfile_dir, task_id):
    """Delete a given task."""
    keyt = "task:" + task_id
    status = redis.hget(keyt, "status")
    if status is None:
        return False, "task does not exist"
    if status != "stopped":
        return False, "status is not stopped"
    with redis.acquire_lock(keyt):
        redis.delete(keyt)
        redis.delete("queue:" + task_id)
        task_dir = os.path.join(taskfile_dir, task_id)
        if os.path.isdir(task_dir):
            shutil.rmtree(task_dir)
    return True


# TODO: create iterator returning directly task_id
def scan_iter(redis, pattern):
    return redis.scan_iter('task:' + pattern)


def id(task_key):
    return task_key[5:]


def beat(redis, task_id, duration, container_id):
    """Sends an update event to the task and add an expiration time
    (set duration to 0 to disable expiration). The task must be running.
    """
    keyt = "task:" + task_id
    if redis.hget(keyt, "status") != "running":
        return
    with redis.acquire_lock(keyt):
        # a beat can only be sent in running mode except if in between, the task stopped
        # or in development mode, no need to raise an alert
        if redis.hget(keyt, "status") != "running":
            return
        if duration is not None:
            if duration == 0:
                redis.delete("beat:" + task_id)
            else:
                redis.set("beat:" + task_id, duration)
                redis.expire("beat:" + task_id, duration)
                queue = redis.get("queue:" + task_id)
                # renew ttl of queue
                if queue is not None:
                    redis.expire("queue:" + task_id, int(queue))
        redis.hset(keyt, "updated_time", int(time.time()))
        if container_id is not None:
            redis.hset(keyt, "container_id", container_id)


def file_list(redis, taskfile_dir, task_id):
    """Returns the list of files attached to a task"""
    task_dir = os.path.join(taskfile_dir, task_id)
    if not os.path.isdir(task_dir):
        return []
    return os.listdir(task_dir)


disclaimer = b'\n\n[FILE TRUNCATED]\n'


def set_file(redis, taskfile_dir, task_id, content, filename, limit=None):
    taskdir = os.path.join(taskfile_dir, task_id)
    if not os.path.isdir(taskdir):
        os.mkdir(taskdir)
    with open(os.path.join(taskdir, filename), "wb") as fh:
        content = six.ensure_binary(content)
        if limit and len(content) >= limit:
            content = content[:limit-len(disclaimer)] + disclaimer
        fh.write(content)
    return content


def append_file(redis, taskfile_dir, task_id, content, filename, limit=None):
    taskdir = os.path.join(taskfile_dir, task_id)
    if not os.path.isdir(taskdir):
        os.mkdir(taskdir)
    filepath = os.path.join(taskdir, filename)
    current_size = 0
    if os.path.isfile(filepath):
        current_size = os.stat(filepath).st_size
    if limit and current_size >= limit:
        return
    content = six.ensure_binary(content, encoding="utf-8")
    if limit and len(content)+current_size >= limit:
        content = content[:limit-len(disclaimer)] + disclaimer
    with open(filepath, "ab") as fh:
        fh.write(content)


def get_file(redis, taskfile_dir, task_id, filename):
    path = os.path.join(taskfile_dir, task_id, filename)
    if not os.path.isfile(path):
        return None
    with open(path, "rb") as fh:
        return fh.read()


def get_log(redis, taskfile_dir, task_id):
    return get_file(redis, taskfile_dir, task_id, "log")


def append_log(redis, taskfile_dir, task_id, content, limit=None):
    return append_file(redis, taskfile_dir, task_id, content, "log", limit)


def set_log(redis, taskfile_dir, task_id, content, limit=None):
    return set_file(redis, taskfile_dir, task_id, content, "log", limit)


def set_stat(redis, task_id, duration, statistics):
    keyt = "task:" + task_id
    redis.hset(keyt, "duration", str(duration))
    redis.hset(keyt, "statistics", json.dumps(statistics))
