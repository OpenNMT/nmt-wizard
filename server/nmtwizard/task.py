import time
import json

ttl_policy_func = None
def set_ttl_policy(func):
    global ttl_policy_func
    ttl_policy_func = func

def set_status(redis, keyt, status):
    """Sets the status and save the time of change."""
    global ttl_policy_func
    redis.hset(keyt, "status", status)
    redis.hset(keyt, status + "_time", time.time())
    if ttl_policy_func is not None and status == "stopped":
        ttl = ttl_policy_func(redis.hgetall(keyt))
        if ttl is not None and ttl != 0:
            print('Apply %d ttl on %s' % (ttl, keyt))
            redis.expire(keyt, ttl)
            keyfile = keyt.replace("task:", "files:")
            redis.expire(keyfile, ttl)
            keylog = keyt.replace("task:", "log:")
            redis.expire(keylog, ttl)

def exists(redis, task_id):
    """Checks if a task exist."""
    return redis.exists("task:" + task_id)

def create(redis, task_id, task_type, parent_task, resource, service, content, files, priority=0, ngpus=1, ncpus=None):
    """Creates a new task and enables it."""
    if ncpus is None:
        ncpus = 2
    keyt = "task:" + task_id
    redis.hset(keyt, "type", task_type)
    if parent_task:
        redis.hset(keyt, "parent", parent_task)
    redis.hset(keyt, "resource", resource)
    redis.hset(keyt, "service", service)
    redis.hset(keyt, "content", json.dumps(content))
    redis.hset(keyt, "priority", priority)
    redis.hset(keyt, "ngpus", ngpus)
    redis.hset(keyt, "ncpus", ncpus)
    for k in files:
        redis.hset("files:" + task_id, k, files[k])
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
    redis.lrem('queued:'+service, task_id)

    redis.hset(keyt, "message", phase)
    set_status(redis, keyt, "terminating")
    work_queue(redis, task_id)

def work_queue(redis, task_id, service=None, delay=0):
    if service is None:
        service = redis.hget('task:'+task_id, 'service')
    """Queues the task in the work queue with a delay."""
    if delay == 0:
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
        redis.lrem('queued:'+service, task_id)
        redis.lpush('queued:'+service, task_id)
        redis.delete('queue:'+task_id)

def enable(redis, task_id, service=None):
    if service is None:
        service = redis.hget('task:'+task_id, 'service')
    """Marks a task as enabled."""
    redis.sadd("active:"+service, task_id)

def disable(redis, task_id, service=None):
    if service is None:
        service = redis.hget('task:'+task_id, 'service')
    """Marks a task as disabled."""
    redis.srem("active:"+service, task_id)

def list_active(redis, service):
    """Returns all active tasks (i.e. non stopped)."""
    return redis.smembers("active:"+service)

def file_list(redis, task_id):
    """Returns the list of files attached to a task"""
    keyf = "files:" + task_id
    return redis.hkeys(keyf)

def info(redis, task_id, fields):
    """Gets information on a task."""
    keyt = "task:" + task_id
    field = None
    if not isinstance(fields, list):
        field = fields
        fields = [field]
    if not fields:
        # only if we want all information - add a lock on the resource
        with redis.acquire_lock(keyt):
            fields = redis.hkeys(keyt)
            fields.append("ttl")
            r=info(redis, task_id, fields)
            r['files'] = file_list(redis, task_id)
            return r
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

def delete(redis, task_id):
    """Delete a given task."""
    keyt = "task:" + task_id
    status = redis.hget(keyt, "status")
    if status is None:
        return (False, "task does not exist")
    if status != "stopped":
        return (False, "status is not stopped")
    with redis.acquire_lock(keyt):
        redis.delete(keyt)
        redis.delete("queue:" + task_id)
        redis.delete("files:" + task_id)
        redis.delete("log:" + task_id)
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

def set_file(redis, task_id, content, filename):
    keyf = "files:" + task_id
    redis.hset(keyf, filename, content)

def get_file(redis, task_id, filename):
    keyf = "files:" + task_id
    return redis.hget(keyf, filename)

def get_log(redis, task_id):
    keyf = "log:" + task_id
    return redis.get(keyf)

def append_log(redis, task_id, content):
    keyf = "log:" + task_id
    redis.append(keyf, content)

def set_log(redis, task_id, content):
    keyf = "log:" + task_id
    redis.set(keyf, content)
