import time
import json

def set_status(redis, keyt, status):
    """Sets the status and save the time of change."""
    redis.hset(keyt, "status", status)
    redis.hset(keyt, status + "_time", time.time())

def exists(redis, task_id):
    """Checks if a task exist."""
    return redis.exists("task:" + task_id)

def create(redis, task_id, task_type, parent_task, resource, service, content, files):
    """Creates a new task and enables it."""
    keyt = "task:" + task_id
    redis.hset(keyt, "type", task_type)
    if parent_task:
        redis.hset(keyt, "parent", parent_task)
    redis.hset(keyt, "resource", resource)
    redis.hset(keyt, "service", service)
    redis.hset(keyt, "content", json.dumps(content))
    set_status(redis, keyt, "queued")
    for k in files:
        redis.hset("files:" + task_id, k, files[k])
    enable(redis, task_id)
    queue(redis, task_id)

def terminate(redis, task_id, phase):
    """Requests task termination (assume it is locked)."""
    if phase is None:
        phase = "aborted"
    keyt = "task:" + task_id
    if redis.hget(keyt, "status") in ("terminating", "stopped"):
        return
    redis.hset(keyt, "message", phase)
    set_status(redis, keyt, "terminating")
    queue(redis, task_id)

def queue(redis, task_id, delay=0):
    """Queues the task in the work queue with a delay."""
    if delay == 0:
        redis.lpush('work', task_id)
        redis.delete('queue:'+task_id)
    else:
        redis.set('queue:'+task_id, delay)
        redis.expire('queue:'+task_id, int(delay))

def unqueue(redis):
    """Pop a task from the work queue."""
    return redis.rpop('work')

def enable(redis, task_id):
    """Marks a task as enabled."""
    redis.sadd("active", task_id)

def disable(redis, task_id):
    """Marks a task as disabled."""
    redis.srem("active", task_id)

def list_active(redis):
    """Returns all active tasks (i.e. non stopped)."""
    return redis.smembers("active")

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
        return (False, "task does not exists")
    if status != "stopped":
        return (False, "status is not stopped")
    with redis.acquire_lock(keyt):
        redis.delete(keyt)
        redis.delete("queue:" + task_id)
        redis.delete("files:" + task_id)
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
    return get_file(redis, task_id, "log")
