import time
import json
import logging
import six

from nmtwizard import task


class Worker(object):

    def __init__(self, redis, services, refresh_counter, index=0):
        self._redis = redis
        self._services = services
        self._logger = logging.getLogger('worker%d' % index)
        self._refresh_counter = refresh_counter

    def run(self):
        self._logger.info('Starting worker')

        # Subscribe to beat expiration.
        pubsub = self._redis.pubsub()
        pubsub.psubscribe('__keyspace@0__:beat:*')
        pubsub.psubscribe('__keyspace@0__:queue:*')
        counter = 0

        while True:
            message = pubsub.get_message()
            if message:
                channel = message['channel']
                data = message['data']
                if data == 'expired':
                    if channel.startswith('__keyspace@0__:beat:'):
                        task_id = channel[20:]
                        self._logger.info('%s: task expired', task_id)
                        with self._redis.acquire_lock(task_id):
                            task.terminate(self._redis, task_id, phase='expired')
                    elif channel.startswith('__keyspace@0__:queue:'):
                        task_id = channel[21:]
                        task.queue(self._redis, task_id)
            else:
                task_id = task.unqueue(self._redis)
                if task_id is not None:
                    try:
                        self._advance_task(task_id)
                    except RuntimeWarning:
                        self._logger.warning(
                            '%s: failed to acquire a lock, retrying', task_id)
                        task.queue(self._redis, task_id)
                    except Exception as e:
                        self._logger.error('%s: %s', task_id, str(e))
                        with self._redis.acquire_lock(task_id):
                            task.terminate(self._redis, task_id, phase="launch_error")
                else:
                    if counter > self._refresh_counter:
                        # check if a resource is under-used and if so try pulling some
                        # task for it
                        for service in self._services:
                            resources = self._services[service].list_resources()
                            for resource in resources:
                                keyr = 'resource:%s:%s' % (service, resource)
                                if self._redis.llen(keyr) < resources[resource]:
                                    self._release_resource(self._services[service], resource)
                        counter = 0
            counter += 1
            time.sleep(0.01)

    def _advance_task(self, task_id):
        """Tries to advance the task to the next status. If it can, re-queue it immediately
        to process the next stage. Otherwise, re-queue it after some delay to try again.
        """
        keyt = 'task:%s' % task_id
        with self._redis.acquire_lock(keyt, acquire_timeout=1, expire_time=600):
            status = self._redis.hget(keyt, 'status')
            if status == 'stopped':
                return

            service_name = self._redis.hget(keyt, 'service')
            if service_name not in self._services:
                raise ValueError('unknown service %s' % service_name)
            service = self._services[service_name]

            self._logger.info('%s: trying to advance from status %s', task_id, status)

            if status == 'queued':
                resource = self._redis.hget(keyt, 'resource')
                parent = self._redis.hget(keyt, 'parent')
                if parent:
                    keyp = 'task:%s' % parent
                    # if the parent task is in the database, check for dependencies
                    if self._redis.exists(keyp):
                        status = self._redis.hget(keyp, 'status')
                        if status == 'stopped':
                            if self._redis.hget(keyp, 'message') != 'completed':
                                task.terminate(self._redis, task_id, phase='dependency_error')
                                return
                        else:
                            self._logger.warning('%s: depending on other task, waiting', task_id)
                            self._wait_for_resource(service, task_id)
                            return
                resource = self._allocate_resource(task_id, resource, service)
                if resource is not None:
                    self._logger.info('%s: resource %s reserved', task_id, resource)
                    self._redis.hset(keyt, 'resource', resource)
                    task.set_status(self._redis, keyt, 'allocated')
                    task.queue(self._redis, task_id)
                else:
                    self._logger.warning('%s: no resources available, waiting', task_id)
                    self._wait_for_resource(service, task_id)

            elif status == 'allocated':
                content = json.loads(self._redis.hget(keyt, 'content'))
                resource = self._redis.hget(keyt, 'resource')
                self._logger.info('%s: launching on %s', task_id, service.name)
                data = service.launch(
                    task_id,
                    content['options'],
                    resource,
                    content['docker']['registry'],
                    content['docker']['image'],
                    content['docker']['tag'],
                    content['docker']['command'],
                    task.file_list(self._redis, task_id),
                    content['wait_after_launch'])
                self._logger.info('%s: task started on %s', task_id, service.name)
                self._redis.hset(keyt, 'job', json.dumps(data))
                task.set_status(self._redis, keyt, 'running')
                # For services that do not notify their activity, we should
                # poll the task status more regularly.
                task.queue(self._redis, task_id, delay=service.is_notifying_activity and 120 or 30)

            elif status == 'running':
                data = json.loads(self._redis.hget(keyt, 'job'))
                status = service.status(data)
                if status == 'dead':
                    self._logger.info('%s: task no longer running on %s, request termination',
                                      task_id, service.name)
                    task.terminate(self._redis, task_id, phase='exited')
                else:
                    task.queue(self._redis, task_id, delay=service.is_notifying_activity and 120 or 30)

            elif status == 'terminating':
                data = self._redis.hget(keyt, 'job')
                if data is not None:
                    data = json.loads(data)
                    self._logger.info('%s: terminating task', task_id)
                    try:
                        service.terminate(data)
                        self._logger.info('%s: terminated', task_id)
                    except Exception:
                        self._logger.warning('%s: failed to terminate', task_id)
                resource = self._redis.hget(keyt, 'resource')
                self._release_resource(service, resource, task_id)
                task.set_status(self._redis, keyt, 'stopped')
                task.disable(self._redis, task_id)

    def _allocate_resource(self, task_id, resource, service):
        """Allocates a resource for task_id and returns the name of the resource
        (or None if none where allocated).
        """
        resources = service.list_resources()
        if resource == 'auto':
            for name, capacity in six.iteritems(resources):
                if self._reserve_resource(service, name, capacity, task_id):
                    return name
        elif resource not in resources:
            raise ValueError('resource %s does not exist for service %s' % (resource, service.name))
        elif self._reserve_resource(service, resource, resources[resource], task_id):
            return resource
        return None

    def _reserve_resource(self, service, resource, capacity, task_id):
        """Reserves the resource for task_id, if possible. The resource is locked
        while we try to reserve it.
        """
        keyr = 'resource:%s:%s' % (service.name, resource)
        with self._redis.acquire_lock(keyr):
            current_usage = self._redis.llen(keyr)
            if current_usage < capacity:
                self._redis.rpush(keyr, task_id)
                return True
            else:
                return False

    def _release_resource(self, service, resource, task_id=None):
        """If task_id is not None - remove the task from resource queue
           Push one task, if any from the queue and push it on the current processing queue
        """
        keyr = 'resource:%s:%s' % (service.name, resource)
        if task_id is not None:
            self._redis.lrem(keyr, task_id)
        queue = 'queued:%s' % service.name
        count = self._redis.llen(queue)
        # Pop a task waiting for a resource on this service, check if it can run (dependency)
        # and queue it for a retry.
        while count > 0:
            next_task_id = self._redis.rpop(queue)
            if next_task_id is not None:
                next_keyt = 'task:%s' % next_task_id
                parent = self._redis.hget(next_keyt, 'parent')
                if parent:
                    keyp = 'task:%s' % parent
                    if self._redis.exists(keyp):
                        # if the parent task is in the database, check for dependencies
                        if self._redis.hget(keyp, 'status') != 'stopped':
                            self._redis.lpush(queue, next_task_id)
                            count -= 1
                            next
                task.queue(self._redis, next_task_id)
            return

    def _wait_for_resource(self, service, task_id):
        # can not have same task twice in a service queue
        self._redis.lrem('queued:%s' % service.name, task_id)
        self._redis.lpush('queued:%s' % service.name, task_id)
