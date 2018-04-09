import time
import json
import logging
import six

from nmtwizard import task


class Worker(object):

    def __init__(self, redis, services, refresh_counter, quarantine_time, index=0):
        self._redis = redis
        self._services = services
        self._logger = logging.getLogger('worker%d' % index)
        self._refresh_counter = refresh_counter
        self._quarantine_time = quarantine_time

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
                    self._logger.warning('received expired event on channel %s', channel)
                    if channel.startswith('__keyspace@0__:beat:'):
                        task_id = channel[20:]
                        self._logger.info('%s: task expired', task_id)
                        with self._redis.acquire_lock(task_id):
                            task.terminate(self._redis, task_id, phase='expired')
                    elif channel.startswith('__keyspace@0__:queue:'):
                        task_id = channel[21:]
                        task.work_queue(self._redis, task_id)
            else:
                task_id = task.work_unqueue(self._redis)
                if task_id is not None:
                    try:
                        self._advance_task(task_id)
                    except RuntimeWarning:
                        self._logger.warning(
                            '%s: failed to acquire a lock, retrying', task_id)
                        task.work_queue(self._redis, task_id)
                    except Exception as e:
                        self._logger.error('%s: %s', task_id, str(e))
                        with self._redis.acquire_lock(task_id):
                            task.set_log(self._redis, task_id, str(e))
                            task.terminate(self._redis, task_id, phase="launch_error")
                else:
                    if counter > self._refresh_counter:
                        # check if a resource is under-used and if so try pulling some
                        # task for it
                        for service in self._services:
                            if self._redis.exists('queued:%s' % service):
                                resources = self._services[service].list_resources()
                                self._logger.debug('checking processes on : %s', service)
                                availableResource = False
                                for resource in resources:
                                    keyr = 'resource:%s:%s' % (service, resource)
                                    if self._redis.llen(keyr) < resources[resource]:
                                        availableResource = True
                                        break
                                if availableResource:
                                    self._logger.debug('resources available on %s - trying dequeuing', service)
                                    self._service_unqueue(self._services[service])
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
                            task.service_queue(self._redis, task_id, service.name)
                            return
                resource = self._allocate_resource(task_id, resource, service)
                if resource is not None:
                    self._logger.info('%s: resource %s reserved', task_id, resource)
                    self._redis.hset(keyt, 'alloc_resource', resource)
                    task.set_status(self._redis, keyt, 'allocated')
                    task.work_queue(self._redis, task_id)
                else:
                    self._logger.warning('%s: no resources available, waiting', task_id)
                    task.service_queue(self._redis, task_id, service.name)

            elif status == 'allocated':
                content = json.loads(self._redis.hget(keyt, 'content'))
                resource = self._redis.hget(keyt, 'alloc_resource')
                self._logger.info('%s: launching on %s', task_id, service.name)
                try:
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
                except Exception as e:
                    # the resource is not available and will be set busy
                    self._block_resource(resource, service, str(e))
                    # set the task as queued again
                    self._redis.hdel(keyt, 'alloc_resource')
                    self._release_resource(service, resource, task_id)
                    task.set_status(self._redis, keyt, 'queued')
                    task.service_queue(self._redis, task_id, service.name)
                    self._logger.info('could not launch %s on %s: blocking resource', task_id, resource)
                    return
                self._logger.info('%s: task started on %s', task_id, service.name)
                self._redis.hset(keyt, 'job', json.dumps(data))
                task.set_status(self._redis, keyt, 'running')
                # For services that do not notify their activity, we should
                # poll the task status more regularly.
                task.work_queue(self._redis, task_id, delay=service.is_notifying_activity and 120 or 30)

            elif status == 'running':
                self._logger.debug('- checking activity of task: %s', task_id)
                data = json.loads(self._redis.hget(keyt, 'job'))
                status = service.status(task_id, data)
                if status == 'dead':
                    self._logger.info('%s: task no longer running on %s, request termination',
                                      task_id, service.name)
                    task.terminate(self._redis, task_id, phase='exited')
                else:
                    task.work_queue(self._redis, task_id, delay=service.is_notifying_activity and 120 or 30)

            elif status == 'terminating':
                data = self._redis.hget(keyt, 'job')
                if data is not None:
                    container_id = self._redis.hget(keyt, 'container_id')
                    data = json.loads(data)
                    data['container_id'] = container_id
                    self._logger.info('%s: terminating task (%s)', task_id, json.dumps(data))
                    try:
                        service.terminate(data)
                        self._logger.info('%s: terminated', task_id)
                    except Exception:
                        self._logger.warning('%s: failed to terminate', task_id)
                resource = self._redis.hget(keyt, 'alloc_resource')
                self._release_resource(service, resource, task_id)
                task.set_status(self._redis, keyt, 'stopped')
                task.disable(self._redis, task_id)

    def _block_resource(self, resource, service, err):
        """Block a resource on which we could not launch a task
        """
        keyb = 'busy:%s:%s' % (service.name, resource)
        self._redis.set(keyb, err)
        self._redis.expire(keyb, self._quarantine_time)

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
        keyb = 'busy:%s:%s' % (service.name, resource)
        with self._redis.acquire_lock(keyr):
            if self._redis.get(keyb) is not None:
                return False
            current_usage = self._redis.llen(keyr)
            if current_usage < capacity:
                self._redis.rpush(keyr, task_id)
                return True
            else:
                return False

    def _release_resource(self, service, resource, task_id):
        """remove the task from resource queue
        """
        keyr = 'resource:%s:%s' % (service.name, resource)
        self._redis.lrem(keyr, task_id)

    def _service_unqueue(self, service):
        """find the best next task to push to the work queue
        """
        with self._redis.acquire_lock('service:'+service.name):
            queue = 'queued:%s' % service.name
            count = self._redis.llen(queue)
            idx = 0
            # Pop a task waiting for a resource on this service, check if it can run (dependency)
            # and queue it for a retry.
            best_task_id = None
            best_task_priority = -10000
            best_task_queued_time = 0
            while count > 0:
                count -= 1
                next_task_id = self._redis.lindex(queue, count)
                if next_task_id is not None:
                    next_keyt = 'task:%s' % next_task_id
                    parent = self._redis.hget(next_keyt, 'parent')
                    priority = int(self._redis.hget(next_keyt, 'priority'))
                    queued_time = float(self._redis.hget(next_keyt, 'queued_time'))
                    if parent:
                        keyp = 'task:%s' % parent
                        if self._redis.exists(keyp):
                            # if the parent task is in the database, check for dependencies
                            if self._redis.hget(keyp, 'status') != 'stopped':
                                continue
                    if priority > best_task_priority or (
                        priority == best_task_priority and best_task_queued_time > queued_time):
                        best_task_priority = priority
                        best_task_id = next_task_id
                        best_task_queued_time = queued_time

            if best_task_id:
                task.work_queue(self._redis, best_task_id)
                self._redis.lrem(queue, best_task_id)
