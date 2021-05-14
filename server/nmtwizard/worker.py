import time
import json
import logging
import traceback
import os
import signal
import sys
import six

from nmtwizard import task, configuration as config
from nmtwizard.capacity import Capacity
from threading import Thread
from config.templates.emails import task_completed, task_failed
from utils.email_utils import EmailUtils
from string import Template
from app import app


def _compatible_resource(resource, request_resource):
    if request_resource in ['auto', resource]:
        return True
    return resource in request_resource.split(",")


def graceful_exit(signum, frame):
    sys.exit(0)


def send_task_status_notification_email(task_infos, status):
    with app.app_context():
        task_id = task_infos["id"]
        task_type = task_infos.get("type")
        model = task_infos.get("model")
        content = json.loads(task_infos["content"])
        trainer_email = content.get("trainer_email")
        trainer_name = content.get("trainer_name")
        it_email = app.get_other_config(['email_sender', 'IT_email'])
        task_link = app.get_other_config(['email_sender', 'task_link']) + task_id
        mode = app.get_other_config(['application', 'mode'])
        task_template = task_completed if status == 'completed' else task_failed
        subject_template = task_template.SUBJECT
        if mode == 'advanced':
            body_template = task_template.BODY_MODE_ADVANCED
        else:
            body_template = task_template.BODY_MODE_LITE

        email_subject = Template(subject_template).safe_substitute(task_id=task_id, task_type=task_type,
                                                                   model=model, task_status=status,
                                                                   last_name=trainer_name)
        email_body = Template(body_template).safe_substitute(task_id=task_id, task_type=task_type, model=model,
                                                             task_status=status,last_name=trainer_name, link=task_link)

        EmailUtils.send_text_mail(email_subject, email_body, [trainer_email, it_email])


class Worker(object):
    class Machine:
        def __init__(self, service, name, initial_capacity, logger):
            self._init_capacity = initial_capacity
            self._name = name
            self._available_cap = Capacity()
            self._tasks = {}
            self._logger = logger
            self._service = service

        def __str__(self):
            return "(%s:available:%s, initial: %s)" % (self._name, self._available_cap, self._init_capacity)

        def add_task(self, task_id, redis):
            if task not in self._tasks:
                redis_key = 'task:%s' % task_id
                task_capacity = Capacity(redis.hget(redis_key, 'ngpus'), redis.hget(redis_key, 'ncpus'))
                self._tasks[task_id] = task_capacity

        def set_available(self, capacity):
            self._available_cap = capacity

        def _is_authorized(self, task_entity, task_capacity):
            only_entities = self._service.get_server_detail(self._name, "entities")
            if only_entities and (task_entity not in only_entities or not only_entities[task_entity]):
                self._logger.debug('[AZ-EXCLUDED-ENTITY] %s , %s', self._name, task_entity)
                return False

            is_only_gpu_task = self._service.get_server_detail(self._name, "only_gpu_task")
            if is_only_gpu_task is True and task_capacity.ngpus <= 0:
                self._logger.debug('[AZ-EXCLUDED-GPU] task %s excluded on Gpu machine %s.', task_capacity, self._name)
                return False
            return True

    class WorkerLogger:
        def __init__(self, service, instance_id, worker_id):
            self._logger = logging.getLogger('worker')
            self._service = service
            self._instance_id = instance_id
            self._worker_id = worker_id

        def override_msg(self, msg):
            override_msg = f'[{self._service}-instance:{self._instance_id}-worker:{self._worker_id}]: {msg}'
            return override_msg

        def debug(self, msg, *args, **kwargs):
            override_msg = self.override_msg(msg)
            return self._logger.debug(override_msg, *args, **kwargs)

        def info(self, msg, *args, **kwargs):
            override_msg = self.override_msg(msg)
            return self._logger.info(override_msg, *args, **kwargs)

        def warning(self, msg, *args, **kwargs):
            override_msg = self.override_msg(msg)
            return self._logger.warning(override_msg, *args, **kwargs)

        def warn(self, msg, *args, **kwargs):
            override_msg = self.override_msg(msg)
            return self._logger.warn(override_msg, *args, **kwargs)

        def error(self, msg, *args, **kwargs):
            override_msg = self.override_msg(msg)
            return self._logger.error(override_msg, *args, **kwargs)

    def __init__(self, redis, mongo_client, services, ttl_policy, refresh_counter,
                 quarantine_time, instance_id, taskfile_dir, work_cycle):
        self._worker_id = os.getpid()
        self._redis = redis
        self._mongo_client = mongo_client
        self._service = next(iter(services))
        self._services = services
        self._logger = logging.getLogger('worker')
        self._instance_id = instance_id
        self._refresh_counter = refresh_counter
        self._quarantine_time = quarantine_time
        self._taskfile_dir = taskfile_dir
        self._work_cycle = work_cycle
        task.set_ttl_policy(ttl_policy)

    def run(self):
        signal.signal(signal.SIGTERM, graceful_exit)
        signal.signal(signal.SIGINT, graceful_exit)
        self._logger.info('Starting...')

        counter = 0

        while True:
            # process one element from work queue
            task_id = task.work_unqueue(self._redis, self._service)
            if task_id is not None:
                try:
                    self._advance_task(task_id)
                except RuntimeWarning:
                    self._logger.warning(
                        '%s: failed to acquire a lock, retrying', task_id)
                    task.work_queue(self._redis, task_id, self._service)
                except Exception as e:
                    self._logger.error('%s: %s', task_id, str(e))
                    with self._redis.acquire_lock(task_id):
                        task.set_log(self._redis, self._taskfile_dir, task_id, str(e))
                        task.terminate(self._redis, task_id, phase="launch_error")
                    self._logger.info(traceback.format_exc())
            # every 0.01s * refresh_counter - check if we can find some free resource
            if counter > self._refresh_counter:
                # if there are some queued tasks, look for free resources
                if self._redis.exists('queued:%s' % self._service):
                    self._logger.debug('checking processes on : %s', self._service)
                    self._select_best_task_to_process(self._services[self._service])
                counter = 0

            counter += 1
            time.sleep(self._work_cycle)

    def _advance_task(self, task_id):
        """Tries to advance the task to the next status. If it can, re-queue it immediately
        to process the next stage. Otherwise, re-queue it after some delay to try again.
        """
        keyt = 'task:%s' % task_id
        with self._redis.acquire_lock(keyt, acquire_timeout=1, expire_time=600):
            status = self._redis.hget(keyt, 'status')
            if status == 'stopped':
                return
            self._logger.info('%s: trying to advance from status %s', task_id, status)
            if status == 'allocated':
                self._handle_allocated_task(task_id=task_id)
            elif status == 'running':
                self._handle_running_task(task_id=task_id)
            elif status == 'terminating':
                self._handle_terminating_task(task_id=task_id)

    def _handle_allocated_task(self, task_id):
        keyt = 'task:%s' % task_id
        service_name, service = self._get_service(keyt=keyt)
        content = json.loads(self._redis.hget(keyt, 'content'))
        resource = self._redis.hget(keyt, 'alloc_resource')
        self._logger.info('%s: launching on %s', task_id, service.name)
        try:
            entity_config = self._get_current_config(task_id)
            keygr = 'gpu_resource:%s:%s' % (service.name, resource)
            lgpu = []
            for k, v in six.iteritems(self._redis.hgetall(keygr)):
                if v == task_id:
                    lgpu.append(k)
            self._redis.hset(keyt, 'alloc_lgpu', ",".join(lgpu))
            keycr = 'cpu_resource:%s:%s' % (service.name, resource)
            lcpu = []
            for k, v in six.iteritems(self._redis.hgetall(keycr)):
                if v == task_id:
                    lcpu.append(k)
            self._redis.hset(keyt, 'alloc_lcpu', ",".join(lcpu))
            data = service.launch(
                task_id,
                content['options'],
                (lgpu, lcpu),
                resource,
                entity_config["storages"],
                entity_config["docker"],
                content['docker']['registry'],
                content['docker']['image'],
                content['docker']['tag'],
                content['docker']['command'],
                task.file_list(self._redis, self._taskfile_dir, task_id),
                content['wait_after_launch'],
                self._redis.hget(keyt, 'token'),
                content.get('support_statistics'))
        except EnvironmentError as e:
            # the resource is not available and will be set busy
            self._block_resource(resource, service, str(e))
            self._redis.hdel(keyt, 'alloc_resource')
            # set the task as queued again
            self._release_resource(service, resource, task_id,
                                   Capacity(self._redis.hget(keyt, 'ngpus'),
                                            self._redis.hget(keyt, 'ncpus')
                                            ))
            task.set_status(self._redis, keyt, 'queued')
            task.service_queue(self._redis, task_id, service.name)
            self._logger.info('could not launch [%s] %s on %s: blocking resource',
                              str(e), task_id, resource)
            self._logger.info(traceback.format_exc())
            return
        except Exception as e:
            # all other errors make the task fail
            self._logger.info('fail task [%s] - %s', task_id, str(e))
            self._logger.info(traceback.format_exc())
            task.append_log(self._redis, self._taskfile_dir, task_id, str(e))
            task.terminate(self._redis, task_id, phase='launch_error')
            self._send_notification_email_when_task_failed(task_id, phase='launch_error')
            self._logger.info(traceback.format_exc())
            return
        self._logger.info('%s: task started on %s', task_id, service.name)
        self._redis.hset(keyt, 'job', json.dumps(data))
        task.set_status(self._redis, keyt, 'running')
        # For services that do not notify their activity, we should
        # poll the task status more regularly.
        task.work_queue(self._redis, task_id, service.name,
                        delay=service.is_notifying_activity and 120 or 30)

    def _handle_running_task(self, task_id):
        keyt = 'task:%s' % task_id
        service_name, service = self._get_service(keyt=keyt)
        self._logger.debug('- checking activity of task: %s', task_id)
        data = json.loads(self._redis.hget(keyt, 'job'))
        try:
            status = service.status(task_id, data)
            if status == 'dead':
                self._logger.info('%s: task no longer running on %s, request termination',
                                  task_id, service.name)
                task.terminate(self._redis, task_id, phase='exited')
            else:
                task.work_queue(self._redis, task_id, service.name,
                                delay=service.is_notifying_activity and 600 or 120)
        except Exception as e:
            self._logger.info('cannot get status for [%s] - %s', task_id, str(e))
            self._redis.hincrby(keyt, 'status_fail', 1)
            if int(self._redis.hget(keyt, 'status_fail')) > 4:
                return task.terminate(self._redis, task_id, phase='lost_connection')
            self._redis.hdel(keyt, 'status_fail')

    def _handle_terminating_task(self, task_id):
        keyt = 'task:%s' % task_id
        service_name, service = self._get_service(keyt=keyt)
        data = self._redis.hget(keyt, 'job')
        nxpus = Capacity(self._redis.hget(keyt, 'ngpus'), self._redis.hget(keyt, 'ncpus'))
        if data is not None:
            container_id = self._redis.hget(keyt, 'container_id')
            data = json.loads(data)
            data['container_id'] = container_id
            self._logger.info('%s: terminating task (job: %s)', task_id, json.dumps(data))
            try:
                service.terminate(data)
                self._logger.info('%s: terminated', task_id)
            except Exception:
                self._logger.warning('%s: failed to terminate', task_id)
                self._logger.info(traceback.format_exc())
        else:
            self._logger.info('%s: terminating task (on error)', task_id)
        resource = self._redis.hget(keyt, 'alloc_resource')
        if resource:
            self._release_resource(service, resource, task_id, nxpus)
        task.set_status(self._redis, keyt, 'stopped')
        task.disable(self._redis, task_id)

    def _get_service(self, keyt):
        service_name = self._redis.hget(keyt, 'service')
        if service_name not in self._services:
            raise ValueError('unknown service %s' % service_name)
        service = self._services[service_name]

        return service_name, service

    def _block_resource(self, resource, service, err):
        """Block a resource on which we could not launch a task
        """
        keyb = 'busy:%s:%s' % (service.name, resource)
        self._redis.set(keyb, err)
        self._redis.expire(keyb, self._quarantine_time)

    def _allocate_resource(self, task_id, request_resource, service, task_expected_capacity):
        """Allocates a resource for task_id and returns the name of the resource
           (or None if none where allocated), and the number of allocated gpus/cpus
        """
        task_entity = task.get_owner_entity(self._redis, task_id)
        resources = service.list_resources()

        # Distribute resource by type
        only_cpus_task_machines, only_gpus_task_machines, mix_task_machines = self._split_machines_by_task_support(
            resources=resources, service=service)
        is_required_gpu_task = self._is_required_gpu_task(task_expected_capacity)

        if is_required_gpu_task:
            best_resource = self._distribute_machine_for_task(task_id, task_entity, task_expected_capacity,
                                                              request_resource, service,
                                                              {**only_gpus_task_machines, **mix_task_machines})
        else:
            best_resource = self._distribute_machine_for_task(task_id, task_entity, task_expected_capacity,
                                                              request_resource, service,
                                                              only_cpus_task_machines)
            if not best_resource:
                best_resource = self._distribute_machine_for_task(task_id, task_entity, task_expected_capacity,
                                                                  request_resource, service,
                                                                  mix_task_machines)
        return best_resource

    def _split_machines_by_task_support(self, resources, service):
        only_cpus_task_machines = {}
        only_gpus_task_machines = {}
        mix_task_machines = {}
        for resource_name in resources:
            resource_specs = resources[resource_name]
            machine = Worker.Machine(service, resource_name, resource_specs, self._logger)
            is_only_gpu_task = service.get_server_detail(resource_name, "only_gpu_task")
            if is_only_gpu_task:
                only_gpus_task_machines[resource_name] = machine
                continue
            resource_gpus = resource_specs.ngpus
            if resource_gpus:
                mix_task_machines[resource_name] = machine
                continue
            only_cpus_task_machines[resource_name] = machine

        return only_cpus_task_machines, only_gpus_task_machines, mix_task_machines

    @staticmethod
    def _is_required_gpu_task(task_expected_capacity):
        task_required_ngpus = task_expected_capacity.ngpus
        return task_required_ngpus > 0

    def _distribute_machine_for_task(self, task_id, task_entity, task_expected_capacity, request_resource, service, machines):
        best_resource = None
        br_remaining_xpus = Capacity(-1, -1)
        for name, machine in six.iteritems(machines):
            if _compatible_resource(name, request_resource) and machine._is_authorized(task_entity, task_expected_capacity):
                better_remaining_xpus = self._reserve_resource(
                    service, name, machine._init_capacity, task_id,
                    task_expected_capacity, br_remaining_xpus)
                if better_remaining_xpus is not None:
                    if best_resource is not None:
                        self._release_resource(service, best_resource, task_id, task_expected_capacity)
                    best_resource = name
                    br_remaining_xpus = better_remaining_xpus
        return best_resource

    def _reserve_resource(self, service, resource, capacity, task_id, task_asked_capacity, br_remaining_xpus):
        """Reserves the resource for task_id, if possible. The resource is locked
        while we try to reserve it.
        Resource should have more gpus available (within ngpus) than br_available_xpus
        or the same number but a smaller size
        """
        self._logger.debug('service.name = %s', service.name)
        self._logger.debug('resource = %s', resource)
        self._logger.debug('capacity = (%d, %d)', capacity.ngpus, capacity.ncpus)
        self._logger.debug('task_id = %s', task_id)
        self._logger.debug('nxpus = (%d, %d)', task_asked_capacity.ngpus, task_asked_capacity.ncpus)
        self._logger.debug('br_remaining_xpus = (%d, %d)', br_remaining_xpus.ngpus, br_remaining_xpus.ncpus)

        for idx, val in enumerate(capacity):
            if val < task_asked_capacity[idx]:
                return None

        keygr = 'gpu_resource:%s:%s' % (service.name, resource)
        keycr = 'cpu_resource:%s:%s' % (service.name, resource)
        key_busy = 'busy:%s:%s' % (service.name, resource)

        with self._redis.acquire_lock(keygr):
            if self._redis.get(key_busy) is not None:
                return None
            # if we need gpus
            remaining_gpus = 0
            remaining_cpus = 0

            # allocate GPU first. For GPU we want to minimise the fragmentation, so minimize
            # br_remainining_xpus.ngpus

            current_usage_cpu = self._redis.hlen(keycr)
            self._logger.debug('current_usage_cpu = %d', current_usage_cpu)
            if current_usage_cpu > 0 and not service.resource_multitask:
                return None
            avail_cpu = capacity.ncpus - current_usage_cpu
            if task_asked_capacity.ncpus > avail_cpu:
                return None

            if task_asked_capacity.ngpus != 0:
                # do not allocate several run on the same GPU
                current_usage_gpu = self._redis.hlen(keygr)
                self._logger.debug('current_usage_gpu = %d', current_usage_gpu)
                if current_usage_gpu > 0 and not service.resource_multitask:
                    return None
                # available gpu is the capacity of the node less number of gpu used
                avail_gpu = capacity.ngpus - current_usage_gpu
                self._logger.debug('avail_gpu = %d', avail_gpu)

                if task_asked_capacity.ngpus > avail_gpu:
                    return None

                remaining_gpus = avail_gpu - task_asked_capacity.ngpus
                self._logger.debug('remaining_gpus = %d', remaining_gpus)

                if br_remaining_xpus.ngpus != -1 and remaining_gpus >= br_remaining_xpus.ngpus:
                    return None

            remaining_cpus = avail_cpu - task_asked_capacity.ncpus
            self._logger.debug('remaining_cpus = %d', remaining_cpus)

            # for mono task service, allocate node with lowest cpu number
            if service.resource_multitask:
                better_cpu_usage = remaining_cpus > br_remaining_xpus.ncpus
            else:
                better_cpu_usage = remaining_cpus < br_remaining_xpus.ncpus

            if br_remaining_xpus.ncpus != -1 and not better_cpu_usage:
                return None

            idx = 1
            for i in range(task_asked_capacity.ngpus):
                while self._redis.hget(keygr, str(idx)) is not None:
                    idx += 1
                    assert idx <= capacity.ngpus, "invalid gpu alloc for %s" % keygr
                self._logger.debug('reserve GPU idx = %d', idx)
                self._redis.hset(keygr, str(idx), task_id)

            cpu_idx = 0
            for i in range(task_asked_capacity.ncpus):
                while self._redis.hget(keycr, str(cpu_idx)) is not None:
                    cpu_idx += 1
                    assert cpu_idx <= capacity.ncpus, "invalid cpu alloc for %s" % keycr
                self._logger.debug('reserve CPU idx = %d', cpu_idx)
                self._redis.hset(keycr, str(cpu_idx), task_id)

            return Capacity(remaining_gpus, remaining_cpus)

    def _release_resource(self, service, resource, task_id, nxpus):
        """remove the task from resource queue
        """
        self._logger.debug('releasing resource:%s on service: %s for %s %s',
                           resource, service.name, task_id, nxpus)
        if nxpus.ngpus != 0:
            keygr = 'gpu_resource:%s:%s' % (service.name, resource)
            with self._redis.acquire_lock(keygr):
                for k, v in six.iteritems(self._redis.hgetall(keygr)):
                    if v == task_id:
                        self._redis.hdel(keygr, k)
        if nxpus.ncpus != 0:
            keycr = 'cpu_resource:%s:%s' % (service.name, resource)
            with self._redis.acquire_lock(keycr):
                for k, v in six.iteritems(self._redis.hgetall(keycr)):
                    if v == task_id:
                        self._redis.hdel(keycr, k)

    def _select_best_task_to_process(self, service):
        """find the best next task to push to the work queue
        """
        class EntityUsage:
            def __init__(self, current_usage, entity_name, usage_coeff):
                self._entity = entity_name
                self._current_usage_capacity = current_usage if current_usage else Capacity()
                self._usage_coeff = usage_coeff

            def __str__(self):
                return 'EntityUsage (%s, Absolute usage :%s . Weighted usage : %s. Weight:%f)' % (
                    self._entity, self._current_usage_capacity, self._weighted_usage, self._usage_coeff)

            @property
            def _weighted_usage(self):
                return self._current_usage_capacity.ncpus * self._usage_coeff, self._current_usage_capacity.ngpus * self._usage_coeff

            def add_current_usage(self, current_usage):
                self._current_usage_capacity += current_usage

            def __eq__(self, other):
                return self._weighted_usage[0] == other._weighted_usage[0] and self._weighted_usage[1] == other._weighted_usage[1]

            def __lt__(self, other):
                return self._weighted_usage[1] < other._weighted_usage[1] or \
                       (self._weighted_usage[1] == other._weighted_usage[1] and self._weighted_usage[0] < other._weighted_usage[0])

            def __le__(self, other):
                return self == other or self < other

            @staticmethod
            def initialize_entities_usage(mongo_client, service_name):
                entity_usage_weights = config.get_entities_limit_rate(mongo_client, service_name)
                weight_sum = float(sum([w for w in entity_usage_weights.values() if w > 0]))
                entities_usage = {e: EntityUsage(None, e, float(weight_sum)/r if r > 0 else 0)
                                  for e, r in six.iteritems(entity_usage_weights)}
                return entities_usage

        class CandidateTask:
            def __init__(self, task_id, task_entity, redis, task_capacity, entity_usage, logger):
                assert task_id
                self._task_id = task_id
                self._entity = task_entity
                self._redis_key = 'task:%s' % next_task_id
                self._priority = int(redis.hget(self._redis_key, 'priority'))
                self._launched_time = float(redis.hget(self._redis_key, 'launched_time'))
                self._runnable_machines = set()
                self._capacity = task_capacity
                self._entity_usage = entity_usage
                self._logger = logger

            def __str__(self):
                return "Task ( %s / %s ; %s ; Priority:%d)" % (self._task_id, self._capacity, self._entity_usage, self._priority)

            def __gt__(self, other):
                return self.is_higher_priority(other)

            def __ge__(self, other):
                return self.is_higher_priority(other)

            def _already_on_node(self):
                result = self._task_id in resource_mgr.preallocated_task_resource
                return result

            def _is_more_respectful_usage(self, other):
                if self._entity == other._entity:  # same entity, go for highest priority
                    is_more_prio = self._priority > other._priority or (self._priority == other._priority and self._launched_time < other._launched_time)
                    return is_more_prio
                my_entity_usage = resource_mgr.entities_usage[self._entity]
                other_entity_usage = resource_mgr.entities_usage[other._entity]
                if my_entity_usage == other_entity_usage:
                    return self._launched_time < other._launched_time

                result = my_entity_usage < other_entity_usage
                self._logger.debug("AZ-COMPUSE: my: %s.Other: %s . Result = %s", my_entity_usage, other_entity_usage, result)
                return result

            def is_higher_priority(self, other_task):
                # Decision tree for the most priority task
                if not other_task:
                    return True

                # go for already allocated resource task
                if self._already_on_node():
                    if not other_task._already_on_node():
                        return True

                    return self._is_more_respectful_usage(other_task)
                if other_task._already_on_node():
                    return False
                return self._is_more_respectful_usage(other_task)

            @staticmethod
            def try_create(next_task_id):
                next_keyt = 'task:%s' % next_task_id
                parent = self._redis.hget(next_keyt, 'parent')
                task_entity = task.get_owner_entity(self._redis, next_task_id)

                if task_entity not in resource_mgr.entities_usage:
                    self._logger.error("\t[Task %s] entity %s - without usage limit !", next_task_id, task_entity)
                    return None

                # check parent dependency
                if parent:
                    keyp = 'task:%s' % parent
                    if self._redis.exists(keyp):
                        # if the parent task is in the database, check for dependencies
                        parent_status = self._redis.hget(keyp, 'status')
                        if parent_status != 'stopped':
                            if parent_status == 'running':
                                # parent is still running so update queued time to be as close
                                # as possible to terminate time of parent task
                                self._redis.hset(next_keyt, "queued_time", time.time())
                            return None

                        if self._redis.hget(keyp, 'message') != 'completed':
                            task.terminate(self._redis, next_task_id, phase='dependency_error')
                            self._send_notification_email_when_task_failed(next_task_id, phase='dependency_error')
                            return None

                task_capacity = Capacity(self._redis.hget(next_keyt, 'ngpus'), self._redis.hget(next_keyt, 'ncpus'))
                candidate_task = CandidateTask(next_task_id, task_entity, self._redis, task_capacity, resource_mgr.entities_usage[task_entity], self._logger)
                # check now the task has a chance to be processed by any machine
                for srv, machine in six.iteritems(resource_mgr._machines):
                    can_be_processed = machine._is_authorized(candidate_task._entity, candidate_task._capacity) \
                                       and candidate_task._capacity.inf_or_eq(machine._init_capacity)
                    if can_be_processed:
                        return candidate_task

                return None

        class ResourceManager:
            def __init__(self, worker):
                self.preallocated_task_resource = {}
                resources = service.list_resources()
                self._machines = {res: Worker.Machine(service, res, resources[res], worker._logger) for res in resources}
                self.entities_usage = {}
                self.worker = worker

            def __str__(self):
                msg = " - ".join(str(m) for m in self._machines.values())
                return "ResourceManager ( %s )." % msg

            def load_machines(self, service_name):
                self.entities_usage = EntityUsage.initialize_entities_usage(self.worker._mongo_client, service_name)
                for resource, machine in six.iteritems(self._machines):
                    current_xpu_usage = Capacity()
                    keygr = 'gpu_resource:%s:%s' % (self.worker._service, resource)
                    keycr = 'cpu_resource:%s:%s' % (self.worker._service, resource)

                    gpu_tasks = self.worker._redis.hgetall(keygr)
                    cpu_tasks = self.worker._redis.hgetall(keycr)

                    # can not launch multiple tasks on service with no multi-tasking (ec2)
                    if not service.resource_multitask and (gpu_tasks or cpu_tasks):
                        continue
                    tmp_tasks = {}
                    for k, v in six.iteritems(gpu_tasks):
                        if v not in tmp_tasks:
                            task_entity = task.get_owner_entity(self.worker._redis, v)
                            tmp_tasks[v] = task_entity
                        else:
                            task_entity = tmp_tasks[v]

                        if v not in self.preallocated_task_resource:
                            self.preallocated_task_resource[v] = resource
                        self._machines[resource].add_task(v, self.worker._redis)
                        current_xpu_usage.incr_ngpus(1)
                        self.entities_usage[task_entity].add_current_usage(Capacity(ngpus=1))

                    for k, v in six.iteritems(cpu_tasks):
                        if v not in tmp_tasks:
                            task_entity = task.get_owner_entity(self.worker._redis, v)
                            tmp_tasks[v] = task_entity
                        else:
                            task_entity = tmp_tasks[v]

                        if v not in self.preallocated_task_resource:
                            self.preallocated_task_resource[v] = resource

                        self._machines[resource].add_task(v, self.worker._redis)
                        current_xpu_usage.incr_ncpus(1)
                        self.entities_usage[task_entity].add_current_usage(Capacity(ncpus=1))

                    available_xpus = machine._init_capacity - current_xpu_usage
                    self._machines[resource].set_available(available_xpus)
                    self.worker._logger.debug("\tresource %s: - free %s", resource, available_xpus)

                return len(resource_mgr._machines) > 0

        with self._redis.acquire_lock('service:'+service.name):
            queue = 'queued:%s' % service.name
            count = self._redis.llen(queue)
            if count == 0:
                return

            resource_mgr = ResourceManager(self)
            if not resource_mgr.load_machines(service.name):
                return

            runnable_tasks = []
            for e in resource_mgr.entities_usage.values():
                self._logger.debug("[AZ-USE] %s" % e)
            while count > 0:
                count -= 1
                next_task_id = self._redis.lindex(queue, count)
                candidate_task = CandidateTask.try_create(next_task_id)
                if candidate_task:
                    runnable_tasks.append(candidate_task)
            num_of_runnable_tasks = len(runnable_tasks)
            self._logger.info('Runnable task count: %d', num_of_runnable_tasks)
            if num_of_runnable_tasks > 0:
                sorted_runnable_tasks = sorted(runnable_tasks, reverse=True)
                for runnable_task in sorted_runnable_tasks:
                    task_id = runnable_task._task_id
                    nxpus = runnable_task._capacity
                    keyt = 'task:%s' % task_id
                    request_resource = self._redis.hget(keyt, 'resource')
                    allocated_resource = self._allocate_resource(task_id, request_resource, service, nxpus)
                    if allocated_resource is not None:
                        self._logger.info('%s: resource %s reserved %s', task_id, allocated_resource, nxpus)
                        self._redis.hset(keyt, 'alloc_resource', allocated_resource)
                        task.set_status(self._redis, keyt, 'allocated')
                        task.work_queue(self._redis, task_id, service.name)
                        self._redis.lrem(queue, 0, task_id)
                        self._logger.info('[AZ-SELECTED] %s to be launched on %s', task_id, service.name)
                        break
                    self._logger.info('[AZ-SELECTED] %s to be launched on %s, but not able to allocate resource',
                                      task_id, service.name)

    def _get_current_config(self, task_id):
        task_entity = task.get_owner_entity(self._redis, task_id)
        storages_entities_filter = task.get_storages_entity(self._redis, task_id)
        current_config = config.get_entity_config(self._mongo_client, self._service, storages_entities_filter, task_entity)
        return current_config

    def _send_notification_email_when_task_failed(self, task_id, phase):
        infos = task.info(self._redis, self._taskfile_dir, task_id,
                          ["type", "content", "queued_time",
                           "running_time", "priority", "ngpus",
                           "ncpus", "alloc_resource", "statistics",
                           "owner", "evaluation_id", "eval_model",
                           "model"])
        Thread(target=send_task_status_notification_email, args=({**infos, **{"id": task_id}}, phase)).start()

