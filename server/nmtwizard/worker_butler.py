import sys
import os
import signal
import time
import logging
import requests

from nmtwizard import task


def graceful_exit(signum, frame):  # pylint: disable=unused-argument
    sys.exit(0)


class WorkerButler:
    class WorkerButlerLogger:
        def __init__(self, service, instance_id):
            self._logger = logging.getLogger(__name__)
            self._service = service
            self._instance_id = instance_id

        def override_msg(self, msg):
            override_msg = f'[{self._service}-instance:{self._instance_id}]: {msg}'
            return override_msg

        def info(self, msg, *args, **kwargs):
            override_msg = self.override_msg(msg)
            return self._logger.info(override_msg, *args, **kwargs)

        def warning(self, msg, *args, **kwargs):
            override_msg = self.override_msg(msg)
            return self._logger.warning(override_msg, *args, **kwargs)

    def __init__(self, redis, services, instance_id, work_cycle):
        service = next(iter(services))
        worker_butler_id = os.getpid()
        self._worker_butler_id = worker_butler_id
        self._redis = redis
        self._services = services
        self._instance_id = instance_id
        self._work_cycle = work_cycle
        self._logger = self.WorkerButlerLogger(service, instance_id)

    def run(self):
        signal.signal(signal.SIGTERM, graceful_exit)
        signal.signal(signal.SIGINT, graceful_exit)

        pubsub = self._redis.pubsub()
        pubsub.psubscribe('__keyspace@0__:beat:*')
        pubsub.psubscribe('__keyspace@0__:queue:*')

        while True:
            message = pubsub.get_message()
            if message:
                channel = message['channel']
                data = message['data']
                if data == 'expired':
                    # task expired, not beat was received
                    if channel.startswith('__keyspace@0__:beat:'):
                        task_id = channel[20:]
                        service = self._redis.hget('task:' + task_id, 'service')
                        if service in self._services:
                            self._logger.info('%s: task expired', task_id)
                            auth_token = self._redis.hget('task:%s' % task_id, 'token')
                            callback_url = self._services[service]._config.get('callback_url')
                            if auth_token:
                                callback_url = callback_url.replace("://", "://" + auth_token + ":x@")
                            r = requests.get(os.path.join(callback_url, "task/terminate", task_id),
                                             params={'phase': 'expired'})
                            if r.status_code != 200:
                                self._logger.warning('incorrect result from \'task/terminate\' service: %s' % r.text)
                            with self._redis.acquire_lock(task_id):
                                task.terminate(self._redis, task_id, phase='expired')
                    # expired in the queue - comes back in the work queue
                    elif channel.startswith('__keyspace@0__:queue:'):
                        task_id = channel[21:]
                        service = self._redis.hget('task:' + task_id, 'service')
                        if service in self._services:
                            self._logger.info('%s: move to work queue', task_id)
                            task.work_queue(self._redis, task_id, service)

            time.sleep(self._work_cycle)
