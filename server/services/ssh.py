import logging
import time

from nmtwizard import common
from nmtwizard.service import Service
from nmtwizard.capacity import Capacity

logger = logging.getLogger(__name__)


def _hostname(server):
    if 'name' in server:
        return server['name']
    if server['port'] == 22:
        return server['host']
    return "%s:%s" % (server['host'], server['port'])


def _get_params(config, options):
    params = {}
    if 'server' not in options:
        server_pool = config['variables']['server_pool']
        if len(server_pool) > 1:
            raise ValueError('server option is required to select a server and a resource')
        resource = _hostname(config['variables']['server_pool'][0])
        options['server'] = resource

    params['server'] = options['server']

    servers = {_hostname(server): server for server in config['variables']['server_pool']}

    if params['server'] not in servers:
        raise ValueError('server %s not in server_pool list' % params['server'])
    params['gpus'] = servers[params['server']]['gpus']
    params['cpus'] = servers[params['server']]['cpus']

    server_cfg = servers[params['server']]

    if 'login' not in server_cfg and 'login' not in options:
        raise ValueError('login not found in server configuration or user options')
    if 'log_dir' not in server_cfg:
        raise ValueError('missing log_dir in the configuration')

    params['login'] = server_cfg.get('login', options.get('login'))
    params['log_dir'] = server_cfg['log_dir']
    params['login_cmd'] = server_cfg.get('login_cmd')
    params['port'] = server_cfg['port']
    params['host'] = server_cfg['host']

    return params


class SSHService(Service):

    def __init__(self, config):
        for server in config['variables']['server_pool']:
            if 'gpus' not in server:
                server['gpus'] = []
            if 'port' not in server:
                server['port'] = 22
            if 'ncpus' in server:
                if 'cpus' in server and len(server['cpus']) != server['ncpus']:
                    raise ValueError("inconsistent ncpus and cpus option for server `%s`" % server)
                server['cpus'] = list(range(server['ncpus']))
            if 'cpus' not in server or len(server['cpus']) == 0:
                raise ValueError("cpus cannot be empty for server `%s`" % server)
        super(SSHService, self).__init__(config)
        server_pool = self._config['variables']['server_pool']
        self._machines = {_hostname(server): server for server in server_pool}
        self._resources = self._list_all_gpus()

    def resource_multitask(self):
        return True

    def _list_all_gpus(self):
        gpus = []
        for server in self._config['variables']['server_pool']:
            for gpu in server['gpus']:
                gpus.append('%s[%d]' % (_hostname(server), gpu))
        return gpus

    def get_server_detail(self, server, field_name):
        # here, server must exist
        return self._machines[server].get(field_name)

    def list_resources(self):
        resources = {server: Capacity(len(self._machines[server]['gpus']), len(self._machines[server]['cpus'])) for server in self._machines}
        return resources

    def get_resource_from_options(self, options):
        if "server" not in options:
            return "auto"
        return options["server"]

    def describe(self):
        has_login = False
        for server in self._config['variables']['server_pool']:
            if 'login' in server:
                has_login = True
                break
        desc = {}
        if len(self._resources) > 1:
            desc['server'] = {
                "title": "server",
                "type": "string",
                "description": "server:gpu",
                "enum": self._resources + ["auto"],
                "default": "auto"
            }
        if not has_login:
            desc['login'] = {
                "type": "string",
                "title": "login",
                "description": "login to use to access the server"
            }
        return desc

    def check(self, options, docker_registries):
        params = _get_params(self._config, options)
        client = self._get_client(params=params)
        try:
            details = common.check_environment(
                client,
                params['gpus'],
                params['log_dir'],
                docker_registries,
                self._config.get('requirements'),
                False)
        finally:
            client.close()
        return details

    def launch(self,
               task_id,
               options,
               xpulist,
               resource,
               storages,
               docker_config,
               docker_registry,
               docker_image,
               docker_tag,
               docker_command,
               docker_files,
               wait_after_launch,
               auth_token,
               support_statistics):
        options['server'] = resource
        params = _get_params(self._config, options)
        client = self._get_client(params=params)
        try:
            callback_url = self._config.get('callback_url')
            if auth_token:
                callback_url = callback_url.replace("://", "://"+auth_token+":x@")
            task = common.launch_task(
                task_id,
                client,
                xpulist,
                params['log_dir'],
                docker_config,
                docker_registry,
                docker_image,
                docker_tag,
                docker_command,
                docker_files,
                wait_after_launch,
                storages,
                callback_url,
                self._config.get('callback_interval'),
                requirements=self._config.get("requirements"),
                support_statistics=support_statistics)
        finally:
            client.close()
        params['model'] = task['model']
        params['pgid'] = task['pgid']
        return params

    def _get_client(self, params):
        client = common.ssh_connect_with_retry(
            params['host'],
            params['port'],
            params['login'],
            pkey=self._config.get('pkey'),
            key_filename=self._config.get('key_filename') or self._config.get('privateKey'),
            login_cmd=params['login_cmd'])
        return client

    def status(self, task_id, params, get_log=True):
        client = common.ssh_connect_with_retry(
            params['host'],
            params['port'],
            params['login'],
            pkey=self._config.get('pkey'),
            key_filename=self._config.get('key_filename') or self._config.get('privateKey'),
            login_cmd=params['login_cmd'])

        if 'container_id' in params:
            exit_status, stdout, stderr = common.run_docker_command(
                client, 'inspect -f {{.State.Status}} %s' % params['container_id'])
        else:
            exit_status, stdout, stderr = common.run_command(client, 'kill -0 -%d' % params['pgid'])

        if get_log:
            common.update_log(task_id, client, params['log_dir'], self._config.get('callback_url'))

        client.close()
        if exit_status != 0:
            return "dead"

        return "running"

    def terminate(self, params):
        client = common.ssh_connect_with_retry(
            params['host'],
            params['port'],
            params['login'],
            pkey=self._config.get('pkey'),
            key_filename=self._config.get('key_filename') or self._config.get('privateKey'),
            login_cmd=params['login_cmd'])
        if 'container_id' in params:
            common.run_docker_command(client, 'rm --force %s' % params['container_id'])
            time.sleep(5)
        exit_status, stdout, stderr = common.run_command(client, 'kill -0 -%d' % params['pgid'])
        if exit_status != 0:
            logger.info("exist_status %d: %s", exit_status, stderr.read())
            client.close()
            return
        exit_status, stdout, stderr = common.run_command(client, 'kill -9 -%d' % params['pgid'])
        if exit_status != 0:
            logger.info("exist_status %d: %s", exit_status, stderr.read())
            client.close()
            return
        logger.info("successfully terminated")
        client.close()


def init(config):
    return SSHService(config)
