import logging
import paramiko

from nmtwizard import common
from nmtwizard.service import Service

logger = logging.getLogger(__name__)

def _get_params(config, options):
    params = {}
    if 'server' not in options:
        server_pool = config['variables']['server_pool']
        if len(server_pool) > 1:
            raise ValueError('server option is required to select a server and a resource')
        resource = config['variables']['server_pool'][0]['host']
        options['server'] = resource

    params['server'] = options['server']

    servers = {server['host']:server for server in config['variables']['server_pool']}

    if params['server'] not in servers:
        raise ValueError('server %s not in server_pool list' % params['server'])
    params['gpus'] = servers[params['server']]['gpus']
    server_cfg = servers[params['server']]

    if 'login' not in server_cfg and 'login' not in options:
        raise ValueError('login not found in server configuration or user options')
    if 'log_dir' not in server_cfg:
        raise ValueError('missing log_dir in the configuration')

    params['login'] = server_cfg.get('login', options.get('login'))
    params['log_dir'] = server_cfg['log_dir']
    params['login_cmd'] = server_cfg.get('login_cmd')

    return params


class SSHService(Service):

    def __init__(self, config):
        super(SSHService, self).__init__(config)
        self._resources = self._list_all_gpus()

    def _list_all_gpus(self):
        gpus = []
        for server in self._config['variables']['server_pool']:
            for gpu in server['gpus']:
                gpus.append('%s:%d' % (server['host'], gpu))
        return gpus

    def list_resources(self):
        return {server['host']:len(server['gpus'])
                    for server in self._config['variables']['server_pool']}

    def get_resource_from_options(self, options):
        if "server" not in options:
            return "auto"
        else:
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

    def check(self, options):
        params = _get_params(self._config, options)
        client = common.ssh_connect_with_retry(
            params['server'],
            params['login'],
            self._config['privateKey'],
            login_cmd=params['login_cmd'])
        try:
            details = common.check_environment(
                client,
                params['gpus'],
                params['log_dir'],
                self._config['docker']['registries'],
                self._config.get('requirements'))
        finally:
            client.close()
        return details

    def launch(self,
               task_id,
               options,
               gpulist,
               resource,
               docker_registry,
               docker_image,
               docker_tag,
               docker_command,
               docker_files,
               wait_after_launch):
        options['server'] = resource
        params = _get_params(self._config, options) 
        client = common.ssh_connect_with_retry(
            params['server'],
            params['login'],
            self._config['privateKey'],
            login_cmd=params['login_cmd'])
        try:
            task = common.launch_task(
                task_id,
                client,
                gpulist,
                params['log_dir'],
                self._config['docker'],
                docker_registry,
                docker_image,
                docker_tag,
                docker_command,
                docker_files,
                wait_after_launch,
                self._config.get('storages'),
                self._config.get('callback_url'),
                self._config.get('callback_interval'),
                requirements=self._config.get("requirements"))
        finally:
            client.close()
        params['model'] = task['model']
        params['pgid'] = task['pgid']
        return params

    def status(self, task_id, params, get_log=True):
        client = common.ssh_connect_with_retry(
            params['server'],
            params['login'],
            self._config['privateKey'],
            login_cmd=params['login_cmd'])

        if 'container_id' in params:
            exit_status, stdout, stderr = common.run_docker_command(client, 'inspect -f {{.State.Status}} %s' %
                                                                    params['container_id'])
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
            params['server'],
            params['login'],
            self._config['privateKey'],
            login_cmd=params['login_cmd'])
        if 'container_id' in params:
            common.run_docker_command(client, 'rm --force %s' % params['container_id'])
        else:
            exit_status, stdout, stderr = common.run_command(client, 'kill -0 -%d' % params['pgid'])
            if exit_status != 0:
                logger.debug("exist_status %d: %s", exit_status, stderr.read())
                client.close()
                return
            exit_status, stdout, stderr = common.run_command(client, 'kill -9 -%d' % params['pgid'])
            if exit_status != 0:
                logger.debug("exist_status %d: %s", exit_status, stderr.read())
                client.close()
                return
        logger.debug("successfully terminated")
        client.close()


def init(config):
    return SSHService(config)
