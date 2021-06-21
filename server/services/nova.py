import logging
import os
import time
import paramiko
import six
from nmtwizard import common
from nmtwizard.service import Service
from nmtwizard.ovh_instance_types import ovh_capacity_map
from nmtwizard.capacity import Capacity
from keystoneauth1 import session
from novaclient import client
import novaclient.exceptions
from keystoneauth1.identity import v3

logger = logging.getLogger(__name__)


def _run_instance(nova_client, params, config, task_id):
    # check fixed instance
    if not params.get("dynamic"):
        try:
            instance = nova_client.servers.find(id=params['instance_id'])
            return instance
        except novaclient.exceptions.NotFound as exc:
            raise EnvironmentError("Instance fixed not found") from exc

    # check if instance already exists
    try:
        instance = nova_client.servers.find(name=task_id)
        # if instance is deleting, wait until successful deleted before create new instance
        while instance._info.get('OS-EXT-STS:task_state') == 'deleting':
            time.sleep(60)
            instance = nova_client.servers.find(name=task_id)
        wait_until_running(nova_client, config, params, name=task_id)
        return instance
    except novaclient.exceptions.NotFound:
        logger.info("Creating instance for task %s", task_id)

    # userdata2 - script to install docker
    scripts = open(os.path.dirname(os.path.realpath(__file__)) + '/setup_ovh_instance.sh').readlines()
    userdata1, userdata2 = ''.join(scripts[0:6]), ''.join(scripts[6:len(scripts)])
    # create folder to mount corpus and temporary model directories
    corpus_dir = config["corpus"]
    if not isinstance(corpus_dir, list):
        corpus_dir = [corpus_dir]
    for corpus_description in corpus_dir:
        userdata1 += "mkdir -p %s && chmod -R 775 %s\n" % (
            corpus_description["mount"], corpus_description["mount"])
    if config["variables"].get("temporary_model_storage"):
        userdata1 += "mkdir -p %s && chmod -R 775 %s\n" % (
            config["variables"]["temporary_model_storage"]["mount"],
            config["variables"]["temporary_model_storage"]["mount"])
    # add the nfs server to the script to mount volume
    userdata1 += "mount %s:/home/ubuntu/model_studio /home/ubuntu/model_studio\n" % (
        config["variables"]["nfs_server_ip_addr"]) + "chown -R ubuntu /home/ubuntu/model_studio\n"
    # add the docker installation script at the end of the instance installation script
    userdata = userdata1 + userdata2

    if params['gpus'].stop != 0:
        image_id = nova_client.glance.find_image(config['variables']['gpu_image']).id
    else:
        image_id = nova_client.glance.find_image(config['variables']['image']).id
    flavor = nova_client.flavors.find(name=params['name'])
    nova_client.servers.create(name=task_id, image=image_id, flavor=flavor.id, nics=config['variables']['nics'],
                               key_name=config['variables']['key_pair'], userdata=userdata)
    wait_until_running(nova_client, config, params, name=task_id)
    return nova_client.servers.find(name=task_id)


def _get_params(templates, options):
    templateName = options["server"]
    p = templateName.rfind(":")
    if p != -1:
        templateName = templateName[:p]
    for t in templates:
        if t["name"] == templateName:
            return t
    raise ValueError('template %s not in template_pool' % templateName)


class NOVAService(Service):

    def __init__(self, config):
        super(NOVAService, self).__init__(config)
        self._nova_client = init_nova_client(config)
        self._templates = []
        self._resources = {}
        self._machines = {}
        for template in config['variables']['template_pool']:
            instance_type = template['name']
            if instance_type not in ovh_capacity_map:
                raise ValueError('unknown instance type: %s' % instance_type)
            xpu = ovh_capacity_map[instance_type]
            try:
                flavor = self._nova_client.flavors.find(name=instance_type)
            except novaclient.exceptions.NotFound as e:
                raise e
            template["id"] = flavor.id
            template["name"] = flavor.name
            template["gpus"] = range(xpu.ngpus)
            template["cpus"] = range(xpu.ncpus)
            maxInstances = template.get("maxInstances", 1)
            self._templates.append(template)
            for idx in range(maxInstances):
                self._resources["%s:%d" % (template["name"], idx)] = \
                    Capacity(len(template["gpus"]), len(template["cpus"]))
                self._machines["%s:%d" % (template["name"], idx)] = template
        logger.info("Initialized OVH instance - found %d templates.",
                    len(config['variables']['template_pool']))

    def get_server_detail(self, server, field_name):
        # here, server must exist
        return self._machines[server].get(field_name)

    def resource_multitask(self):
        return 'hybrid'

    def list_resources(self):
        return self._resources

    def get_resource_from_options(self, options):
        if "launchTemplateName" not in options:
            return 'auto'
        return [r for r in self._resources if r.startswith(options["launchTemplateName"]+":")]

    def select_resource_from_capacity(self, request_resource, request_capacity):
        capacity_resource = []
        for resource, capacity in six.iteritems(self._resources):
            if request_resource == 'auto' or resource == request_resource or \
                    (isinstance(request_resource, list) and resource in request_resource):
                if request_capacity <= capacity:
                    capacity_resource.append(resource)
        return capacity_resource

    def describe(self):
        return {
            "launchTemplateName": {
                "title": "OVH Launch Template",
                "type": "string",
                "description": "The name of the OVH launch template to use",
                "enum": [t["name"] for t in self._templates]
            }
        }

    def check(self, options, docker_registries_list):
        # TODO: Check create new instance.
        return ""

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
        params = _get_params(self._templates, options)
        params['service'] = 'nova'
        nova_client = self._nova_client
        instance = _run_instance(nova_client, params, self._config, task_id=task_id)
        if not instance:
            raise RuntimeError("no instances were created")
        logger.info("OVH - Instance %s is running.", instance.id)
        public_dns_name = [addr for addr in instance.addresses['Ext-Net'] if addr.get('version') == 4][0]['addr']
        ssh_client = paramiko.SSHClient()
        try:
            ssh_client = common.ssh_connect_with_retry(
                public_dns_name,
                22,
                params['login'],
                pkey=self._config.get('pkey'),
                key_filename=self._config.get('key_filename') or self._config.get('privateKey'),
                delay=self._config["variables"]["sshConnectionDelay"],
                retry=self._config["variables"]["maxSshConnectionRetry"])

            callback_url = self._config.get('callback_url')
            if auth_token:
                callback_url = callback_url.replace("://", "://"+auth_token+":x@")
            task = common.launch_task(
                task_id,
                ssh_client,
                (xpulist[0], None),
                params,
                docker_config,
                docker_registry,
                docker_image,
                docker_tag,
                docker_command,
                docker_files,
                wait_after_launch,
                self._config.get('storages'),
                callback_url,
                self._config.get('callback_interval'),
                support_statistics=support_statistics)
        except Exception as e:
            if self._config["variables"].get("terminateOnError", True):
                params['instance_id'] = instance.id
                params['host'] = public_dns_name
                params['port'] = 22
                self.terminate(params)
                logger.info("Terminated instance (on launch error): %s.", instance.id)
            ssh_client.close()
            raise e
        finally:
            ssh_client.close()
        task['instance_id'] = instance.id
        task['host'] = public_dns_name
        task['port'] = 22
        task['login'] = params['login']
        task['log_dir'] = params['log_dir']
        task['dynamic'] = params.get('dynamic')
        return task

    def status(self, task_id, params, get_log=True):
        ssh_client = common.ssh_connect_with_retry(
            params['host'],
            params['port'],
            params['login'],
            pkey=self._config.get('pkey'),
            key_filename=self._config.get('key_filename') or self._config.get('privateKey'),
            delay=self._config["variables"]["sshConnectionDelay"],
            retry=self._config["variables"]["maxSshConnectionRetry"])

        if 'container_id' in params:
            exit_status, stdout, stderr = common.run_docker_command(
                ssh_client, 'inspect -f {{.State.Status}} %s' % params['container_id'])
        else:
            exit_status, stdout, stderr = common.run_command(ssh_client, 'kill -0 -%d' % params['pgid'])

        if get_log:
            common.update_log(task_id, ssh_client, params['log_dir'], self._config.get('callback_url'))

        ssh_client.close()
        if exit_status != 0:
            return "dead"
        return "running"

    def terminate(self, params):
        instance_id = params["instance_id"]
        if params.get('dynamic'):
            nova_client = self._nova_client
            nova_client.servers.delete(instance_id)
        else:
            ssh_client = common.ssh_connect_with_retry(
                params['host'],
                params['port'],
                params['login'],
                pkey=self._config.get('pkey'),
                key_filename=self._config.get('key_filename') or self._config.get('privateKey'),
                delay=self._config["variables"]["sshConnectionDelay"],
                retry=self._config["variables"]["maxSshConnectionRetry"])
            if 'container_id' in params:
                common.run_docker_command(ssh_client, 'rm --force %s' % params['container_id'])
                time.sleep(5)
            if 'pgid' in params:
                exit_status, stdout, stderr = common.run_command(ssh_client, 'kill -0 -%d' % params['pgid'])
                if exit_status != 0:
                    logger.info("exist_status %d: %s", exit_status, stderr.read())
                    ssh_client.close()
                    return
                exit_status, stdout, stderr = common.run_command(client, 'kill -9 -%d' % params['pgid'])
                if exit_status != 0:
                    logger.info("exist_status %d: %s", exit_status, stderr.read())
                    ssh_client.close()
                    return
            ssh_client.close()

        logger.info("Terminated instance (on terminate): %s.", instance_id)


def init(config):
    return NOVAService(config)


def init_nova_client(config):
    auth = v3.Password(auth_url=config["variables"]["auth_url"],
                       username=config["variables"]["username"],
                       password=config["variables"]["password"],
                       project_id=config["variables"]["project_id"],
                       project_name=config["variables"]["project_name"],
                       user_domain_name=config["variables"]["user_domain_name"],
                       project_domain_name=config["variables"]["project_domain_name"])

    sess = session.Session(auth=auth)
    nova = client.Client(version=config["variables"]["nova_client_version"], session=sess,
                         region_name=config["variables"]["region_name"])
    return nova


def wait_until_running(nova_client, config, params, name):
    status = ''
    instance_wait_count = config["variables"]["instanceWaitCount"]
    instance = {}
    while status != 'ACTIVE' and instance_wait_count > 0:
        time.sleep(60)
        instance_wait_count -= 1
        instance = nova_client.servers.find(name=name)
        status = instance.status
        if status == 'ERROR':
            nova_client.servers.delete(instance.id)
            raise EnvironmentError("OVH - Create instance failed")
        elif status == 'ACTIVE':
            ssh_client = common.ssh_connect_with_retry(
                [addr for addr in instance.addresses['Ext-Net'] if addr.get('version') == 4][0]['addr'],
                22,
                params['login'],
                pkey=config.get('pkey'),
                key_filename=config.get('key_filename') or config.get('privateKey'),
                delay=config["variables"]["sshConnectionDelay"],
                retry=config["variables"]["maxSshConnectionRetry"])
            # check instance until docker installation is complete
            docker_wait_count = config["variables"]["dockerWaitCount"]
            while docker_wait_count > 0:
                time.sleep(60)
                if common.program_exists(ssh_client, "docker"):
                    time.sleep(20)
                    break
                docker_wait_count -= 1
            if docker_wait_count == 0:
                nova_client.servers.delete(instance.id)
                raise EnvironmentError("Timeout to install docker for OVH instance")
            # check mount instance dirs with nfs server dirs
            if not common.run_and_check_command(ssh_client, "mount -l | grep nfs"):
                nova_client.servers.delete(instance.id)
                raise EnvironmentError("Unable to mount instance dirs with nfs server dirs")

    if instance_wait_count == 0:
        nova_client.servers.delete(instance.id)
        raise EnvironmentError("Timeout to create new OVH instance")
