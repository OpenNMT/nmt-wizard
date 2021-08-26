import logging
import os
import time
import paramiko
import six
from keystoneauth1 import session
from keystoneauth1.identity import v3
from novaclient import client
import novaclient.exceptions
from nmtwizard import common
from nmtwizard.service import Service
from nmtwizard.ovh_instance_types import ovh_capacity_map
from nmtwizard.capacity import Capacity

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
        delete_wait_count = config["variables"]["deleteWaitCount"]
        while instance._info.get('OS-EXT-STS:task_state') == 'deleting' and delete_wait_count > 0:
            time.sleep(60)
            delete_wait_count -= 1
            instance = nova_client.servers.find(name=task_id)
        if delete_wait_count == 0:
            raise EnvironmentError("Timeout to delete old instance")

        wait_until_running(nova_client, config, params, name=task_id)
        return instance
    except novaclient.exceptions.NotFound:
        logger.info("Creating instance for task %s", task_id)

    # userdata2 - script to install docker
    with open(os.path.dirname(os.path.realpath(__file__)) + '/setup_ovh_instance.sh') as setup_file:
        scripts = setup_file.readlines()
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
    userdata1 += "mount %s:%s %s\n" % (
        config["variables"]["nfs_server_ip_addr"], config["variables"]["nfs_server_dir"]["mount"],
        config["variables"]["nfs_server_dir"]["mount"]) + "chown -R %s %s\n" % (
            params["login"], config["variables"]["nfs_server_dir"]["mount"])
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
        super().__init__(config)
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

    @property
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

    @staticmethod
    def check(options, docker_registries_list):  # pylint: disable=unused-argument
        # TODO: Check create new instance.
        return ""

    def launch(self,
               task_id,
               options,
               xpulist,
               resource,
               storages,  # pylint: disable=unused-argument
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
        if not params.get('port'):
            params['port'] = 22
        nova_client = self._nova_client
        instance = _run_instance(nova_client, params, self._config, task_id=task_id)
        if not instance:
            raise RuntimeError("no instances were created")
        logger.info("OVH - Instance %s is running.", instance.id)
        params['host'] = [addr for addr in instance.addresses['Ext-Net'] if addr.get('version') == 4][0]['addr']
        ssh_client = paramiko.SSHClient()
        try:
            ssh_client = get_client(params, self._config)
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
                self.terminate(params)
                logger.info("Terminated instance (on launch error): %s.", instance.id)
            ssh_client.close()
            raise e
        finally:
            ssh_client.close()
        task['instance_id'] = instance.id
        task['host'] = params['host']
        task['port'] = params['port']
        task['login'] = params['login']
        task['log_dir'] = params['log_dir']
        task['dynamic'] = params.get('dynamic')
        return task

    def status(self, task_id, params, get_log=True):  # pylint: disable=arguments-differ
        ssh_client = get_client(params, self._config)
        if 'container_id' in params:
            exit_status, _, _ = common.run_docker_command(
                ssh_client, 'inspect -f {{.State.Status}} %s' % params['container_id'])
        else:
            exit_status, _, _ = common.run_command(ssh_client, 'kill -0 -%d' % params['pgid'])

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
            ssh_client = get_client(params, self._config)
            if 'container_id' in params:
                common.run_docker_command(ssh_client, 'rm --force %s' % params['container_id'])
                time.sleep(5)
            if 'pgid' in params:
                exit_status, _, stderr = common.run_command(ssh_client, 'kill -0 -%d' % params['pgid'])
                if exit_status != 0:
                    logger.info("exist_status %d: %s", exit_status, stderr.read())
                    ssh_client.close()
                    return
                exit_status, _, stderr = common.run_command(client, 'kill -9 -%d' % params['pgid'])
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
        if status == 'ACTIVE':
            params['host'] = [addr for addr in instance.addresses['Ext-Net'] if addr.get('version') == 4][0]['addr']
            ssh_client = paramiko.SSHClient()
            try:
                ssh_client = get_client(params, config)
            except Exception as e:
                nova_client.servers.delete(instance.id)
                ssh_client.close()
                raise EnvironmentError("%s" % e) from e
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
            ssh_client.close()

    if instance_wait_count == 0:
        nova_client.servers.delete(instance.id)
        raise EnvironmentError("Timeout to create new OVH instance")


def get_client(params, config):
    ssh_client = common.ssh_connect_with_retry(
        params['host'],
        params['port'],
        params['login'],
        pkey=config.get('pkey'),
        key_filename=config.get('key_filename') or config.get('privateKey'),
        delay=config["variables"]["sshConnectionDelay"],
        retry=config["variables"]["maxSshConnectionRetry"])
    return ssh_client
