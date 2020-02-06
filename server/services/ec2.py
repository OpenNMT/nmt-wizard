import os
import logging
import boto3
import paramiko
import six

from botocore.exceptions import ClientError
from nmtwizard import common
from nmtwizard.service import Service
from nmtwizard.ec2_instance_types import ec2_capacity_map
from nmtwizard.capacity import Capacity

logger = logging.getLogger(__name__)


def _run_instance(client, launch_template_name, task_id="None", dry_run=False):
    return client.run_instances(
        MaxCount=1,
        MinCount=1,
        DryRun=dry_run,
        LaunchTemplate={
            "LaunchTemplateName": launch_template_name},
        TagSpecifications=[
            {
                'ResourceType': 'instance',
                'Tags': [
                    {
                        'Key': 'task_id',
                        'Value': task_id
                    },
                ]
            }
        ])


def _get_params(templates, options):
    templateName = options["server"]
    p = templateName.rfind(":")
    if p != -1:
        templateName = templateName[:p]
    params = {}
    for t in templates:
        if t["name"] == templateName:
            return t
    raise ValueError('template %s not in template_pool' % templateName)


class EC2Service(Service):

    def __init__(self, config):
        super(EC2Service, self).__init__(config)
        self._session = boto3.Session(
            aws_access_key_id=config["variables"]["awsAccessKeyId"],
            aws_secret_access_key=config["variables"]["awsSecretAccessKey"],
            region_name=config["variables"]["awsRegion"])
        ec2_client = self._session.client("ec2")
        self._templates = []
        self._resources = {}
        self._machines = {}
        for template in config['variables']['template_pool']:
            response = ec2_client.describe_launch_template_versions(
                DryRun=False,
                LaunchTemplateName=template['name'],
                Filters=[
                    {
                        'Name': 'is-default-version',
                        'Values': ["true"]
                    }
                ]
            )
            if not response or not response["LaunchTemplateVersions"]:
                raise ValueError('cannot retrieve launch template')
            template_description = response["LaunchTemplateVersions"][0]
            if "LaunchTemplateData" not in template_description:
                raise ValueError('invalid template_description')
            launch_template_data = template_description["LaunchTemplateData"]
            if "InstanceType" not in launch_template_data or \
                    launch_template_data["InstanceType"] not in ec2_capacity_map:
                raise ValueError('unknown instance type: %s' % launch_template_data["InstanceType"])
            xpu = ec2_capacity_map[launch_template_data["InstanceType"]]
            maxInstances = template.get("maxInstances", 1)
            template["id"] = template_description["LaunchTemplateId"]
            template["name"] = template_description["LaunchTemplateName"]
            template["gpus"] = range(xpu.ngpus)
            template["cpus"] = range(xpu.ncpus)
            self._templates.append(template)
            for idx in xrange(maxInstances):
                self._resources["%s:%d" % (template["name"], idx)] = \
                    Capacity(len(template["gpus"]), len(template["cpus"]))
                self._machines["%s:%d" % (template["name"], idx)]= template
        logger.info("Initialized EC2 - found %d templates.",
                    len(config['variables']['template_pool']))

    def get_server_detail(self, server, field_name):
        return self._machines[server].get(field_name) #here, server must exist

    def resource_multitask(self):
        return False

    def list_resources(self):
        return self._resources

    def get_resource_from_options(self, options):
        if "launchTemplateName" not in options:
            return 'auto'
        return [r for r in self._resources if r.startswith(options["launchTemplateName"]+":")]

    def select_resource_from_capacity(self, request_resource, request_capacity):
        min_capacity = None
        min_capacity_resource = []
        for resource, capacity in six.iteritems(self._resources):
            if request_resource == 'auto' or resource == request_resource or \
                    (isinstance(request_resource, list) and resource in request_resource):
                if request_capacity <= capacity:
                    if min_capacity_resource == [] or capacity == min_capacity:
                        min_capacity_resource.append(resource)
                        min_capacity = capacity
                    elif capacity < min_capacity:
                        min_capacity_resource = [resource]
                        min_capacity = capacity
        return min_capacity_resource

    def describe(self):
        return {
            "launchTemplateName": {
                "title": "EC2 Launch Template",
                "type": "string",
                "description": "The name of the EC2 launch template to use",
                "enum": [t["name"] for t in self._templates]
            }
        }

    def check(self, options, docker_registries_list):
        if "launchTemplateName" not in options:
            raise ValueError("missing launchTemplateName option")
        try:
            ec2_client = self._session.client("ec2")
            _ = _run_instance(ec2_client, options["launchTemplateName"], dry_run=True)
        except ClientError as e:
            if e.response["Error"]["Code"] == "DryRunOperation":
                pass
            elif e.response["Error"]["Code"] == "UnauthorizedOperation":
                raise RuntimeError("not authorized to run instances")
            else:
                raise e
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
        ec2_client = self._session.client("ec2")
        response = _run_instance(ec2_client, params["name"], task_id=task_id)
        if response is None:
            raise RuntimeError("empty response from boto3.run_instances")
        if not response["Instances"]:
            raise RuntimeError("no instances were created")
        instance_id = response["Instances"][0]["InstanceId"]
        ec2 = self._session.resource("ec2")
        instance = ec2.Instance(instance_id)
        instance.wait_until_running()
        logger.info("EC2 - Instance %s is running.", instance.id)

        client = paramiko.SSHClient()
        try:
            client = common.ssh_connect_with_retry(
                instance.public_dns_name,
                22,
                params['login'],
                pkey=self._config.get('pkey'),
                key_filename=self._config.get('key_filename') or self._config.get('privateKey'),
                delay=self._config["variables"]["sshConnectionDelay"],
                retry=self._config["variables"]["maxSshConnectionRetry"])

            # mounting corpus and temporary model directories
            corpus_dir = self._config["corpus"]
            if not isinstance(corpus_dir, list):
                corpus_dir = [corpus_dir]
            for corpus_description in corpus_dir:
                common.fuse_s3_bucket(client, corpus_description)
            if self._config["variables"].get("temporary_model_storage"):
                common.fuse_s3_bucket(client, self._config["variables"]["temporary_model_storage"])

            callback_url = self._config.get('callback_url')
            if auth_token:
                callback_url = callback_url.replace("://", "://"+auth_token+":x@")
            task = common.launch_task(
                task_id,
                client,
                (xpulist[0], None),
                params['log_dir'],
                docker_config,
                [docker_registry],
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
                instance.terminate()
                logger.info("Terminated instance (on launch error): %s.", instance_id)
            client.close()
            raise e
        finally:
            client.close()
        task["instance_id"] = instance.id
        return task

    def status(self, task_id, params):
        instance_id = params["instance_id"] if isinstance(params, dict) else params
        ec2_client = self._session.client("ec2")
        status = ec2_client.describe_instance_status(
            InstanceIds=[instance_id], IncludeAllInstances=True)

        # TODO: actually check if the task is running, not only the instance.

        return status["InstanceStatuses"][0]["InstanceState"]["Name"]

    def terminate(self, params):
        instance_id = params["instance_id"] if isinstance(params, dict) else params
        ec2 = self._session.resource("ec2")
        instance = ec2.Instance(instance_id)
        instance.terminate()
        logger.info("Terminated instance (on terminate): %s.", instance_id)

    def get_server_detail(self, server, field_name):
        return self._machines[server].get(field_name)  # here, server must exist


def init(config):
    return EC2Service(config)
