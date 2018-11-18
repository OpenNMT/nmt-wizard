import os
import logging
import boto3
import paramiko

from botocore.exceptions import ClientError
from nmtwizard import common
from nmtwizard.service import Service

logger = logging.getLogger(__name__)


def _run_instance(client, launch_template_name, dry_run=False):
    return client.run_instances(
        MaxCount=1,
        MinCount=1,
        DryRun=dry_run,
        LaunchTemplate={
            "LaunchTemplateName": launch_template_name})


class EC2Service(Service):

    def __init__(self, config):
        super(EC2Service, self).__init__(config)
        self._session = boto3.Session(
            aws_access_key_id=config["awsAccessKeyId"],
            aws_secret_access_key=config["awsSecretAccessKey"],
            region_name=config["awsRegion"])
        self._launch_template_names = self._get_launch_template_names()

    def _get_launch_template_names(self):
        ec2_client = self._session.client("ec2")
        response = ec2_client.describe_launch_templates()
        if not response or not response["LaunchTemplates"]:
            raise ValueError('no EC2 launch templates are available to use')
        return [template["LaunchTemplateName"] for template in response["LaunchTemplates"]]

    def list_resources(self):
        return {
            name:self._config['maxInstancePerTemplate']
            for name in self._launch_template_names}

    def get_resource_from_options(self, options):
        return options["launchTemplateName"]

    def describe(self):
        return {
            "launchTemplateName": {
                "title": "EC2 Launch Template",
                "type": "string",
                "description": "The name of the EC2 launch template to use",
                "enum": self._launch_template_names}}

    def check(self, options):
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
               gpulist,
               resource,
               docker_registry,
               docker_image,
               docker_tag,
               docker_command,
               docker_files,
               wait_after_launch,
               auth_token):
        ec2_client = self._session.client("ec2")
        response = _run_instance(ec2_client, resource)
        if response is None:
            raise RuntimeError("empty response from boto3.run_instances")
        if not response["Instances"]:
            raise RuntimeError("no instances were created")
        instance_id = response["Instances"][0]["InstanceId"]
        ec2 = self._session.resource("ec2")
        instance = ec2.Instance(instance_id)
        instance.wait_until_running()
        logger.info("Instance %s is running.", instance.id)

        key_path = os.path.join(
            self._config["privateKeysDirectory"], "%s.pem" % instance.key_pair.name)
        client = paramiko.SSHClient()
        try:
            client = common.ssh_connect_with_retry(
                instance.public_dns_name,
                self._config["amiUsername"],
                key_path,
                delay=self._config["sshConnectionDelay"],
                retry=self._config["maxSshConnectionRetry"])
            common.fuse_s3_bucket(client, self._config["corpus"])
            gpu_id = 1 if common.has_gpu_support(client) else 0
            callback_url = self._config.get('callback_url')
            if auth_token:
                callback_url = callback_url.replace("://","://"+auth_token+":x@")
            task = common.launch_task(
                task_id,
                client,
                gpu_id,
                self._config["logDir"],
                self._config["docker"],
                docker_registry,
                docker_image,
                docker_tag,
                docker_command,
                docker_files,
                wait_after_launch,
                self._config.get('storages'),
                callback_url,
                self._config.get('callback_interval'))
        except Exception as e:
            if self._config.get("terminateOnError", True):
                instance.terminate()
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
        logger.info("Instance %s is terminated.", instance.id)


def init(config):
    return EC2Service(config)
