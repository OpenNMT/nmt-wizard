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


class InvalidService(Service):
    """Proxy class to keep worker in invalid state when there is an invalid configuration"""

    def __init__(self, config):
        super(InvalidService, self).__init__(config)
        self._resources = []

    def valid(self):
        return False

    def resource_multitask(self):
        return False

    def list_resources(self):
        return {}

    def get_resource_from_options(self, options):
        return "auto"

    def describe(self):
        return {
            "launchTemplateName": {
                "title": "EC2 Launch Template",
                "type": "string",
                "description": "The name of the EC2 launch template to use",
                "enum": [t["name"] for t in self._templates]
            }
        }

    def check(self, options):
        raise NotImplementedError()

    def launch(self,
               task_id,
               options,
               xpulist,
               resource,
               docker_registry,
               docker_image,
               docker_tag,
               docker_command,
               docker_files,
               wait_after_launch,
               auth_token):
        raise NotImplementedError()

    def status(self, task_id, params):
        raise NotImplementedError()

    def terminate(self, params):
        raise NotImplementedError()

    def get_server_detail(self, server, field_name):
        raise NotImplementedError()


def init(config):
    return InvalidService(config)
