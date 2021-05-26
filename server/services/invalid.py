import logging
from nmtwizard.service import Service


logger = logging.getLogger(__name__)


class InvalidService(Service):
    """Proxy class to keep worker in invalid state when there is an invalid configuration"""

    def __init__(self, config):
        super().__init__(config)
        self._resources = []

    @property
    def valid(self):
        return False

    @property
    def resource_multitask(self):
        return False

    @staticmethod
    def list_resources():
        return {}

    @staticmethod
    def get_resource_from_options(options):  # pylint: disable=unused-argument
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

    def check(self, options, docker_registries_list):
        raise NotImplementedError()

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
        raise NotImplementedError()

    def status(self, params):
        raise NotImplementedError()

    def terminate(self, params):
        raise NotImplementedError()

    def get_server_detail(self, server, field_name):
        raise NotImplementedError()


def init(config):
    return InvalidService(config)
