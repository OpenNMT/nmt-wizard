"""Base class for services: objects that can start, monitor, and terminate
Docker-based tasks.
"""

import logging
import abc
import six


@six.add_metaclass(abc.ABCMeta)
class Service(object):
    """Base class for services."""

    def __init__(self, config):
        self._config = config

    @property
    def name(self):
        """Returns the name of the service."""
        return self._config['name']

    @property
    def display_name(self):
        """Returns the detailed name of the service."""
        return self._config['description']

    @property
    def is_notifying_activity(self):
        return self._config.get('callback_url')

    @property
    def total_capacity(self):
        """Total capacity of the service (i.e. the total number of tasks that
        can run at the same time).
        """
        return sum(six.itervalues(self.list_resources()))

    @abc.abstractmethod
    def list_resources(self):
        """Lists resources covered by the service.

        Returns:
          A dictionary of resource name to their maximum capacity (-1 for unbounded).
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def get_resource_from_options(self, options):
        """Returns the selected resource.

        Args:
          options: The options provided by the user.

        Returns:
          The name of the selected resource.

        See Also:
          describe().
        """
        raise NotImplementedError()

    def describe(self):
        """Describe the service options.

        Returns:
          A (possibly empty) dictionary following the JSON form structure.
        """
        return {}

    @abc.abstractmethod
    def check(self, options):
        """Checks if a task can be launched on the service.

        Args:
          options: The user options to use for the launch.

        Returns:
          A (possibly empty) string with details on the target service and
          resource.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def launch(self,
               task_id,
               options,
               resource,
               docker_registry,
               docker_image,
               docker_tag,
               docker_command,
               docker_files,
               wait_after_launch):
        """Launches a new task."""
        raise NotImplementedError()

    @abc.abstractmethod
    def status(self, params):
        """Returns the status of a task as string."""
        raise NotImplementedError()

    @abc.abstractmethod
    def terminate(self, params):
        """Terminates a (possibly) running task."""
        raise NotImplementedError()
