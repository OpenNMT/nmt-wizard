"""Base class for services: objects that can start, monitor, and terminate
Docker-based tasks.
"""

import logging
import abc
import six
from nmtwizard.capacity import Capacity


@six.add_metaclass(abc.ABCMeta)
class Service(object):
    """Base class for services."""

    def __init__(self, config):
        self._config = config
        self._default_ms = None
        self._default_msr = None
        self._default_msw = None
        self._temporary_ms = None
        if "storages" in config:
            for storage_name, storage_desc in six.iteritems(config["storages"]):
                if storage_desc.get("temporary_ms"):
                    self._temporary_ms = storage_name
                elif storage_desc.get("default_ms"):
                    self._default_ms = storage_name
                elif storage_desc.get("default_msr"):
                    self._default_msr = storage_name
                elif storage_desc.get("default_msw"):
                    self._default_msw = storage_name
        # check exclusivity of default_msr/default_msw and default_ms
        if self._default_ms and (self._default_msr or self._default_msw):
            raise ValueError('default_ms and default_ms[rw] are exclusive')

    def __getstate__(self):
        """Return state values to be pickled."""
        return (self._config, self._resources)

    def __setstate__(self, state):
        """Restore state from the unpickled state values."""
        self._config, self._resources = state

    @property
    def temporary_ms(self):
        return self._temporary_ms

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
        tc = Capacity()
        for v in six.itervalues(self.list_resources()):
            tc += v
        return tc

    @property
    @abc.abstractmethod
    def resource_multitask(self):
        raise NotImplementedError()

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

    def select_resource_from_capacity(self, request_resource, capacity):
        """Given expected capacity, restrict or not resource list to the capacity

        Args:
          request_resource: name of a resource, 'auto', or list of resource name
          capacity: expected capacity - see Capacity

        Returns:
          New resource name/auto/resource list
        """

        return request_resource

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
               gpulist,
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
