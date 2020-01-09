import os
import json
import logging
import importlib
import six

logger = logging.getLogger(__name__)


def add_log_handler(fh):
    logger.addHandler(fh)


def merge_config(a, b, name):
    if isinstance(a, dict):
        for k in six.iterkeys(b):
            if k not in a or type(a[k]) != type(b[k]):
                a[k] = b[k]
            elif isinstance(a[k], dict):
                merge_config(a[k], b[k], name)


def load_service(config_path, base_config=None):
    """Loads a service configuration.

    Args:
      config_path: Path the service configuration to load.
      base_config: The shared configuration to include in this service.

    Returns:
      name: The service name
      service: The service manager.
    """
    with open(config_path) as config_file:
        config = json.load(config_file)
    name = config["name"]

    if not os.path.basename(config_path).startswith(name):
        raise ValueError("config name (%s) does not match filename (%s)" % (name, config_path))

    if base_config is not None:
        merge_config(config, base_config, config_path)
    if config.get("disabled") == 1:
        return name, None

    try:
        if "module" not in config or "docker" not in config or "description" not in config:
            raise ValueError("invalid service definition in %s" % config_path)
        service = importlib.import_module(config["module"]).init(config)
    except Exception as e:
        config["description"] = "**INVALID CONFIG: %s" % str(e)
        service = importlib.import_module("services.invalid").init(config)

    return name, service


def load_service_config(filename, base_config):
    """Load configured service given a json file applying on a provided base configuration

    Args:
      directory: The path to the json file configuring the service.

    Returns:
      A map of service name to service module.
    """
    if not os.path.isfile(filename):
        raise ValueError("invalid path to service configuration: %s" % filename)

    directory = os.path.dirname(os.path.abspath(filename))

    logger.info("Loading services from %s", directory)
    services = {}

    logger.info("Loading service configuration %s", filename)
    name, service = load_service(filename, base_config=base_config)
    if service is None:
        raise RuntimeError("disabled service %s/%s" % (filename, name))
    services[name] = service
    logger.info("Loaded service %s (total capacity: %s)", name, service.total_capacity)

    return services
