import os
import json
import logging
import importlib
import six

logger = logging.getLogger(__name__)

_BASE_CONFIG_NAME = "default.json"


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
    if "module" not in config or "docker" not in config or "description" not in config:
        raise ValueError("invalid service definition in %s" % config_path)
    service = importlib.import_module(config["module"]).init(config)
    return name, service

def load_service_config(filename):
    """Load configured service given a json file and corresponding default.json
       Both should be in the same directory

    Args:
      directory: The path to the json file configuring the service.

    Returns:
      A map of service name to service module.
    """
    if not os.path.isfile(filename):
        raise ValueError("invalid path to service configuration: %s" % filename)

    directory = os.path.dirname(os.path.abspath(filename))

    base_config = {}
    base_config_path = os.path.join(directory, _BASE_CONFIG_NAME)
    if os.path.exists(base_config_path):
        logger.info("Reading base configuration %s", base_config_path)
        with open(base_config_path) as base_config_file:
            base_config = json.load(base_config_file)

    logger.info("Loading services from %s", directory)
    services = {}

    logger.info("Loading service configuration %s", filename)
    name, service = load_service(filename, base_config=base_config)
    if service is None:
        raise RuntimeError("disabled service %s/%s" % (filename, name))
    services[name] = service
    logger.info("Loaded service %s (total capacity: %s)", name, service.total_capacity)

    return services, base_config
