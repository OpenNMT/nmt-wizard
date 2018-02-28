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
    assert type(a) == type(b), "default and %s config file are not compatible" % name
    if isinstance(a, dict):
        for k in six.iterkeys(b):
            if k not in a:
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
    if base_config is not None:
        merge_config(config, base_config, config_path)
    name = config["name"]
    if config.get("disabled") == 1:
        return name, None
    if "module" not in config or "docker" not in config or "description" not in config:
        raise ValueError("invalid service definition in %s" % config_path)
    service = importlib.import_module(config["module"]).init(config)
    return name, service

def load_services(directory):
    """Loads configured services.

    Each service is configured by a JSON file and a optional shared configuration
    named "default.json".

    Args:
      directory: The directory to load services from.

    Returns:
      A map of service name to service module.
    """
    if not os.path.isdir(directory):
        raise ValueError("invalid service directory %s" % os.path.abspath(directory))

    base_config = {}
    base_config_path = os.path.join(directory, _BASE_CONFIG_NAME)
    if os.path.exists(base_config_path):
        logger.info("Reading base configuration %s", base_config_path)
        with open(base_config_path) as base_config_file:
            base_config = json.load(base_config_file)

    logger.info("Loading services from %s", directory)
    services = {}
    for filename in os.listdir(directory):
        config_path = os.path.join(directory, filename)
        if (not os.path.isfile(config_path)
                or not filename.endswith(".json")
                or filename == _BASE_CONFIG_NAME):
            continue
        logger.info("Loading service configuration %s", config_path)
        name, service = load_service(config_path, base_config=base_config)
        if service is None:
            logger.info("Skipping disabled service %s", name)
            continue
        if name in services:
            raise RuntimeError("%s duplicates service %s definition" % (filename, name))
        services[name] = service
        logger.info("Loaded service %s (total capacity: %s)", name, service.total_capacity)
    return services
