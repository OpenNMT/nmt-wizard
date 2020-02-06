import os
import json
import logging
import importlib
import six

logger = logging.getLogger(__name__)
CONFIG_DEFAULT = "CONF_DEFAULT"

def add_log_handler(fh):
    logger.addHandler(fh)


def merge_config(a, b, name):
    if isinstance(a, dict):
        for k in six.iterkeys(b):
            if k not in a or type(a[k]) != type(b[k]):
                a[k] = b[k]
            elif isinstance(a[k], dict):
                merge_config(a[k], b[k], name)


def validate_polyentity_pool_format(config):
    if not config:
        raise ValueError("config empty")

    if not isinstance(config["entities"], dict):
        raise ValueError("The 'entities' key must contains a dictionary.")

    for entity, entity_desc in six.iteritems(config["entities"]):
        if not isinstance(entity_desc, dict):
            raise ValueError("The entity key '%s' must contain a dictionary." % entity)


def get_entities(config):
    entities = config["entities"].keys() if is_polyentity_config(config) else [config["name"][0:2]]
    return [i.upper() for i in entities if i]


def get_entities_from_service(redis, service_name):
    service_config = _get_config_from_redis(redis, service_name)
    entities = get_entities(service_config)
    return entities


def is_polyentity_config(config):
    return "entities" in config


def get_docker(config, entity):
    if is_polyentity_config(config):
        if entity in config["entities"].keys():
            return config["entities"][entity]["docker"]
        else:
            raise ValueError("cannot find the config for the entity %s" % entity)
    else:
        return config["docker"]


def get_registries(redis, service):
    base_config = get_default_storage(redis)
    service_config = _get_config_from_redis(redis, service)
    registries=[]
    if "docker" in base_config and "registries" in base_config["docker"]:
        registries = base_config["docker"]["registries"]

    if is_polyentity_config(service_config):
        for ent in service_config["entities"]:
            if service_config["entities"][ent].get("docker") and service_config["entities"][ent]["docker"].get("registries"):
                registries.update(service_config["entities"][ent]["docker"]["registries"])
    elif service_config.get("docker") and service_config["docker"].get("registries"):
        registries.update (service_config["docker"]["registries"])

    return registries


def _get_config_from_redis(redis, service):
    current_configuration_name = redis.hget("admin:service:%s" % service, "current_configuration")
    configurations = json.loads(redis.hget("admin:service:%s" % service, "configurations"))
    current_configuration = json.loads(configurations[current_configuration_name][1])
    return current_configuration


def get_default_storage(redis):
    default_config = redis.hget('default', 'configuration')
    base_config = json.loads(default_config)
    return base_config


def get_entity_cfg_from_redis(redis, service, entities_filters, entity_owner):
    base_config = get_default_storage(redis)
    service_config = _get_config_from_redis(redis, service)

    if is_polyentity_config(service_config) and entity_owner in service_config["entities"]:
        # remove other entities + and entities tag to have the same format as default config.
        owner_config = service_config["entities"][entity_owner]
        if entities_filters:
            for ent in entities_filters:
                if ent in service_config["entities"]:
                    ent_config = service_config["entities"][ent]
                    if "storages" in ent_config:
                        if "storages" in owner_config:
                            owner_config["storages"].update(ent_config["storages"])
                        else:
                            owner_config["storages"] = ent_config["storages"]

                    if "docker" in ent_config and "envvar" in ent_config["docker"]:
                        if "docker" in owner_config and "envvar" in owner_config["docker"]:
                            owner_config["docker"]["envvar"].update(ent_config["docker"]["envvar"])
                        else:
                            owner_config["docker"]["envvar"] = ent_config["docker"]["envvar"]

        # put entity config outside entities
        for k in owner_config:
            service_config[k] = owner_config[k]
        del service_config["entities"]

    merge_config(service_config, base_config, "")
    return service_config


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

        if is_polyentity_config(config):
            validate_polyentity_pool_format(config)

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

        return name, service, config

    raise ValueError("cannot open the config (%s)" % config_path)


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
    name, service, merged_config = load_service(filename, base_config=base_config)
    if service is None:
        raise RuntimeError("disabled service %s/%s" % (filename, name))
    services[name] = service
    logger.info("Loaded service %s (total capacity: %s)", name, service.total_capacity)

    return services, merged_config




