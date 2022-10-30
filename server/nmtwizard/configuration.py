import logging
import importlib
import os
import json
import six
import yaml

logger = logging.getLogger(__name__)
CONFIG_DEFAULT = "CONF_DEFAULT"
system_config_file = os.getenv('LAUNCHER_CONFIG', 'settings.yaml')


def add_log_handler(fh):
    logger.addHandler(fh)


def merge_config(a, b):
    if isinstance(a, dict):
        for k in six.iterkeys(b):
            if k not in a or not isinstance(a[k], type(b[k])):
                a[k] = b[k]
            elif isinstance(a[k], dict):
                merge_config(a[k], b[k])


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


def get_entities_from_service(mongo_client, service_name):
    service_config = get_service_config(mongo_client, service_name)
    entities = get_entities(service_config)
    return entities


def is_polyentity_service(mongo_client, service_name):
    service_config = get_service_config(mongo_client, service_name)
    if service_config is None:
        raise ValueError(f"Can't find service config: {service_name}")
    is_polyentity = is_polyentity_config(service_config)
    return is_polyentity


def is_polyentity_config(config):
    return "entities" in config


def get_docker(config, entity):
    if is_polyentity_config(config):
        if entity in config["entities"].keys():
            if "registries" not in config["entities"][entity]["docker"]:
                config["entities"][entity]["docker"]["registries"] = config["docker"]["registries"]
            return config["entities"][entity]["docker"]
        raise ValueError("cannot find the config for the entity %s" % entity)
    return config["docker"]


def get_entities_limit_rate(mongo_client, service):
    service_config = get_service_config(mongo_client, service)
    entities = service_config.get("entities")
    entities_rate = {}
    if entities:
        entities_rate = {e.upper(): entities[e]["occup_weight"] for e in entities if "occup_weight" in entities[e] and
                         entities[e]["occup_weight"] > 0}
    # mono entity so
    else:
        entity_name = service_config["name"][0:2].upper()
        entities_rate[entity_name] = 1

    return entities_rate


def get_registries(mongo_client, service):
    base_config = get_base_config(mongo_client)
    service_config = get_service_config(mongo_client, service)
    registries = []
    if "docker" in base_config and "registries" in base_config["docker"]:
        registries = base_config["docker"]["registries"]

    if is_polyentity_config(service_config):
        for ent in service_config["entities"]:
            if service_config["entities"][ent].get("docker") and \
                    service_config["entities"][ent]["docker"].get("registries"):
                registries.update(service_config["entities"][ent]["docker"]["registries"])
    elif service_config.get("docker") and service_config["docker"].get("registries"):
        registries.update(service_config["docker"]["registries"])

    return registries


def get_service_config(mongo_client, service_name):
    service_config = mongo_client.get_service_config(service_name)
    return service_config


def get_base_config(mongo_client):
    base_config = mongo_client.get_base_config()
    return base_config


def get_entity_config(mongo_client, service, entities_filters, entity_owner):
    base_config = get_base_config(mongo_client)
    service_config = get_service_config(mongo_client, service)

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

    merge_config(service_config, base_config)
    return service_config


def load_service(service_config, base_config=None):
    """Loads a service configuration.

    Args:
      service_config: Configuration of service.
      base_config: The shared configuration to include in this service.

    Returns:
      name: The service name
      service: The service manager.
    """

    if is_polyentity_config(service_config):
        validate_polyentity_pool_format(service_config)

    name = service_config["name"]

    if base_config is not None:
        merge_config(service_config, base_config)
    if service_config.get("disabled") == 1:
        return name, None, None

    try:
        if "module" not in service_config or "docker" not in service_config or "description" not in service_config:
            raise ValueError(f"invalid service definition in config of {name}")

        service = importlib.import_module(service_config["module"]).init(service_config)
    except Exception as e:
        service_config["description"] = "**INVALID CONFIG: %s" % str(e)
        service = importlib.import_module("services.invalid").init(service_config)

    return name, service, service_config


def load_service_config(service_config, base_config):
    """Load configured service given a json file applying on a provided base configuration

    Args:
      service_config: Configuration of service.
      base_config: Base configuration

    Returns:
      A map of service name to service module.
    """
    services = {}

    name, service, merged_config = load_service(service_config, base_config=base_config)
    if service is None:
        raise RuntimeError("disabled service %s" % name)
    services[name] = service
    logger.info("Loaded service %s (total capacity: %s)", name, service.total_capacity)

    return services, merged_config


def set_service_config(mongo_client, service_name, config_data):
    mongo_client.update_insert_service_config(service_name, config_data)


def read_yaml_file(file_path):
    with open(file_path, "r") as file:
        content = yaml.safe_load(file)
        return content


def get_system_config():
    assert system_config_file is not None and os.path.isfile(
        system_config_file), f"missing `{system_config_file}` file in current directory"
    config = read_yaml_file(system_config_file)

    return config


def process_base_config(mongo_client):
    system_config = get_system_config()
    config = mongo_client.get_base_config(views={"_id": 0})
    if config is not None:
        config["database"] = system_config["database"]
        resource_types = get_storage_resource_type(config['storages'])
        if system_config['application']['mode'] == 'lite' and system_config.get('similar_search') and system_config[
                'similar_search'].get('active'):
            assert resource_types and {'similar_base', 'similar_result'}.issubset(
                set(resource_types)), "Missing similar search storage config"
        if system_config['application']['mode'] == 'advanced' and system_config.get('regex_search') and system_config[
                'regex_search'].get('active'):
            assert resource_types and 'regex_result' in resource_types, "Missing regex search storage config"
        return config

    base_config_file = os.path.join(os.path.dirname(system_config_file), "configurations", "default.json")
    assert os.path.isfile(base_config_file), "Cannot find default.json: %s" % base_config_file
    with open(base_config_file) as base_config_binary:
        config = json.loads(base_config_binary.read())
        mongo_client.update_insert_base_config(config)
        config["database"] = system_config["database"]
        assert 'storages' in config, "incomplete configuration - missing " \
                                     "`storages` in %s" % base_config_file
        return config


def process_service_config(mongo_client, service_name):
    config = get_service_config(mongo_client, service_name)
    if config is not None:
        return config
    config = get_service_config_from_file(service_name)
    save_service_config_to_mongo(mongo_client, service_name, config)
    return config


def get_service_config_from_file(service_name):
    service_config_file = f"configurations/{service_name}.json"
    assert os.path.isfile(service_config_file), f"missing {service_name}.json file in configurations"
    with open(service_config_file) as service_config_binary:
        config = json.loads(service_config_binary.read())
        config["updated_at"] = os.path.getmtime(service_config_file)

        return config


def save_service_config_to_mongo(mongo_client, service_name, config_data):
    mongo_client.update_insert_service_config(service_name, config_data)


def is_db_service_config_outdated(mongo_client, service_name):
    config = mongo_client.get_service_config(service_name)
    config_file = f"configurations/{service_name}.json"
    is_outdated = config is not None and os.path.isfile(config_file) and config[
        "updated_at"] < os.path.getmtime(config_file)
    return config is None or is_outdated


def get_service_configs(mongo_client, services):
    service_configs = mongo_client.get_service_configs(services)
    return service_configs


def get_storage_resource_type(storages):
    return [storage['resource_type'] for storage in storages.values() if storage.get('resource_type')]
