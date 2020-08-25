import os
import json
import logging
import importlib
import six

logger = logging.getLogger(__name__)
CONFIG_DEFAULT = "CONF_DEFAULT"


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
    entities = config["entities"].keys() if is_polyentity_config(config) else [config["service_name"][0:2]]
    return [i.upper() for i in entities if i]


def get_entities_from_service(mongo_client, service_name):
    service_config = get_current_config_of_service(mongo_client, service_name)
    entities = get_entities(service_config)
    return entities


def is_polyentity_service(mongo_client, service_name):
    service_config = get_current_config_of_service(mongo_client, service_name)
    is_polyentity = is_polyentity_config(service_config)
    return is_polyentity


def is_polyentity_config(config):
    return "entities" in config


def get_docker(config, entity):
    if is_polyentity_config(config):
        if entity in config["entities"].keys():
            return config["entities"][entity]["docker"]
        raise ValueError("cannot find the config for the entity %s" % entity)
    return config["docker"]


def get_entities_limit_rate(mongo_client, service):
    service_config = get_current_config_of_service(mongo_client, service)
    entities = service_config.get("entities")
    entities_rate = {}
    if entities:
        entities_rate = {e.upper(): entities[e]["occup_weight"] for e in entities if "occup_weight" in entities[e] and
                         entities[e]["occup_weight"] > 0}
    # mono entity so
    else:
        entity_name = service_config["service_name"][0:2].upper()
        entities_rate[entity_name] = 1

    return entities_rate


def get_registries(mongo_client, service):
    base_config = get_default_config(mongo_client)
    service_config = get_current_config_of_service(mongo_client, service)
    registries = []
    if "docker" in base_config and "registries" in base_config["docker"]:
        registries = base_config["docker"]["registries"]

    if is_polyentity_config(service_config):
        for ent in service_config["entities"]:
            if service_config["entities"][ent].get("docker") and service_config["entities"][ent]["docker"].get("registries"):
                registries.update(service_config["entities"][ent]["docker"]["registries"])
    elif service_config.get("docker") and service_config["docker"].get("registries"):
        registries.update(service_config["docker"]["registries"])

    return registries


def _get_config_from_redis(redis, service):
    current_configuration_name = redis.hget("admin:service:%s" % service, "current_configuration")
    configurations = json.loads(redis.hget("admin:service:%s" % service, "configurations"))
    current_configuration = json.loads(configurations[current_configuration_name][1])
    return current_configuration


# for now, we try to update only the entity
def set_entity_config(redis, service, pool_entity, the_config):
    if pool_entity not in the_config["entities"]:
        raise ValueError("Cannot modify the entity '%s'. Config is not valid" % pool_entity)

    keys = 'admin:service:%s' % service
    service_config = _get_config_from_redis(redis, service)
    service_config["entities"][pool_entity] = the_config["entities"][pool_entity]
    redis.hset(keys, "configurations", json.dumps(service_config))


def get_entity_cfg_from_mongo(mongo_client, service, entities_filters, entity_owner):
    all_activated_configs = mongo_client.get_all_active_config()
    base_config = get_default_config(mongo_client)
    service_config = get_current_config_of_service(mongo_client, service)

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

        # Get all share storages
    for config in all_activated_configs:
        if not is_polyentity_config(config):
            continue
        for entity in config["entities"]:
            if entity == entity_owner:
                continue
            ent_config = config["entities"][entity]
            if "storages" not in ent_config:
                continue
            storages_config = ent_config["storages"]
            for storage_name in storages_config:
                storage_config = storages_config[storage_name]
                if "shared" not in storage_config:
                    continue
                shared_config = storage_config["shared"]
                for ent in entities_filters:
                    if ent not in shared_config:
                        continue
                    storage = {
                        storage_name: storage_config
                    }
                    if "storages" in service_config:
                        service_config["storages"].update(storage)
                    else:
                        service_config["storages"] = storage

                    if "docker" in ent_config and "envvar" in ent_config["docker"]:
                        if "docker" in service_config and "envvar" in service_config["docker"]:
                            service_config["docker"]["envvar"].update(ent_config["docker"]["envvar"])
                        else:
                            service_config["docker"]["envvar"] = ent_config["docker"]["envvar"]

    merge_config(service_config, base_config)
    return service_config


def load_service(current_service_config, base_config=None):
    """Loads a service configuration.

    Args:
      current_service_config: Path the service configuration to load.
      base_config: The shared configuration to include in this service.

    Returns:
      name: The service name
      service: The service manager.
    """
    if is_polyentity_config(current_service_config):
        validate_polyentity_pool_format(current_service_config)

    service_name = current_service_config["service_name"]

    if base_config is not None:
        merge_config(current_service_config, base_config)
    if current_service_config.get("disabled") == 1:
        return service_name, None

    try:
        if "module" not in current_service_config or "docker" not in current_service_config or "description" not in current_service_config:
            raise ValueError("invalid service definition in %s" % service_name)
        service = importlib.import_module(current_service_config["module"]).init(current_service_config)
    except Exception as e:
        current_service_config["description"] = "**INVALID CONFIG: %s" % str(e)
        service = importlib.import_module("services.invalid").init(current_service_config)

    return service_name, service, current_service_config


def load_service_config(current_service_config, base_config):
    """Load configured service given a json file applying on a provided base configuration

    Args:
      current_service_config: The path to the json file configuring the service.
      base_config: Configuration for service

    Returns:
      A map of service name to service module.
    """
    services = {}

    service_name, service, merged_config = load_service(current_service_config, base_config=base_config)
    if service is None:
        raise RuntimeError("disabled service %s" % service_name)
    services[service_name] = service
    logger.info("Loaded service %s (total capacity: %s)", service_name, service.total_capacity)

    return services, merged_config


def get_service_config_from_mongo(mongo_client, service_name, config_name, views=None):
    if views is None:
        views = {"id": 0}
    service_config = mongo_client.get_config_of_service(service_name, config_name, views)
    return service_config


def get_service_config_from_file(service_name, config_name):
    config_path = os.path.join("configurations", f"{service_name}_{config_name}.json")
    assert os.path.isfile(config_path), f"Config file not found {config_path}"
    with open(config_path) as config_file:
        service_config = json.load(config_file)
        service_config["updated_at"] = os.path.getmtime(config_path)
        return service_config


def get_default_config_from_file(default_config_path):
    assert os.path.isfile(default_config_path), f"Config file not found {default_config_path}"
    with open(default_config_path) as config_file:
        default_config = json.load(config_file)
        return default_config


# Is content of config file match content of config in mongo
def is_conflict_config(mongo_client, service_name, config_name):
    config_from_file = get_service_config_from_file(service_name, config_name)
    views = {
        "_id": 0,
        "service_name": 0,
        "config_name": 0,
        "activated": 0
    }
    config_from_mongo = get_service_config_from_mongo(mongo_client, service_name, config_name, views)
    return config_from_mongo is not None and ordered(config_from_file) != ordered(config_from_mongo)


def is_conflict_default_config(mongo_client, default_config_path):
    config_from_file = get_default_config_from_file(default_config_path)
    views = {
        "_id": 0,
        "config_name": 0
    }
    config_from_mongo = get_default_config(mongo_client, views)
    return config_from_mongo is not None and ordered(config_from_file) != ordered(config_from_mongo)


def ordered(obj):
    if isinstance(obj, dict):
        return sorted((k, ordered(v)) for k, v in obj.items())
    if isinstance(obj, list):
        return sorted(ordered(x) for x in obj)
    else:
        return obj


def upsert_config(mongo_client, service_name, config_name, data):
    mongo_client.upsert_config(service_name, config_name, data)


def upsert_default_config(mongo_client, data):
    mongo_client.upsert_default_config(data)


def active_config(mongo_client, service_name, config_name):
    mongo_client.active_config(service_name, config_name)


def deactivate_current_config(mongo_client, service_name):
    mongo_client.deactivate_current_config(service_name)


def get_default_config(mongo_client, views=None):
    if views is None:
        views = {"id": 0}
    default_config = mongo_client.get_default_config(views)
    return default_config


def get_current_config_of_service(mongo_client, service_name):
    current_config = mongo_client.get_current_config_of_service(service_name)
    return current_config


def get_current_configs_of_services(mongo_client, service_names):
    current_configs = mongo_client.get_current_configs_of_services(service_names)
    return current_configs


def get_all_config_of_service(mongo_client, service_name, views):
    if views is None:
        views = {}
    configs = mongo_client.get_all_config_of_service(service_name, views)
    return configs


def get_service_config_by_name(mongo_client, service_name, config_name):
    config = mongo_client.get_service_config_by_name(service_name, config_name)
    return config


def delete_service_config_by_name(mongo_client, service_name, config_name):
    config = mongo_client.delete_service_config_by_name(service_name, config_name)
    return config
