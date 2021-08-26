import builtins
from nmtwizard import configuration as config
from nmtwizard.common import rmprivate
from systran_storages import StorageClient


class StorageUtils:
    @staticmethod
    def get_default_storages():
        return builtins.pn9model_db._StorageClient._config

    @staticmethod
    def get_local_storages_with_service(service, mongo_client, redis_db, has_ability, g):
        def check_service(service_name):
            entities = config.get_entities_from_service(mongo_client, service_name)
            is_ok = any(has_ability(g, "train", pool_entity) for pool_entity in entities)
            return is_ok

        services = []
        if service is None or service == '':
            for service_name in redis_db.smembers("admin:services"):
                if check_service(service_name):
                    services.append(service_name)
        elif check_service(service):
            services.append(service)

        local_storages = {}
        for service_name in services:
            json_config = config.get_service_config(mongo_client, service_name)
            if "storages" in json_config:
                pool_entity = service_name[0:2].upper()
                local_storages[service_name] = {"entities": {pool_entity: json_config["storages"]}}
            elif "entities" in json_config:
                local_storages[service_name] = {"entities": {ent: json_config["entities"][ent]["storages"]
                                                             for ent in json_config["entities"].keys()
                                                             if has_ability(g, "train", ent)}}
        return local_storages

    @staticmethod
    def get_local_storages(service, mongo_client, redis_db, has_ability, g):
        result = {}

        local_storages = StorageUtils.get_local_storages_with_service(service, mongo_client, redis_db, has_ability, g)

        for _, service_value in local_storages.items():
            cur_service = service_value["entities"]
            for ent in cur_service:
                if not has_ability(g, 'train', ent):
                    continue
                result.update(cur_service[ent])

        return result

    @staticmethod
    def get_accessible_storages(service, mongo_client, redis_db, has_ability, g):
        default_storages = StorageUtils.get_default_storages()
        local_storages = StorageUtils.get_local_storages(service, mongo_client, redis_db, has_ability, g)

        accessible_storages = {**default_storages, **local_storages}

        return accessible_storages

    @staticmethod
    def get_global_storage_name(accessible_storages):
        return next(filter(lambda key: accessible_storages[key].get("is_global"), accessible_storages.keys()))

    @staticmethod
    def get_storage_client(accessible_storages):
        storage_client = StorageClient(rmprivate(accessible_storages))
        return storage_client

    @staticmethod
    def get_storages(service, mongo_client, redis_db, has_ability, g):
        accessible_storages = StorageUtils.get_accessible_storages(service, mongo_client, redis_db, has_ability, g)
        global_storage_name = StorageUtils.get_global_storage_name(accessible_storages)
        storage_client = StorageUtils.get_storage_client(accessible_storages)

        return storage_client, global_storage_name
