from pymongo import MongoClient

tables = {
    "configs": "pn9-config",
    "tags": "pn9-tag",
    "dockers": "pn9-docker",
    "evaluations": "pn9-evaluation"
}


def get_connection_uri(cfg):
    uri = 'mongodb://'
    if 'user' in cfg and 'login' in cfg['user'] and 'password' in cfg['user'] \
            and cfg['user']['login'] != '' and cfg['user']['password'] != '':
        uri += cfg['user']['login'] + ':' + cfg['user']['password'] + '@'

    first = True
    for host in cfg['hosts']:
        if not first:
            uri += ','
        else:
            first = False
        uri += host['host'] + ':' + (str(host['port']) if 'port' in host else '27017')

    uri += '/' + (cfg['db_name'] if 'db_name' in cfg else 'snw_db')

    if 'replica_set' in cfg and cfg['replica_set'] != '':
        uri += '?replicaSet=' + cfg['replica_set'] + "&"
    else:
        uri += "?"

    auth_source = cfg['auth_source'] if "auth_source" in cfg and cfg['auth_source'] else "admin"

    uri += "authSource=" + auth_source
    return uri


def get_connection_options(cfg):
    opts = {}
    if 'max_pool_size' in cfg:
        opts['maxPoolSize'] = cfg['max_pool_size']
    if 'min_pool_size' in cfg:
        opts['maxPoolSize'] = cfg['max_pool_size']

    return opts


class MongoDatabase:
    def __init__(self, config):
        self._client = MongoClient(host=get_connection_uri(config), connect=False,
                                   **get_connection_options(config))
        self._mongodb = self._client[config['db_name']]

    def get(self, table):
        return self._mongodb[tables[table]]

    def get_base_config(self, views=None):
        the_table = self.get("configs")
        if views is None:
            views = {
                "_id": 0
            }
        query = {
            "name": "_base"
        }

        default_config = the_table.find_one(query, views)

        return default_config

    def update_insert_base_config(self, config_data):
        the_table = self.get("configs")
        config_data["name"] = "_base"
        query = {
            "name": "_base"
        }
        the_table.replace_one(query, config_data, upsert=True)

    def get_service_config(self, service_name, views=None):
        the_table = self.get("configs")
        if views is None:
            views = {
                "_id": 0
            }
        query = {
            "name": service_name
        }

        service_config = the_table.find_one(query, views)

        return service_config

    def update_insert_service_config(self, service_name, config_data):
        the_table = self.get("configs")
        config_data["name"] = service_name
        query = {
            "name": service_name
        }
        the_table.replace_one(query, config_data, upsert=True)

    def get_service_configs(self, services, views=None):
        the_table = self.get("configs")
        if views is None:
            views = {
                "_id": 0
            }
        query = {
            "name": {
                "$in": services
            }
        }

        service_configs = the_table.find(query, views)

        return service_configs

    def tags_put(self, items):
        the_table = self.get("tags")
        for item in items:
            item["tag"] = item["tag"].lower().strip()
        the_table.insert_many(items)

    def get_tags_by_ids(self, tag_ids):
        the_table = self.get("tags")
        conditions = {
            "_id": {
                "$in": tag_ids
            }
        }

        tags = the_table.find(conditions)
        return tags

    def get_tags_by_value(self, tags, entity_code):
        the_table = self.get("tags")
        conditions = {
            "tag": {
                "$in": tags
            }
        }
        if entity_code:
            conditions["entity"] = entity_code

        tags = the_table.find(conditions)
        return tags

    def get_docker_images(self, image):
        the_table = self.get("dockers")
        conditions = {
            "image": {
                '$regex': f'^{image}:*'
            }
        }
        images = the_table.find(conditions)
        return images

    def create_evaluation_catalog(self, evaluation_catalog):
        the_table = self.get("evaluations")
        the_table.insert(evaluation_catalog)
