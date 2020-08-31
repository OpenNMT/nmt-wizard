from nmtwizard.mongo_database import MongoDatabase
from nmtwizard.redis_database import RedisDatabase


class DatabaseUtils:
    @staticmethod
    def get_mongo_client(system_config):
        assert "database" in system_config, "Can't read mongo config from system config"
        mongo_config = system_config["database"]
        return MongoDatabase(mongo_config)

    @staticmethod
    def get_redis_client(system_config, decode_response=True):
        assert "redis" in system_config, "Can't read redis config from system config"
        redis_config = system_config["redis"]
        redis_password = None
        if "password" in redis_config:
            redis_password = redis_config["password"]

        redis_client = RedisDatabase(redis_config["host"],
                                     redis_config["port"],
                                     redis_config["db"],
                                     redis_password, decode_response)

        return redis_client
