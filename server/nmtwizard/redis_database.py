import uuid
import time
import logging
import zlib
import json
import redis

from nmtwizard.helper import cust_jsondump

LOGGER = logging.getLogger(__name__)


class RedisDatabase(redis.Redis):
    """Extension to redis.Redis."""
    ROOT_CACHE_KEY = "cache"

    @staticmethod
    def get_cache_key(cache_key):
        return RedisDatabase.ROOT_CACHE_KEY + ":" + cache_key

    def __init__(self, host, port, db, password, decode_responses=True):
        """Creates a new database instance."""
        super(RedisDatabase, self).__init__(
            host=host,
            port=port,
            db=db,
            password=password,
            decode_responses=decode_responses)

    def acquire_lock(self, name, acquire_timeout=20, expire_time=60):
        return RedisLock(self, name, acquire_timeout=acquire_timeout, expire_time=expire_time)

    def get_model(self, name, function, *args, **kwargs):
        expired_time_ss = 3600 * 24 * 3  # 3 days
        root_key = RedisDatabase.get_cache_key(name)
        key = "||".join(map(str, args))
        compressed_value = self.hget(root_key, key)
        if compressed_value is None:
            LOGGER.debug('[MODEL_CACHE_NOT_FOUND]: %s %s', root_key, key)
            value = function(*args, **kwargs)
            str_value = cust_jsondump(value)
            compressed_value = zlib.compress(str_value.encode("utf-8"))
            result = self.hset(root_key, key, compressed_value)
            if result == 0:  # continue even in Redis error case , log a Warning
                LOGGER.error('Cannot save the model cache: %s %s', root_key, key)
            else:
                self.expire(root_key, expired_time_ss)

            return value

        LOGGER.debug('[MODEL_CACHE_FOUND]: %s %s', root_key, key)
        self.expire(root_key, expired_time_ss)
        uncompressed_value = zlib.decompress(compressed_value)
        return json.loads(uncompressed_value)

    def get_cache(self, name, parameter, f):
        key = 'cache:%s' % name
        ser_parameter = json.dumps(parameter)
        v = self.hget(key, ser_parameter)
        if v is None:
            v = f(parameter)
            ser_v = cust_jsondump(v)
            self.hset(key, ser_parameter, ser_v)
            self.expire(key, 300)
            return v
        return json.loads(v)

    def del_cache(self, name):
        key = 'cache:%s' % name
        self.delete(key)


class RedisLock(object):
    def __init__(self, redis, name, acquire_timeout=20, expire_time=60):
        self._redis = redis
        self._name = name
        self._acquire_timeout = acquire_timeout
        self._expire_time = expire_time
        self._identifier = None

    def __enter__(self):
        """Adds a lock for a specific name and expires the lock after some delay."""
        LOGGER.debug('Acquire lock for %s', self._name)
        separator = ':time:'
        self._identifier = f'{uuid.uuid4()}{separator}{time.time()}'
        end = time.time() + self._acquire_timeout
        lock = 'lock:%s' % self._name

        while time.time() < end:
            identifier = self._redis.get(lock)
            ttl = self._redis.ttl(lock)
            if identifier and ttl == -1:
                split_identifier = identifier.split(separator)
                if len(split_identifier) == 2:
                    created_at = float(split_identifier[1])
                    # If ttl is not set after 3s when created, delete it
                    if created_at + 3 < time.time():
                        LOGGER.debug('Delete lock: %s' % lock)
                        self._redis.delete(lock)
            if self._redis.setnx(lock, self._identifier):
                self._redis.expire(lock, self._expire_time)
                return self
            time.sleep(.1)
        raise RuntimeWarning("failed to acquire lock on %s" % self._name)

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Releases a lock given some identifier and makes sure it is the one we set
        (could have been destroyed in the meantime).
        """
        LOGGER.debug('Release lock for %s', self._name)
        pipe = self._redis.pipeline(True)
        lock = 'lock:%s' % self._name
        while True:
            try:
                pipe.watch(lock)
                if pipe.get(lock) == self._identifier:
                    pipe.multi()
                    pipe.delete(lock)
                    pipe.execute()
                pipe.unwatch()
                break
            except redis.exceptions.WatchError:
                pass
            return False
