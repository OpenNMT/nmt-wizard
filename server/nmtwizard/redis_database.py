import uuid
import time
import logging
import redis
import json
from decimal import Decimal

logger = logging.getLogger(__name__)


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return json.JSONEncoder.default(self, obj)


class RedisDatabase(redis.Redis):
    """Extension to redis.Redis."""

    def __init__(self, host, port, db, password):
        """Creates a new database instance."""
        super(RedisDatabase, self).__init__(
            host=host,
            port=port,
            db=db,
            password=password,
            decode_responses=False)

    def acquire_lock(self, name, acquire_timeout=20, expire_time=60):
        return RedisLock(self, name, acquire_timeout=acquire_timeout, expire_time=expire_time)

    def get_cache(self, name, parameter, f):
        key = 'cache:%s' % name
        ser_parameter = json.dumps(parameter)
        v = self.hget(key, ser_parameter)
        if v is None:
            v = f(parameter)
            ser_v = json.dumps(v, cls=DecimalEncoder)
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
        logger.debug('Acquire lock for %s', self._name)
        self._identifier = str(uuid.uuid4())
        end = time.time() + self._acquire_timeout
        lock = 'lock:%s' % self._name
        while time.time() < end:
            if self._redis.setnx(lock, self._identifier):
                self._redis.expire(lock, self._expire_time)
                return self
            time.sleep(.1)
        raise RuntimeWarning("failed to acquire lock on %s" % self._name)

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Releases a lock given some identifier and makes sure it is the one we set
        (could have been destroyed in the meantime).
        """
        logger.debug('Release lock for %s', self._name)
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
