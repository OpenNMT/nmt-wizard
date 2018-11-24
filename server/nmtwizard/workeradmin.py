import re
import json
import os
import time
import shutil
import sys

re_badchar = re.compile(r"[^-A-Za-z_0-9]")


def result(redis, cmd, message):
    cmd[1] = 'configresult'
    key = ':'.join(cmd)
    redis.set(key, message)
    redis.expire(key, 120)


def process(logger, redis, service):
    for cmd_key in redis.scan_iter('admin:config:%s:*' % service):
        v = redis.get(cmd_key)
        redis.delete(cmd_key)
        cmd = cmd_key.split(':')
        if len(cmd) != 6:
            result(redis, cmd, 'ERROR: invalid admin:config cmd - should have 6 fields')
            return

        keys = 'admin:service:%s' % service
        current_configuration = redis.hget(keys, "current_configuration")
        configurations = json.loads(redis.hget(keys, "configurations"))

        if cmd[3] == 'set':
            if cmd[4] == 'base' or re_badchar.search(cmd[4]):
                result(redis, cmd, 'ERROR: invalid name `%s` for set' % cmd[4])
                return
            if cmd[4] == current_configuration:
                result(redis, cmd, 'ERROR: cannot set current configuration')
                return
            try:
                config = json.loads(v)
                assert config.get("name") == service, "config name does not match"
            except Exception as err:
                result(redis, cmd, 'ERROR: cannot set configuration: %s' % str(err))
                return
            with open(os.path.join("configurations", "%s_%s.json" % (service, cmd[4])), "w") as f:
                f.write(v)
            configurations[cmd[4]] = (time.time(), v)
            redis.hset(keys, "configurations", json.dumps(configurations))
            result(redis, cmd, 'ok')
            pass
        elif cmd[3] == 'del':
            if cmd[4] == 'base':
                result(redis, cmd, 'ERROR: cannot delete `base` configuration')
                return
            if cmd[4] == current_configuration:
                result(redis, cmd, 'ERROR: cannot delete current configuration')
                return
            if cmd[4] not in configurations:
                result(redis, cmd, 'ERROR: cannot delete `%s` configuration - unknown' % cmd[4])
                return
            os.remove(os.path.join("configurations", "%s_%s.json" % (service, cmd[4])))
            del configurations[cmd[4]]
            redis.hset(keys, "configurations", json.dumps(configurations))
            result(redis, cmd, 'ok')
            pass
        elif cmd[3] == 'select':
            if cmd[4] == current_configuration:
                result(redis, cmd, 'ERROR: `%s` configuration already set' % cmd[4])
                return
            if cmd[4] not in configurations:
                result(redis, cmd, 'ERROR: cannot set `%s` configuration - unknown' % cmd[4])
                return
            config_file = os.path.join("configurations", "%s_%s.json" % (service, cmd[4]))
            if not os.path.isfile(config_file):
                result(redis, cmd, 'INTERNAL ERROR: missing configuration %s' % cmd[4])
                logger.info("INTERNAL ERROR: missing configuration (=> %s) - restarting" % cmd[4])
            else:
                shutil.copyfile("configurations/%s_%s.json" % (service, cmd[4]),
                                "%s.json" % service)
                result(redis, cmd, 'ok')
                logger.info("restarting worker after configuration change (=> %s)" % cmd[4])
            sys.exit(0)
        elif cmd[3] == 'restart':
            result(redis, cmd, 'ok')
            logger.info("restarting worker `%s` by admin request" % cmd[4])
            sys.exit(1)
        elif cmd[3] == 'stop':
            result(redis, cmd, 'ok')
            logger.info("stopping worker `%s` by admin request" % cmd[4])
            sys.exit(55)
        else:
            result(redis, cmd, 'ERROR: invalid admin:config action `%s`' % cmd[3])
