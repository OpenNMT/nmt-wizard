import sys


def result(redis, cmd, message):
    cmd[1] = 'command_result'
    key = ':'.join(cmd)
    redis.set(key, message)
    redis.expire(key, 120)


def process(logger, redis, service, instance_id):
    for cmd_key in redis.scan_iter('admin:command:%s:*' % service):
        redis.delete(cmd_key)
        cmd = cmd_key.split(':')
        if len(cmd) != 5:
            result(redis, cmd, 'ERROR: invalid admin:config cmd - should have 5 fields')
            return

        if cmd[3] == 'restart':
            result(redis, cmd, 'ok')
            redis.delete(instance_id)
            logger.info("restarting worker `%s` by admin request" % cmd[4])
            sys.exit(1)
        elif cmd[3] == 'stop':
            result(redis, cmd, 'ok')
            redis.delete(instance_id)
            logger.info("stopping worker `%s` by admin request" % cmd[4])
            sys.exit(55)
        else:
            result(redis, cmd, 'ERROR: invalid admin:config action `%s`' % cmd[3])
