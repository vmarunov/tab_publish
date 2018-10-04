# -*- coding: utf-8 -*-
import json
import traceback

import redis
import redis.client

STAT_REDIS = None
STAT_KEY = 'redisStat'


def patch_redis(clazz, host='127.0.0.1', port=6379, password=None):
    global STAT_REDIS
    STAT_REDIS = redis.Redis(host=host, port=port, password=password)
    STAT_REDIS.delete(STAT_KEY)
    if isinstance(clazz, redis.Redis.__class__):
        setattr(redis.client, 'Pipeline', PatchedPipeline)
        setattr(redis, 'Redis', PatchedRedis)
    if isinstance(clazz, redis.StrictRedis.__class__):
        setattr(redis.client, 'Pipeline', PatchedPipeline)
        setattr(redis, 'StrictRedis', PatchedStrictRedis)
    return clazz


_DEL_COMMANDS = {
    'DEL', 'HDEL', 'XDEL', 'FLUSHALL', 'FLUSHDB', 'BLPOP', 'BRPOP', 'BZPOPMIN',
    'BZPOPMAX', 'LPOP', 'LREM', 'RPOP', 'RPOPLPUSH', 'SPOP', 'SREM', 'ZPOPMAX',
    'ZPOPMIN', 'ZREM', 'ZREMRANGEBYLEX', 'ZREMRANGEBYRANK', 'ZREMRANGEBYSCORE'}
_REGISTERED_COMMAND = {
    'APPEND', 'DECR', 'DECRBY', 'DEL', 'EXISTS', 'GET', 'HDEL', 'HEXISTS',
    'HGET', 'HGETALL', 'HINCRBY', 'HKEYS', 'HLEN', 'HMGET', 'HSET', 'HMSET',
    'INCR', 'KEYS', 'LINDEX', 'LINSERT', 'LLEN', 'LPOP', 'LPUSH', 'LREM',
    'LRANGE', 'LSET', 'LTRIM', 'MSET', 'RPOP', 'RPUSH', 'SADD', 'SET',
    'SINTER', 'SISMEMBER', 'SMEMBERS', 'SMOVE', 'SPOP', 'SREM', 'SUNION'}

# (source_line, command, command_type, key)
# _REDIS_STAT = []

# Коэффициент для определения паттернов
_PATTERN_NUM = 0.05


def save_stat(file_name, raw=False, command_type='a'):
    """
    :param file_name: csv file name
    :param raw: output raw statistics
    :param command_type: a - all, d - delete, m - modify
    """
    rows = map(json.loads, STAT_REDIS.lrange(STAT_KEY, 0, -1))
    patterns = None
    if not raw:
        patterns = _collect_patterns(rows)
    stat = dict()
    for row in rows:
        if command_type != 'a' and command_type != row[2]:
            continue
        if raw:
            pattern = row[3]
        else:
            pattern = _get_pattern(row[3], patterns)
        stat.setdefault(pattern, set()).add((row[0], row[1]))

    with open(file_name, 'wb') as fl:
        fl.write('PATTERN;SOURCE;REDIS_CMD\n')
        for pattern, lines in stat.iteritems():
            for line, command in lines:
                fl.write('{};{};{}\n'.format(pattern, line, command))


def _get_pattern(key, patterns):

    data = key.split(':')

    def _match(pattern):
        if len(pattern) != len(data):
            return False

        for idx, elm in enumerate(pattern):
            if elm != '*' and elm != data[idx]:
                return False

        return True

    for pattern in patterns:
        if _match(pattern):
            return ':'.join(pattern)
    return key


def _collect_patterns(rows):

    all_keys = set(row[3] for row in rows)

    def _to_pattern(keys):
        levels = len(keys[0])
        all_length = float(len(keys))
        groups = []
        for cnt in range(1, levels):
            if len(set(key[cnt] for key in keys)) / all_length > _PATTERN_NUM:
                groups.append(cnt)
        templates = set()
        for key in keys:
            template_key = list(key)
            for cnt in groups:
                template_key[cnt] = '*'
            templates.add(':'.join(template_key))
        return templates

    # Делим на части
    parts = map(lambda x: x.split(':'), all_keys)

    # Разбиваем по размерностям
    dims = dict()
    for part in parts:
        top = part[0]
        dims.setdefault(top, dict())
        length = len(part)
        dims[top].setdefault(length, []).append(part)

    # Разбиваем на паттерны
    patterns = []
    for top, part_count_dict in dims.iteritems():
        for count, keys in part_count_dict.iteritems():
            patterns.extend(_to_pattern(keys))

    return map(lambda x: x.split(':'), patterns)


def _process(*args, **options):
    if args[0] == STAT_KEY:
        return
    stack = traceback.extract_stack()
    line = stack[-4]
    command = args[0]
    command_type = 'd' if command in _DEL_COMMANDS else 'm'
    key = args[1] if len(args) > 1 else ''
    source_line = '{}:{}'.format(line[0], line[1])
    STAT_REDIS.rpush(
        STAT_KEY, json.dumps([source_line, command, command_type, key]))


class PatchedRedis(redis.Redis):
    def execute_command(self, *args, **options):
        if args[0] in _REGISTERED_COMMAND:
            _process(*args, **options)
        return super(PatchedRedis, self).execute_command(*args, **options)


class PatchedStrictRedis(redis.StrictRedis):

    def execute_command(self, *args, **options):
        if args[0] in _REGISTERED_COMMAND:
            _process(*args, **options)
        return super(PatchedStrictRedis, self).execute_command(*args, **options)


class PatchedPipeline(redis.client.Pipeline):

    def execute_command(self, *args, **options):
        if args[0] in _REGISTERED_COMMAND:
            _process(*args, **options)
        return super(PatchedPipeline, self).execute_command(*args, **options)
