"""
Lightweight Python utilities for working with Redis.
"""

__author__ = 'Charles Leifer'
__license__ = 'MIT'
__version__ = '0.1.0'

from copy import deepcopy
from functools import wraps
import datetime
import hashlib
import json
import pickle
import threading
import time
import uuid

try:
    from redis import Redis
except ImportError:
    Redis = None


class Database(Redis):
    def __init__(self, *args, **kwargs):
        super(Database, self).__init__(*args, **kwargs)
        self.__mapping = {
            'list': self.List,
            'set': self.Set,
            'zset': self.ZSet,
            'hash': self.Hash}

    def get_temp_key(self):
        return 'temp.%s' % uuid.uuid4()

    def get_intersection(self, indexes, results_exact=None):
        tmp_key = self.get_temp_key()
        if results_exact:
            n_results = self.execute_command(
                'ZINTERSTORE',
                tmp_key,
                str(results_exact),
                *indexes)
            if n_results != results_exact:
                raise ValueError('Expected %s results, got %s' % (
                    results_exact, n_results))
        else:
            self.zinterstore(tmp_key, indexes)

        results = self.zrange(tmp_key, 0, -1)
        self.delete(tmp_key)
        return results

    def __iter__(self):
        return iter(self.scan_iter())

    def search(self, pattern):
        return self.scan_iter(pattern)

    def get_key(self, key):
        return self.__mapping.get(self.type(key), self.__getitem__)(key)

    def cache(self, name='cache', default_timeout=3600):
        return Cache(self, name=name, default_timeout=default_timeout)

    def List(self, key):
        return List(self, key)

    def Hash(self, key):
        return Hash(self, key)

    def Set(self, key):
        return Set(self, key)

    def ZSet(self, key):
        return ZSet(self, key)

    def HyperLogLog(self, key):
        return HyperLogLog(self, key)

    def listener(self, channels=None, patterns=None, async=False):
        def decorator(fn):
            _channels = channels or []
            _patterns = patterns or []

            @wraps(fn)
            def inner():
                pubsub = self.pubsub()

                def listen():
                    for channel in _channels:
                        pubsub.subscribe(channel)
                    for pattern in _patterns:
                        pubsub.psubscribe(pattern)

                    for data_dict in pubsub.listen():
                        try:
                            ret = fn(**data_dict)
                        except StopIteration:
                            pubsub.close()
                            break

                if async:
                    worker = threading.Thread(target=listen)
                    worker.start()
                    return worker
                else:
                    listen()

            return inner
        return decorator

    def stream_log(self, callback, connection_id='monitor'):
        conn = self.connection_pool.get_connection(connection_id, None)
        conn.send_command('monitor')
        while callback(conn.read_response()):
            pass


def chainable_method(fn):
    @wraps(fn)
    def inner(self, *args, **kwargs):
        fn(self, *args, **kwargs)
        return self
    return inner


class Container(object):
    def __init__(self, database, key):
        self.database = database
        self.key = key

    def expire(self, ttl=None):
        if ttl is not None:
            self.database.expire(self.key, ttl)
        else:
            self.database.persist(self.key)

    def dump(self):
        return self.database.dump(self.key)

    @chainable_method
    def clear(self):
        self.database.delete(self.key)


class Hash(Container):
    def __repr__(self):
        l = len(self)
        if l > 5:
            # Get a few keys.
            data = self.database.hscan(self.key, count=5)
        else:
            data = self.as_dict()
        return '<Hash "%s": %s>' % (self.key, data)

    def __getitem__(self, item):
        if isinstance(item, (list, tuple)):
            return self.database.hmget(self.key, item)
        else:
            return self.database.hget(self.key, item)

    def __setitem__(self, key, value):
        return self.database.hset(self.key, key, value)

    def __delitem__(self, key):
        return self.database.hdel(self.key, key)

    def __contains__(self, key):
        return self.database.hexists(self.key, key)

    def __len__(self):
        return self.database.hlen(self.key)

    def __iter__(self):
        return iter(self.database.hscan_iter(self.key))

    def search(self, pattern, count=None):
        return self.database.hscan_iter(self.key, pattern, count)

    def keys(self):
        return self.database.hkeys(self.key)

    def values(self):
        return self.database.hvals(self.key)

    def items(self, lazy=False):
        if lazy:
            return self.database.hscan_iter(self.key)
        else:
            return list(self)

    @chainable_method
    def update(self, *args, **kwargs):
        if args:
            self.database.hmset(self.key, *args)
        else:
            self.database.hmset(self.key, kwargs)

    def as_dict(self):
        return self.database.hgetall(self.key)

    def incr(self, key, incr_by=1):
        return self.database.hincrby(self.key, key, incr_by)


class List(Container):
    def __repr__(self):
        l = len(self)
        n_items = min(l, 10)
        return '<List "%s": %s%s>' % (
            self.key,
            ', '.join(self[:n_items]),
            n_items < l and '...' or '')

    def __getitem__(self, item):
        if isinstance(item, slice):
            start = item.start or 0
            stop = item.stop
            if not stop:
                stop = -1
            else:
                stop -= 1
            return self.database.lrange(self.key, start, stop)
        return self.database.lindex(self.key, item)

    def __setitem__(self, idx, value):
        return self.database.lset(self.key, idx, value)

    def __delitem__(self, item):
        if isinstance(item, int):
            item = self[item]
            if item is None:
                return
        return self.database.lrem(self.key, item)

    def __len__(self):
        return self.database.llen(self.key)

    def __iter__(self):
        return iter(self.database.lrange(self.key, 0, -1))

    def append(self, value):
        return self.database.rpush(self.key, value)

    def prepend(self, value):
        return self.database.lpush(self.key, value)

    def extend(self, value):
        return self.database.rpush(self.key, *value)

    def insert(self, value, pivot, where):
        return self.database.linsert(self.key, where, pivot, value)

    def insert_before(self, value, key):
        self.insert(value, key, 'before')

    def insert_after(self, value, key):
        self.insert(value, key, 'after')

    def popleft(self):
        return self.database.lpop(self.key)

    def popright(self):
        return self.database.rpop(self.key)
    pop = popright

    def move_tail(self, key):
        return self.database.rpoplpush(self.key, key)


class Set(Container):
    def __repr__(self):
        return '<Set "%s": %s items>' % (self.key, len(self))

    def add(self, *items):
        return self.database.sadd(self.key, *items)

    def __delitem__(self, item):
        return self.remove(item)

    def remove(self, *items):
        return self.database.srem(self.key, *items)

    def pop(self):
        return self.database.spop(self.key)

    def __contains__(self, item):
        return self.database.sismember(self.key, item)

    def __len__(self):
        return self.database.scard(self.key)

    def __iter__(self):
        return iter(self.database.sscan_iter(self.key))

    def search(self, pattern, count=None):
        return self.database.sscan_iter(self.key, pattern, count)

    def members(self):
        return self.database.smembers(self.key)

    def random(self, n=None):
        return self.database.srandmember(self.key, n)

    def __sub__(self, other):
        return self.database.sdiff(self.key, other.key)

    def __or__(self, other):
        return self.database.sunion(self.key, other.key)

    def __and__(self, other):
        return self.database.sinter(self.key, other.key)

    @chainable_method
    def __isub__(self, other):
        self.diffstore(self.key, other)

    @chainable_method
    def __ior__(self, other):
        self.unionstore(self.key, other)

    @chainable_method
    def __iand__(self, other):
        self.interstore(self.key, other)

    def diffstore(self, dest, *others):
        keys = [self.key]
        keys.extend([other.key for other in others])
        self.database.sdiffstore(dest, keys)
        return Set(self.database, dest)

    def interstore(self, dest, *others):
        keys = [self.key]
        keys.extend([other.key for other in others])
        self.database.sinterstore(dest, keys)
        return Set(self.database, dest)

    def unionstore(self, dest, *others):
        keys = [self.key]
        keys.extend([other.key for other in others])
        self.database.sunionstore(dest, keys)
        return Set(self.database, dest)


class ZSet(Container):
    def __repr__(self):
        l = len(self)
        n_items = min(l, 5)
        return '<ZSet "%s": %s%s>' % (
            self.key,
            ', '.join(self[:n_items, False]),
            n_items < l and '...' or '')

    def add(self, *args, **kwargs):
        return self.database.zadd(self.key, *args, **kwargs)

    def _convert_slice(self, s):
        def _slice_to_indexes(s):
            start = s.start
            stop = s.stop
            if isinstance(start, int) or isinstance(stop, int):
                return start, stop
            if start:
                start = self.database.zrank(self.key, start)
                if start is None:
                    raise KeyError(s.start)
            if stop:
                stop = self.database.zrank(self.key, stop)
                if stop is None:
                    raise KeyError(s.stop)
            return start, stop
        start, stop = _slice_to_indexes(s)
        start = start or 0
        if not stop:
            stop = -1
        else:
            stop -= 1
        return start, stop

    def __getitem__(self, item):
        if isinstance(item, tuple) and len(item) == 2:
            item, withscores = item
        else:
            withscores = False

        if isinstance(item, slice):
            start, stop = self._convert_slice(item)
        else:
            start = stop = item

        return self.database.zrange(
            self.key,
            start,
            stop,
            withscores=withscores)

    def __setitem__(self, item, score):
        return self.database.zadd(self.key, item, score)

    def __delitem__(self, item):
        if isinstance(item, slice):
            start, stop = self._convert_slice(item)
            return self.database.zremrangebyrank(self.key, start, stop)
        else:
            return self.remove(item)

    def remove(self, *items):
        return self.database.zrem(self.key, *items)

    def __contains__(self, item):
        return not (self.rank(item) is None)

    def __len__(self):
        return self.database.zcard(self.key)

    def __iter__(self):
        return iter(self.database.zscan_iter(self.key))

    def search(self, pattern, count=None):
        return self.database.zscan_iter(self.key, pattern, count)

    def score(self, item):
        return self.database.zscore(self.key, item)

    def rank(self, item, reverse=False):
        fn = reverse and self.database.zrevrank or self.database.zrank
        return fn(self.key, item)

    def count(self, low, high=None):
        if high is None:
            high = low
        return self.database.zcount(self.key, low, high)

    def lex_count(self, low, high):
        """
        Count the number of members in a sorted set between a given
        lexicographical range.
        """
        return self.database.zlexcount(self.key, low, high)

    def range(self, low, high, with_scores=False, reverse=False):
        return self.database.zrange(self.key, low, high, reverse, with_scores)

    def range_by_score(self, low, high, start=None, num=None,
                       with_scores=False, reverse=False):
        fn = (reverse and
              self.database.zrevrangebyscore or
              self.database.zrangebyscore)
        return fn(self.key, low, high, start, num, with_scores)

    def range_by_lex(self, low, high, start=None, num=None, reverse=False):
        """
        Return a range of members in a sorted set, by lexicographical range.
        """
        fn = (reverse and
              self.database.zrevrangebylex or
              self.database.zrangebylex)
        return fn(self.key, low, high, start, num)

    def remove_by_rank(self, low, high=None):
        if high is None:
            high = low
        return self.database.zremrangebyrank(self.key, low, high)

    def remove_by_score(self, low, high=None):
        if high is None:
            high = low
        return self.database.zremrangebyscore(self.key, low, high)

    def remove_by_lex(self, low, high):
        return self.database.zremrangebylex(self.key, low, high)

    def incr(self, key, incr_by=1):
        return self.database.zincrby(self.key, key, incr_by)

    @chainable_method
    def __ior__(self, other):
        self.unionstore(self.key, other)
        return self

    @chainable_method
    def __iand__(self, other):
        self.interstore(self.key, other)
        return self

    def interstore(self, dest, *others, **kwargs):
        keys = [self.key]
        keys.extend([other.key for other in others])
        self.database.zinterstore(dest, keys, **kwargs)
        return ZSet(self.database, dest)

    def unionstore(self, dest, *others, **kwargs):
        keys = [self.key]
        keys.extend([other.key for other in others])
        self.database.zunionstore(dest, keys, **kwargs)
        return ZSet(self.database, dest)


class HyperLogLog(Container):
    def add(self, *items):
        return self.database.pfadd(self.key, *items)

    def __len__(self):
        return self.database.pfcount(self.key)

    def __ior__(self, other):
        if not isinstance(other, (list, tuple)):
            other = [other]
        return self.merge(self.key, *other)

    def merge(self, dest, *others):
        items = (self.key,) + others
        self.database.pfmerge(dest, *items)
        return HyperLogLog(self.database, dest)


class Cache(object):
    def __init__(self, database, name='cache', default_timeout=None):
        self.database = database
        self.name = name
        self.default_timeout = default_timeout

    def make_key(self, s):
        return ':'.join((self.name, s))

    def get(self, key, default=None):
        key = self.make_key(key)
        try:
            value = self.database[key]
        except KeyError:
            return default
        else:
            return pickle.loads(value)

    def set(self, key, value, timeout=None):
        key = self.make_key(key)
        if timeout is None:
            timeout = self.default_timeout

        pickled_value = pickle.dumps(value)
        if timeout:
            return self.database.setex(key, pickled_value, int(timeout))
        else:
            return self.database.set(key, pickled_value)

    def delete(self, key):
        self.database.delete(self.make_key(key))

    def keys(self):
        return self.database.keys(self.make_key('') + '*')

    def flush(self):
        return self.database.delete(*self.keys())

    def incr(self, key, delta=1):
        return self.database.incr(self.make_key(key), delta)

    def _key_fn(a, k):
        return hashlib.md5(pickle.dumps((a, k))).hexdigest()

    def cached(self, key_fn=_key_fn, timeout=3600):
        def decorator(fn):
            def bust(*args, **kwargs):
                return self.delete(key_fn(args, kwargs))
            @wraps(fn)
            def inner(*args, **kwargs):
                key = key_fn(args, kwargs)
                res = self.get(key)
                if res is None:
                    res = fn(*args, **kwargs)
                    self.set(key, res, timeout)
                return res
            inner.bust = bust
            return inner
        return decorator


OP_AND = 'and'
OP_OR = 'or'
OP_EQ = '=='
OP_NE = '!='
OP_LT = '<'
OP_LTE = '<='
OP_GT = '>'
OP_GTE = '>='

ABSOLUTE = set([OP_EQ, OP_NE])
CONTINUOUS = set([OP_LT, OP_LTE, OP_GT, OP_GTE])


class Node(object):
    def _e(op, inv=False):
        def inner(self, rhs):
            if inv:
                return Expression(rhs, op, self)
            return Expression(self, op, rhs)
        return inner
    __and__ = _e(OP_AND)
    __or__ = _e(OP_OR)
    __rand__ = _e(OP_AND, inv=True)
    __ror__ = _e(OP_OR, inv=True)
    __eq__ = _e(OP_EQ)
    __ne__ = _e(OP_NE)
    __lt__ = _e(OP_LT)
    __le__ = _e(OP_LTE)
    __gt__ = _e(OP_GT)
    __ge__ = _e(OP_GTE)


class Expression(Node):
    def __init__(self, lhs, op, rhs):
        self.lhs = lhs
        self.op = op
        self.rhs = rhs

    def __repr__(self):
        return '(%s %s %s)' % (self.lhs, self.op, self.rhs)


class BaseIndex(object):
    operations = None

    def __init__(self, field):
        self.field = field
        self.database = self.field.model_class.database
        self.query_helper = self.field.model_class._query

    def field_value(self, instance):
        return self.field.db_value(getattr(instance, self.field.name))

    def get_key(self, instance, value):
        raise NotImplementedError

    def store_instance(self, key, instance, value):
        raise NotImplementedError

    def delete_instance(self, key, instance, value):
        raise NotImplementedError

    def save(self, instance):
        value = self.field_value(instance)
        key = self.get_key(value)
        self.store_instance(key, instance, value)

    def remove(self, instance):
        value = self.field_value(instance)
        key = self.get_key(value)
        self.delete_instance(key, instance, value)


class AbsoluteIndex(BaseIndex):
    operations = ABSOLUTE

    def get_key(self, value):
        key = self.query_helper.make_key(
            self.field.name,
            'absolute',
            value)
        return self.database.Set(key)

    def store_instance(self, key, instance, value):
        key.add(instance.get_hash_id())

    def delete_instance(self, key, instance, value):
        key.remove(instance.get_hash_id())


class ContinuousIndex(BaseIndex):
    operations = CONTINUOUS

    def get_key(self, value):
        key = self.query_helper.make_key(
            self.field.name,
            'continuous')
        return self.database.ZSet(key)

    def store_instance(self, key, instance, value):
        key[instance.get_hash_id()] = value

    def delete_instance(self, key, instance, value):
        del key[instance.get_hash_id()]


class Field(Node):
    _coerce = None

    def __init__(self, index=False, as_json=False, primary_key=False,
                 pickled=False, default=None):
        """
        :param bool index: Use this field as an index. Indexed fields will
            support :py:meth:`Model.get` lookups.
        :param bool as_json: Whether the value should be serialized as JSON
            when storing in the database. Useful for collections or objects.
        :param bool primary_key: Use this field as the primary key.
        :param bool pickled: Whether the value should be pickled when storing
            in the database. Useful for non-primitive content types.
        """
        self._index = index or primary_key
        self._as_json = as_json
        self._primary_key = primary_key
        self._pickled = pickled
        self._default = default

    def _generate_key(self):
        raise NotImplementedError

    def db_value(self, value):
        if self._pickled:
            return pickle.dumps(value)
        elif self._as_json:
            return json.dumps(value)
        elif self._coerce:
            return self._coerce(value)
        return value

    def python_value(self, value):
        if self._pickled:
            return pickle.loads(value)
        elif self._as_json:
            return json.loads(value)
        elif self._coerce:
            return self._coerce(value)
        return value

    def add_to_class(self, model_class, name):
        self.model_class = model_class
        self.name = name
        setattr(model_class, name, self)

    def __get__(self, instance, instance_type=None):
        if instance is not None:
            return instance._data[self.name]
        return self

    def __set__(self, instance, value):
        instance._data[self.name] = value

    def get_index(self, op):
        indexes = self.get_indexes()
        for index in indexes:
            if op in index.operations:
                return index

        raise ValueError('Operation %s is not supported by an index.' % op)

    def get_indexes(self):
        return [AbsoluteIndex(self)]


class _ScalarField(Field):
    def get_indexes(self):
        return [AbsoluteIndex(self), ContinuousIndex(self)]


class IntegerField(_ScalarField):
    _coerce = int


class AutoIncrementField(IntegerField):
    def __init__(self, *args, **kwargs):
        kwargs['primary_key'] = True
        return super(AutoIncrementField, self).__init__(*args, **kwargs)

    def _generate_key(self):
        query_helper = self.model_class._query
        key = query_helper.make_key(self.name, '_sequence')
        return self.model_class.database.incr(key)


class FloatField(_ScalarField):
    _coerce = float


class ByteField(Field):
    _coerce = str


class TextField(Field):
    def db_value(self, value):
        if value is None:
            return value
        elif isinstance(value, unicode):
            return value.encode('utf-8')
        return value

    def python_value(self, value):
        if value:
            return value.decode('utf-8')
        return value


class BooleanField(Field):
    def db_value(self, value):
        return value and 1 or 0

    def python_value(self, value):
        return str(value) == '1'


class UUIDField(Field):
    def __init__(self, **kwargs):
        kwargs['index'] = True
        super(UUIDField, self).__init__(**kwargs)

    def db_value(self, value):
        return str(value)

    def python_value(self, value):
        return uuid.UUID(value)

    def _generate_key(self):
        return uuid.uuid4()


class DateTimeField(_ScalarField):
    def db_value(self, value):
        timestamp = time.mktime(value.timetuple())
        micro = value.microsecond * (10 ** -6)
        return timestamp + micro

    def python_value(self, value):
        if isinstance(value, (basestring, int, float)):
            return datetime.datetime.fromtimestamp(float(value))
        return value


class DateField(DateTimeField):
    def python_value(self, value):
        if isinstance(value, (basestring, int, float)):
            return datetime.datetime.fromtimestamp(float(value)).date()
        return value


class JSONField(Field):
    def __init__(self, *args, **kwargs):
        kwargs['as_json'] = True
        super(JSONField, self).__init__(*args, **kwargs)


class Query(object):
    def __init__(self, model_class):
        self.model_class = model_class

    @property
    def _base_key(self):
        model_name = self.model_class.__name__.lower()
        if self.model_class.namespace:
            return '%s|%s:' % (self.model_class.namespace, model_name)
        return '%s:' % model_name

    def make_key(self, *parts):
        """Generate a namespaced key for the given path."""
        return '%s%s' % (self._base_key, '.'.join(map(str, parts)))

    def get_primary_hash_key(self, primary_key):
        pk_field = self.model_class._fields[self.model_class._primary_key]
        return self.make_key('id', pk_field.db_value(primary_key))

    def all_index(self):
        return self.model_class.database.Set(self.make_key('all'))


class Executor(object):
    def __init__(self, database, temp_key_expire=30):
        self.database = database
        self.temp_key_expire = 30
        self._mapping = {
            OP_OR: self.execute_or,
            OP_AND: self.execute_and,
            OP_EQ: self.execute_eq,
            OP_NE: self.execute_ne,
            #OP_GT: self.execute_gt,
            OP_GTE: self.execute_gte,
            #OP_LT: self.execute_lt,
            OP_LTE: self.execute_lte,
        }

    def execute(self, expression):
        op = expression.op
        return self._mapping[op](expression.lhs, expression.rhs)

    def execute_eq(self, lhs, rhs):
        index = lhs.get_index(OP_EQ)
        return index.get_key(lhs.db_value(rhs))

    def execute_ne(self, lhs, rhs):
        all_set = lhs.model_class._query.all_index()
        index = lhs.get_index(OP_NE)
        exclude_set = index.get_key(lhs.db_value(rhs))
        tmp_set = all_set.diffstore(self.database.get_temp_key(), exclude_set)
        tmp_set.expire(self.temp_key_expire)
        return tmp_set

    def _zset_score_filter(self, zset, low, high):
        values = zset.range_by_score(low, high)
        tmp_set = self.database.Set(self.database.get_temp_key())
        tmp_set.add(*values)
        tmp_set.expire(self.temp_key_expire)
        return tmp_set

    def execute_lte(self, lhs, rhs):
        index = lhs.get_index(OP_LTE)
        db_value = lhs.db_value(rhs)
        zset = index.get_key(db_value)
        return self._zset_score_filter(zset, float('-inf'), db_value)

    def execute_gte(self, lhs, rhs):
        index = lhs.get_index(OP_GTE)
        db_value = lhs.db_value(rhs)
        zset = index.get_key(db_value)
        return self._zset_score_filter(zset, db_value, float('inf'))

    def _combine_sets(self, lhs, rhs, operation):
        if not isinstance(lhs, (Set, ZSet)):
            lhs = self.execute(lhs)
        if not isinstance(rhs, (Set, ZSet)):
            rhs = self.execute(rhs)
        if operation == 'AND':
            method = lhs.interstore
        elif operation == 'OR':
            method = lhs.unionstore
        else:
            raise ValueError('Unrecognized operation: "%s".' % operation)
        tmp_set = method(self.database.get_temp_key(), rhs)
        tmp_set.expire(self.temp_key_expire)
        return tmp_set

    def execute_or(self, lhs, rhs):
        return self._combine_sets(lhs, rhs, 'OR')

    def execute_and(self, lhs, rhs):
        return self._combine_sets(lhs, rhs, 'AND')


class BaseModel(type):
    def __new__(cls, name, bases, attrs):
        if not bases:
            return super(BaseModel, cls).__new__(cls, name, bases, attrs)

        ignore = set()
        primary_key = None

        for key, value in attrs.items():
            if isinstance(value, Field) and value._primary_key:
                primary_key = (key, value)

        for base in bases:
            for key, value in base.__dict__.items():
                if key in attrs:
                    continue
                if isinstance(value, Field):
                    if value._primary_key and primary_key:
                        ignore.add(key)
                    else:
                        if value._primary_key:
                            primary_key = (key, value)
                        attrs[key] = deepcopy(value)

        if not primary_key:
            attrs['_id'] = AutoIncrementField()
            primary_key = ('_id', attrs['_id'])

        model_class = super(BaseModel, cls).__new__(cls, name, bases, attrs)
        model_class._data = None

        defaults = {}
        fields = {}
        indexes = []
        for key, value in model_class.__dict__.items():
            if isinstance(value, Field) and key not in ignore:
                value.add_to_class(model_class, key)
                if value._index:
                    indexes.append(value)
                fields[key] = value
                if value._default:
                    defaults[key] = value._default

        model_class._defaults = defaults
        model_class._fields = fields
        model_class._indexes = indexes
        model_class._primary_key = primary_key[0]
        model_class._query = Query(model_class)
        return model_class


def _with_metaclass(meta, base=object):
    return meta("NewBase", (base,), {'database': None, 'namespace': None})


class Model(_with_metaclass(BaseModel)):
    database = None
    namespace = None

    def __init__(self, *args, **kwargs):
        self._data = {}
        self._load_default_dict()
        for k, v in kwargs.items():
            setattr(self, k, v)

    def __repr__(self):
        return '<%s: %s>' % (type(self).__name__, self.get_id())

    def _load_default_dict(self):
        for field_name, default in self._defaults.items():
            if callable(default):
                default = default()
            setattr(self, field_name, default)

    def get_id(self):
        return getattr(self, self._primary_key)

    def get_hash_id(self):
        return self._query.get_primary_hash_key(self.get_id())

    def _get_data_dict(self):
        data = {}
        for name, field in self._fields.items():
            if name in self._data:
                data[name] = field.db_value(self._data[name])
        return data

    def to_hash(self):
        return self.database.Hash(self.get_hash_id())

    @classmethod
    def create(cls, **kwargs):
        instance = cls(**kwargs)
        instance.save()
        return instance

    @classmethod
    def all(cls):
        for result in cls._query.all_index():
            yield cls.load(result, convert_key=False)

    @classmethod
    def filter(cls, expression):
        executor = Executor(cls.database)
        result = executor.execute(expression)
        for hash_id in result:
            yield cls.load(hash_id, convert_key=False)

    @classmethod
    def get(cls, expression):
        executor = Executor(cls.database)
        result = executor.execute(expression)
        if len(result) != 1:
            raise ValueError('Got %s results, expected 1.' % len(result))
        return cls.load(result.pop(), convert_key=False)

    @classmethod
    def load(cls, primary_key, convert_key=True):
        if convert_key:
            primary_key = cls._query.get_primary_hash_key(primary_key)
        raw_data = cls.database.hgetall(primary_key)
        data = {}
        for name, field in cls._fields.items():
            if name not in raw_data:
                data[name] = None
            else:
                data[name] = field.python_value(raw_data[name])
        return cls(**data)

    def delete(self):
        hash_key = self.get_hash_id()
        original_instance = self.load(hash_key, convert_key=False)

        # Remove from the `all` index.
        all_index = self._query.all_index()
        all_index.remove(hash_key)

        # Remove from the secondary indexes.
        for field in self._indexes:
            for index in field.get_indexes():
                index.remove(self)

        # Remove the object itself.
        self.database.delete(hash_key)

    def save(self):
        pk_field = self._fields[self._primary_key]
        if not self._data.get(self._primary_key):
            setattr(self, self._primary_key, pk_field._generate_key())
            require_delete = False
        else:
            require_delete = True

        if require_delete:
            self.delete()

        data = self._get_data_dict()
        hash_obj = self.to_hash()
        hash_obj.clear()
        hash_obj.update(data)

        all_index = self._query.all_index()
        all_index.add(self.get_hash_id())

        for field in self._indexes:
            for index in field.get_indexes():
                index.save(self)
