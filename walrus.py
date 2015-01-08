"""
Lightweight Python utilities for working with Redis.
"""

__author__ = 'Charles Leifer'
__license__ = 'MIT'
__version__ = '0.1.2'

from copy import deepcopy
from functools import wraps
import datetime
import glob
import hashlib
import json
import os
import pickle
import re
import threading
import time
import uuid

try:
    from redis import Redis
except ImportError:
    Redis = None


class Database(Redis):
    """
    Redis-py client with some extras.
    """
    def __init__(self, *args, **kwargs):
        """
        :param args: Arbitrary positional arguments to pass to the
            base ``Redis`` instance.
        :param kwargs: Arbitrary keyword arguments to pass to the
            base ``Redis`` instance.
        :param str script_dir: Path to directory containing walrus
            scripts.
        """
        script_dir = kwargs.pop('script_dir', None)
        super(Database, self).__init__(*args, **kwargs)
        self.__mapping = {
            'list': self.List,
            'set': self.Set,
            'zset': self.ZSet,
            'hash': self.Hash}
        self.init_scripts(script_dir=script_dir)

    def init_scripts(self, script_dir=None):
        self._scripts = {}
        if not script_dir:
            script_dir = os.path.join(os.path.dirname(__file__), 'scripts')
        for filename in glob.glob(os.path.join(script_dir, '*.lua')):
            with open(filename, 'r') as fh:
                script_obj = self.register_script(fh.read())
                script_name = os.path.splitext(os.path.basename(filename))[0]
                self._scripts[script_name] = script_obj

    def run_script(self, script_name, keys=None, args=None):
        """
        Execute a walrus script with the given arguments.

        :param script_name: The base name of the script to execute.
        :param list keys: Keys referenced by the script.
        :param list args: Arguments passed in to the script.
        :returns: Return value of script.

        .. note:: Redis scripts require two parameters, ``keys``
            and ``args``, which are referenced in lua as ``KEYS``
            and ``ARGV``.
        """
        return self._scripts[script_name](keys, args)

    def get_temp_key(self):
        """
        Generate a temporary random key using UUID4.
        """
        return 'temp.%s' % uuid.uuid4()

    def __iter__(self):
        """
        Iterate over the keys of the selected database.
        """
        return iter(self.scan_iter())

    def search(self, pattern):
        """
        Search the keyspace of the selected database using the
        given search pattern.

        :param str pattern: Search pattern using wildcards.
        :returns: Iterator that yields matching keys.
        """
        return self.scan_iter(pattern)

    def get_key(self, key):
        """
        Return a rich object for the given key. For instance, if
        a hash key is requested, then a :py:class:`Hash` will be
        returned.

        :param str key: Key to retrieve.
        :returns: A hash, set, list, zset or array.
        """
        return self.__mapping.get(self.type(key), self.__getitem__)(key)

    def cache(self, name='cache', default_timeout=3600):
        """
        Create a cache instance.

        :param str name: The name used to prefix keys used to
            store cached data.
        :param int default_timeout: The default key expiry.
        :returns: A :py:class:`Cache` instance.
        """
        return Cache(self, name=name, default_timeout=default_timeout)

    def List(self, key):
        """
        Create a :py:class:`List` instance wrapping the given key.
        """
        return List(self, key)

    def Hash(self, key):
        """
        Create a :py:class:`Hash` instance wrapping the given key.
        """
        return Hash(self, key)

    def Set(self, key):
        """
        Create a :py:class:`Set` instance wrapping the given key.
        """
        return Set(self, key)

    def ZSet(self, key):
        """
        Create a :py:class:`ZSet` instance wrapping the given key.
        """
        return ZSet(self, key)

    def HyperLogLog(self, key):
        """
        Create a :py:class:`HyperLogLog` instance wrapping the given
        key.
        """
        return HyperLogLog(self, key)

    def Array(self, key):
        """
        Create a :py:class:`Array` instance wrapping the given key.
        """
        return Array(self, key)

    def listener(self, channels=None, patterns=None, async=False):
        """
        Decorator for wrapping functions used to listen for Redis
        pub-sub messages.

        The listener will listen until the decorated function
        raises a ``StopIteration`` exception.

        :param list channels: Channels to listen on.
        :param list patterns: Patterns to match.
        :param bool async: Whether to start the listener in a
            separate thread.
        """
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
        """
        Stream Redis activity one line at a time to the given
        callback.

        :param callback: A function that accepts a single argument,
            the Redis command.
        """
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
    """
    Base-class for rich Redis object wrappers.
    """
    def __init__(self, database, key):
        self.database = database
        self.key = key

    def expire(self, ttl=None):
        """
        Expire the given key in the given number of seconds.
        If ``ttl`` is ``None``, then any expiry will be cleared
        and key will be persisted.
        """
        if ttl is not None:
            self.database.expire(self.key, ttl)
        else:
            self.database.persist(self.key)

    def dump(self):
        """
        Dump the contents of the given key using Redis' native
        serialization format.
        """
        return self.database.dump(self.key)

    @chainable_method
    def clear(self):
        """
        Clear the contents of the container by deleting the key.
        """
        self.database.delete(self.key)


class Hash(Container):
    """
    Redis Hash object wrapper. Supports a dictionary-like interface
    with some modifications.

    See `Hash commands <http://redis.io/commands#hash>`_ for more info.
    """
    def __repr__(self):
        l = len(self)
        if l > 5:
            # Get a few keys.
            data = self.database.hscan(self.key, count=5)
        else:
            data = self.as_dict()
        return '<Hash "%s": %s>' % (self.key, data)

    def __getitem__(self, item):
        """
        Retrieve the value at the given key. To retrieve multiple
        values at once, you can specify multiple keys as a tuple or
        list:

        .. code-block:: python

            hsh = db.Hash('my-hash')
            first, last = hsh['first_name', 'last_name']
        """
        if isinstance(item, (list, tuple)):
            return self.database.hmget(self.key, item)
        else:
            return self.database.hget(self.key, item)

    def __setitem__(self, key, value):
        """Set the value of the given key."""
        return self.database.hset(self.key, key, value)

    def __delitem__(self, key):
        """Delete the key from the hash."""
        return self.database.hdel(self.key, key)

    def __contains__(self, key):
        """
        Return a boolean valud indicating whether the given key
        exists.
        """
        return self.database.hexists(self.key, key)

    def __len__(self):
        """Return the number of keys in the hash."""
        return self.database.hlen(self.key)

    def __iter__(self):
        """Iterate over the items in the hash."""
        return iter(self.database.hscan_iter(self.key))

    def search(self, pattern, count=None):
        """
        Search the keys of the given hash using the specified pattern.

        :param str pattern: Pattern used to match keys.
        :param int count: Limit number of results returned.
        :returns: An iterator yielding matching key/value pairs.
        """
        return self.database.hscan_iter(self.key, pattern, count)

    def keys(self):
        """Return the keys of the hash."""
        return self.database.hkeys(self.key)

    def values(self):
        """Return the values stored in the hash."""
        return self.database.hvals(self.key)

    def items(self, lazy=False):
        """
        Like Python's ``dict.items()`` but supports an optional
        parameter ``lazy`` which will return a generator rather than
        a list.
        """
        if lazy:
            return self.database.hscan_iter(self.key)
        else:
            return list(self)

    @chainable_method
    def update(self, *args, **kwargs):
        """
        Update the hash using the given dictionary or key/value pairs.
        """
        if args:
            self.database.hmset(self.key, *args)
        else:
            self.database.hmset(self.key, kwargs)

    def as_dict(self):
        """
        Return a dictionary containing all the key/value pairs in the
        hash.
        """
        return self.database.hgetall(self.key)

    def incr(self, key, incr_by=1):
        """Increment the key by the given amount."""
        return self.database.hincrby(self.key, key, incr_by)


class List(Container):
    """
    Redis List object wrapper. Supports a list-like interface.

    See `List commands <http://redis.io/commands#list>`_ for more info.
    """
    def __repr__(self):
        l = len(self)
        n_items = min(l, 10)
        return '<List "%s": %s%s>' % (
            self.key,
            ', '.join(self[:n_items]),
            n_items < l and '...' or '')

    def __getitem__(self, item):
        """
        Retrieve an item from the list by index. In addition to
        integer indexes, you can also pass a ``slice``.
        """
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
        """Set the value of the given index."""
        return self.database.lset(self.key, idx, value)

    def __delitem__(self, item):
        """
        By default Redis treats deletes as delete by value, as
        opposed to delete by index. If an integer is passed into the
        function, it will be treated as an index, otherwise it will
        be treated as a value.
        """
        if isinstance(item, int):
            item = self[item]
            if item is None:
                return
        return self.database.lrem(self.key, item)

    def __len__(self):
        """Return the length of the list."""
        return self.database.llen(self.key)

    def __iter__(self):
        """Iterate over the items in the list."""
        return iter(self.database.lrange(self.key, 0, -1))

    def append(self, value):
        """Add the given value to the end of the list."""
        return self.database.rpush(self.key, value)

    def prepend(self, value):
        """Add the given value to the beginning of the list."""
        return self.database.lpush(self.key, value)

    def extend(self, value):
        """Extend the list by the given value."""
        return self.database.rpush(self.key, *value)

    def insert(self, value, pivot, where):
        return self.database.linsert(self.key, where, pivot, value)

    def insert_before(self, value, key):
        """
        Insert the given value into the list before the index
        containing ``key``.
        """
        self.insert(value, key, 'before')

    def insert_after(self, value, key):
        """
        Insert the given value into the list after the index
        containing ``key``.
        """
        self.insert(value, key, 'after')

    def popleft(self):
        """Remove the first item from the list."""
        return self.database.lpop(self.key)

    def popright(self):
        """Remove the last item from the list."""
        return self.database.rpop(self.key)
    pop = popright

    def move_tail(self, key):
        return self.database.rpoplpush(self.key, key)


class Set(Container):
    """
    Redis Set object wrapper. Supports a set-like interface.

    See `Set commands <http://redis.io/commands#set>`_ for more info.
    """
    def __repr__(self):
        return '<Set "%s": %s items>' % (self.key, len(self))

    def add(self, *items):
        """Add the given items to the set."""
        return self.database.sadd(self.key, *items)

    def __delitem__(self, item):
        """Remove the given item from the set."""
        return self.remove(item)

    def remove(self, *items):
        """Remove the given item(s) from the set."""
        return self.database.srem(self.key, *items)

    def pop(self):
        """Remove an element from the set."""
        return self.database.spop(self.key)

    def __contains__(self, item):
        """
        Return a boolean value indicating whether the given item is
        a member of the set.
        """
        return self.database.sismember(self.key, item)

    def __len__(self):
        """Return the number of items in the set."""
        return self.database.scard(self.key)

    def __iter__(self):
        """Return an iterable that yields the items of the set."""
        return iter(self.database.sscan_iter(self.key))

    def search(self, pattern, count=None):
        """
        Search the values of the given set using the specified pattern.

        :param str pattern: Pattern used to match keys.
        :param int count: Limit number of results returned.
        :returns: An iterator yielding matching values.
        """
        return self.database.sscan_iter(self.key, pattern, count)

    def members(self):
        """Return a ``set()`` containing the members of the set."""
        return self.database.smembers(self.key)

    def random(self, n=None):
        """Return a random member of the given set."""
        return self.database.srandmember(self.key, n)

    def __sub__(self, other):
        """
        Return the set difference of the current set and the left-
        hand :py:class:`Set` object.
        """
        return self.database.sdiff(self.key, other.key)

    def __or__(self, other):
        """
        Return the set union of the current set and the left-hand
        :py:class:`Set` object.
        """
        return self.database.sunion(self.key, other.key)

    def __and__(self, other):
        """
        Return the set intersection of the current set and the left-
        hand :py:class:`Set` object.
        """
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
        """
        Store the set difference of the current set and one or more
        others in a new key.

        :param dest: the name of the key to store set difference
        :param others: One or more :py:class:`Set` instances
        :returns: A :py:class:`Set` referencing ``dest``.
        """
        keys = [self.key]
        keys.extend([other.key for other in others])
        self.database.sdiffstore(dest, keys)
        return Set(self.database, dest)

    def interstore(self, dest, *others):
        """
        Store the intersection of the current set and one or more
        others in a new key.

        :param dest: the name of the key to store intersection
        :param others: One or more :py:class:`Set` instances
        :returns: A :py:class:`Set` referencing ``dest``.
        """
        keys = [self.key]
        keys.extend([other.key for other in others])
        self.database.sinterstore(dest, keys)
        return Set(self.database, dest)

    def unionstore(self, dest, *others):
        """
        Store the union of the current set and one or more
        others in a new key.

        :param dest: the name of the key to store union
        :param others: One or more :py:class:`Set` instances
        :returns: A :py:class:`Set` referencing ``dest``.
        """
        keys = [self.key]
        keys.extend([other.key for other in others])
        self.database.sunionstore(dest, keys)
        return Set(self.database, dest)


class ZSet(Container):
    """
    Redis ZSet object wrapper. Acts like a set and a dictionary.

    See `Sorted set commands <http://redis.io/commands#sorted_set>`_
    for more info.
    """
    def __repr__(self):
        l = len(self)
        n_items = min(l, 5)
        return '<ZSet "%s": %s%s>' % (
            self.key,
            ', '.join(self[:n_items, False]),
            n_items < l and '...' or '')

    def add(self, *args, **kwargs):
        """
        Add the given item/score pairs to the ZSet. Arguments are
        specified as ``item1, score1, item2, score2...``.
        """
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
        """
        Retrieve the given values from the sorted set. Accepts a
        variety of parameters for the input:

        .. code-block:: python

            zs = db.ZSet('my-zset')

            # Return the first 10 elements with their scores.
            zs[:10, True]

            # Return the first 10 elements without scores.
            zs[:10]
            zs[:10, False]

            # Return the range of values between 'k1' and 'k10' along
            # with their scores.
            zs['k1':'k10', True]

            # Return the range of items preceding and including 'k5'
            # without scores.
            zs[:'k5', False]
        """
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
        """Add item to the set with the given score."""
        return self.database.zadd(self.key, item, score)

    def __delitem__(self, item):
        """
        Delete the given item(s) from the set. Like
        :py:meth:`~ZSet.__getitem__`, this method supports a wide
        variety of indexing and slicing options.
        """
        if isinstance(item, slice):
            start, stop = self._convert_slice(item)
            return self.database.zremrangebyrank(self.key, start, stop)
        else:
            return self.remove(item)

    def remove(self, *items):
        """Remove the given items from the ZSet."""
        return self.database.zrem(self.key, *items)

    def __contains__(self, item):
        """
        Return a boolean indicating whether the given item is in the
        sorted set.
        """
        return not (self.rank(item) is None)

    def __len__(self):
        """Return the number of items in the sorted set."""
        return self.database.zcard(self.key)

    def __iter__(self):
        """
        Return an iterator that will yield (item, score) tuples.
        """
        return iter(self.database.zscan_iter(self.key))

    def iterator(self, with_scores=False, reverse=False):
        if with_scores and not reverse:
            return self.database.search(None)
        return self.range(
            0,
            -1,
            with_scores=with_scores,
            reverse=reverse)

    def search(self, pattern, count=None):
        """
        Search the set, returning items that match the given search
        pattern.

        :param str pattern: Search pattern using wildcards.
        :param int count: Limit result set size.
        :returns: Iterator that yields matching item/score tuples.
        """
        return self.database.zscan_iter(self.key, pattern, count)

    def score(self, item):
        """Return the score of the given item."""
        return self.database.zscore(self.key, item)

    def rank(self, item, reverse=False):
        """Return the rank of the given item."""
        fn = reverse and self.database.zrevrank or self.database.zrank
        return fn(self.key, item)

    def count(self, low, high=None):
        """
        Return the number of items between the given bounds.
        """
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
        """
        Return a range of items between ``low`` and ``high``. By
        default scores will not be included, but this can be controlled
        via the ``with_scores`` parameter.

        :param low: Lower bound.
        :param high: Upper bound.
        :param bool with_scores: Whether the range should include the
            scores along with the items.
        :param bool reverse: Whether to return the range in reverse.
        """
        return self.database.zrange(self.key, low, high, reverse, with_scores)

    def range_by_score(self, low, high, start=None, num=None,
                       with_scores=False, reverse=False):
        if reverse:
            fn = self.database.zrevrangebyscore
            low, high = high, low
        else:
            fn = self.database.zrangebyscore
        return fn(self.key, low, high, start, num, with_scores)

    def range_by_lex(self, low, high, start=None, num=None, reverse=False):
        """
        Return a range of members in a sorted set, by lexicographical range.
        """
        if reverse:
            fn = self.database.zrevrangebylex
            low, high = high, low
        else:
            fn = self.database.zrangebylex
        return fn(self.key, low, high, start, num)

    def remove_by_rank(self, low, high=None):
        """
        Remove elements from the ZSet by their rank (relative position).

        :param low: Lower bound.
        :param high: Upper bound.
        """
        if high is None:
            high = low
        return self.database.zremrangebyrank(self.key, low, high)

    def remove_by_score(self, low, high=None):
        """
        Remove elements from the ZSet by their score.

        :param low: Lower bound.
        :param high: Upper bound.
        """
        if high is None:
            high = low
        return self.database.zremrangebyscore(self.key, low, high)

    def remove_by_lex(self, low, high):
        return self.database.zremrangebylex(self.key, low, high)

    def incr(self, key, incr_by=1):
        """
        Increment the score of an item in the ZSet.

        :param key: Item to increment.
        :param incr_by: Amount to increment item's score.
        """
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
        """
        Store the intersection of the current zset and one or more
        others in a new key.

        :param dest: the name of the key to store intersection
        :param others: One or more :py:class:`ZSet` instances
        :returns: A :py:class:`ZSet` referencing ``dest``.
        """
        keys = [self.key]
        keys.extend([other.key for other in others])
        self.database.zinterstore(dest, keys, **kwargs)
        return ZSet(self.database, dest)

    def unionstore(self, dest, *others, **kwargs):
        """
        Store the union of the current set and one or more
        others in a new key.

        :param dest: the name of the key to store union
        :param others: One or more :py:class:`ZSet` instances
        :returns: A :py:class:`ZSet` referencing ``dest``.
        """
        keys = [self.key]
        keys.extend([other.key for other in others])
        self.database.zunionstore(dest, keys, **kwargs)
        return ZSet(self.database, dest)


class HyperLogLog(Container):
    """
    Redis HyperLogLog object wrapper.

    See `HyperLogLog commands <http://redis.io/commands#hyperloglog>`_
    for more info.
    """
    def add(self, *items):
        """
        Add the given items to the HyperLogLog.
        """
        return self.database.pfadd(self.key, *items)

    def __len__(self):
        return self.database.pfcount(self.key)

    def __ior__(self, other):
        if not isinstance(other, (list, tuple)):
            other = [other]
        return self.merge(self.key, *other)

    def merge(self, dest, *others):
        """
        Merge one or more :py:class:`HyperLogLog` instances.

        :param dest: Key to store merged result.
        :param others: One or more ``HyperLogLog`` instances.
        """
        items = [self.key]
        items.extend([other.key for other in others])
        self.database.pfmerge(dest, *items)
        return HyperLogLog(self.database, dest)


class Array(Container):
    """
    Custom container that emulates an array (as opposed to the
    linked-list implementation of :py:class:`List`). This gives:

    * O(1) append, get, len, pop last, set
    * O(n) remove from middle

    :py:class:`Array` is built on top of the hash data type and
    is implemented using lua scripts.
    """
    def __getitem__(self, idx):
        """Get the value stored in the given index."""
        return self.database.run_script(
            'array_get',
            keys=[self.key],
            args=[idx])

    def __setitem__(self, idx, value):
        """Set the value at the given index."""
        return self.database.run_script(
            'array_set',
            keys=[self.key],
            args=[idx, value])

    def __delitem__(self, idx):
        """Delete the given index."""
        return self.pop(idx)

    def __len__(self):
        """Return the number of items in the array."""
        return self.database.hlen(self.key)

    def append(self, value):
        """Append a new value to the end of the array."""
        self.database.run_script(
            'array_append',
            keys=[self.key],
            args=[value])

    def extend(self, values):
        """Extend the array, appending the given values."""
        self.database.run_script(
            'array_extend',
            keys=[self.key],
            args=values)

    def pop(self, idx=None):
        """
        Remove an item from the array. By default this will be the
        last item by index, but any index can be specified.
        """
        if idx is not None:
            return self.database.run_script(
                'array_remove',
                keys=[self.key],
                args=[idx])
        else:
            return self.database.run_script(
                'array_pop',
                keys=[self.key],
                args=[])

    def __contains__(self, item):
        """
        Return a boolean indicating whether the given item is stored
        in the array. O(n).
        """
        for value in self:
            if value == item:
                return True
        return False

    def __iter__(self):
        """Return an iterable that yields array items."""
        return iter(
            item[1] for item in sorted(self.database.hscan_iter(self.key)))


class Cache(object):
    """
    Cache implementation with simple ``get``/``set`` operations,
    and a decorator.
    """
    def __init__(self, database, name='cache', default_timeout=None):
        """
        :param database: :py:class:`Database` instance.
        :param name: Namespace for this cache.
        :param int default_timeout: Default cache timeout.
        """
        self.database = database
        self.name = name
        self.default_timeout = default_timeout

    def make_key(self, s):
        return ':'.join((self.name, s))

    def get(self, key, default=None):
        """
        Retreive a value from the cache. In the event the value
        does not exist, return the ``default``.
        """
        key = self.make_key(key)
        try:
            value = self.database[key]
        except KeyError:
            return default
        else:
            return pickle.loads(value)

    def set(self, key, value, timeout=None):
        """
        Cache the given ``value`` in the specified ``key``. If no
        timeout is specified, the default timeout will be used.
        """
        key = self.make_key(key)
        if timeout is None:
            timeout = self.default_timeout

        pickled_value = pickle.dumps(value)
        if timeout:
            return self.database.setex(key, pickled_value, int(timeout))
        else:
            return self.database.set(key, pickled_value)

    def delete(self, key):
        """Remove the given key from the cache."""
        self.database.delete(self.make_key(key))

    def keys(self):
        """
        Return all keys for cached values.
        """
        return self.database.keys(self.make_key('') + '*')

    def flush(self):
        """Remove all cached objects from the database."""
        return self.database.delete(*self.keys())

    def incr(self, key, delta=1):
        return self.database.incr(self.make_key(key), delta)

    def _key_fn(a, k):
        return hashlib.md5(pickle.dumps((a, k))).hexdigest()

    def cached(self, key_fn=_key_fn, timeout=3600):
        """
        Decorator that will transparently cache calls to the
        wrapped function. By default, the cache key will be made
        up of the arguments passed in (like memoize), but you can
        override this by specifying a custom ``key_fn``.

        Usage::

            cache = Cache(my_database)

            @cache.cached(timeout=60)
            def add_numbers(a, b):
                return a + b

            print add_numbers(3, 4)  # Function is called.
            print add_numbers(3, 4)  # Not called, value is cached.

            add_numbers.bust(3, 4)  # Clear cache for (3, 4).
            print add_numbers(3, 4)  # Function is called.

        The decorated function also gains a new attribute named
        ``bust`` which will clear the cache for the given args.
        """
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
OP_BETWEEN = 'between'
OP_MATCH = 'match'

ABSOLUTE = set([OP_EQ, OP_NE])
CONTINUOUS = set([OP_LT, OP_LTE, OP_GT, OP_GTE])
FTS = set([OP_MATCH])


class Node(object):
    def __init__(self):
        self._ordering = None

    def desc(self):
        return Desc(self)

    def between(self, low, high):
        return Expression(self, OP_BETWEEN, (low, high))

    def match(self, search):
        return Expression(self, OP_MATCH, search)

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


class Desc(Node):
    def __init__(self, node):
        self.node = node


class Expression(Node):
    def __init__(self, lhs, op, rhs):
        self.lhs = lhs
        self.op = op
        self.rhs = rhs

    def __repr__(self):
        return '(%s %s %s)' % (self.lhs, self.op, self.rhs)


class Field(Node):
    """
    Named attribute on a model that will hold a value of the given
    type.
    """
    _coerce = None

    def __init__(self, index=False, as_json=False, primary_key=False,
                 pickled=False, default=None):
        """
        :param bool index: Use this field as an index. Indexed
            fields will support :py:meth:`Model.get` lookups.
        :param bool as_json: Whether the value should be serialized
            as JSON when storing in the database. Useful for
            collections or objects.
        :param bool primary_key: Use this field as the primary key.
        :param bool pickled: Whether the value should be pickled when
            storing in the database. Useful for non-primitive content
            types.
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
    """Store integer values."""
    _coerce = int


class AutoIncrementField(IntegerField):
    """Auto-incrementing primary key field."""
    def __init__(self, *args, **kwargs):
        kwargs['primary_key'] = True
        return super(AutoIncrementField, self).__init__(*args, **kwargs)

    def _generate_key(self):
        query_helper = self.model_class._query
        key = query_helper.make_key(self.name, '_sequence')
        return self.model_class.database.incr(key)


class FloatField(_ScalarField):
    """Store floating point values."""
    _coerce = float


class ByteField(Field):
    """Store arbitrary bytes."""
    _coerce = str


class TextField(Field):
    """
    Store unicode strings, encoded as UTF-8. :py:class:`TextField`
    also supports full-text search through the optional ``fts``
    parameter.

    :param bool fts: Enable simple full-text search.
    """
    def __init__(self, *args, **kwargs):
        self._fts = kwargs.pop('fts', False)
        super(TextField, self).__init__(*args, **kwargs)
        if self._fts:
            self._index = True

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

    def get_indexes(self):
        indexes = super(TextField, self).get_indexes()
        if self._fts:
            indexes.append(FullTextIndex(self))
        return indexes


class BooleanField(Field):
    """Store boolean values."""
    def db_value(self, value):
        return value and 1 or 0

    def python_value(self, value):
        return str(value) == '1'


class UUIDField(Field):
    """Store unique IDs. Can be used as primary key."""
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
    """Store Python datetime objects."""
    def db_value(self, value):
        timestamp = time.mktime(value.timetuple())
        micro = value.microsecond * (10 ** -6)
        return timestamp + micro

    def python_value(self, value):
        if isinstance(value, (basestring, int, float)):
            return datetime.datetime.fromtimestamp(float(value))
        return value


class DateField(DateTimeField):
    """Store Python date objects."""
    def db_value(self, value):
        return time.mktime(value.timetuple())

    def python_value(self, value):
        if isinstance(value, (basestring, int, float)):
            return datetime.datetime.fromtimestamp(float(value)).date()
        return value


class JSONField(Field):
    """Store arbitrary JSON data."""
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


class FullTextIndex(BaseIndex):
    operations = FTS
    _stopwords = set()
    _stopwords_file = 'stopwords.txt'

    def __init__(self, *args, **kwargs):
        super(FullTextIndex, self).__init__(*args, **kwargs)
        self._load_stopwords()

    def _load_stopwords(self):
        stopwords = load_stopwords(self._stopwords_file)
        if stopwords:
            self._stopwords = set(stopwords.splitlines())

    def tokenize(self, value):
        value = re.sub('[\.,;:"\'\\/!@#\$%\*\(\)]', ' ', value)
        words = value.lower().split()
        fraction = 1. / len(words)
        scores = {}
        for token in words:
            token = token.strip()
            if token in self._stopwords:
                continue
            scores.setdefault(token, 0)
            scores[token] += fraction
        return scores

    def get_key(self, value):
        key = self.query_helper.make_key(
            self.field.name,
            'fts',
            value)
        return self.database.ZSet(key)

    def store_instance(self, key, instance, value):
        hash_id = instance.get_hash_id()
        for word, score in self.tokenize(value).items():
            key = self.get_key(word)
            key[instance.get_hash_id()] = score

    def delete_instance(self, key, instance, value):
        hash_id = instance.get_hash_id()
        for word in self.tokenize(value):
            key = self.get_key(word)
            del key[instance.get_hash_id()]
            if len(key) == 0:
                key.clear()


class Executor(object):
    def __init__(self, database, temp_key_expire=30):
        self.database = database
        self.temp_key_expire = 30
        self._mapping = {
            OP_OR: self.execute_or,
            OP_AND: self.execute_and,
            OP_EQ: self.execute_eq,
            OP_NE: self.execute_ne,
            OP_GT: self.execute_gt,
            OP_GTE: self.execute_gte,
            OP_LT: self.execute_lt,
            OP_LTE: self.execute_lte,
            OP_BETWEEN: self.execute_between,
            OP_MATCH: self.execute_match,
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
        tmp_set = self.database.Set(self.database.get_temp_key())
        self.database.run_script(
            'zset_score_filter',
            keys=[zset.key, tmp_set.key],
            args=[low, high])
        tmp_set.expire(self.temp_key_expire)
        return tmp_set

    def execute_between(self, lhs, rhs):
        index = lhs.get_index(OP_LT)
        low, high = map(lhs.db_value, rhs)
        zset = index.get_key(None)  # No value necessary.
        return self._zset_score_filter(zset, low, high)

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

    def execute_lt(self, lhs, rhs):
        index = lhs.get_index(OP_LTE)
        db_value = lhs.db_value(rhs)
        zset = index.get_key(db_value)
        return self._zset_score_filter(zset, float('-inf'), '(%s' % db_value)

    def execute_gt(self, lhs, rhs):
        index = lhs.get_index(OP_GTE)
        db_value = lhs.db_value(rhs)
        zset = index.get_key(db_value)
        return self._zset_score_filter(zset, '(%s' % db_value, float('inf'))

    def execute_match(self, lhs, rhs):
        index = lhs.get_index(OP_MATCH)
        db_value = lhs.db_value(rhs)
        words = index.tokenize(db_value)
        index_keys = []
        for word in words:
            index_keys.append(index.get_key(word).key)

        results = self.database.ZSet(self.database.get_temp_key())
        self.database.zinterstore(results.key, index_keys)
        return results

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
    """
    A collection of fields to be stored in the database. Walrus
    stores model instance data in hashes keyed by a combination of
    model name and primary key value. Instance attributes are
    automatically converted to values suitable for storage in Redis
    (i.e., datetime becomes timestamp), and vice-versa.

    Additionally, model fields can be ``indexed``, which allows
    filtering. There are two types of indexes:

    * Absolute
    * Scalar

    Absolute indexes are used for values like strings or UUIDs and
    support only equality and inequality checks.

    Scalar indexes are for numeric values as well as datetimes,
    and support equality, inequality, and greater or less-than.
    """
    #: **Required**: the :py:class:`Database` instance to use to
    #: persist model data.
    database = None

    #: **Optional**: namespace to use for model data.
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
        """
        Return a :py:class:`Hash` instance corresponding to the
        raw model data.
        """
        return self.database.Hash(self.get_hash_id())

    @classmethod
    def create(cls, **kwargs):
        """
        Create a new model instance and save it to the database.
        Values are passed in as keyword arguments.

        Example::

            User.create(first_name='Charlie', last_name='Leifer')
        """
        instance = cls(**kwargs)
        instance.save()
        return instance

    @classmethod
    def all(cls):
        """
        Return an iterator that successively yields saved model
        instances. Models are saved in an unordered :py:class:`Set`,
        so the iterator will return them in arbitrary order.

        To return models in sorted order, see :py:meth:`Model.query`.
        """
        for result in cls._query.all_index():
            yield cls.load(result, convert_key=False)

    @classmethod
    def query(cls, expression=None, order_by=None):
        """
        Return model instances matching the given expression (if
        specified). Additionally, matching instances can be returned
        sorted by field value.

        Example::

            # Get administrators sorted by username.
            admin_users = User.query(
                (User.admin == True),
                order_by=User.username)

            # List blog entries newest to oldest.
            entries = Entry.query(order_by=Entry.timestamp.desc())

            # Perform a complex filter.
            values = StatData.query(
                (StatData.timestamp < datetime.date.today()) &
                ((StatData.type == 'pv') | (StatData.type == 'cv')))

        :param expression: A boolean expression to filter by.
        :param order_by: A field whose value should be used to
            sort returned instances.
        """
        if expression is not None:
            executor = Executor(cls.database)
            result = executor.execute(expression)
        else:
            result = cls._query.all_index()

        if order_by is not None:
            desc = False
            if isinstance(order_by, Desc):
                desc = True
                order_by = order_by.node

            alpha = not isinstance(order_by, _ScalarField)
            result = cls.database.sort(
                result.key,
                by='*->%s' % order_by.name,
                alpha=alpha,
                desc=desc)
        elif isinstance(result, ZSet):
            result = result.iterator(reverse=True)

        for hash_id in result:
            yield cls.load(hash_id, convert_key=False)

    @classmethod
    def get(cls, expression):
        """
        Retrieve the model instance matching the given expression.
        If the number of matching results is not equal to one, then
        a ``ValueError`` will be raised.

        :param expression: A boolean expression to filter by.
        """
        executor = Executor(cls.database)
        result = executor.execute(expression)
        if len(result) != 1:
            raise ValueError('Got %s results, expected 1.' % len(result))
        return cls.load(result.pop(), convert_key=False)

    @classmethod
    def load(cls, primary_key, convert_key=True):
        """
        Retrieve a model instance by primary key.

        :param primary_key: The primary key of the model instance.
        """
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
        """
        Delete the given model instance.
        """
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
        """
        Save the given model instance. If the model does not have
        a primary key value, one will be generated automatically.
        """
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


class memoize(dict):
    def __init__(self, fn):
        self._fn = fn

    def __call__(self, *args):
        return self[args]

    def __missing__(self, key):
        result = self[key] = self._fn(*key)
        return result


@memoize
def load_stopwords(stopwords_file):
    path, filename = os.path.split(stopwords_file)
    if not path:
        path = os.path.dirname(__file__)
    filename = os.path.join(path, filename)
    if not os.path.exists(filename):
        return

    with open(filename) as fh:
        return fh.read()
