import hashlib
import operator
import struct
import uuid
from functools import reduce
from functools import wraps
from itertools import islice
try:
    from redis.exceptions import ResponseError
except ImportError:
    ResponseError = Exception

from walrus.utils import decode as _decode
from walrus.utils import decode_dict
from walrus.utils import encode
from walrus.utils import make_python_attr
from walrus.utils import safe_decode_list


def chainable_method(fn):
    @wraps(fn)
    def inner(self, *args, **kwargs):
        fn(self, *args, **kwargs)
        return self
    return inner


def _slice_bounds(s):
    if s.step not in (None, 1):
        raise ValueError('Slices with a step are not supported.')
    return s.start, s.stop


def _redis_range(start, stop):
    """
    Convert Python slice bounds to an inclusive Redis range. A stop of 0
    selects nothing, so it maps to (1, 0), which every range command treats
    as empty.
    """
    if stop == 0:
        return 1, 0
    return start or 0, -1 if stop is None else stop - 1


class Sortable(object):
    def sort(self, pattern=None, limit=None, offset=None, get_pattern=None,
             ordering=None, alpha=True, store=None):
        if limit or offset:
            offset = offset or 0
            limit = limit or -1
        return self.database.sort(
            self.key,
            start=offset,
            num=limit,
            by=pattern,
            get=get_pattern,
            desc=ordering in ('DESC', 'desc'),
            alpha=alpha,
            store=store)


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

    def pexpire(self, ttl=None):
        """
        Expire the given key in the given number of milliseconds.
        If ``ttl`` is ``None``, then any expiry will be cleared
        and key will be persisted.
        """
        if ttl is not None:
            self.database.pexpire(self.key, ttl)
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
            data = dict(islice(self._scan(count=5), 5))
        else:
            data = self.as_dict()
        return '<Hash "%s": %s%s>' % (self.key, data, l > 5 and '...' or '')

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

    def get(self, key, fallback=None):
        val = self.database.hget(self.key, key)
        return val if val is not None else fallback

    def __setitem__(self, key, value):
        """Set the value of the given key."""
        return self.database.hset(self.key, key, value)

    def __delitem__(self, key):
        """Delete the key from the hash."""
        return self.database.hdel(self.key, key)

    def __contains__(self, key):
        """
        Return a boolean value indicating whether the given key
        exists.
        """
        return self.database.hexists(self.key, key)

    def __len__(self):
        """Return the number of keys in the hash."""
        return self.database.hlen(self.key)

    def _scan(self, *args, **kwargs):
        return self.database.hscan_iter(self.key, *args, **kwargs)

    def __iter__(self):
        """Iterate over the items in the hash."""
        return iter(self._scan())

    def search(self, pattern, count=None):
        """
        Search the keys of the given hash using the specified pattern.

        :param str pattern: Pattern used to match keys.
        :param int count: Hint for the number of keys each SCAN call
            examines. All matches are returned regardless.
        :returns: An iterator yielding matching key/value pairs.
        """
        return self._scan(match=pattern, count=count)

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
            return self._scan()
        else:
            return list(self)

    def setnx(self, key, value):
        """
        Set ``key`` to ``value`` if ``key`` does not exist.

        :returns: True if successfully set or False if the key already existed.
        """
        return bool(self.database.hsetnx(self.key, key, value))

    @chainable_method
    def update(self, _data=None, **kwargs):
        """
        Update the hash using the given dictionary or key/value pairs.
        """
        data = dict(_data or {}, **kwargs)
        if data:
            self.database.hset(self.key, mapping=data)

    def incr(self, key, incr_by=1):
        """Increment the key by the given amount."""
        return self.database.hincrby(self.key, key, incr_by)

    def incr_float(self, key, incr_by=1.):
        """Increment the key by the given amount."""
        return self.database.hincrbyfloat(self.key, key, incr_by)

    def as_dict(self, decode=False):
        """
        Return a dictionary containing all the key/value pairs in the
        hash.
        """
        res = self.database.hgetall(self.key)
        return decode_dict(res) if decode else res

    @classmethod
    def from_dict(cls, database, key, data, clear=False):
        """
        Create and populate a Hash object from a data dictionary.
        """
        hsh = cls(database, key)
        if clear:
            hsh.clear()
        hsh.update(data)
        return hsh


class List(Sortable, Container):
    """
    Redis List object wrapper. Supports a list-like interface.

    See `List commands <http://redis.io/commands#list>`_ for more info.
    """
    def __repr__(self):
        l = len(self)
        n_items = min(l, 10)
        items = safe_decode_list(self[:n_items])
        return '<List "%s": %s%s>' % (
            self.key,
            ', '.join(items),
            n_items < l and '...' or '')

    def __getitem__(self, item):
        """
        Retrieve an item from the list by index. In addition to
        integer indexes, you can also pass a ``slice``.
        """
        if isinstance(item, slice):
            start, stop = _redis_range(*_slice_bounds(item))
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

        If a slice is passed, then the list will be trimmed so that it *ONLY*
        contains the items ``l[start:stop]`` would return. Note that this
        differs from the default behavior of Python's `list` type.
        """
        if isinstance(item, slice):
            start, stop = _redis_range(*_slice_bounds(item))
            return self.database.ltrim(self.key, start, stop)
        elif isinstance(item, int):
            # Mark the item, then remove the mark. LSET fails on an index
            # that is out of range, which leaves nothing to remove.
            marker = uuid.uuid4().hex
            _, removed = (self.database.pipeline()
                          .lset(self.key, item, marker)
                          .lrem(self.key, 1, marker)
                          .execute(raise_on_error=False))
            if isinstance(removed, Exception):
                raise removed
            return removed
        return self.database.lrem(self.key, 1, item)

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
        value = list(value)
        if not value:
            return len(self)
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

    def bpopleft(self, timeout=0):
        """
        Remove the first item from the list, blocking until an item becomes
        available or timeout is reached (0 for no timeout, default).
        """
        ret = self.database.blpop(self.key, timeout)
        if ret is not None:
            return ret[1]

    def bpopright(self, timeout=0):
        """
        Remove the last item from the list, blocking until an item becomes
        available or timeout is reached (0 for no timeout, default).
        """
        ret = self.database.brpop(self.key, timeout)
        if ret is not None:
            return ret[1]

    def move_tail(self, key):
        return self.database.rpoplpush(self.key, key)

    def bmove_tail(self, key, timeout=0):
        return self.database.brpoplpush(self.key, key, timeout)

    def as_list(self, decode=False):
        """
        Return a list containing all the items in the list.
        """
        items = self.database.lrange(self.key, 0, -1)
        return [_decode(item) for item in items] if decode else items

    @classmethod
    def from_list(cls, database, key, data, clear=False):
        """
        Create and populate a List object from a data list.
        """
        lst = cls(database, key)
        if clear:
            lst.clear()
        lst.extend(data)
        return lst


class Set(Sortable, Container):
    """
    Redis Set object wrapper. Supports a set-like interface.

    See `Set commands <http://redis.io/commands#set>`_ for more info.
    """
    def __repr__(self):
        return '<Set "%s": %s items>' % (self.key, len(self))

    def add(self, *items):
        """Add the given items to the set."""
        if not items:
            return 0
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

    def _first_or_any(self):
        return self.random()

    def __contains__(self, item):
        """
        Return a boolean value indicating whether the given item is
        a member of the set.
        """
        return self.database.sismember(self.key, item)

    def __len__(self):
        """Return the number of items in the set."""
        return self.database.scard(self.key)

    def _scan(self, *args, **kwargs):
        return self.database.sscan_iter(self.key, *args, **kwargs)

    def __iter__(self):
        """Return an iterable that yields the items of the set."""
        return iter(self._scan())

    def search(self, pattern, count=None):
        """
        Search the values of the given set using the specified pattern.

        :param str pattern: Pattern used to match keys.
        :param int count: Hint for the number of members each SCAN call
            examines. All matches are returned regardless.
        :returns: An iterator yielding matching values.
        """
        return self._scan(match=pattern, count=count)

    def members(self):
        """Return a ``set()`` containing the members of the set."""
        return self.database.smembers(self.key)

    def random(self, n=None):
        """
        Return a random member of the given set. If ``n`` is given,
        return a list of up to ``n`` distinct members.
        """
        return self.database.srandmember(self.key, n)

    def __sub__(self, other):
        """
        Return the set difference of the current set and the right-
        hand :py:class:`Set` object.
        """
        return self.database.sdiff(self.key, other.key)

    def __or__(self, other):
        """
        Return the set union of the current set and the right-hand
        :py:class:`Set` object.
        """
        return self.database.sunion(self.key, other.key)

    def __and__(self, other):
        """
        Return the set intersection of the current set and the right-
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
        return self.database.Set(dest)

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
        return self.database.Set(dest)

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
        return self.database.Set(dest)

    def as_set(self, decode=False):
        """
        Return a Python set containing all the items in the collection.
        """
        items = self.database.smembers(self.key)
        return set(_decode(item) for item in items) if decode else items

    @classmethod
    def from_set(cls, database, key, data, clear=False):
        """
        Create and populate a Set object from a data set.
        """
        s = cls(database, key)
        if clear:
            s.clear()
        s.add(*data)
        return s


class ZSet(Sortable, Container):
    """
    Redis ZSet object wrapper. Acts like a set and a dictionary.

    See `Sorted set commands <http://redis.io/commands#sorted_set>`_
    for more info.
    """
    def __repr__(self):
        l = len(self)
        n_items = min(l, 5)
        items = safe_decode_list(self[:n_items, False])
        return '<ZSet "%s": %s%s>' % (
            self.key,
            ', '.join(items),
            n_items < l and '...' or '')

    def add(self, _mapping=None, **kwargs):
        """
        Add the given item/score pairs to the ZSet. Arguments are
        specified as a dictionary of item: score, or as keyword arguments.
        """
        mapping = dict(_mapping or {}, **kwargs)
        if not mapping:
            return 0
        return self.database.zadd(self.key, mapping)

    def _convert_slice(self, s):
        def _rank(value):
            if value is None or isinstance(value, int):
                return value
            rank = self.database.zrank(self.key, value)
            if rank is None:
                raise KeyError(value)
            return rank
        start, stop = _slice_bounds(s)
        return _redis_range(_rank(start), _rank(stop))

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

            # Return the values from 'k1' up to but not including 'k10'
            # along with their scores.
            zs['k1':'k10', True]

            # Return the range of items preceding 'k5' without scores.
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
        return self.database.zadd(self.key, {item: score})

    def __delitem__(self, item):
        """
        Delete the given item(s) from the set. A slice or an int removes by
        rank, as :py:meth:`~ZSet.__getitem__` reads. Any other value is
        removed as a member. Use :py:meth:`~ZSet.remove` for members that
        are ints.
        """
        if isinstance(item, slice):
            start, stop = self._convert_slice(item)
            return self.database.zremrangebyrank(self.key, start, stop)
        elif isinstance(item, int):
            return self.database.zremrangebyrank(self.key, item, item)
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

    def _scan(self, *args, **kwargs):
        return self.database.zscan_iter(self.key, *args, **kwargs)

    def __iter__(self):
        """
        Return an iterator that will yield (item, score) tuples in score
        order.
        """
        return iter(self.iterator(with_scores=True))

    def iterator(self, with_scores=False, reverse=False):
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
        :param int count: Hint for the number of items each SCAN call
            examines. All matches are returned regardless.
        :returns: Iterator that yields matching item/score tuples.
        """
        return self._scan(match=pattern, count=count)

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

    def range(self, low, high, with_scores=False, desc=False, reverse=False):
        """
        Return a range of items between ``low`` and ``high``. By
        default scores will not be included, but this can be controlled
        via the ``with_scores`` parameter.

        :param low: Lower bound.
        :param high: Upper bound.
        :param bool with_scores: Whether the range should include the
            scores along with the items.
        :param bool desc: Count ranks from the highest score, so the
            results come back in descending order.
        :param bool reverse: Same as ``desc``.
        """
        if reverse:
            return self.database.zrevrange(self.key, low, high, with_scores)
        else:
            return self.database.zrange(self.key, low, high, desc, with_scores)

    def range_by_score(self, low, high, start=None, num=None,
                       with_scores=False, reverse=False):
        """Return the items with scores between ``low`` and ``high``."""
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
        """Remove the members in the given lexicographical range."""
        return self.database.zremrangebylex(self.key, low, high)

    def incr(self, key, incr_by=1.):
        """
        Increment the score of an item in the ZSet.

        :param key: Item to increment.
        :param incr_by: Amount to increment item's score.
        """
        return self.database.zincrby(self.key, incr_by, key)

    def _first_or_any(self):
        item = self[0]
        if item:
            return item[0]

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
        :param kwargs: passed to ``zinterstore()``, e.g. ``aggregate='MAX'``
        :returns: A :py:class:`ZSet` referencing ``dest``.
        """
        keys = [self.key]
        keys.extend([other.key for other in others])
        self.database.zinterstore(dest, keys, **kwargs)
        return self.database.ZSet(dest)

    def unionstore(self, dest, *others, **kwargs):
        """
        Store the union of the current set and one or more
        others in a new key.

        :param dest: the name of the key to store union
        :param others: One or more :py:class:`ZSet` instances
        :param kwargs: passed to ``zunionstore()``, e.g. ``aggregate='MAX'``
        :returns: A :py:class:`ZSet` referencing ``dest``.
        """
        keys = [self.key]
        keys.extend([other.key for other in others])
        self.database.zunionstore(dest, keys, **kwargs)
        return self.database.ZSet(dest)

    def popmin(self, count=1):
        """
        Atomically remove the lowest-scoring item(s) in the set.

        :returns: a list of item, score tuples, empty if the set is empty.
        """
        return self.database.zpopmin(self.key, count)

    def popmax(self, count=1):
        """
        Atomically remove the highest-scoring item(s) in the set.

        :returns: a list of item, score tuples, empty if the set is empty.
        """
        return self.database.zpopmax(self.key, count)

    def bpopmin(self, timeout=0):
        """
        Atomically remove the lowest-scoring item from the set, blocking until
        an item becomes available or timeout is reached (0 for no timeout,
        default).

        Returns a 2-tuple of (item, score), or None if the timeout is
        reached.
        """
        res = self.database.bzpopmin(self.key, timeout)
        if res is not None:
            return (res[1], res[2])

    def bpopmax(self, timeout=0):
        """
        Atomically remove the highest-scoring item from the set, blocking until
        an item becomes available or timeout is reached (0 for no timeout,
        default).

        Returns a 2-tuple of (item, score), or None if the timeout is
        reached.
        """
        res = self.database.bzpopmax(self.key, timeout)
        if res is not None:
            return (res[1], res[2])

    def popmin_compat(self, count=1):
        """
        Atomically remove the lowest-scoring item(s) in the set. Compatible
        with Redis versions < 5.0.

        :returns: a list of item, score tuples, empty if the set is empty.
        """
        pipe = self.database.pipeline()
        r1, r2 = (pipe
                  .zrange(self.key, 0, count - 1, withscores=True)
                  .zremrangebyrank(self.key, 0, count - 1)
                  .execute())
        return r1

    def popmax_compat(self, count=1):
        """
        Atomically remove the highest-scoring item(s) in the set. Compatible
        with Redis versions < 5.0.

        :returns: a list of item, score tuples, empty if the set is empty.
        """
        pipe = self.database.pipeline()
        r1, r2 = (pipe
                  .zrange(self.key, 0 - count, -1, withscores=True)
                  .zremrangebyrank(self.key, 0 - count, -1)
                  .execute())
        return r1[::-1]

    def as_items(self, decode=False):
        """
        Return a list of 2-tuples consisting of key/score.
        """
        items = self.database.zrange(self.key, 0, -1, withscores=True)
        if decode:
            items = [(_decode(k), score) for k, score in items]
        return items

    @classmethod
    def from_dict(cls, database, key, data, clear=False):
        """
        Create and populate a ZSet object from a data dictionary.
        """
        zset = cls(database, key)
        if clear:
            zset.clear()
        zset.add(data)
        return zset


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
        """Return the estimated number of distinct items."""
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
        if not self.database.run_script('array_set', keys=[self.key],
                                        args=[idx, value]):
            raise IndexError('array assignment index out of range')

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
        item = self.database.connection_pool.get_encoder().encode(item)
        for value in self:
            if encode(value) == item:
                return True
        return False

    def __iter__(self):
        """Return an iterable that yields array items."""
        items = sorted(self.database.hscan_iter(self.key),
                       key=lambda item: int(item[0]))
        return iter(item[1] for item in items)

    def as_list(self, decode=False):
        """
        Return a list of items in the array.
        """
        return [_decode(i) for i in self] if decode else list(self)

    @classmethod
    def from_list(cls, database, key, data, clear=False):
        """
        Create and populate an Array object from a list.
        """
        arr = cls(database, key)
        if clear:
            arr.clear()
        arr.extend(data)
        return arr


def _normalize_stream_keys(keys, default_id='0-0'):
    if isinstance(keys, (str, bytes)):
        return {keys: default_id}
    elif isinstance(keys, (list, tuple)):
        return dict([(key, default_id) for key in keys])
    elif isinstance(keys, dict):
        return keys
    else:
        raise ValueError('keys must be either a stream key, a list of '
                         'stream keys, or a dictionary mapping key to '
                         'minimum message id.')


class Stream(Container):
    """
    Redis stream container.
    """
    def add(self, data, id='*', maxlen=None, approximate=True):
        """
        Add data to a stream.

        :param dict data: data to add to stream
        :param id: identifier for message ('*' to automatically append)
        :param maxlen: maximum length for stream
        :param approximate: allow stream max length to be approximate
        :returns: the added message id.
        """
        return self.database.xadd(self.key, data, id, maxlen, approximate)

    def __getitem__(self, item):
        """
        Read a range of values from a stream.

        The index must be a message id or a slice. An empty slice will result
        in reading all values from the stream. Message ids provided as lower or
        upper bounds are inclusive.

        To specify a maximum number of messages, use the "step" parameter of
        the slice.
        """
        if isinstance(item, slice):
            return self.range(item.start or '-', item.stop or '+', item.step)
        return self.get(item)

    def get(self, docid):
        """
        Get a message by id.

        :param docid: the message id to retrieve.
        :returns: a 2-tuple of (message id, data) or None if not found.
        """
        items = self[docid:docid:1]
        if items:
            return items[0]

    def range(self, start='-', stop='+', count=None):
        """
        Read a range of values from a stream.

        :param start: start key of range (inclusive) or '-' for oldest message
        :param stop: stop key of range (inclusive) or '+' for newest message
        :param count: limit number of messages returned
        """
        return self.database.xrange(self.key, start, stop, count)

    def revrange(self, start='+', stop='-', count=None):
        """
        Read a range of values from a stream in reverse.

        :param start: start key of range (inclusive) or '+' for newest message
        :param stop: stop key of range (inclusive) or '-' for oldest message
        :param count: limit number of messages returned
        """
        return self.database.xrevrange(self.key, start, stop, count)

    def __iter__(self):
        """Iterate over all messages in the stream."""
        return iter(self[:])

    def __delitem__(self, item):
        """
        Delete one or more messages by id. The index can be either a single
        message id or a list/tuple of multiple ids.
        """
        if not isinstance(item, (list, tuple)):
            item = (item,)
        self.delete(*item)

    def delete(self, *id_list):
        """
        Delete one or more messages by id.

        :returns: number of messages deleted.
        """
        return self.database.xdel(self.key, *id_list)

    def length(self):
        """
        Return the length of a stream.
        """
        return self.database.xlen(self.key)
    __len__ = length

    def read(self, count=None, block=None, last_id=None):
        """
        Monitor stream for new data.

        :param int count: limit number of messages returned
        :param int block: milliseconds to block, 0 for indefinitely
        :param last_id: Last id read (an exclusive lower-bound). If the '$'
            value is given, we will only read values added *after* our command
            started blocking.
        :returns: a list of (message id, data) 2-tuples.
        """
        if last_id is None: last_id = '0-0'
        resp = self.database.xread({self.key: _decode(last_id)}, count, block)

            # resp is a 2-tuple of stream name -> message list.
        return resp[0][1] if resp else []

    def info(self):
        """
        Retrieve information about the stream. Wraps call to
        ``xinfo_stream()``.

        :returns: a dictionary containing stream metadata
        """
        return self.database.xinfo_stream(self.key)

    def groups_info(self):
        """
        Retrieve information about consumer groups for the stream. Wraps call
        to ``xinfo_groups()``.

        :returns: a list of dictionaries containing consumer group metadata
        """
        return self.database.xinfo_groups(self.key)

    def consumers_info(self, group):
        """
        Retrieve information about consumers within the given consumer group
        operating on the stream. Calls ``xinfo_consumers()``.

        :param group: consumer group name
        :returns: a list of dictionaries containing consumer metadata
        """
        return self.database.xinfo_consumers(self.key, group)

    def set_id(self, id):
        """
        Set the last id of the stream (``XSETID``). Ids generated by later
        calls to :py:meth:`~Stream.add` will be greater than it.

        :param id: new last id, which cannot be lower than the id of the
            newest message.
        """
        return self.database.xsetid(self.key, id)

    def trim(self, count=None, approximate=True, minid=None, limit=None):
        """
        Trim the stream to the given "count" of messages, discarding the oldest
        messages first.

        :param count: maximum size of stream (maxlen)
        :param approximate: allow size to be approximate
        :param minid: evicts entries with IDs lower than the given min id.
        :param limit: maximum number of entries to evict.
        """
        return self.database.xtrim(self.key, maxlen=count,
                                   approximate=approximate, minid=minid,
                                   limit=limit)


class ConsumerGroupStream(Stream):
    """
    Helper for working with an individual stream within the context of a
    consumer group. This object is exposed as an attribute on a
    :py:class:`.ConsumerGroup` object using the stream key for the attribute
    name.

    This class should not be created directly. It will automatically be added
    to the ``ConsumerGroup`` object.

    For example::

        cg = db.consumer_group('groupname', ['stream-1', 'stream-2'])
        cg.stream_1  # ConsumerGroupStream for "stream-1"
        cg.stream_2  # ConsumerGroupStream for "stream-2"
    """
    __slots__ = ('database', 'group', 'key', '_consumer')

    def __init__(self, database, group, key, consumer):
        self.database = database
        self.group = group
        self.key = key
        self._consumer = consumer

    def consumers_info(self):
        """
        Retrieve information about consumers within the given consumer group
        operating on the stream. Calls ``xinfo_consumers()``.

        :returns: a list of dictionaries containing consumer metadata
        """
        return self.database.xinfo_consumers(self.key, self.group)

    def ack(self, *id_list):
        """
        Acknowledge that the message(s) have been processed by the consumer
        associated with the parent :py:class:`.ConsumerGroup`.

        :param id_list: one or more message ids to acknowledge
        :returns: number of messages marked acknowledged
        """
        return self.database.xack(self.key, self.group, *id_list)

    def claim(self, *id_list, **kwargs):
        """
        Claim pending - but unacknowledged - messages for this stream within
        the context of the parent :py:class:`.ConsumerGroup`.

        :param id_list: one or more message ids to claim
        :param min_idle_time: minimum idle time in milliseconds (keyword-arg).
        :returns: list of (message id, data) 2-tuples of messages that were
            successfully claimed
        """
        min_idle_time = kwargs.pop('min_idle_time', None) or 0
        if kwargs: raise ValueError('incorrect arguments for claim()')
        return self.database.xclaim(self.key, self.group, self._consumer,
                                    min_idle_time, id_list)

    def pending(self, start='-', stop='+', count=1000, consumer=None,
                idle=None):
        """
        List pending messages within the consumer group for this stream.

        :param start: start id (or '-' for oldest pending)
        :param stop: stop id (or '+' for newest pending)
        :param count: limit number of messages returned
        :param consumer: restrict message list to the given consumer
        :param int idle: filter by idle-time in milliseconds (6.2)
        :returns: A list containing status for each pending message. Each
            pending message is a dict with ``message_id``, ``consumer``,
            ``time_since_delivered`` and ``times_delivered``.
        """
        return self.database.xpending_range(self.key, self.group, min=start,
                                            max=stop, count=count,
                                            consumername=consumer, idle=idle)

    def autoclaim(self, consumer, min_idle_time, start_id=0, count=None, justid=False):
        """
        Transfer ownership of pending stream entries that match the specified
        criteria. Similar to calling XPENDING and XCLAIM, but provides a more
        straightforward way to deal with message delivery failures.

        :param consumer: name of consumer that claims the message.
        :param min_idle_time: in milliseconds
        :param start_id: start id
        :param count: optional, upper limit of entries to claim. Default 100.
        :param justid: return just IDs of messages claimed.
        :returns: [next start id, claimed (message id, data) 2-tuples, ids
            of deleted messages], or a list of claimed ids if ``justid``.
        """
        return self.database.xautoclaim(self.key, self.group, consumer,
                                        min_idle_time, start_id, count, justid)

    def read(self, count=None, block=None, last_id=None):
        """
        Monitor the stream for new messages within the context of the parent
        :py:class:`.ConsumerGroup`.

        :param int count: limit number of messages returned
        :param int block: milliseconds to block, 0 for indefinitely.
        :param str last_id: optional last ID, by default uses the special
            token ">", which reads messages not yet delivered to any
            consumer in the group.
        :returns: a list of (message id, data) 2-tuples.
        """
        key = {self.key: '>' if last_id is None else last_id}
        resp = self.database.xreadgroup(self.group, self._consumer, key, count,
                                        block)
        return resp[0][1] if resp else []

    def set_id(self, id='$'):
        """
        Set the last-read message id for the stream within the context of the
        parent :py:class:`.ConsumerGroup`. By default this will be the special
        "$" identifier, meaning all messages are marked as having been read.

        :param id: id of last-read message (or "$").
        """
        return self.database.xgroup_setid(self.key, self.group, id)

    def delete_consumer(self, consumer=None):
        """
        Remove a specific consumer from a consumer group.

        :param consumer: name of consumer to delete. If not provided, will be
            the default consumer for this stream.
        :returns: number of pending messages that the consumer had before
            being deleted.
        """
        if consumer is None: consumer = self._consumer
        return self.database.xgroup_delconsumer(self.key, self.group, consumer)


class ConsumerGroup(object):
    """
    Helper for working with Redis Streams consumer groups functionality. Each
    stream associated with the consumer group is exposed as a special attribute
    of the ``ConsumerGroup`` object, exposing stream-specific functionality
    within the context of the group.

    Rather than creating this class directly, use the
    :py:meth:`Database.consumer_group` method.

    Each registered stream within the group is exposed as a special attribute
    that provides stream-specific APIs within the context of the group. For
    more information see :py:class:`.ConsumerGroupStream`.

    The streams managed by a consumer group must exist before the consumer
    group can be created. By default, calling :py:meth:`ConsumerGroup.create`
    will automatically create stream keys for any that do not exist.

    Example::

        cg = db.consumer_group('groupname', ['stream-1', 'stream-2'])
        cg.create()  # Create consumer group.
        cg.stream_1  # ConsumerGroupStream for "stream-1"
        cg.stream_2  # ConsumerGroupStream for "stream-2"
        # or, alternatively:
        cg.streams['stream-1']

    A stream whose attribute name would collide with a ``ConsumerGroup``
    attribute (such as ``read`` or ``keys``), or with an earlier stream, is
    only available through ``cg.streams``.

    :param Database database: Redis client
    :param name: consumer group name
    :param keys: stream identifier(s) to monitor. May be a single stream
        key, a list of stream keys, or a key-to-minimum id mapping. The
        minimum id for each stream should be considered an exclusive
        lower-bound. Use '$' to read only messages added after the group
        is created.
    :param consumer: name for consumer
    """
    stream_key_class = ConsumerGroupStream

    def __init__(self, database, name, keys, consumer=None):
        self.database = database
        self.name = name
        self.keys = _normalize_stream_keys(keys)
        self._read_keys = _normalize_stream_keys(list(self.keys), '>')
        self._consumer = consumer or (self.name + '.c1')
        self.streams = {}  # Dict of key->ConsumerGroupStream.

        # Add attributes for each stream exposed as part of the group. A name
        # that collides with an attribute or an earlier stream is skipped.
        for key in self.keys:
            attr = make_python_attr(key)
            stream = self.stream_key_class(database, name, key, self._consumer)
            if not hasattr(self, attr):
                setattr(self, attr, stream)
            self.streams[key] = stream

    def consumer(self, name):
        """
        Create a new consumer for the :py:class:`ConsumerGroup`.

        :param name: name of consumer
        :returns: a :py:class:`ConsumerGroup` using the given consumer name.
        """
        return type(self)(self.database, self.name, self.keys, name)

    def create(self, ensure_keys_exist=True, mkstream=False):
        """
        Create the consumer group and register it with the group's stream keys.

        :param ensure_keys_exist: Ensure that the streams exist before creating
            the consumer group. Streams that do not exist will be created.
        :param mkstream: Use the "MKSTREAM" option to ensure stream exists.
        :returns: a dictionary mapping stream key to True, or to False if the
            group already exists on that stream.
        """
        if ensure_keys_exist:
            for key in self.keys:
                if not self.database.exists(key):
                    msg_id = self.database.xadd(key, {'': ''}, id=b'0-1')
                    self.database.xdel(key, msg_id)
                elif _decode(self.database.type(key)) != 'stream':
                    raise ValueError('Consumer group key "%s" exists and is '
                                     'not a stream. To prevent data-loss '
                                     'this key will not be deleted.' % key)

        resp = {}

        # Mapping of key -> last-read message ID.
        for key, value in self.keys.items():
            try:
                resp[key] = self.database.xgroup_create(key, self.name, value,
                                                        mkstream)
            except ResponseError as exc:
                if str(exc).startswith('BUSYGROUP'):
                    resp[key] = False
                else:
                    raise
        return resp

    def reset(self):
        """
        Reset the consumer group, clearing the last-read status for each
        stream so it will read from the beginning of each stream.
        """
        return self.set_id('0-0')

    def destroy(self):
        """
        Destroy the consumer group.
        """
        resp = {}
        for key in self.keys:
            resp[key] = self.database.xgroup_destroy(key, self.name)
        return resp

    def read(self, count=None, block=None, consumer=None):
        """
        Read unseen messages from all streams in the consumer group. Wrapper
        for ``Database.xreadgroup()``.

        :param int count: limit number of messages returned
        :param int block: milliseconds to block, 0 for indefinitely.
        :param consumer: consumer name
        :returns: a list of [stream key, messages] lists, where messages is
            a list of (message id, data) 2-tuples.
        """
        if consumer is None: consumer = self._consumer
        return self.database.xreadgroup(self.name, consumer, self._read_keys,
                                        count, block)

    def set_id(self, id='$'):
        """
        Set the last-read message id for each stream in the consumer group. By
        default, this will be the special "$" identifier, meaning all messages
        are marked as having been read.

        :param id: id of last-read message (or "$").
        """
        accum = {}
        for key in self.keys:
            accum[key] = self.database.xgroup_setid(key, self.name, id)
        return accum

    def stream_info(self):
        """
        Retrieve information for each stream managed by the consumer group.
        Calls ``xinfo_stream()`` for each stream.

        :returns: a dictionary mapping stream key to a dictionary of metadata
        """
        accum = {}
        for key in self.keys:
            accum[key] = self.database.xinfo_stream(key)
        return accum


class BitFieldOperation(object):
    """
    Command builder for BITFIELD commands.
    """
    def __init__(self, database, key):
        self.database = database
        self.key = key
        self.operations = []
        self._last_overflow = None  # Default is "WRAP".

    def incrby(self, fmt, offset, increment, overflow=None):
        """
        Increment a bitfield by a given amount.

        :param fmt: format-string for the bitfield being updated, e.g. u8 for
            an unsigned 8-bit integer.
        :param int offset: offset (in number of bits).
        :param int increment: value to increment the bitfield by.
        :param str overflow: overflow algorithm. Defaults to WRAP, or to the
            overflow set earlier in the same chain. Other acceptable values
            are SAT and FAIL. See the Redis docs for descriptions of these
            algorithms.
        :returns: a :py:class:`.BitFieldOperation` instance.
        """
        if overflow is not None and overflow != self._last_overflow:
            self._last_overflow = overflow
            self.operations.append(('OVERFLOW', overflow))

        self.operations.append(('INCRBY', fmt, offset, increment))
        return self

    def get(self, fmt, offset):
        """
        Get the value of a given bitfield.

        :param fmt: format-string for the bitfield being read, e.g. u8 for an
            unsigned 8-bit integer.
        :param int offset: offset (in number of bits).
        :returns: a :py:class:`.BitFieldOperation` instance.
        """
        self.operations.append(('GET', fmt, offset))
        return self

    def set(self, fmt, offset, value):
        """
        Set the value of a given bitfield.

        :param fmt: format-string for the bitfield being written, e.g. u8 for
            an unsigned 8-bit integer.
        :param int offset: offset (in number of bits).
        :param int value: value to set at the given position.
        :returns: a :py:class:`.BitFieldOperation` instance.
        """
        self.operations.append(('SET', fmt, offset, value))
        return self

    @property
    def command(self):
        return reduce(operator.add, self.operations, ('BITFIELD', self.key))

    def execute(self):
        """
        Execute the operation(s) in a single BITFIELD command. The return value
        is a list of values corresponding to each operation.
        """
        return self.database.execute_command(*self.command)

    def __iter__(self):
        """
        Implicit execution and iteration of the return values for a sequence of
        operations.
        """
        return iter(self.execute())


class BitField(Container):
    """
    Wrapper that provides a convenient API for constructing and executing Redis
    BITFIELD commands. The BITFIELD command can pack multiple operations into a
    single logical command, so the :py:class:`BitField` supports a method-
    chaining API that allows multiple operations to be performed atomically.

    Rather than instantiating this class directly, you should use the
    :py:meth:`Database.bit_field` method to obtain a :py:class:`BitField`.
    """
    def incrby(self, fmt, offset, increment, overflow=None):
        """
        Increment a bitfield by a given amount.

        :param fmt: format-string for the bitfield being updated, e.g. u8 for
            an unsigned 8-bit integer.
        :param int offset: offset (in number of bits).
        :param int increment: value to increment the bitfield by.
        :param str overflow: overflow algorithm. Defaults to WRAP, but other
            acceptable values are SAT and FAIL. See the Redis docs for
            descriptions of these algorithms.
        :returns: a :py:class:`.BitFieldOperation` instance.
        """
        bfo = BitFieldOperation(self.database, self.key)
        return bfo.incrby(fmt, offset, increment, overflow)

    def get(self, fmt, offset):
        """
        Get the value of a given bitfield.

        :param fmt: format-string for the bitfield being read, e.g. u8 for an
            unsigned 8-bit integer.
        :param int offset: offset (in number of bits).
        :returns: a :py:class:`.BitFieldOperation` instance.
        """
        bfo = BitFieldOperation(self.database, self.key)
        return bfo.get(fmt, offset)

    def set(self, fmt, offset, value):
        """
        Set the value of a given bitfield.

        :param fmt: format-string for the bitfield being written, e.g. u8 for
            an unsigned 8-bit integer.
        :param int offset: offset (in number of bits).
        :param int value: value to set at the given position.
        :returns: a :py:class:`.BitFieldOperation` instance.
        """
        bfo = BitFieldOperation(self.database, self.key)
        return bfo.set(fmt, offset, value)

    def _validate_slice(self, item):
        if not isinstance(item, slice):
            raise ValueError('Must use a slice.')
        if item.stop is None or item.stop < 0:
            raise ValueError('slice must have a non-negative upper-bound')
        start = item.start or 0
        if not 0 < item.stop - start < 64:
            raise ValueError('slice must span 1 to 63 bits')
        return start, item.stop

    def __getitem__(self, item):
        """
        Short-hand for getting a range of bits in a bitfield. Note that the
        item **must** be a slice specifying the start and end of the range of
        bits to read.
        """
        start, stop = self._validate_slice(item)
        return self.get('u%s' % (stop - start), start).execute()[0]

    def __setitem__(self, item, value):
        """
        Short-hand for setting a range of bits in a bitfield. Note that the
        item **must** be a slice specifying the start and end of the range of
        bits to read. If the value is negative or exceeds the number of bits
        implied by the slice range, a ``ValueError`` is raised.
        """
        start, stop = self._validate_slice(item)
        nbits = stop - start
        if not 0 <= value < (1 << nbits):
            raise ValueError('value does not fit width specified by slice')
        self.set('u%s' % nbits, start, value).execute()

    def __delitem__(self, item):
        """
        Clear a range of bits in a bitfield. Note that the item **must** be a
        slice specifying the start and end of the range of bits to clear.
        """
        start, stop = self._validate_slice(item)
        self.set('u%s' % (stop - start), start, 0).execute()

    def get_raw(self):
        """
        Return the raw bytestring that comprises the bitfield. Equivalent to a
        normal GET command.
        """
        return self.database.get(self.key)

    def set_raw(self, value):
        """
        Set the raw bytestring that comprises the bitfield. Equivalent to a
        normal SET command.
        """
        return self.database.set(self.key, value)

    def bit_count(self, start=None, end=None):
        """
        Count the set bits in a string. Note that the `start` and `end`
        parameters are offsets in **bytes**.
        """
        return self.database.bitcount(self.key, start, end)

    def get_bit(self, offset):
        """
        Get the bit value at the given offset (in bits).

        :param int offset: bit offset
        :returns: value at bit offset, 1 or 0
        """
        return self.database.getbit(self.key, offset)

    def set_bit(self, offset, value):
        """
        Set the bit value at the given offset (in bits).

        :param int offset: bit offset
        :param int value: new value for bit, 1 or 0
        :returns: previous value at bit offset, 1 or 0
        """
        return self.database.setbit(self.key, offset, value)


class BloomFilter(Container):
    """
    Bloom-filters are probabilistic data-structures that are used to answer the
    question: "is X a member of set S?" It is possible to receive a false
    positive, but impossible to receive a false negative (in other words, if
    the bloom filter contains a value, it will never erroneously report that it
    does *not* contain such a value). The likelihood of a false positive can
    be reduced by increasing the size of the bloomfilter. The default size is
    64KB (or 524,288 bits).

    Rather than instantiate this class directly, use
    :py:meth:`Database.bloom_filter`.
    """
    def __init__(self, database, key, size=64 * 1024):
        super(BloomFilter, self).__init__(database, key)
        self.size = size
        self.bits = self.size * 8
        self._bf = BitField(self.database, self.key)

    def _get_seeds(self, data):
        # Hash the data into a 16-byte digest, then break that up into 4 4-byte
        # (32-bit) unsigned integers. We use the modulo operator to normalize
        # these 32-bit ints to bit-indices.
        seeds = struct.unpack('>IIII', hashlib.md5(encode(data)).digest())
        return [seed % self.bits for seed in seeds]

    def add(self, data):
        """
        Add an item to the bloomfilter.

        :param data: a str or bytestring representing the item to add.
        """
        bfo = BitFieldOperation(self.database, self.key)
        for bit_index in self._get_seeds(data):
            bfo.set('u1', bit_index, 1)
        bfo.execute()

    def contains(self, data):
        """
        Check if an item has been added to the bloomfilter.

        :param data: a str or bytestring representing the item to check.
        :returns: a boolean indicating whether or not the item is present in
            the bloomfilter. False-positives are possible, but a negative
            return value is definitive.
        """
        bfo = BitFieldOperation(self.database, self.key)
        for bit_index in self._get_seeds(data):
            bfo.get('u1', bit_index)
        return all(bfo.execute())
    __contains__ = contains

    def __len__(self):
        return self.size
