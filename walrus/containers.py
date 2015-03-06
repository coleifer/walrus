from functools import wraps


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

    def incr_float(self, key, incr_by=1.):
        """Increment the key by the given amount."""
        return self.database.hincrbyfloat(self.key, key, incr_by)


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
        if isinstance(item, slice):
            start = item.start or 0
            stop = item.stop or -1
            if stop > 0:
                stop -= 1
            return self.database.ltrim(self.key, start, stop)
        elif isinstance(item, int):
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

    def range(self, low, high, with_scores=False, desc=False, reverse=False):
        """
        Return a range of items between ``low`` and ``high``. By
        default scores will not be included, but this can be controlled
        via the ``with_scores`` parameter.

        :param low: Lower bound.
        :param high: Upper bound.
        :param bool with_scores: Whether the range should include the
            scores along with the items.
        :param bool desc: Whether to sort the results descendingly.
        :param bool reverse: Whether to select the range in reverse.
        """
        if reverse:
            return self.database.zrevrange(self.key, low, high, with_scores)
        else:
            return self.database.zrange(self.key, low, high, desc, with_scores)

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
