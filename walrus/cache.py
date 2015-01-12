from functools import wraps
import hashlib
import pickle


class Cache(object):
    """
    Cache implementation with simple ``get``/``set`` operations,
    and a decorator.
    """
    def __init__(self, database, name='cache', default_timeout=None,
                 debug=False):
        """
        :param database: :py:class:`Database` instance.
        :param name: Namespace for this cache.
        :param int default_timeout: Default cache timeout.
        :param debug: Disable cache for debugging purposes. Cache will no-op.
        """
        self.database = database
        self.name = name
        self.default_timeout = default_timeout
        self.debug = debug

    def make_key(self, s):
        return ':'.join((self.name, s))

    def get(self, key, default=None):
        """
        Retreive a value from the cache. In the event the value
        does not exist, return the ``default``.
        """
        key = self.make_key(key)

        if self.debug:
            return default

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

        if self.debug:
            return True

        pickled_value = pickle.dumps(value)
        if timeout:
            return self.database.setex(key, pickled_value, int(timeout))
        else:
            return self.database.set(key, pickled_value)

    def delete(self, key):
        """Remove the given key from the cache."""
        if not self.debug:
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
            def make_key(args, kwargs):
                return '%s:%s' % (fn.__name__, key_fn(args, kwargs))

            def bust(*args, **kwargs):
                return self.delete(make_key(args, kwargs))

            @wraps(fn)
            def inner(*args, **kwargs):
                key = make_key(args, kwargs)
                res = self.get(key)
                if res is None:
                    res = fn(*args, **kwargs)
                    self.set(key, res, timeout)
                return res
            inner.bust = bust
            inner.make_key = make_key
            return inner
        return decorator
