from functools import wraps
import hashlib
import pickle
import threading
try:
    from Queue import Queue  # Python 2
except ImportError:
    from queue import Queue  # Python 3


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

    def cached(self, key_fn=_key_fn, timeout=None):
        """
        Decorator that will transparently cache calls to the
        wrapped function. By default, the cache key will be made
        up of the arguments passed in (like memoize), but you can
        override this by specifying a custom ``key_fn``.

        :param key_fn: Function used to generate a key from the
            given args and kwargs.
        :param timeout: Time to cache return values.
        :returns: Return the result of the decorated function
            call with the given args and kwargs.

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

    def cached_property(self, key_fn=_key_fn, timeout=None):
        """
        Decorator that will transparently cache calls to the wrapped
        method. The method will be exposed as a property.

        Usage::

            cache = Cache(my_database)

            class Clock(object):
                @cache.cached_property()
                def now(self):
                    return datetime.datetime.now()

            clock = Clock()
            print clock.now
        """
        this = self

        class _cached_property(object):
            def __init__(self, fn):
                self._fn = this.cached(key_fn, timeout)(fn)

            def __get__(self, instance, instance_type=None):
                if instance is None:
                    return self
                return self._fn(instance)

            def __delete__(self, obj):
                self._fn.bust(obj)

            def __set__(self, instance, value):
                raise ValueError('Cannot set value of a cached property.')

        def decorator(fn):
            return _cached_property(fn)

        return decorator

    def cache_async(self, key_fn=_key_fn, timeout=3600):
        """
        Decorator that will execute the cached function in a separate
        thread. The function will immediately return, returning a
        callable to the user. This callable can be used to check for
        a return value.

        For details, see the :ref:`cache-async` section of the docs.

        :param key_fn: Function used to generate cache key.
        :param int timeout: Cache timeout in seconds.
        :returns: A new function which can be called to retrieve the
            return value of the decorated function.
        """
        def decorator(fn):
            wrapped = self.cached(key_fn, timeout)(fn)

            @wraps(fn)
            def inner(*args, **kwargs):
                q = Queue()
                def _sub_fn():
                    q.put(wrapped(*args, **kwargs))
                def _get_value(block=True, timeout=None):
                    if not hasattr(_get_value, '_return_value'):
                        result = q.get(block=block, timeout=timeout)
                        _get_value._return_value = result
                    return _get_value._return_value

                thread = threading.Thread(target=_sub_fn)
                thread.start()
                return _get_value
            return inner
        return decorator


class sentinel(object):
    pass
