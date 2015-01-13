from functools import wraps
import hashlib
import pickle
import threading
from Queue import Empty, Queue


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

    def cached_property(self, key_fn=_key_fn, timeout=3600):
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

        To show how this works. We'll add a call to ``time.sleep`` in
        the decorated function to simulate a function that takes a
        while to run, and we'll also print a message indicating that
        we're inside the function body.

        .. code-block:: pycon

            >>> import time
            >>> @cache.cache_async()
            ... def get_now(seed=None):
            ...     print 'About to sleep for 5 seconds.'
            ...     time.sleep(5)
            ...     return datetime.datetime.now()

        The first time we call our function we will see the message
        indicating our function is sleeping, but the function will
        return immediately! The return value can be used to get the
        *actual* return value of the decorated function:

        .. code-block:: pycon

            >>> result = get_now()
            About to sleep for 5 seconds.
            >>> result
            <function _get_value at 0x7fe3a4685de8>

        If we attempt to check the result immediately, there will be
        no value because the function is still sleeping. In this
        case a queue ``Empty`` exception is raised:

        .. code-block:: pycon

            >>> result(block=False)
            Traceback (most recent call last):
              File "<stdin>", line 1, in <module>
              File "/usr/lib/python2.7/Queue.py", line 165, in get
                raise Empty
            Queue.Empty

        We can force our code to block until the result is ready,
        though:

        .. code-block:: pycon

            >>> print result(block=True)
            2015-01-12 21:28:25.266448

        Now that the result has been calculated and cached, a
        subsequent call to ``get_now()`` will not execute the
        function body. We can tell because the function does not
        print *About to sleep for 5 seconds*.

        .. code-block:: pycon

            >>> result = get_now()
            >>> print result()
            2015-01-12 21:28:25.266448

        The result function can be called any number of times. It
        will always return the same value:

        .. code-block:: pycon

            >>> print result()
            2015-01-12 21:28:25.266448

        Another trick is passing a timeout to the result function.
        Let's see what happens when we call ``get_now()`` using a
        different seed, then specify a timeout to block for the
        return value. Since we hard-coded a delay of 5 seconds,
        let's see what happens when we specify a timeout of 4
        seconds:

        .. code-block:: pycon

            >>> print get_now('foo')(timeout=4)
            About to sleep for 5 seconds.
            Traceback (most recent call last):
              File "<stdin>", line 1, in <module>
              File "/home/charles/pypath/walrus/cache.py",
                line 160, in _get_value
                result = q.get(block=block, timeout=timeout)
              File "/usr/lib/python2.7/Queue.py", line 176, in get
                raise Empty
            Queue.Empty

        Now let's try with a timeout of 6 seconds (being sure to use
        a different seed so we trigger the 5 second delay):

        .. code-block:: pycon

            >>> print get_now('bar')(timeout=6)
            About to sleep for 5 seconds.
            2015-01-12 21:46:49.060883

        Since the function returns a value within the given timeout,
        the value is returned.
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
