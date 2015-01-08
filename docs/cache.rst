.. _cache:

.. py:module:: walrus

Cache
=====

Walrus provides a simple :py:class:`Cache` implementation that makes use of Redis' key expiration feature. The cache can be used to set or retrieve values, and also provides a decorator (:py:meth:`Cache.cached`) for wrapping function or methods.

Basic usage
-----------

You can :py:meth:`~Cache.get`, :py:meth:`~Cache.set` and :py:meth:`~Cache.delete` objects directly from the cache.

.. code-block:: pycon

    >>> from walrus import *
    >>> db = Database()
    >>> cache = db.cache()

    >>> cache.set('foo', 'bar', 10)  # Set foo=bar, expiring in 10s.
    >>> cache.get('foo')
    'bar'

    >>> time.sleep(10)
    >>> cache.get('foo') is None
    True

Decorator
---------

The :py:meth:`Cache.cached` decorator will *memoize* the return values from the wrapped function for the given arguments. One way to visualize this is by creating a function that returns the current time and wrap it with the decorator:

.. code-block:: pycon

    >>> @cache.cached(timeout=10)
    ... def get_time():
    ...     return datetime.datetime.now()

    >>> print get_time()  # First call, return value cached.
    2015-01-07 18:26:42.730638

    >>> print get_time()  # Hits the cache.
    2015-01-07 18:26:42.730638

    >>> time.sleep(10)  # Wait for cache to expire then call again.
    >>> print get_time()
    2015-01-07 18:26:53.529011

If a decorated function accepts arguments, then values will be cached based on the arguments specified:

.. code-block:: pycon

    >>> @cache.cached(timeout=60)
    ... def get_time(seed=None):
    ...     return datetime.datetime.now()

    >>> print get_time()
    2015-01-07 18:30:53.831977

    >>> print get_time()
    2015-01-07 18:30:53.831977

    >>> print get_time('foo')
    2015-01-07 18:30:56.614064

    >>> print get_time('foo')
    2015-01-07 18:30:56.614064

    >>> print get_time('bar')
    2015-01-07 18:31:01.497050

    >>> print get_time('foo')
    2015-01-07 18:30:56.614064

To clear the cache, you can call the special ``bust()`` method on the decorated function:

.. code-block:: pycon

    >>> get_time.bust('foo')
    >>> print get_time('foo')
    2015-01-07 18:31:15.326435
