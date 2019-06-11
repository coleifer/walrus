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

    >>> cache.set_many({'k1': 'v1', 'k2': 'v2'}, 300)
    True
    >>> cache.get_many(['k1', 'kx', 'k2'])
    {'k1': 'v1', 'k2': 'v2'}
    >>> cache.delete_many(['k1', 'kx', 'k2'])
    2

Simple Decorator
----------------

The :py:meth:`Cache.cached` decorator will *memoize* the return values from the wrapped function for the given arguments. One way to visualize this is by creating a function that returns the current time and wrap it with the decorator. The decorated function will run the first time it is called and the return value is stored in the cache. Subsequent calls will not execute the function, but will instead return the cached value from the previous call, until the cached value expires.

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

If a decorated function accepts arguments, then values will be cached based on the arguments specified. In the example below we'll pass a garbage argument to the ``get_time`` function to show how the cache varies for different arguments:

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

Cached Property
---------------

Python supports dynamic instance attributes through the ``property`` decorator. A property looks like a normal instance attribute, but it's value is calculated at run-time. Walrus comes with a special decorator designed for implementing *cached properties*. Here is how you might use :py:meth:`~Cache.cached_property`:

.. code-block:: pycon

    >>> class Clock(object):
    ...     @cache.cached_property()
    ...     def now(self):
    ...         return datetime.datetime.now()

    >>> print clock.now
    2015-01-12 21:10:34.335755

    >>> print clock.now
    2015-01-12 21:10:34.335755

.. _cache-async:

Cache Asynchronously
--------------------

If you have a function that runs slowly and would like to be able to perform other operations while waiting for the return value, you might try the *asynchronous cache decorator*, :py:meth:`~Cache.cache_async`.

The :py:meth:`~Cache.cache_async` decorator will run the decorated function in a separate thread. The function therefore will return immediately, even though your code may be processing in the background. Calls to the decorated function will return a method on a synchronized queue object. When the value is calculated (or returned from the cache), it will be placed in the queue and you can retrieve it.

Let's see how this works. We'll add a call to ``time.sleep`` in the decorated function to simulate a function that takes a while to run, and we'll also print a message indicating that we're inside the function body.

.. code-block:: pycon

    >>> import time
    >>> @cache.cache_async()
    ... def get_now(seed=None):
    ...     print 'About to sleep for 5 seconds.'
    ...     time.sleep(5)
    ...     return datetime.datetime.now()

The first time we call our function we will see the message indicating our function is sleeping, but the function will return immediately! The return value can be used to get the *actual* return value of the decorated function:

.. code-block:: pycon

    >>> result = get_now()
    About to sleep for 5 seconds.
    >>> result
    <function _get_value at 0x7fe3a4685de8>

If we attempt to check the result immediately, there will be no value because the function is still sleeping. In this case a queue ``Empty`` exception is raised:

.. code-block:: pycon

    >>> result(block=False)
    Traceback (most recent call last):
      File "<stdin>", line 1, in <module>
      File "/usr/lib/python2.7/Queue.py", line 165, in get
        raise Empty
    Queue.Empty

We can force our code to block until the result is ready, though:

.. code-block:: pycon

    >>> print result(block=True)
    2015-01-12 21:28:25.266448

Now that the result has been calculated and cached, a subsequent call to ``get_now()`` will not execute the function body. We can tell because the function does not print *About to sleep for 5 seconds*.

.. code-block:: pycon

    >>> result = get_now()
    >>> print result()
    2015-01-12 21:28:25.266448

The result function can be called any number of times. It will always return the same value:

.. code-block:: pycon

    >>> print result()
    2015-01-12 21:28:25.266448

Another trick is passing a timeout to the result function. Let's see what happens when we call ``get_now()`` using a different seed, then specify a timeout to block for the return value. Since we hard-coded a delay of 5 seconds, let's see what happens when we specify a timeout of 4 seconds:

.. code-block:: pycon

    >>> print get_now('foo')(timeout=4)
    About to sleep for 5 seconds.
    Traceback (most recent call last):
      File "<stdin>", line 1, in <module>
      File "/home/charles/pypath/walrus/cache.py", line 160, in _get_value
        result = q.get(block=block, timeout=timeout)
      File "/usr/lib/python2.7/Queue.py", line 176, in get
        raise Empty
    Queue.Empty

Now let's try with a timeout of 6 seconds (being sure to use a different seed so we trigger the 5 second delay):

.. code-block:: pycon

    >>> print get_now('bar')(timeout=6)
    About to sleep for 5 seconds.
    2015-01-12 21:46:49.060883

Since the function returns a value within the given timeout, the value is returned.
