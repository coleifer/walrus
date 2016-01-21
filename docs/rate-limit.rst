.. _rate-limit:

.. py:module:: walrus

Rate Limit
==========

Walrus provides a simple :py:class:`RateLimit` implementation that makes use of Redis' :py:class:`List` object to store a series of event timestamps.

As the rate-limiter logs events, it maintains a fixed-size list of timestamps. When the list of timestamps is at max capacity, Walrus will look at the difference between the oldest timestamp and the present time to determine if a new event can be logged.

Example with a rate limiter that allows 2 events every 10 seconds.

* Log event from IP 192.168.1.2
* List for key ``192.168.1.2`` now contains ``['14:42:27.04521']`` (these are actually unix timestamps, but are shown as times for readability).
* Five seconds later log another event from the same IP.
* List for ``192.168.1.2`` now contains ``['14:42:32.08293', '14:42:27.04521']``
* Two seconds later attempt another event from the same IP. Since the list is "at capacity", and the time difference between the oldest event and the newest is less than 10 seconds, the event will not be logged and the event will be rate-limited.

Basic usage
-----------

You can :py:meth:`~RateLimit.limit` to log an event and check whether it should be rate-limited:

.. code-block:: pycon

    >>> from walrus import *
    >>> db = Database()
    >>> rate_limit = db.rate_limit('mylimit', limit=2, per=60)  # 2 events per minute.

    >>> rate_limit.limit('user-1')
    False
    >>> rate_limit.limit('user-1')
    False
    >>> rate_limit.limit('user-1')  # Slow down, user-1!
    True

    >>> rate_limit.limit('user-2')  # User 2 has not performed any events yet.
    False

Decorator
---------

The :py:meth:`RateLimit.rate_limited` decorator can be used to restrict calls to a function or method. The decorator accepts a ``key_function`` parameter which instructs it how to uniquely identify the source of the function call. For example, on a web-site, you might want the key function to be derived from the requesting user's IP address.

.. code-block:: python

    rate_limit = walrus.rate_limit('login-limiter', limit=3, per=60)

    @app.route('/login/', methods=['GET', 'POST'])
    @rate_limit.rate_limited(lambda: request.remote_addr)
    def login():
        # Accept user login, etc.
        pass

.. note::

    The :py:meth:`~RateLimit.rate_limited` decorator will raise a ``RateLimitException`` when an attempt to call the decorated function would exceed the allowed number of events. In your application you can catch these and perform the appropriate action.

If no key function is supplied, then Walrus will simply take the hash of all the arguments the function was called with and treat that as the key. Except for very simple functions, this is probably not waht you want, so take care to ensure your ``key_function`` works as you expect.
