from functools import wraps
import os
import threading


class _BaseLock(object):
    # Subclasses set the key prefix, which also names their Lua scripts, and
    # the number of holders allowed at once.
    prefix = None
    value = 1

    def __init__(self, database, name, ttl=None, lock_id=None):
        """
        :param database: A walrus ``Database`` instance.
        :param str name: The name for the lock.
        :param int ttl: The time-to-live for the lock in milliseconds.
        :param str lock_id: Token used by every acquire of this instance. By
            default each acquire uses a new random token.
        """
        self.database = database
        self.name = name
        self.ttl = ttl or 0
        self._lock_id = lock_id
        # Each thread keeps a stack of the tokens it acquired.
        self._local = threading.local()

    @property
    def key(self):
        return '%s:%s' % (self.prefix, self.name)

    @property
    def event(self):
        return '%s.event:%s' % (self.prefix, self.name)

    @property
    def _tokens(self):
        if not hasattr(self._local, 'tokens'):
            self._local.tokens = []
        return self._local.tokens

    def _script(self, name, token):
        return self.database.run_script(
            '%s_%s' % (self.prefix, name),
            keys=[self.key, self.event],
            args=[token, self.ttl, self.value])

    def acquire(self, block=True):
        """
        Acquire the lock. The lock will be held until it is released
        by calling :py:meth:`~Lock.release`. If the lock was
        initialized with a ``ttl``, then the lock will be released
        automatically after the given number of milliseconds.

        By default this method will block until the lock becomes
        free (either by being released or expiring). The blocking is
        accomplished by performing a blocking left-pop on a list, as
        opposed to a spin-loop.

        If you specify ``block=False``, then the method will return
        ``False`` if the lock could not be acquired.

        :param bool block: Whether to block while waiting to acquire
            the lock.
        :returns: Returns ``True`` if the lock was acquired.
        """
        # Wake at least once per TTL, so an abandoned lock is retried once it
        # expires, and well before the socket times out.
        event_wait = self.ttl / 1000. if self.ttl else 1
        socket_timeout = self.database.connection_pool.connection_kwargs.get(
            'socket_timeout')
        if socket_timeout:
            event_wait = min(event_wait, socket_timeout / 2.)
        token = self._lock_id or os.urandom(32)

        while True:
            if self._script('acquire', token) == 1:
                self._tokens.append(token)
                return True
            elif not block:
                return False

            # Perform a blocking pop on the event key. When a lock
            # is released, a value is pushed into the list, which
            # signals listeners that the lock is available.
            self.database.blpop(self.event, event_wait)

    def release(self):
        """
        Release the lock most recently acquired by the current thread.

        :returns: Returns ``True`` if the lock was released.
        """
        tokens = self._tokens
        token = tokens.pop() if tokens else self._lock_id or ''
        return self._script('release', token) != 0

    def clear(self):
        """
        Clear the lock, allowing it to be acquired. Do not use this
        method except to recover from a deadlock. Otherwise you should
        use :py:meth:`~Lock.release`.
        """
        self.database.delete(self.key)
        self.database.delete(self.event)

    def __enter__(self):
        self.acquire()

    def __exit__(self, exc_type, exc_val, exc_tb):
        # A failed release must not hide an exception from the body.
        if not self.release() and exc_type is None:
            raise RuntimeError('Error releasing %s "%s".' % (
                self.prefix, self.name))

    def __call__(self, fn):
        @wraps(fn)
        def inner(*args, **kwargs):
            with self:
                return fn(*args, **kwargs)
        return inner


class Lock(_BaseLock):
    """
    Lock implementation. Can also be used as a context-manager or
    decorator.

    Unlike the redis-py lock implementation, this Lock does not
    use a spin-loop when blocking to acquire the lock. Instead,
    it performs a blocking pop on a list. When a lock is released,
    a value is pushed into this list, signalling that the lock is
    available.

    .. warning::
        The event list for each lock persists
        indefinitely unless removed using :py:meth:`Lock.clear` or
        otherwise manually in the Redis database. For this reason,
        be cautious when creating locks dynamically, or your
        keyspace might grow in an unbounded way.

    The lock uses Lua scripts to ensure the atomicity of its
    operations.

    You can set a TTL on a lock to reduce the potential for deadlocks
    in the event of a crash. If a lock is not released before it
    exceeds its TTL, then threads that are blocked waiting for the
    lock could potentially re-acquire it.

    .. note:: TTL is specified in **milliseconds**.

    Locks can be used as context managers or as decorators:

    .. code-block:: python

        lock = db.lock('my-lock')

        with lock:
            perform_some_calculations()

        @lock
        def another_function():
            # The lock will be acquired when this function is
            # called, and released when the function returns.
            do_some_more_calculations()
    """
    prefix = 'lock'


class Semaphore(_BaseLock):
    """
    A lock that up to ``value`` holders can hold at once. It has the same
    API as :py:class:`Lock`, and a single thread may hold several slots.

    Holders are stored in a sorted set scored by their expiry time, so with
    a ``ttl`` the slot of a crashed holder frees itself once the TTL passes.

    .. code-block:: python

        sem = db.semaphore('workers', 5)

        with sem:
            run_job()
    """
    prefix = 'semaphore'

    def __init__(self, database, name, value=1, ttl=None):
        """
        :param database: A walrus ``Database`` instance.
        :param str name: The name for the semaphore.
        :param int value: Number of holders allowed at once.
        :param int ttl: The time-to-live for each holder in milliseconds.
        """
        super(Semaphore, self).__init__(database, name, ttl)
        self.value = value
