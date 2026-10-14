from functools import wraps
import os
import threading
import time

from redis.exceptions import RedisError


class _Primitive(object):
    # Subclasses set the key prefix, which also names their Lua scripts.
    prefix = None

    def __init__(self, database, name):
        self.database = database
        self.name = name

    @property
    def key(self):
        return '%s:%s' % (self.prefix, self.name)

    @property
    def event(self):
        return '%s.event:%s' % (self.prefix, self.name)

    def _cap(self, seconds):
        # Keep blocking calls well under the socket timeout.
        socket_timeout = self.database.connection_pool.connection_kwargs.get(
            'socket_timeout')
        if socket_timeout:
            return min(seconds, socket_timeout / 2.)
        return seconds

    def _block(self, key, timeout, done=None):
        # Rotating the list at key onto itself leaves its signal in place, so
        # one push wakes every waiter. Wait in short steps, so done() can be
        # polled in case the push was missed.
        deadline = None if timeout is None else time.time() + timeout
        while True:
            seconds = 1 if deadline is None else min(1, deadline - time.time())
            if seconds <= 0:
                return False
            if self.database.brpoplpush(key, key, self._cap(seconds)):
                return True
            if done is not None and done():
                return True


class _BaseLock(_Primitive):
    # Subclasses set the number of holders allowed at once.
    value = 1

    def __init__(self, database, name, ttl=None, lock_id=None,
                 auto_renew=False):
        """
        :param database: A walrus ``Database`` instance.
        :param str name: The name for the lock.
        :param int ttl: The time-to-live for the lock in milliseconds.
        :param str lock_id: Token used by every acquire of this instance. By
            default each acquire uses a new random token.
        :param bool auto_renew: Renew the lock from a background thread
            every third of its ``ttl`` until it is released.
        :raises: ``ValueError`` if ``auto_renew`` is set without a ``ttl``.
        """
        if auto_renew and not ttl:
            raise ValueError('auto_renew requires a ttl.')
        super(_BaseLock, self).__init__(database, name)
        self.ttl = ttl or 0
        self._lock_id = lock_id
        self.auto_renew = auto_renew
        self._local = threading.local()

    @property
    def _stack(self):
        # Each thread keeps a stack of (token, stop renewal event) for the
        # locks it acquired.
        if not hasattr(self._local, 'stack'):
            self._local.stack = []
        return self._local.stack

    def _latest(self, pop=False):
        stack = self._stack
        if not stack:
            return self._lock_id or '', None
        return stack.pop() if pop else stack[-1]

    def _script(self, name, token, ttl=None):
        return self.database.run_script(
            '%s_%s' % (self.prefix, name),
            keys=[self.key, self.event],
            args=[token, self.ttl if ttl is None else ttl, self.value])

    def _renew_in_background(self, token):
        stop = threading.Event()

        def renew():
            while not stop.wait(self.ttl / 3000.):
                try:
                    if self._script('renew', token) != 1:
                        return  # The lock was lost.
                except RedisError:
                    pass  # Retry at the next interval.

        thread = threading.Thread(target=renew)
        thread.daemon = True
        thread.start()
        return stop

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
        # expires.
        event_wait = self._cap(self.ttl / 1000. if self.ttl else 1)
        token = self._lock_id or os.urandom(32)

        while True:
            if self._script('acquire', token) == 1:
                stop = None
                if self.auto_renew:
                    stop = self._renew_in_background(token)
                self._stack.append((token, stop))
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
        token, stop = self._latest(pop=True)
        if stop is not None:
            stop.set()
        return self._script('release', token) != 0

    def renew(self, ttl=None):
        """
        Reset the TTL of the lock most recently acquired by the current
        thread, if it still holds it.

        :param int ttl: The new time-to-live in milliseconds. Defaults to
            the ``ttl`` of the lock.
        :returns: Returns ``True`` if the lock is still held.
        """
        return self._script('renew', self._latest()[0], ttl) == 1

    def held(self):
        """
        Check with Redis whether the current thread still holds the lock it
        most recently acquired.

        :returns: Returns ``True`` if the lock is still held.
        """
        # A TTL of 0 renews nothing, so the script only checks the token.
        return self._script('renew', self._latest()[0], 0) == 1

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

    def __init__(self, database, name, value=1, ttl=None, auto_renew=False):
        """
        :param database: A walrus ``Database`` instance.
        :param str name: The name for the semaphore.
        :param int value: Number of holders allowed at once.
        :param int ttl: The time-to-live for each holder in milliseconds.
        :param bool auto_renew: As in :py:class:`Lock`.
        """
        super(Semaphore, self).__init__(database, name, ttl,
                                        auto_renew=auto_renew)
        self.value = value


class Event(_Primitive):
    """
    A flag shared across processes, like ``threading.Event``. A single call
    to :py:meth:`~Event.set` wakes every waiter.

    .. code-block:: python

        ready = db.event('ready')

        # Workers block until the flag is set.
        ready.wait()

        # Elsewhere:
        ready.set()
    """
    prefix = 'event'

    def set(self):
        """Set the flag, waking every waiter."""
        pipeline = self.database.pipeline()
        pipeline.lpush(self.key, 1)
        pipeline.ltrim(self.key, 0, 0)
        pipeline.execute()

    def is_set(self):
        """Return ``True`` if the flag is set."""
        return self.database.exists(self.key) == 1

    def clear(self):
        """Reset the flag, so :py:meth:`~Event.wait` blocks again."""
        self.database.delete(self.key)

    def wait(self, timeout=None):
        """
        Block until the flag is set.

        :param float timeout: Seconds to wait. By default, wait forever.
        :returns: ``True`` if the flag is set, ``False`` on timeout.
        """
        return self._block(self.key, timeout) or self.is_set()


class Barrier(_Primitive):
    """
    Blocks callers until ``parties`` of them are waiting, then releases them
    all, like ``threading.Barrier``. The barrier resets after each release,
    so it can be reused.

    .. code-block:: python

        barrier = db.barrier('start', 3)

        # Each of three workers:
        barrier.wait()
        run_job()
    """
    prefix = 'barrier'

    def __init__(self, database, name, parties):
        """
        :param database: A walrus ``Database`` instance.
        :param str name: The name for the barrier.
        :param int parties: Number of callers to wait for.
        """
        super(Barrier, self).__init__(database, name)
        self.parties = parties

    def _signal(self, gen):
        # Consecutive generations alternate between two signal lists.
        return '%s:%s' % (self.event, gen % 2)

    def _script(self, name, gen=0):
        return self.database.run_script(
            'barrier_%s' % name,
            keys=[self.key, self._signal(0), self._signal(1)],
            args=[self.parties, gen])

    def wait(self, timeout=None):
        """
        Block until ``parties`` callers are waiting.

        :param float timeout: Seconds to wait. By default, wait forever. On
            timeout the caller withdraws, so it does not count toward the
            next release.
        :returns: ``True`` once all parties arrive, ``False`` on timeout.
        """
        gen, released = self._script('arrive')
        if released:
            return True

        def done():
            return int(self.database.hget(self.key, 'gen') or 0) > gen

        if self._block(self._signal(gen), timeout, done):
            return True
        return self._script('leave', gen) == 0

    def clear(self):
        """
        Reset the barrier. Do not use this method while callers are waiting.
        """
        self.database.delete(self.key, self._signal(0), self._signal(1))
