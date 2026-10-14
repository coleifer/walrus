.. _locks:

.. currentmodule:: walrus

Locks
=====

A :py:class:`Lock` is held by one thread at a time, across processes and
machines. Waiters block on a Redis list rather than polling, so they wake as
soon as the lock is released.

.. code-block:: python

    lock = db.lock('reports', ttl=10000)

    with lock:
        build_report()

    @lock
    def nightly_job():
        build_report()

The ``ttl`` is in milliseconds. Without one, a lock held by a crashed process
is never released. With one, the lock expires and a waiter can take it.

:py:meth:`~Lock.acquire` blocks by default. With ``block=False`` it returns
``False`` at once if the lock is taken:

.. code-block:: pycon

    >>> lock = db.lock('reports')
    >>> lock.acquire(block=False)
    True
    >>> db.lock('reports').acquire(block=False)
    False
    >>> lock.release()
    True

.. note::
    Each lock keeps an event list in Redis until :py:meth:`~Lock.clear`
    removes it, so take care when creating locks dynamically.

Semaphore
---------

A :py:class:`Semaphore` is a lock that up to ``value`` holders can hold at
once. A thread may hold several slots:

.. code-block:: pycon

    >>> sem = db.semaphore('crawlers', 2)
    >>> sem.acquire(), sem.acquire()
    (True, True)
    >>> sem.acquire(block=False)
    False

The ``ttl`` applies to each holder, so the slot of a crashed holder frees
itself once the TTL passes:

.. code-block:: python

    sem = db.semaphore('crawlers', 5, ttl=60000)

    with sem:
        crawl(url)
