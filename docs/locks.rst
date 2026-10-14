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

Renewing
--------

A long task can outlive the ``ttl``. :py:meth:`~Lock.renew` resets the TTL if
the current thread still holds the lock, and :py:meth:`~Lock.held` checks
without renewing:

.. code-block:: pycon

    >>> lock = db.lock('reports', ttl=10000)
    >>> lock.acquire()
    True
    >>> lock.renew()
    True
    >>> lock.held()
    True

With ``auto_renew=True``, a background thread renews the lock every third of
its TTL until it is released or the thread that acquired it exits. Semaphores
support all three.

Leader Election
---------------

An auto-renewing lock elects a leader among processes. Each one runs:

.. code-block:: python

    lock = db.lock('scheduler', ttl=10000, auto_renew=True)

    with lock:  # Blocks until this process is the leader.
        while lock.held():
            run_scheduled_jobs()

If the leader dies, its lock expires within the TTL and a waiting process
takes over. If the leader stalls past the TTL and loses the lock, ``held()``
returns ``False`` and leaving the ``with`` block raises ``RuntimeError``.

Event
-----

An :py:class:`Event` is a flag shared across processes, like
``threading.Event``. One call to :py:meth:`~Event.set` wakes every waiter:

.. code-block:: pycon

    >>> ready = db.event('ready')
    >>> ready.wait(timeout=0.1)
    False
    >>> ready.set()
    >>> ready.wait()
    True
    >>> ready.clear()
    >>> ready.is_set()
    False

Barrier
-------

A :py:class:`Barrier` blocks callers until ``parties`` of them are waiting,
then releases them all. It resets after each release, so it can be reused:

.. code-block:: python

    barrier = db.barrier('start', 3)

    # In each of three workers:
    if barrier.wait(timeout=30):
        run_job()

On timeout, :py:meth:`~Barrier.wait` returns ``False`` and the caller
withdraws, so it does not count toward the next release.
