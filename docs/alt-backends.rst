.. _alt-backends:

Alternative Backends ("tusks")
==============================

In addition to `Redis <http://redis.io>`_, I've been experimenting with adding support for alternative *redis-like* backends. These alternative backends are referred to as *tusks*, and currently Walrus supports the following:

* `RLite <https://github.com/seppo0010/rlite>`_, a self-contained and serverless Redis-compatible database engine. Use ``rlite`` if you want all the features of Redis, without the separate server process..
* `Vedis <http://vedis.symisc.net/index.html>`_, an embeddable data-store written in C with over 70 commands similar in concept to Redis. Vedis is built on a fast key/value store and supports writing custom commands in Python. Use ``vedis`` if you are OK working with a smaller subset of commands out-of-the-box or are interested in writing your own commands.
* `ledisdb <https://ledisdb.io/>`_, Redis-like database written in Golang. Supports almost all the Redis commands. Requires `ledis-py <https://github.com/holys/ledis-py>`_.

rlite
-----

`rlite <https://github.com/seppo0010/rlite>`_ is an embedded Redis-compatible database.

According to the project's README,

  rlite is to Redis what SQLite is to Postgres.

The project's features are:

* **Supports virtually every Redis command**.
* Self-contained embedded data-store.
* Serverless / zero-configuration.
* Transactions.
* Databases can be in-memory or stored in a single file on-disk.

Use-cases for ``rlite``:

* **Mobile** environments, where it is not practical to run a database server.
* **Development** or **testing** environments. Database fixtures can be distributed as a simple binary file.
* **Slave of Redis** for additional durability.
* Application file format, alternative to a proprietary format or SQLite.

Python bindings
^^^^^^^^^^^^^^^

`rlite-py <https://github.com/seppo0010/rlite-py>`_ allows ``rlite`` to be embedded in your Python apps. To install ``rlite-py``, you can use ``pip``:

.. code-block:: console

    $ pip install hirlite

Using with Walrus
^^^^^^^^^^^^^^^^^

To use ``rlite`` instead of Redis in your ``walrus`` application, simply use the ``WalrusLite`` in place of the usual :py:class:`Walrus` object:

.. code-block:: python

    from walrus.tusks.rlite import WalrusLite

    walrus = WalrusLite('/path/to/database.db')

``WalrusLite`` can also be used as an in-memory database by omitting a path to a database file when instantiating, or by passing the special string ``':memory:'``:

.. code-block:: python

    from walrus.tusks.rlite import WalrusLite

    walrus_mem_db = WalrusLite(':memory:')

Vedis
-----

`Vedis <http://vedis.symisc.net/>`_ is an embedded Redis-like database with over 70 commands. ``Vedis``, like ``rlite``, does not have a separate server process. And like ``rlite``, ``Vedis`` supports both file-backed databases and transient in-memory databases.

According to the project's README,

    Vedis is a self-contained C library without dependency. It requires very minimal support from external libraries or from the operating system. This makes it well suited for use in embedded devices that lack the support infrastructure of a desktop computer. This also makes Vedis appropriate for use within applications that need to run without modification on a wide variety of computers of varying configurations.

The project's features are:

* Serverless / zero-configuration.
* Transactional (ACID) datastore.
* Databases can be in-memory or stored in a single file on-disk.
* Over 70 commands covering many Redis features.
* Cross-platform file format.
* Includes fast low-level key/value store.
* Thread-safe and fully re-entrant.
* Support for Terabyte-sized databases.
* `Python bindings <https://vedis-python.readthedocs.io/en/latest/>`_ allow you to `write your own Vedis commands in Python <https://vedis-python.readthedocs.io/en/latest/custom_commands.html>`_.

Use-cases for ``Vedis``:

* **Mobile** environments, where it is not practical to run a database server.
* **Development** or **testing** environments. Database fixtures can be distributed as a simple binary file.
* Application file format, alternative to a proprietary format or SQLite.
* Extremely large databases that do not fit in RAM.
* Embedded platforms with limited resources.

.. note::
    Unlike ``rlite``, which supports virtually all the Redis commands, ``Vedis`` supports a more limited subset. Notably lacking are sorted-set operations and many of the list operations. Hashes, Sets and key/value operations are very well supported, though.

.. warning::
    The authors of Vedis have indicated that they are not actively working on new features for Vedis right now.

Python bindings
^^^^^^^^^^^^^^^

`vedis-python <https://github.com/coleifer/vedis-python>`_ allows ``Vedis`` to be embedded in your Python apps. To install ``vedis-python``, you can use ``pip``:

.. code-block:: console

    $ pip install vedis

Using with Walrus
^^^^^^^^^^^^^^^^^

To use ``Vedis`` instead of Redis in your ``walrus`` application, simply use the ``WalrusVedis`` in place of the usual :py:class:`Walrus` object:

.. code-block:: python

    from walrus.tusks.vedisdb import WalrusVedis

    walrus = WalrusVedis('/path/to/database.db')

``WalrusVedis`` can also be used as an in-memory database by omitting a path to a database file when instantiating, or by passing the special string ``':memory:'``:

.. code-block:: python

    from walrus.tusks.vedisdb import WalrusVedis

    walrus_mem_db = WalrusVedis(':memory:')

Writing a custom command
^^^^^^^^^^^^^^^^^^^^^^^^

One of the neat features of ``Vedis`` is the ease with which you can write your own commands. Here are a couple examples:

.. code-block:: python

    from walrus.tusks.vedisdb import WalrusVedis

    db = WalrusVedis()  # Create an in-memory database.

    @db.command('SUNION')  # Vedis supports SDIFF and SINTER, but not SUNION.
    def sunion(context, key1, key2):
        return list(db.smembers(key1) | db.smembers(key2))

    @db.command('KTITLE')  # Access the low-level key/value store via the context.
    def ktitle(context, source, dest_key):
        source_val = context[source]
        if source_val:
            context[dest_key] = source_val.title()
            return True
        return False

We can use these commands like so:

.. code-block:: pycon

    >>> s1 = db.Set('s1')
    >>> s1.add(*range(3))
    3
    >>> s2.add(*range(1, 5))
    4
    >>> db.SUNION('s1', 's2')
    ['1', '0', '3', '2', '4']

    >>> db['user.1.username'] = 'charles'
    >>> db.KTITLE('user.1.username', 'user.1.display_name')
    1
    >>> print(db['user.1.display_name'])
    Charles

Ledis
-----

`ledis <https://ledisdb.io/>`_ is a Redis-like database written in Golang.

The project's features are:

* **Supports virtually every Redis command**.
* Supports multiple backends, including LevelDB, RocksDB, LMDB, BoltDB and in-memory databases.
* Data storage is not limited by RAM, since the databases are disk-based.
* Transactions.
* Supports the Redis protocol for communication, so most Redis clients work with Ledis.
* Written in golang, easy to deploy.

Use-cases for ``ledisdb``:

* Store data-sets that exceed RAM.
* Use with LevelDB, RocksDB, etc.

Python bindings
^^^^^^^^^^^^^^^

`ledis-py <https://github.com/holys/ledis-py>`_ allows you to connect to ``ledisdb``. To install ``ledis-py``, you can use ``pip``:

.. code-block:: console

    $ pip install ledis

Using with Walrus
^^^^^^^^^^^^^^^^^

To use ``ledisdb`` instead of Redis in your ``walrus`` application, simply use the ``WalrusLedis`` in place of the usual :py:class:`Walrus` object:

.. code-block:: python

    from walrus.tusks.ledisdb import WalrusLedis

    walrus = WalrusLedis()
