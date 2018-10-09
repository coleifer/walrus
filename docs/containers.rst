.. _containers:

.. py:module:: walrus

Containers
==========

At the most basic level, Redis acts like an in-memory Python dictionary:

.. code-block:: pycon

    >>> db['walrus'] = 'tusk'
    >>> print db['walrus']
    tusk

    >>> db['not-here']
    Traceback (most recent call last):
      File "<stdin>", line 1, in <module>
      File "/home/charles/pypath/redis/client.py", line 817, in __getitem__
        raise KeyError(name)
    KeyError: 'not-here'

    >>> db.get('not-here') is None
    True

Redis also supports several primitive data-types:

* :py:class:`Hash`: dictionary
* :py:class:`List`: linked list
* :py:class:`Set`
* :py:class:`ZSet`: a sorted set
* :py:class:`HyperLogLog`: probability data-structure
* :py:class:`Array`: like a Python list (custom data type implemented on top of ``Hash`` using lua scripts).
* For stream types (:py:class:`Stream`: and :py:class:`ConsumerGroup`) see
  the :ref:`streams` documentation.

Let's see how to use these types.

Hashes
------

The :py:class:`Hash` acts like a Python ``dict``.

.. code-block:: pycon

    >>> h = db.Hash('charlie')
    >>> h.update(name='Charlie', favorite_cat='Huey')
    <Hash "charlie": {'name': 'Charlie', 'favorite_cat': 'Huey'}>

We can use common Python interfaces like iteration, len, contains, etc.

.. code-block:: pycon

    >>> print h['name']
    Charlie

    >>> for key, value in h:
    ...     print key, '=>', value
    name => Charlie
    favorite_cat => Huey

    >>> del h['favorite_cat']
    >>> h['age'] = 31
    >>> print h
    <Hash "charlie": {'age': '31', 'name': 'Charlie'}>

    >>> 'name' in h
    True
    >>> len(h)
    2

Lists
-----

The :py:class:`List` acts like a Python ``list``.

.. code-block:: pycon

    >>> l = db.List('names')
    >>> l.extend(['charlie', 'huey', 'mickey', 'zaizee'])
    4L
    >>> print l[:2]
    ['charlie', 'huey']
    >>> print l[-2:]
    ['mickey', 'zaizee']
    >>> l.pop()
    'zaizee'
    >>> l.prepend('scout')
    4L
    >>> len(l)
    4

Sets
----

The :py:class:`Set` acts like a Python ``set``.

.. code-block:: python

    >>> s1 = db.Set('s1')
    >>> s2 = db.Set('s2')
    >>> s1.add(*range(5))
    5
    >>> s2.add(*range(3, 8))
    5

    >>> s1 | s2
    {'0', '1', '2', '3', '4', '5', '6', '7'}
    >>> s1 & s2
    {'3', '4'}
    >>> s1 - s2
    {'0', '1', '2'}

    >>> s1 -= s2
    >>> s1.members()
    {'0', '1', '2'}

    >>> len(s1)
    3

Sorted Sets (ZSet)
------------------

The :py:class:`ZSet` acts a bit like a sorted dictionary, where the values are the scores used for sorting the keys.

.. code-block:: pycon

    >>> z1 = db.ZSet('z1')
    >>> z1.add('charlie', 31, 'huey', 3, 'mickey', 6, 'zaizee', 2.5)
    4
    >>> z1['huey'] = 3.5

Sorted sets provide a number of complex slicing and indexing options when retrieving values. You can slice by key or rank, and optionally include scores in the return value.

.. code-block:: pycon

    >>> z1[:'mickey']  # Who is younger than Mickey?
    ['zaizee', 'huey']

    >>> z1[-2:]  # Who are the two oldest people?
    ['mickey', 'charlie']

    >>> z1[-2:, True]  # Who are the two oldest, and what are their ages?
    [('mickey', 6.0), ('charlie', 31.0)]

There are quite a few methods for working with sorted sets, so if you're curious then check out the :py:class:`ZSet` API documentation.

HyperLogLog
-----------

The :py:class:`HyperLogLog` provides an estimation of the number of distinct elements in a collection.

.. code-block:: python

    >>> hl = db.HyperLogLog('hl')
    >>> hl.add(*range(100))
    >>> len(hl)
    100
    >>> hl.add(*range(1, 100, 2))
    >>> hl.add(*range(1, 100, 3))
    >>> len(hl)
    102

Arrays
------

The final object type is an :py:class:`Array` implemented using `lua scripts <https://github.com/andymccurdy/redis-py#lua-scripting>`_. Unlike :py:class:`List` which is implemented as a linked-list, the ``Array`` is built on top of a Redis hash and has better run-times for certain operations (indexing, for instance). Like :py:class:`List`, :py:class:`Array` acts like a Python ``list``.

.. code-block:: pycon

    >>> a = db.Array('arr')
    >>> a.extend(['foo', 'bar', 'baz', 'nugget'])
    >>> a[-1] = 'nize'
    >>> list(a)
    ['foo', 'bar', 'baz', 'nize']
    >>> a.pop(2)
    'baz'
