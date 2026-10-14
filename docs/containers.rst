.. _containers:

.. currentmodule:: walrus

Containers
==========

The examples below assume ``db = Database(decode_responses=True)``, so values
are returned as ``str`` rather than ``bytes``.

At the most basic level, Redis acts like an in-memory Python dictionary:

.. code-block:: pycon

    >>> db['walrus'] = 'tusk'
    >>> print(db['walrus'])
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
* :py:class:`HyperLogLog`: probabilistic data-structure for cardinality estimation.
* :py:class:`Array`: like a Python list (custom data type implemented on top of ``Hash`` using lua scripts).
* :py:class:`BitField`: a bitmap that supports random access.
* :py:class:`BloomFilter`: probabilistic data-structure for testing set membership.
* For stream types (:py:class:`Stream` and :py:class:`ConsumerGroup`) see
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

    >>> print(h['name'])
    Charlie

    >>> for key, value in h:
    ...     print(key, '=>', value)
    name => Charlie
    favorite_cat => Huey

    >>> del h['favorite_cat']
    >>> h['age'] = 31
    >>> print(h)
    <Hash "charlie": {'name': 'Charlie', 'age': '31'}>

    >>> 'name' in h
    True
    >>> len(h)
    2

Fields can also expire individually. This requires Redis 7.4 or newer.

.. code-block:: pycon

    >>> h.expire_fields(60, 'age')
    [1]
    >>> h.ttl_fields('name', 'age')
    [-1, 60]

Lists
-----

The :py:class:`List` acts like a Python ``list``.

.. code-block:: pycon

    >>> l = db.List('names')
    >>> l.extend(['charlie', 'huey', 'mickey', 'zaizee'])
    4
    >>> print(l[:2])
    ['charlie', 'huey']
    >>> print(l[-2:])
    ['mickey', 'zaizee']
    >>> l.pop()
    'zaizee'
    >>> l.prepend('scout')
    4
    >>> len(l)
    4

Sets
----

The :py:class:`Set` acts like a Python ``set``.

.. code-block:: pycon

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
    >>> z1.add({'charlie': 31, 'huey': 3, 'mickey': 6, 'zaizee': 2.5})
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

.. code-block:: pycon

    >>> hl = db.HyperLogLog('hl')
    >>> hl.add(*range(100))
    1
    >>> len(hl)
    100
    >>> hl.add(*range(1, 100, 2))
    0
    >>> hl.add(*range(1, 100, 3))
    0
    >>> len(hl)
    100

Arrays
------

The :py:class:`Array` type is implemented using `lua scripts <https://github.com/andymccurdy/redis-py#lua-scripting>`_. Unlike :py:class:`List` which is implemented as a linked-list, the ``Array`` is built on top of a Redis hash and has better run-times for certain operations (indexing, for instance). Like :py:class:`List`, :py:class:`Array` acts like a Python ``list``.

.. code-block:: pycon

    >>> a = db.Array('arr')
    >>> a.extend(['foo', 'bar', 'baz', 'nugget'])
    >>> a[-1] = 'nize'
    >>> list(a)
    ['foo', 'bar', 'baz', 'nize']
    >>> a.pop(2)
    'baz'

BitField
--------

The :py:class:`BitField` type acts as a bitmap that supports random access
read, write and increment operations. Operations use a format string (e.g. "u8"
for unsigned 8bit integer 0-255, "i4" for signed integer -8-7).

.. code-block:: pycon

    >>> bf = db.bit_field('bf')
    >>> (bf
    ...  .set('u8', 8, 255)
    ...  .get('u8', 0)  # 00000000
    ...  .get('u4', 8)  # 1111
    ...  .get('u4', 12)  # 1111
    ...  .get('u4', 13)  # 111? -> 1110
    ...  .execute())
    ...
    [0, 0, 15, 15, 14]

    >>> resp = (bf
    ...         .set('u8', 4, 1)  # 00ff -> 001f (returns old val, 0x0f).
    ...         .get('u16', 0)  # 001f (00011111)
    ...         .set('u16', 0, 0))  # 001f -> 0000
    ...
    >>> for item in resp:  # bitfield responses are iterable!
    ...     print(item)
    ...
    15
    31
    31

    >>> resp = (bf
    ...         .incrby('u8', 8, 254)  # 0000 0000 1111 1110
    ...         .get('u16', 0)
    ...         .incrby('u8', 8, 2, 'FAIL')  # increment 254 -> 256? overflow!
    ...         .incrby('u8', 8, 1)  # increment 254 -> 255. success!
    ...         .incrby('u8', 8, 1)  # 255->256? overflow, will fail.
    ...         .get('u16', 0))
    ...
    >>> resp.execute()
    [254, 254, None, 255, None, 255]

:py:class:`BitField` also supports slice notation, using bit-offsets. The
return values are always unsigned integers:

.. code-block:: pycon

    >>> bf = db.bit_field('bits')
    >>> bf.set('u8', 0, 166).execute()  # 10100110
    [0]

    >>> bf[:8]  # Read first 8 bits as unsigned byte.
    166

    >>> bf[:4]  # 1010
    10
    >>> bf[4:8]  # 0110
    6
    >>> bf[2:6]  # 1001
    9
    >>> bf[6:10]  # 10?? -> 1000
    8
    >>> bf[8:16]  # ???????? -> 00000000
    0

    >>> bf[:8] = 89  # 01011001
    >>> bf[:8]
    89

    >>> bf[:8] = 255  # 1111 1111
    >>> bf[:4]  # 1111
    15
    >>> del bf[2:6]  # 1111 1111 -> 1100 0011
    >>> bf[:8]  # 1100 0011
    195

Bitmaps can be combined with :py:meth:`~BitField.bitop`, which stores the
result in a new key, or in place with ``&=``, ``|=`` and ``^=``:

.. code-block:: pycon

    >>> a = db.bit_field('a')
    >>> b = db.bit_field('b')
    >>> a[:8] = 0b1100
    >>> b[:8] = 0b1010
    >>> both = a.bitop('and', 'both', b)
    >>> both[:8]  # 0000 1000
    8
    >>> a ^= b
    >>> a[:8]  # 0000 0110
    6

BloomFilter
-----------

A :py:class:`BloomFilter` is a probabilistic data-structure used for answering
the question: "is X a member of set S?" The bloom-filter may return a false
positive, but it is impossible to receive a false negative (in other words, if
the bloom-filter contains a value, it will never erroneously report that it
does *not* contain such a value). The likelihood of a false positive can be
reduced by increasing the size of the bloom-filter buffer. The default size is
64KB (or 524,288 bits).

.. code-block:: pycon

    >>> bf = db.bloom_filter('bloom')  # Create a bloom-filter in key "bloom".

    >>> data = ('foo', 'bar', 'baz', 'nugget', 'this is a test', 'testing')
    >>> for item in data:
    ...     bf.add(item)  # Add the above items to the bloom-filter.
    ...

    >>> for item in data:
    ...     assert item in bf  # Verify that all items are present.
    ...

    >>> for item in data:
    ...     assert item.upper() not in bf  # FOO, BAR, etc, are *not* present.
    ...     assert item.title() not in bf  # Foo, Bar, etc, are *not* present.
    ...

:py:class:`BloomFilter` implements two methods:

* :py:meth:`~BloomFilter.add` - to add an item to the bloom-filter.
* :py:meth:`~BloomFilter.contains` - test whether an item exists in the filter
  (also available as ``item in bf``).

.. note::
    Items cannot be removed from a bloom-filter.

.. warning::
    Once a :py:class:`BloomFilter` has been created and items have been added,
    you must not modify the size of the buffer.
