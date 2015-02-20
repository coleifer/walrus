.. _autocomplete:

.. py:module:: walrus

Autocomplete
============

Provide suggestions based on partial string search. Walrus' autocomplete library is based on the implementation from `redis-completion <https://github.com/coleifer/redis-completion>`_.

.. note:: The walrus implementation of autocomplete relies on the ``HSCAN`` command and therefore requires Redis >= 2.8.0.

Overview
--------

The :py:class:`Autocomplete` engine works by storing substrings and mapping them to user-defined data.

Features

* Perform searches using partial words or phrases.
* Store rich metadata along with substrings.
* Boosting.

Simple example
--------------

Walrus :py:class:`Autocomplete` can be used to index words and phrases, and then make suggestions based on user searches.

To begin, call :py:meth:`Database.autocomplete` to create an instance of the autocomplete index.

.. code-block:: pycon

    >>> database = Database()
    >>> ac = database.autocomplete()

Phrases can be stored by calling :py:meth:`Autocomplete.store`:

.. code-block:: pycon

    >>> phrases = [
    ...     'the walrus and the carpenter',
    ...     'walrus tusks',
    ...     'the eye of the walrus']

    >>> for phrase in phrases:
    ...     ac.store(phrase)

To search for results, use :py:meth:`Autocomplete.search`.

.. code-block:: pycon

    >>> ac.search('wal')
    ['the walrus and the carpenter',
     'walrus tusks',
     'the eye of the walrus']

    >>> ac.search('wal car')
    ['the walrus and the carpenter']

To boost a result, we can specify one or more *boosts* when searching:

.. code-block:: pycon

    >>> ac.search('wal', boosts={'walrus tusks': 2})
    ['walrus tusks',
     'the walrus and the carpenter',
     'the eye of the walrus']

To remove a phrase from the index, use :py:meth:`Autocomplete.remove`:

.. code-block:: pycon

    >>> ac.remove('walrus tusks')

We can also check for the existence of a phrase in the index using :py:meth:`Autocomplete.exists`:

.. code-block:: pycon

    >>> ac.exists('the walrus and the carpenter')
    True

    >>> ac.exists('walrus tusks')
    False

Complete example
----------------

While walrus can work with just simple words and phrases, the :py:class:`Autocomplete` index was really developed to be able to provide meaningful typeahead suggestions for sites containing rich content. To this end, the autocomplete search allows you to store arbitrary metadata in the index, which will then be returned when a search is performed.

.. code-block:: pycon

    >>> database = Database()
    >>> ac = database.autocomplete()

Suppose we have a blog site and wish to add search for the entries. We'll use the blog entry's title for the search, and return, along with title, a thumbnail image and a link to the entry's detail page. That way when we display results we have all the information we need to display a nice-looking link:

.. code-block:: pycon

    >>> for blog_entry in Entry.select():
    ...     metadata = {
    ...         'image': blog_entry.get_primary_thumbnail(),
    ...         'title': blog_entry.title,
    ...         'url': url_for('entry_detail', entry_id=blog_entry.id)}
    ...
    ...     ac.store(
    ...         obj_id=blog_entry.id,
    ...         title=blog_entry.title,
    ...         data=metadata,
    ...         obj_type='entry')

When we search we receive the metadata that was stored in the index:

.. code-block:: pycon

    >>> ac.search('walrus')
    [{'image': '/images/walrus-logo.jpg',
      'title': 'Walrus: Lightweight Python utilities for working with Redis',
      'url': '/blog/walrus-lightweight-python-utilities-for-working-with-redis/'},
     {'image': '/images/walrus-tusk.jpg',
      'title': 'Building Autocomplete with Walrus',
      'url': '/blog/building-autocomplete-with-redis/'}]

Whenever an entry is created or updated, we will want to update the index. By keying off the entry's primary key and object type (*'entry'*), walrus will handle this correctly:

.. code-block:: python

    def save_entry(entry):
        entry.save_to_db()  # Save entry to relational database, etc.

        ac.store(
            obj_id=entry.id,
            title=entry.title,
            data={
                'image': entry.get_primary_thumbnail(),
                'title': entry.title,
                'url': url_for('entry_detail', entry_id=entry.id)},
            obj_type='entry')

Suppose we have a very popular blog entry that is frequently searched for. We can *boost* that entry's score by calling :py:meth:`~Autocomplete.boost_object`:

.. code-block:: pycon

    >>> popular_entry = Entry.get(Entry.title == 'Some popular entry')
    >>> ac.boost_object(
    ...     obj_id=popular_entry.id,
    ...     obj_type='entry',
    ...     multiplier=2.0)

To perform boosts on a one-off basis while searching, we can specify a dictionary mapping object IDs or types to a particular multiplier:

.. code-block:: pycon

    >>> ac.search(
    ...     'some phrase',
    ...     boosts={popular_entry.id: 2.0, unpopular_entry.id, 0.5})
    ...
    [ list of matching entry's metadata ]

To remove an entry from the index, we just need to specify the object's id and type:

.. code-block:: python

    def delete_entry(entry):
        entry.delete_from_db()  # Remove from relational database, etc.

        ac.remove(
            obj_id=entry.id,
            obj_type='entry')

We can also check whether an entry exists in the index:

.. code-block:: pycon

    >>> entry = Entry.get(Entry.title == 'Building Autocomplete with Walrus')
    >>> ac.exists(entry.id, 'entry')
    True

Scoring
-------

Walrus implements a scoring algorithm that considers the words and also their position relative to the entire phrase. Let's look at some simple searches. We'll index the following strings:

* ``"aa bb"``
* ``"aa cc"``
* ``"bb cc"``
* ``"bb aa cc"``
* ``"cc aa bb"``

.. code-block:: pycon

    >>> phrases = ['aa bb', 'aa cc', 'bb cc', 'bb aa cc', 'cc aa bb']
    >>> for phrase in phrases:
    ...     ac.store(phrase)

Note how when we search for *aa* that the results with *aa* towards the front of the string score higher:

.. code-block:: pycon

    >>> ac.search('aa')
    ['aa bb',
     'aa cc',
     'bb aa cc',
     'cc aa bb']

This is even more clear when we search for *bb* and *cc*:

.. code-block:: pycon

    >>> ac.search('bb')
    ['bb aa cc',
     'bb cc',
     'aa bb',
     'cc aa bb']

    >>> ac.search('cc')
    ['cc aa bb',
     'aa cc',
     'bb cc',
     'bb aa cc']

As you can see, results are scored by the proximity of the match to the front of the string, then alphabetically.

Boosting
^^^^^^^^

To modify the score of certain words or phrases, we can apply *boosts* when searching. Boosts consist of a dictionary mapping identifiers to multipliers. Multipliers greater than 1 will move results to the top, while multipliers between 0 and 1 will push results to the bottom.

In this example, we'll take the 3rd result, *bb cc* and bring it to the top:

.. code-block:: pycon

    >>> ac.search('cc', boosts={'bb cc': 2})
    ['bb cc',
     'cc aa bb',
     'aa cc',
     'bb aa cc']

In this example, we'll take the best result, *cc aa bb*, and push it back a spot:

.. code-block:: pycon

    >>> ac.search('cc', boosts={'cc aa bb': .75})
    ['aa cc',
     'cc aa bb',
     'bb cc',
     'bb aa cc']

Persisting boosts
^^^^^^^^^^^^^^^^^

While boosts can be specified on a one-off basis while searching, we can also permanently store boosts that will be applied to *all* searches. To store a boost for a particular object or object type, call the :py:meth:`~Autocomplete.boost_object` method:

.. code-block:: pycon

    >>> ac.boost_object(obj_id='bb cc', multiplier=2.0)
    >>> ac.boost_object(obj_id='cc aa bb', multiplier=.75)

Now we can search and our boosts will automatically be in effect:

.. code-block:: pycon

    >>> ac.search('cc')
    ['bb cc',
     'aa cc',
     'cc aa bb',
     'bb aa cc']

ZRANGEBYLEX
-----------

Because I wanted to implement a slightly more complex scoring algorithm, I chose not to use the ``ZRANGEBYLEX`` command while implementing autocomplete. For very simple use-cases, though, ``ZRANGEBYLEX`` will certainly offer better performance. Depending on your application's needs, you may be able to get by just storing your words in a sorted set and calling ``ZRANGEBYLEX`` on that set.
