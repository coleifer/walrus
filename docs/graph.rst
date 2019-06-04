.. _graph:

.. py:module:: walrus

Graph
=====

The walrus ``graph`` module provides a lightweight `hexastore <http://redis.io/topics/indexes#representing-and-querying-graphs-using-an-hexastore>`_ implementation. The :py:class:`Graph` class uses Redis :py:class:`ZSet` objects to store collections of ``subject - predicate - object`` triples. These relationships can then be queried in a very flexible manner.

.. note:: The hexastore logic is expecting UTF-8 encoded values.  If you are using Python 2.X unicode text, you are responsible for encoding prior to storing/querying with those values.

For example, we might store things like:

* charlie -- friends -- huey
* charlie -- lives -- Kansas
* huey -- lives -- Kansas

We might wish to ask questions of our data-store like "which of charlie's friends live in Kansas?" To do this, we will store every permutation of the S-P-O triples, then we can efficiently query using the parts of the relationship we know beforehand.

* query the "object" portion of the "charlie -- friends" subject/predicate.
* for each object returned, turn it into the subject of a second query whose predicate is "lives" and whose object is "Kansas".

So we would return the subjects that satisfy the following expression::

    ("charlie -- friends") -- lives -- Kansas

Let's go through this simple example to illustrate how the :py:class:`Graph` class works.

.. code-block:: python

    from walrus import Database

    # Begin by instantiating a `Graph` object.
    db = Database()
    graph = db.graph('people')

    # Store my friends.
    # "charlie" is subject, "friends" is predicate, "huey" is object.
    graph.store('charlie', 'friends', 'huey')

    # Can also store multiple relationships at once.
    graph.store_many((
        ('charlie', 'friends', 'zaizee'),
        ('charlie', 'friends', 'nuggie')))

    # Store where people live.
    graph.store_many((
        ('huey', 'lives', 'Kansas'),
        ('zaizee', 'lives', 'Missouri'),
        ('nuggie', 'lives', 'Kansas'),
        ('mickey', 'lives', 'Kansas')))

    # We are now ready to search. We'll use a variable (X) to indicate
    # the value we're interested in.
    X = graph.v.X  # Create a variable placeholder.

    # In the first clause we indicate we are searching for my friends.
    # In the second clause, we only want those friends who also live
    # in Kansas.
    results = graph.search(
        {'s': 'charlie', 'p': 'friends', 'o': X},
        {'s': X, 'p': 'lives', 'o': 'Kansas'})

    print(results)

    # Prints: {'X': {'huey', 'nuggie'}}

In the above example, the result value is a dictionary of variable values that satisfy the search expressions. The :py:meth:`~Graph.search` method is quite powerful!

An even simpler example
^^^^^^^^^^^^^^^^^^^^^^^

Let's say we wish only to retrieve a list of Charlie's friends. In this case we do not need to use a variable. We can use the simpler :py:meth:`~Graph.query` method. This method optionally takes a subject, predicate and/or object and, using the provided data, returns all objects that "match" the given pieces.

So to find Charlie's friends, we would write:

.. code-block:: python

    query = graph.query(s='charlie', p='friends')
    for result in query:
        print(result['o'])  # Print the object for the corresponding S/P.
