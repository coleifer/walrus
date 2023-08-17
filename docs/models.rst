.. _models:

.. py:module:: walrus

Models
======

.. warning::
    Walrus models should **not** be considered production-grade code and I
    strongly advise against anyone actually using it for anything other than
    experimenting or for inspiration/learning.

    My advice: just use hashes for your structured data. If you need ad-hoc
    queries, then use a relational database.

Walrus provides a lightweight :py:class:`Model` class for storing structured data and executing queries using secondary indexes.

.. code-block:: pycon

    >>> from walrus import *
    >>> db = Database()

Let's create a simple data model to store some users.

.. code-block:: pycon

    >>> class User(Model):
    ...     __database__ = db
    ...     name = TextField(primary_key=True)
    ...     dob = DateField(index=True)

.. note::
    As of 0.4.0, the ``Model.database`` attribute has been renamed to
    ``Model.__database__``. Similarly, ``Model.namespace`` is now
    ``Model.__namespace__``.

Creating, Updating and Deleting
-------------------------------

To add objects to a collection, you can use :py:meth:`Model.create`:

.. code-block:: pycon

    >>> User.create(name='Charlie', dob=datetime.date(1983, 1, 1))
    <User: Charlie>

    >>> names_dobs = [
    ...     ('Huey', datetime.date(2011, 6, 1)),
    ...     ('Zaizee', datetime.date(2012, 5, 1)),
    ...     ('Mickey', datetime.date(2007, 8, 1)),

    >>> for name, dob in names_dobs:
    ...     User.create(name=name, dob=dob)

We can retrieve objects by primary key (name in this case). Objects can be modified or deleted after they have been created.

.. code-block:: pycon

    >>> zaizee = User.load('Zaizee')  # Get object by primary key.
    >>> zaizee.name
    'Zaizee'
    >>> zaizee.dob
    datetime.date(2012, 5, 1)

    >>> zaizee.dob = datetime.date(2012, 4, 1)
    >>> zaizee.save()

    >>> nobody = User.create(name='nobody', dob=datetime.date(1990, 1, 1))
    >>> nobody.delete()

Retrieving all records in a collection
--------------------------------------

We can retrieve all objects in the collection by calling :py:meth:`Model.all`, which returns an iterator that successively yields model instances:

.. code-block:: pycon

    >>> for user in User.all():
    ...     print(user.name)
    Huey
    Zaizee
    Charlie
    Mickey

.. note:: The objects from :py:meth:`~Model.all` are returned in an undefined order. This is because the index containing all primary keys is implemented as an unordered :py:class:`Set`.

Sorting records
---------------

To get the objects in order, we can use :py:meth:`Model.query`:

.. code-block:: pycon

    >>> for user in User.query(order_by=User.name):
    ...     print(user.name)
    Charlie
    Huey
    Mickey
    Zaizee

    >>> for user in User.query(order_by=User.dob.desc()):
    ...     print(user.dob)
    2012-04-01
    2011-06-01
    2007-08-01
    1983-01-01

Filtering records
-----------------

Walrus supports basic filtering. The filtering options available vary by field type, so that :py:class:`TextField`, :py:class:`UUIDField` and similar non-scalar types support only equality and inequality tests. Scalar values, on the other hand, like integers, floats or dates, support range operations.

.. warning:: You must specify ``index=True`` to be able to use a field for filtering.

Let's see how this works by filtering on name and dob. The :py:meth:`~Model.query` method returns zero or more objects, while the :py:meth:`~Model.get` method requires that there be exactly one result:

.. code-block:: pycon

    >>> for user in User.query(User.dob <= datetime.date(2009, 1, 1)):
    ...     print(user.dob)
    2007-08-01
    1983-01-01

    >>> charlie = User.get(User.name == 'Charlie')
    >>> User.get(User.name = 'missing')
    Traceback (most recent call last):
      File "<stdin>", line 1, in <module>
      File "/home/charles/pypath/walrus.py", line 1662, in get
        raise ValueError('Got %s results, expected 1.' % len(result))
    ValueError: Got 0 results, expected 1.

We can combine multiple filters using bitwise *and* and *or*:

.. code-block:: pycon

    >>> low = datetime.date(2006, 1, 1)
    >>> high = datetime.date(2012, 1, 1)
    >>> query = User.query(
    ...     (User.dob >= low) &
    ...     (User.dob <= high))

    >>> for user in query:
    ...     print(user.dob)

    2011-06-01
    2007-08-01

    >>> query = User.query(User.dob.between(low, high))  # Equivalent to above.
    >>> for user in query:
    ...     print(user.dob)

    2011-06-01
    2007-08-01

    >>> query = User.query(
    ...     (User.dob <= low) |
    ...     (User.dob >= high))

    >>> for user in query:
    ...     print(user.dob)
    2012-04-01
    1983-01-01

You can combine filters with ordering:

.. code-block:: pycon

    >>> expr = (User.name == 'Charlie') | (User.name == 'Zaizee')
    >>> for user in User.query(expr, order_by=User.name):
    ...     print(user.name)
    Charlie
    Zaizee

    >>> for user in User.query(User.name != 'Charlie', order_by=User.name.desc()):
    ...     print(user.name)
    Zaizee
    Mickey
    Huey

.. _container-fields:

Container Fields
----------------

Up until now the fields we've used have been simple key/value pairs that are stored directly in the hash of model data. In this section we'll look at a group of special fields that correspond to Redis container types.

Let's create a model for storing personal notes. The notes will have a text field for the content and a timestamp, and as an interesting flourish we'll add a :py:class:`SetField` to store a collection of tags.

.. code-block:: python

    class Note(Model):
        __database__ = db
        text = TextField()
        timestamp = DateTimeField(
            default=datetime.datetime.now,
            index=True)
        tags = SetField()

.. note:: Container fields cannot be used as a secondary index, nor can they be used as the primary key for a model. Finally, they do not accept a default value.

.. warning:: Due to the implementation, it is necessary that the model instance have a primary key value before you can access the container field. This is because the key identifying the container field needs to be associated with the instance, and the way we do that is with the primary key.

Here is how we might use the new note model:

.. code-block:: pycon

    >>> note = Note.create(content='my first note')
    >>> note.tags
    <Set "note:container.tags.note:id.3": 0 items>
    >>> note.tags.add('testing', 'walrus')

    >>> Note.load(note._id).tags
    <Set "note:container.tags.note:id.3": 0 items>

In addition to :py:class:`SetField`, there is also :py:class:`HashField`, :py:class:`ListField`, :py:class:`ZSetField`.


.. _fts:

Full-text search
----------------

I've added a really (really) simple full-text search index type. Here is how to use it:

.. code-block:: pycon

    >>> class Note(Model):
    ...     __database__ = db
    ...     content = TextField(fts=True)  # Note the "fts=True".

When a field contains an full-text index, then the index will be populated when new objects are added to the database:

.. code-block:: pycon

    >>> Note.create(content='this is a test of walrus FTS.')
    >>> Note.create(content='favorite food is walrus-mix.')
    >>> Note.create(content='do not forget to take the walrus for a walk.')

Use :py:meth:`TextField.search` to create a search expression, which is then passed to the :py:meth:`Model.query` method:

.. code-block:: pycon

    >>> for note in Note.query(Note.content.search('walrus')):
    ...     print(note.content)
    do not forget to take the walrus for a walk.
    this is a test of walrus FTS.
    favorite food is walrus-mix.

    >>> for note in Note.query(Note.content.search('walk walrus')):
    ...     print(note.content)
    do not forget to take the walrus for a walk.

    >>> for note in Note.query(Note.content.search('walrus mix')):
    ...     print(note.content)
    favorite food is walrus-mix.

We can also specify complex queries using ``AND`` and ``OR`` conjunctions:

.. code-block:: pycon

    >>> for note in Note.query(Note.content.search('walrus AND (mix OR fts)')):
    ...     print(note.content)
    this is a test of walrus FTS.
    favorite food is walrus-mix.

    >>> query = '(test OR food OR walk) AND walrus AND (favorite OR forget)'
    >>> for note in Note.query(Note.content.search(query)):
    ...     print(note.content)
    do not forget to take the walrus for a walk.
    favorite food is walrus-mix.

Features
^^^^^^^^

* Automatic removal of stop-words
* Porter stemmer on by default
* Optional double-metaphone implementation
* Default conjunction is *AND*, but there is also support for *OR*.

Limitations
^^^^^^^^^^^

* Partial strings are not matched.
* Very naive scoring function.
* Quoted multi-word matches do not work.

Need more power?
----------------

walrus' querying capabilities are extremely basic. If you want more sophisticated querying, check out `StdNet <https://github.com/lsbardel/python-stdnet>`_. StdNet makes extensive use of Lua scripts to provide some really neat querying/filtering options.
