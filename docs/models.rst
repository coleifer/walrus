.. _models:

.. py:module:: walrus

Models
======

Walrus provides a lightweight :py:class:`Model` class for storing structured data and executing queries using secondary indexes.

.. code-block:: pycon

    >>> from walrus import *
    >>> db = Database()

Let's create a simple data model to store some users.

.. code-block:: pycon

    >>> class User(Model):
    ...     database = db
    ...     name = TextField(primary_key=True)
    ...     dob = DateField(index=True)

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
    ...     print user.name
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
    ...     print user.name
    Charlie
    Huey
    Mickey
    Zaizee

    >>> for user in User.query(order_by=User.dob.desc()):
    ...     print user.dob
    2012-04-01
    2011-06-01
    2007-08-01
    1983-01-01

Filtering records
-----------------

Walrus supports basic filtering. The filtering options available vary by field type, so that :py:class:`TextField`, :py:class:`UUIDField` and similar non-scalar types support only equality and inequality tests. Scalar values, on the other hand, like integers, floats or dates, support range operations.

Let's see how this works by filtering on name and dob. The :py:meth:`~Model.query` method returns zero or more objects, while the :py:meth:`~Model.get` method requires that there be exactly one result:

.. code-block:: pycon

    >>> for user in User.query(User.dob <= datetime.date(2009, 1, 1)):
    ...     print user.dob
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
    ...     print user.dob

    2011-06-01
    2007-08-01

    >>> query = User.query(
    ...     (User.dob <= low) |
    ...     (User.dob >= high))

    >>> for user in query:
    ...     print user.dob
    2012-04-01
    1983-01-01

You can combine filters with ordering:

.. code-block:: pycon

    >>> expr = (User.name == 'Charlie') | (User.name == 'Zaizee')
    >>> for user in User.query(expr, order_by=User.name):
    ...     print user.name
    Charlie
    Zaizee

    >>> for user in User.query(User.name != 'Charlie', order_by=User.name.desc()):
    ...     print user.name
    Zaizee
    Mickey
    Huey

Need more power?
----------------

walrus' querying capabilities are extremely basic. If you want more sophisticated querying, check out `StdNet <https://github.com/lsbardel/python-stdnet>`_. StdNet makes extensive use of Lua scripts to provide some really neat querying/filtering options.
