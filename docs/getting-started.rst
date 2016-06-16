.. _getting-started:

.. py:module:: walrus

Getting Started
===============

The purpose of `walrus <https://github.com/coleifer/walrus>`_ is to make working with Redis in Python a little easier by wrapping rich objects in Pythonic containers.

Let's see how this works by using ``walrus`` in the Python interactive shell. Make sure you have `redis <http://redis.io>`_ installed and running locally.

Introducing walrus
------------------

To begin using walrus, we'll start by importing it and creating a :py:class:`Database` instance. The ``Database`` object is a thin wrapper over the `redis-py <https://redis-py.readthedocs.io/>`_ ``Redis`` class, so any methods available on ``Redis`` will also be available on the walrus ``Database`` object.

.. code-block:: pycon

    >>> from walrus import *
    >>> db = Database(host='localhost', port=6379, db=0)

If you like fun names, you can also use ``Walrus`` instead:

.. code-block:: pycon

    >>> from walrus import *
    >>> db = Walrus(host='localhost', port=6379, db=0)
