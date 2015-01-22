.. walrus documentation master file, created by
   sphinx-quickstart on Sun Jan  4 00:39:19 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

walrus
======

.. py:module:: walrus

.. image:: http://media.charlesleifer.com/blog/photos/walrus-logo.png

Lightweight Python utilities for working with `Redis <http://redis.io>`_.

The purpose of `walrus <https://github.com/coleifer/walrus>`_ is to make working with Redis in Python a little easier by wrapping rich objects in Pythonic containers. walrus consists of:

* Containers for the Redis object types :py:class:`Hash`, :py:class:`List`, :py:class:`Set`, :py:class:`ZSet`, :py:class:`HyperLogLog` as well as a custom :py:class:`Array` type.
* :py:class:`Cache` implementation that exposes several decorators for caching function calls.
* :py:class:`Lock` implementation that can also be used as a context manager or decorator.
* :py:class:`Model` layer that support persisting structured information and performing queries using secondary indexes.

Contents:

.. toctree::
   :maxdepth: 2
   :glob:

   installation
   getting-started
   containers
   autocomplete
   cache
   models
   api
   contributing


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

