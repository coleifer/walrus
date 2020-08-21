.. walrus documentation master file, created by
   sphinx-quickstart on Sun Jan  4 00:39:19 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

walrus
======

.. image:: http://media.charlesleifer.com/blog/photos/walrus-logo-0.png

.. py:module:: walrus

Lightweight Python utilities for working with `Redis <http://redis.io>`_.

The purpose of `walrus <https://github.com/coleifer/walrus>`_ is to make
working with Redis in Python a little easier. Rather than ask you to learn a
new library, walrus subclasses and extends the popular ``redis-py`` client,
allowing it to be used as a drop-in replacement. In addition to all the
features in ``redis-py``, walrus adds support for some newer commands,
including full support for streams and consumer groups.

walrus consists of:

* pythonic container classes for the Redis data-types.
* support for stream APIs, plus regular and blocking ``zpop`` variants.
* autocomplete
* bloom filter
* cache
* full-text search
* graph store
* rate limiting
* locks
* **experimental** active-record models (secondary indexes, full-text search, composable query filters, etc)
* more? more!

My hope is that walrus saves you time developing your application by providing
useful Redis-specific components. If you have an idea for a new feature, please
don't hesitate to `tell me about it <https://github.com/coleifer/walrus/issues/new>`_.

Table of contents
-----------------

Contents:

.. toctree::
   :maxdepth: 2
   :glob:

   installation
   getting-started
   containers
   autocomplete
   cache
   full-text-search
   graph
   rate-limit
   streams
   models
   api
   alt-backends
   contributing


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

