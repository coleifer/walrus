## Walrus

Lightweight Python utilities for working with Redis.

### Containers

Pythonic container objects for working with the primary Redis data-types:

* ``Hash``
* ``Set``
* ``ZSet`` (sorted set)
* ``List``
* ``Array`` (custom type)

These objects are meant to look and act like their native Python equivalents, except sorted sets, which are kind of their own thing...

### Cache

Simple cache implementation and caching decorator.

### Models

Persistent structures implemented on top of Hashes. Supports indexing to allow filtering with equality conditions.
