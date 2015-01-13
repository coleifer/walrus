## Walrus

![](http://media.charlesleifer.com/blog/photos/walrus-logo.png)

Lightweight Python utilities for working with [Redis](http://redis.io).

The purpose of [walrus](https://github.com/coleifer/walrus) is to make working with Redis in Python a little easier by wrapping rich objects in Pythonic containers. walrus consists of:

* Wrappers for the Redis object types:
    * [Hash](https://walrus.readthedocs.org/en/latest/containers.html#hashes)
    * [List](https://walrus.readthedocs.org/en/latest/containers.html#lists)
    * [Set](https://walrus.readthedocs.org/en/latest/containers.html#sets)
    * [Sorted Set](https://walrus.readthedocs.org/en/latest/containers.html#sorted-sets-zset)
    * [HyperLogLog](https://walrus.readthedocs.org/en/latest/containers.html#hyperloglog)
    * [Array](https://walrus.readthedocs.org/en/latest/containers.html#arrays) (custom type)
* A simple [Cache](https://walrus.readthedocs.org/en/latest/cache.html) implementation that exposes several decorators for caching function and method calls.
* Lightweight data [Model](https://walrus.readthedocs.org/en/latest/models.html) objects that support persisting structured information and performing complex queries using secondary indexes.

### Models

Persistent structures implemented on top of Hashes. Supports secondary indexes to allow filtering on equality, inequality, ranges, less/greater-than, and a basic full-text search index. The full-text search features a boolean search query parser, porter stemmer, stop-word filtering, and optional double-metaphone implementation.

### Found a bug?

![](http://media.charlesleifer.com/blog/photos/p1420743625.21.png)

Please open a [github issue](https://github.com/coleifer/walrus/issues/new) and I will try my best to fix it!
