## Changelog

This document describes changes to the APIs.

### master

[View changes](https://github.com/coleifer/walrus/compare/0.9.2...HEAD)

### 0.9.2

* Upstream decided to play around with XPENDING again.

[View changes](https://github.com/coleifer/walrus/compare/0.9.1...0.9.2)

### 0.9.1

* Add support for `minid` and `limit` parameters on `Stream.trim()`, #169
* Cache decorators now cache calls that return `None`, #171

[View changes](https://github.com/coleifer/walrus/compare/0.9.0...0.9.1)

### 0.9.0

* **Backwards incompatible change:** redis-py changed the signature of the
  `xpending_range` function. This is resolved in walrus 0.9.0.
* Add support for `autoclaim()` to the `ConsumerGroupStream` class.
* Minor changes to the stop-word handling in `autocomplete` module.

[View changes](https://github.com/coleifer/walrus/compare/0.8.2...0.9.0)

### 0.8.2

* Use `HSET` instead of `HMSET`.
* Add a `search_items()` method to the full-text index which returns a 2-tuple
  of `(key, doc)`.
* Add a timeout parameter to `bmove_tail` and `brpoplpush` wrappers.
* Allow disabling lua script-loading for faster initialization (for
  applications that do not intend to utilize these features).

[View changes](https://github.com/coleifer/walrus/compare/0.8.1...0.8.2)

### 0.8.1

* Fix missing parameter in error message, #105.
* Remove redundant call to `delete()` when using Model `create()` API.
* Fix TTL units and lock event wait timeout handling.
* Adds `Hash.setnx()` method to the hash container.
* Do not double-decode strings when user has enabled decode_responses, #121.
* Fix mapping of types in `get_key()` method to return the proper container
  type, #120.

[View all changes](https://github.com/coleifer/walrus/compare/0.8.0...0.8.1)

### 0.8.0

* Adds efficient bulk get, set and delete methods to the `Cache` class.
* Fixes `repr` issues with some of the container types.
* Fixed an inefficiency in the implementation of the `Graph` storage that
  cuts the amount of memory needed in half.
* Fixed issues with unicode handling in the full-text search implementation.

[View all changes](https://github.com/coleifer/walrus/compare/0.7.0...0.8.0)

### 0.7.0

Depends on [redis-py](https://github.com/andymccurdy/redis-py) 3.0 or newer.
There are a number of backwards-incompatible changes in redis-py. Because
walrus provides high-level abstractions for the Redis data-types/commands, your
walrus code should work with little or no modifications. Refer to the [list of changes](https://github.com/andymccurdy/redis-py#upgrading-from-redis-py-2x-to-30)
for more information.

[redis-py](https://github.com/andymccurdy/redis-py) added support for stream
commands as well as zpop/bzpop. As a result, walrus no longer contains separate
implementations for these commands. For the majority of cases the low-level
method signatures and return values are unchanged, notably, the `XREADGROUP`
return value is slightly different. The `timeout` parameter, where it was
accepted, has been renamed to `block` for greater compatibility with redis-py.

Prior to 0.7.0, you would read from a consumer-group (which might contain one
or more streams) and receive a `dict` keyed by the stream, whose value was a
list of `(message_id, data)` 2-tuples. Going forward, the return value will be
a list of `[stream_name, [(message_id, data), ...]]`. To retain the
functionality of walrus prior-to 0.7.0, just wrap the return value in a call to
the `dict` constructor: `ret = dict(consumer_group.read(count=1))`.

* Added [BloomFilter](https://walrus.readthedocs.io/en/latest/api.html#walrus.Database.bloom_filter)
  container type, which supports `add()` and `contains()`.
* Added a high-level [BitField](https://walrus.readthedocs.io/en/latest/api.html#walrus.BitField)
  container type.

[View all changes](https://github.com/coleifer/walrus/compare/0.6.4...0.7.0)

### 0.6.0

* Stream support added, including consumer groups.
* Support for `zpopmin`, `zpopmax`, and the blocking `bzpopmin`, `bzpopmax`
* Added APIs to container classes for converting to-and-from native data-types.
* Added atomic compare-and-set method to database.

### 0.5.2

* Fixed incompatibilities with Python 3.7.
* Fixed incorrect result scoring in full-text search model.

### 0.5.1

* Added standalone [full-text search](https://walrus.readthedocs.io/en/latest/full-text-search.html).
* Refactored various internal classes that support the new standalone full-text
  search index.

### 0.5.0

* The `models` API uses a backwards-incompatible serialization approach. This
  means that data stored using 0.4.1 cannot be read back using 0.5.0.
* `Field()` no longer supports `pickled` or `as_json` parameters. Instead, use
  the `PickledField` and `JSONField`.
