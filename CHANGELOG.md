## Changelog

This document describes changes to the APIs.

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
