## Changelog

This document describes changes to the APIs.

### 0.5.0

* The `models` API uses a backwards-incompatible serialization approach. This
  means that data stored using 0.4.1 cannot be read back using 0.5.0.
* `Field()` no longer supports `pickled` or `as_json` parameters. Instead, use
  the `PickledField` and `JSONField`.
