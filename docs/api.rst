.. _api:

API Documentation
=================

.. py:module:: walrus

.. autoclass:: Database(Redis)
    :members:
      __init__,
      run_script,
      get_temp_key,
      __iter__,
      search,
      get_key,
      cache,
      counter,
      graph,
      lock,
      rate_limit,
      List,
      Hash,
      Set,
      ZSet,
      HyperLogLog,
      Array,
      listener,
      stream_log

Container types
---------------

.. autoclass:: Container
    :members:


.. autoclass:: Hash(Container)
    :members:
      __getitem__,
      __setitem__,
      __delitem__,
      __contains__,
      __len__,
      __iter__,
      search,
      keys,
      values,
      items,
      update,
      as_dict,
      incr

.. autoclass:: List(Container)
    :members:
      __getitem__,
      __setitem__,
      __delitem__,
      __len__,
      __iter__,
      append,
      prepend,
      extend,
      insert_before,
      insert_after,
      popleft,
      popright

.. autoclass:: Set(Container)
    :members:
      add,
      __delitem__,
      remove,
      pop,
      __contains__,
      __len__,
      __iter__,
      search,
      members,
      random,
      __sub__,
      __or__,
      __and__,
      diffstore,
      interstore,
      unionstore

.. autoclass:: ZSet(Container)
    :members:
      add,
      __getitem__,
      __setitem__,
      __delitem__,
      remove,
      __contains__,
      __len__,
      __iter__,
      search,
      score,
      rank,
      count,
      lex_count,
      range,
      range_by_score,
      range_by_lex,
      remove_by_rank,
      remove_by_score,
      remove_by_lex,
      incr,
      interstore,
      unionstore

.. autoclass:: HyperLogLog(Container)
    :members:
      add,
      __len__,
      merge

.. autoclass:: Array(Container)
    :members:
      __getitem__,
      __setitem__,
      __delitem__,
      __len__,
      append,
      extend,
      pop,
      __contains__,
      __iter__

High-level APIs
---------------

.. autoclass:: Autocomplete
    :members:
      __init__,
      store,
      search,
      exists,
      boost_object,
      remove,
      flush

.. autoclass:: Cache
    :members:
      __init__,
      get,
      set,
      delete,
      keys,
      flush,
      incr,
      cached,
      cached_property,
      cache_async

.. autoclass:: Counter
    :members:
      __init__,
      incr,
      decr,
      value

.. autoclass:: Graph
    :members:
      __init__,
      store,
      store_many,
      delete,
      query,
      search,
      v

.. autoclass:: Lock
    :members:
      __init__,
      acquire,
      release,
      clear

.. autoclass:: Model
    :members:
      database,
      namespace,
      index_separator,
      __init__,
      incr,
      to_hash,
      create,
      all,
      query,
      query_delete,
      get,
      load,
      delete,
      save,
      count

.. autoclass:: RateLimit
    :members:
      __init__,
      limit,
      rate_limited

Field types
-----------

.. autoclass:: Field
    :members:
      __init__,
      get_indexes

.. autoclass:: TextField
    :members:

    .. py:method:: search(query[, default_conjunction='and'])

        :param str query: Search query.
        :param str default_conjunction: Either ``'and'`` or ``'or'``.

        Create an expression corresponding to the given search query. Search queries can contain conjunctions (``AND`` and ``OR``).

        Example:

        .. code-block:: python

            class Message(Model):
                database = my_db
                content = TextField(fts=True)

            expression = Message.content.search('python AND (redis OR walrus)')
            messages = Message.query(expression)
            for message in messages:
                print message.content

.. autoclass:: IntegerField
    :members:

.. autoclass:: AutoIncrementField(IntegerField)
    :members:

.. autoclass:: FloatField
    :members:

.. autoclass:: ByteField
    :members:

.. autoclass:: BooleanField
    :members:

.. autoclass:: UUIDField
    :members:

.. autoclass:: DateTimeField
    :members:

.. autoclass:: DateField
    :members:

.. autoclass:: JSONField
    :members:

Container Field Types
^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: HashField
    :members:

.. autoclass:: ListField
    :members:

.. autoclass:: SetField
    :members:

.. autoclass:: ZSetField
    :members:
