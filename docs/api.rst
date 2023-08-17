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
      rate_limit_lua,
      Index,
      List,
      Hash,
      Set,
      ZSet,
      HyperLogLog,
      Array,
      Stream,
      consumer_group,
      time_series,
      bit_field,
      bloom_filter,
      cas,
      xsetid,
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
      incr,
      as_dict

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
      popright,
      as_list

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
      unionstore,
      as_set

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
      unionstore,
      popmin,
      popmax,
      bpopmin,
      bpopmax,
      popmin_compat,
      popmax_compat,
      as_items

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
      __iter__,
      as_list

.. autoclass:: Stream(Container)
    :members:
      __getitem__,
      __delitem__,
      __len__,
      add,
      get,
      range,
      read,
      delete,
      trim,
      info,
      groups_info,
      consumers_info,
      set_id,
      __iter__

.. autoclass:: ConsumerGroup
    :members:
      consumer,
      create,
      destroy,
      reset,
      set_id,
      read,
      stream_info

.. autoclass:: walrus.containers.ConsumerGroupStream(Stream)
    :members:
      consumers_info,
      ack,
      claim,
      pending,
      autoclaim,
      read,
      set_id

.. autoclass:: BitField(Container)
    :members:
      incrby,
      get,
      set,
      __getitem__,
      __setitem__,
      __delitem__,
      get_raw,
      set_raw,
      bit_count,
      get_bit,
      set_bit

.. autoclass:: walrus.containers.BitFieldOperation
    :members:
      incrby,
      get,
      set,
      execute,
      __iter__

.. autoclass:: BloomFilter(Container)
    :members:
      add,
      contains,
      __contains__

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
      list_data,
      list_titles,
      flush

.. autoclass:: Cache
    :members:
      __init__,
      get,
      set,
      delete,
      get_many,
      set_many,
      delete_many,
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

.. autoclass:: Index
    :members:
      __init__,
      get_document,
      add,
      remove,
      update,
      replace,
      search,
      search_items

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
      __database__,
      __namespace__,
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

.. autoclass:: RateLimitLua(RateLimit)
    :members:
      limit

.. autoclass:: TimeSeries(ConsumerGroup)
    :members:
      consumer,
      create,
      destroy,
      reset,
      read,
      set_id

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
                print(message.content)

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
