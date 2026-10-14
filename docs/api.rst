.. _api:

API Documentation
=================

.. py:module:: walrus

.. autoclass:: Database(Redis)
    :members:
      __init__,
      from_url,
      atomic,
      commit_transaction,
      clear_transaction,
      run_script,
      get_temp_key,
      __iter__,
      search,
      get_key,
      autocomplete,
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
      xpending_summary,
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
      setnx,
      incr,
      incr_float,
      as_dict,
      from_dict

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
      pop,
      popleft,
      popright,
      bpopleft,
      bpopright,
      as_list,
      from_list

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
      as_set,
      from_set

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
      as_items,
      from_dict

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
      as_list,
      from_list

.. autoclass:: Stream(Container)
    :members:
      __getitem__,
      __delitem__,
      __len__,
      length,
      add,
      get,
      range,
      revrange,
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
      set_id,
      delete_consumer

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
      get_counter,
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

.. autoclass:: RateLimit
    :members:
      __init__,
      limit,
      rate_limited

.. autoclass:: RateLimitLua(RateLimit)
    :members:
      limit

.. autoclass:: RateLimitException

.. autoclass:: TimeSeries(ConsumerGroup)
    :members:
      consumer,
      create,
      destroy,
      reset,
      read,
      set_id

.. autoclass:: walrus.streams.TimeSeriesStream(ConsumerGroupStream)
    :members:
      add,
      get,
      range,
      revrange,
      read,
      delete,
      trim,
      ack,
      claim,
      autoclaim,
      pending,
      set_id

.. autoclass:: Message
