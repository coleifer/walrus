from functools import wraps
import glob
import os
import sys
import threading
import uuid

try:
    from redis import Redis
    from redis.client import pairs_to_dict
    from redis.client import zset_score_pairs
    from redis.exceptions import ConnectionError
    from redis.exceptions import TimeoutError
except ImportError:
    Redis = object
    zset_score_pairs = None
    ConnectionError = TimeoutError = Exception

from walrus.autocomplete import Autocomplete
from walrus.cache import Cache
from walrus.containers import Array
from walrus.containers import ConsumerGroup
from walrus.containers import Hash
from walrus.containers import HyperLogLog
from walrus.containers import List
from walrus.containers import Set
from walrus.containers import Stream
from walrus.containers import ZSet
from walrus.counter import Counter
from walrus.fts import Index
from walrus.graph import Graph
from walrus.lock import Lock
from walrus.rate_limit import RateLimit
from walrus.streams import TimeSeries
from walrus.utils import basestring_type


class TransactionLocal(threading.local):
    def __init__(self, **kwargs):
        super(TransactionLocal, self).__init__(**kwargs)
        self.pipes = []

    @property
    def pipe(self):
        if len(self.pipes):
            return self.pipes[-1]

    def commit(self):
        pipe = self.pipes.pop()
        return pipe.execute()

    def abort(self):
        pipe = self.pipes.pop()
        pipe.reset()


# XREVRANGE, XRANGE.
def _stream_list(response):
    if response is None: return
    return [(ts_seq, pairs_to_dict(kv_list)) for ts_seq, kv_list in response]

# XREAD.
def _multi_stream_list(response):
    if response is None: return
    accum = {}
    for (identifier, stream_response) in response:
        accum[identifier.decode('utf-8')] = _stream_list(stream_response)
    return accum


class Database(Redis):
    """
    Redis-py client with some extras.
    """
    RESPONSE_CALLBACKS = getattr(Redis, 'RESPONSE_CALLBACKS', {})
    RESPONSE_CALLBACKS.update(
        XCLAIM=_stream_list,
        XDEL=int,
        XLEN=int,
        XRANGE=_stream_list,
        XREVRANGE=_stream_list,
        XREAD=_multi_stream_list,
        XREADGROUP=_multi_stream_list,
        XTRIM=int)

    def __init__(self, *args, **kwargs):
        """
        :param args: Arbitrary positional arguments to pass to the
            base ``Redis`` instance.
        :param kwargs: Arbitrary keyword arguments to pass to the
            base ``Redis`` instance.
        :param str script_dir: Path to directory containing walrus
            scripts.
        """
        script_dir = kwargs.pop('script_dir', None)
        super(Database, self).__init__(*args, **kwargs)
        self.__mapping = {
            'list': self.List,
            'set': self.Set,
            'zset': self.ZSet,
            'hash': self.Hash}
        self._transaction_local = TransactionLocal()
        self._transaction_lock = threading.RLock()
        self.init_scripts(script_dir=script_dir)

        if not hasattr(self, 'zpopmin'):
            self._add_zset_pop_methods()

    def execute_command(self, *args, **options):
        # Override `Redis.execute_command`, which could run the same command
        # twice in the event of a connection error during the receipt of the
        # response.
        pool = self.connection_pool
        command_name = args[0]
        connection = pool.get_connection(command_name, **options)
        try:
            try:
                connection.send_command(*args)
                # NB: removed "return self.parse_response(...)"
            except (ConnectionError, TimeoutError) as e:
                connection.disconnect()
                if not connection.retry_on_timeout and isinstance(e, TimeoutError):
                    raise
                # Try sending the command using a fresh connection.
                connection.send_command(*args)
            return self.parse_response(connection, command_name, **options)
        finally:
            pool.release(connection)

    def _add_zset_pop_methods(self):
        def _zpopcmd(cmd):
            def zpopcmd(key, count=1):
                res = self.execute_command(cmd, key, count)
                return zset_score_pairs(res, withscores=True)
            return zpopcmd
        self.zpopmin = _zpopcmd('zpopmin')
        self.zpopmax = _zpopcmd('zpopmax')
        def _bzpopcmd(cmd):
            def bzpopcmd(keys, timeout=0):
                a = [keys] if isinstance(keys, basestring_type) else list(keys)
                a.append(timeout or 0)
                res = self.execute_command(cmd, *a)
                if res is not None:
                    res[2] = float(res[2])
                    return res
            return bzpopcmd
        self.bzpopmin = _bzpopcmd('bzpopmin')
        self.bzpopmax = _bzpopcmd('bzpopmax')

    def xadd(self, key, data, id='*', maxlen=None, approximate=True):
        """
        Add a new message to a stream.

        :param key: stream identifier
        :param dict data: data to add to stream
        :param id: identifier for message ('*' to automatically append)
        :param maxlen: maximum length for stream
        :param approximate: allow stream max length to be approximate
        :returns: the added message's id.
        """
        parts = []
        if maxlen is not None:
            if not isinstance(maxlen, int) or maxlen < 1:
                raise ValueError('XADD maxlen must be a positive integer')
            parts.append('MAXLEN')
            if approximate:
                parts.append('~')
            parts.append(str(maxlen))
        parts.append(id)
        for k, v in data.items():
            parts.append(k)
            parts.append(v)
        return self.execute_command('XADD', key, *parts)

    def _xrange(self, cmd, key, start, stop, count):
        parts = [start, stop]
        if count is not None:
            if not isinstance(count, int) or count < 1:
                raise ValueError('%s count must be a positive integer' % cmd)
            parts.append('COUNT')
            parts.append(str(count))
        return self.execute_command(cmd, key, *parts)

    def xrange(self, key, start='-', stop='+', count=None):
        """
        Read a range of values from a stream.

        :param key: stream identifier
        :param start: starting id ('-' for oldest available)
        :param stop: stop id ('+' for latest available)
        :param count: limit number of messages returned
        :returns: a list of (message id, data) 2-tuples.
        """
        return self._xrange('XRANGE', key, start, stop, count)

    def xrevrange(self, key, start='+', stop='-', count=None):
        """
        Read a range of values from a stream in reverse order.

        :param key: stream identifier
        :param start: starting id ('+' for latest available)
        :param stop: stop id ('-' for oldest available)
        :param count: limit number of messages returned
        :returns: a list of (message id, data) 2-tuples.
        """
        return self._xrange('XREVRANGE', key, start, stop, count)

    def xlen(self, key):
        """
        Return the length of a stream.

        :param key: stream identifier
        :returns: length of the stream
        """
        return self.execute_command('XLEN', key)

    def _normalize_stream_keys(self, keys, default_id='0-0'):
        if isinstance(keys, basestring_type):
            return {keys: default_id}
        elif isinstance(keys, (list, tuple)):
            return dict([(key, default_id) for key in keys])
        elif isinstance(keys, dict):
            return keys
        else:
            raise ValueError('keys must be either a stream key, a list of '
                             'stream keys, or a dictionary mapping key to '
                             'minimum message id.')

    def xread(self, keys, count=None, timeout=None):
        """
        Monitor one or more streams for new data.

        :param keys: stream identifier(s) to monitor. May be a single stream
            key, a list of stream keys, or a key-to-minimum id mapping. The
            minimum id for each stream should be considered an exclusive
            lower-bound. The '$' value can also be used to only read values
            added *after* our command started blocking.
        :param int count: limit number of messages returned
        :param int timeout: milliseconds to block, 0 for indefinitely.
        :returns: a dict keyed by the stream key, whose value is a list of
            (message id, data) 2-tuples. If no data is available or a timeout
            occurs, ``None`` is returned.
        """
        key_to_id = self._normalize_stream_keys(keys)
        parts = []
        if timeout is not None:
            if not isinstance(timeout, int) or timeout < 0:
                raise ValueError('XREAD timeout must be >= 0')
            parts.append('BLOCK')
            parts.append(str(timeout))
        if count is not None:
            if not isinstance(count, int) or count < 1:
                raise ValueError('XREAD count must be a positive integer')
            parts.append('COUNT')
            parts.append(str(count))
        parts.append('STREAMS')
        stream_ids = []
        for key, stream_id in key_to_id.items():
            parts.append(key)
            stream_ids.append(str(stream_id))
        parts.extend(stream_ids)
        return self.execute_command('XREAD', *parts)

    def xdel(self, key, *id_list):
        """
        Remove one or more messages from a stream.

        :param key: stream identifier
        :param id_list: one or more message ids to remove.
        :returns: number of messages deleted.
        """
        return self.execute_command('XDEL', key, *id_list)

    def xtrim(self, key, count, approximate=True):
        """
        Trim the stream to the given "count" of messages, discarding the oldest
        messages first.

        :param key: stream identifier
        :param count: maximum size of stream
        :param approximate: allow size to be approximate
        :returns: number of messages discarded
        """
        parts = ['MAXLEN']
        if approximate:
            parts.append('~')
        parts.append(str(count))
        return self.execute_command('XTRIM', key, *parts)

    def xinfo_stream(self, key):
        """
        Retrieve information about a stream.

        :param key: stream key
        :returns: a dictionary containing stream metadata
        """
        return pairs_to_dict(self.execute_command('XINFO', 'STREAM', key))

    def xinfo_groups(self, key):
        """
        Retrieve information about consumer groups for the given stream.

        :param key: stream key
        :returns: a list of dictionaries containing consumer group metadata
        """
        resp = self.execute_command('XINFO', 'GROUPS', key)
        return [pairs_to_dict(cg) for cg in resp]

    def xinfo_consumers(self, key, group):
        """
        Retrieve information about consumers within the given consumer group
        operating on the stream.

        :param key: stream key
        :param group: consumer group name
        :returns: a list of dictionaries containing consumer metadata
        """
        resp = self.execute_command('XINFO', 'CONSUMERS', key, group)
        return [pairs_to_dict(c) for c in resp]

    def xgroup_create(self, key, group, id='$'):
        """
        Create a consumer group.

        :param key: stream key -- must exist before creating a group
        :param group: consumer group name
        :param id: set the id of the last-received-message
        """
        resp = self.execute_command('XGROUP', 'CREATE', key, group, id)
        return resp == b'OK'

    def xgroup_setid(self, key, group, id='$'):
        """
        Set the id of the last-received-message for a stream

        :param key: stream key
        :param group: consumer group name
        :param id: set the id of the last-received-message
        """
        return self.execute_command('XGROUP', 'SETID', key, group, id) == b'OK'

    def xgroup_destroy(self, key, group):
        """
        Destroy a consumer group.

        :param key: stream key
        :param group: consumer group name
        """
        return self.execute_command('XGROUP', 'DESTROY', key, group)

    def xgroup_delete_consumer(self, key, group, consumer):
        """
        Delete a consumer within a consumer group.

        :param key: stream key
        :param group: consumer group name
        :param consumer: consumer name
        """
        return self.execute_command('XGROUP', 'DELCONSUMER', key, group,
                                    consumer)

    def xreadgroup(self, group, consumer, keys, count=None, timeout=None):
        """
        Monitor one or more streams for new data using a consumer group.

        :param group: group name
        :param consumer: consumer name
        :param keys: stream identifier(s) to monitor. May be a single stream
            key, a list of stream keys, or a key-to-minimum id mapping. The
            minimum id for each stream should be considered an exclusive
            lower-bound. The '>' value can also be used to only read values
            that have never been delivered to a consumer.
        :param int count: limit number of messages returned
        :param int timeout: milliseconds to block, 0 for indefinitely.
        :returns: a dict keyed by the stream key, whose value is a list of
            (message id, data) 2-tuples. If no data is available or a timeout
            occurs, ``None`` is returned.
        """
        key_to_id = self._normalize_stream_keys(keys, '>')
        parts = ['XREADGROUP', 'GROUP', group, consumer]
        if count is not None:
            if not isinstance(count, int) or count < 1:
                raise ValueError('XREADGROUP count must be a positive integer')
            parts.append('COUNT')
            parts.append(str(count))
        if timeout is not None:
            if not isinstance(timeout, int) or timeout < 0:
                raise ValueError('XREADGROUP timeout must be >= 0')
            parts.append('BLOCK')
            parts.append(str(timeout))
        parts.append('STREAMS')
        stream_ids = []
        for key, stream_id in key_to_id.items():
            parts.append(key)
            stream_ids.append(str(stream_id))
        parts.extend(stream_ids)
        return self.execute_command(*parts)

    def xack(self, key, group, *id_list):
        """
        Acknowledge that a message has been processed by a consumer.

        :param key: stream identifier
        :param group: consumer group name
        :param id_list: one or more message ids to acknowledge
        :returns: number of messages marked acknowledged
        """
        return self.execute_command('XACK', key, group, *id_list)

    def xclaim(self, key, group, consumer, min_idle_time, *id_list):
        """
        Claim pending - but unacknowledged - messages.

        :param key: stream identifier
        :param group: consumer group name
        :param min_idle_time: minimum idle time in milliseconds
        :param id_list: one or more message ids to acknowledge
        :returns: list of (message id, data) 2-tuples of messages that were
            successfully claimed
        """
        if not isinstance(min_idle_time, int) or min_idle_time < 0:
            raise ValueError('min_idle_time must be a non-negative number')
        return self.execute_command('XCLAIM', key, group, consumer,
                                    str(min_idle_time), *id_list)

    def xpending(self, key, group, start='-', stop='+', count=-1,
                 consumer=None):
        """
        List pending messages.

        :param key: stream identifier
        :param group: consumer group name
        :param start: start id (or '-' for oldest pending)
        :param stop: stop id (or '+' for newest pending)
        :param count: limit number of messages returned
        :param consumer: restrict message list to the given consumer
        :returns: A list containing status for each pending message. Each
            pending message returns [id, consumer, idle time, deliveries].
        """
        parts = ['XPENDING', key, group, start, stop, str(count)]
        if consumer is not None:
            parts.append(consumer)
        return self.execute_command(*parts)

    def xpending_summary(self, key, group):
        """
        Pending message summary report.

        :param key: stream identifier
        :param group: consumer group name
        :returns: (count, min id, max id, {consumer: count, ...})
        """
        resp = self.execute_command('XPENDING', key, group)
        count, min_id, max_id, consumer_counts = resp
        return count, min_id, max_id, dict(consumer_counts)

    def get_transaction(self):
        with self._transaction_lock:
            local = self._transaction_local
            local.pipes.append(self.pipeline())
            return local.pipe

    def commit_transaction(self):
        """
        Commit the currently active transaction (Pipeline). If no
        transaction is active in the current thread, an exception
        will be raised.

        :returns: The return value of executing the Pipeline.
        :raises: ``ValueError`` if no transaction is active.
        """
        with self._transaction_lock:
            local = self._transaction_local
            if not local.pipes:
                raise ValueError('No transaction is currently active.')
            return local.commit()

    def clear_transaction(self):
        """
        Clear the currently active transaction (if exists). If the
        transaction stack is not empty, then a new pipeline will
        be initialized.

        :returns: No return value.
        :raises: ``ValueError`` if no transaction is active.
        """
        with self._transaction_lock:
            local = self._transaction_local
            if not local.pipes:
                raise ValueError('No transaction is currently active.')
            local.abort()

    def atomic(self):
        return _Atomic(self)

    def init_scripts(self, script_dir=None):
        self._scripts = {}
        if not script_dir:
            script_dir = os.path.join(os.path.dirname(__file__), 'scripts')
        for filename in glob.glob(os.path.join(script_dir, '*.lua')):
            with open(filename, 'r') as fh:
                script_obj = self.register_script(fh.read())
                script_name = os.path.splitext(os.path.basename(filename))[0]
                self._scripts[script_name] = script_obj

    def run_script(self, script_name, keys=None, args=None):
        """
        Execute a walrus script with the given arguments.

        :param script_name: The base name of the script to execute.
        :param list keys: Keys referenced by the script.
        :param list args: Arguments passed in to the script.
        :returns: Return value of script.

        .. note:: Redis scripts require two parameters, ``keys``
            and ``args``, which are referenced in lua as ``KEYS``
            and ``ARGV``.
        """
        return self._scripts[script_name](keys, args)

    def get_temp_key(self):
        """
        Generate a temporary random key using UUID4.
        """
        return 'temp.%s' % uuid.uuid4()

    def __iter__(self):
        """
        Iterate over the keys of the selected database.
        """
        return iter(self.scan_iter())

    def search(self, pattern):
        """
        Search the keyspace of the selected database using the
        given search pattern.

        :param str pattern: Search pattern using wildcards.
        :returns: Iterator that yields matching keys.
        """
        return self.scan_iter(pattern)

    def get_key(self, key):
        """
        Return a rich object for the given key. For instance, if
        a hash key is requested, then a :py:class:`Hash` will be
        returned.

        :param str key: Key to retrieve.
        :returns: A hash, set, list, zset or array.
        """
        return self.__mapping.get(self.type(key), self.__getitem__)(key)

    def hash_exists(self, key):
        return self.exists(key)

    def autocomplete(self, namespace='autocomplete', **kwargs):
        return Autocomplete(self, namespace, **kwargs)

    def cache(self, name='cache', default_timeout=3600):
        """
        Create a :py:class:`Cache` instance.

        :param str name: The name used to prefix keys used to
            store cached data.
        :param int default_timeout: The default key expiry.
        :returns: A :py:class:`Cache` instance.
        """
        return Cache(self, name=name, default_timeout=default_timeout)

    def counter(self, name):
        """
        Create a :py:class:`Counter` instance.

        :param str name: The name used to store the counter's value.
        :returns: A :py:class:`Counter` instance.
        """
        return Counter(self, name=name)

    def graph(self, name, *args, **kwargs):
        """
        Creates a :py:class:`Graph` instance.

        :param str name: The namespace for the graph metadata.
        :returns: a :py:class:`Graph` instance.
        """
        return Graph(self, name, *args, **kwargs)

    def lock(self, name, ttl=None, lock_id=None):
        """
        Create a named :py:class:`Lock` instance. The lock implements
        an API similar to the standard library's ``threading.Lock``,
        and can also be used as a context manager or decorator.

        :param str name: The name of the lock.
        :param int ttl: The time-to-live for the lock in milliseconds
            (optional). If the ttl is ``None`` then the lock will not
            expire.
        :param str lock_id: Optional identifier for the lock instance.
        """
        return Lock(self, name, ttl, lock_id)

    def rate_limit(self, name, limit=5, per=60, debug=False):
        """
        Rate limit implementation. Allows up to `limit` of events every `per`
        seconds.

        See :ref:`rate-limit` for more information.
        """
        return RateLimit(self, name, limit, per, debug)

    def Index(self, name, **options):
        """
        Create a :py:class:`Index` full-text search index with the given
        name and options.
        """
        return Index(self, name, **options)

    def List(self, key):
        """
        Create a :py:class:`List` instance wrapping the given key.
        """
        return List(self, key)

    def Hash(self, key):
        """
        Create a :py:class:`Hash` instance wrapping the given key.
        """
        return Hash(self, key)

    def Set(self, key):
        """
        Create a :py:class:`Set` instance wrapping the given key.
        """
        return Set(self, key)

    def ZSet(self, key):
        """
        Create a :py:class:`ZSet` instance wrapping the given key.
        """
        return ZSet(self, key)

    def HyperLogLog(self, key):
        """
        Create a :py:class:`HyperLogLog` instance wrapping the given
        key.
        """
        return HyperLogLog(self, key)

    def Array(self, key):
        """
        Create a :py:class:`Array` instance wrapping the given key.
        """
        return Array(self, key)

    def Stream(self, key):
        """
        Create a :py:class:`Stream` instance wrapping the given key.
        """
        return Stream(self, key)

    def consumer_group(self, group, keys, consumer=None):
        """
        Create a named :py:class:`ConsumerGroup` instance for the given key(s).

        :param group: name of consumer group
        :param keys: stream identifier(s) to monitor. May be a single stream
            key, a list of stream keys, or a key-to-minimum id mapping. The
            minimum id for each stream should be considered an exclusive
            lower-bound. The '$' value can also be used to only read values
            added *after* our command started blocking.
        :param consumer: name for consumer within group
        :returns: a :py:class:`ConsumerGroup` instance
        """
        return ConsumerGroup(self, group, keys, consumer=consumer)

    def time_series(self, group, keys, consumer=None):
        """
        Create a named :py:class:`TimeSeries` consumer-group for the
        given key(s). TimeSeries objects are almost identical to
        :py:class:`ConsumerGroup` except they offer a higher level of
        abstraction and read/write message ids as datetimes.

        :param group: name of consumer group
        :param keys: stream identifier(s) to monitor. May be a single stream
            key, a list of stream keys, or a key-to-minimum id mapping. The
            minimum id for each stream should be considered an exclusive
            lower-bound. The '$' value can also be used to only read values
            added *after* our command started blocking.
        :param consumer: name for consumer within group
        :returns: a :py:class:`TimeSeries` instance
        """
        return TimeSeries(self, group, keys, consumer=consumer)

    def cas(self, key, value, new_value):
        """
        Perform an atomic compare-and-set on the value in "key", using a prefix
        match on the provided value.
        """
        return self.run_script('cas', keys=[key], args=[value, new_value])

    def listener(self, channels=None, patterns=None, is_async=False):
        """
        Decorator for wrapping functions used to listen for Redis
        pub-sub messages.

        The listener will listen until the decorated function
        raises a ``StopIteration`` exception.

        :param list channels: Channels to listen on.
        :param list patterns: Patterns to match.
        :param bool is_async: Whether to start the listener in a
            separate thread.
        """
        def decorator(fn):
            _channels = channels or []
            _patterns = patterns or []

            @wraps(fn)
            def inner():
                pubsub = self.pubsub()

                def listen():
                    for channel in _channels:
                        pubsub.subscribe(channel)
                    for pattern in _patterns:
                        pubsub.psubscribe(pattern)

                    for data_dict in pubsub.listen():
                        try:
                            ret = fn(**data_dict)
                        except StopIteration:
                            pubsub.close()
                            break

                if is_async:
                    worker = threading.Thread(target=listen)
                    worker.start()
                    return worker
                else:
                    listen()

            return inner
        return decorator

    def stream_log(self, callback, connection_id='monitor'):
        """
        Stream Redis activity one line at a time to the given
        callback.

        :param callback: A function that accepts a single argument,
            the Redis command.
        """
        conn = self.connection_pool.get_connection(connection_id, None)
        conn.send_command('monitor')
        while callback(conn.read_response()):
            pass


class _Atomic(object):
    def __init__(self, db):
        self.db = db

    @property
    def pipe(self):
        return self.db._transaction_local.pipe

    def __enter__(self):
        self.db.get_transaction()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.clear(False)
        else:
            self.commit(False)

    def commit(self, begin_new=True):
        ret = self.db.commit_transaction()
        if begin_new:
            self.db.get_transaction()
        return ret

    def clear(self, begin_new=True):
        self.db.clear_transaction()
        if begin_new:
            self.db.get_transaction()
