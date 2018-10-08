import datetime
import operator
import time

from walrus.utils import basestring_type
from walrus.utils import decode
from walrus.utils import decode_dict
from walrus.utils import make_python_attr


def id_to_datetime(ts):
    tsm, seq = ts.split(b'-', 1)
    return datetime.datetime.fromtimestamp(int(tsm) / 1000.), int(seq)

def datetime_to_id(dt, seq=0):
    tsm = time.mktime(dt.timetuple()) * 1000
    return '%s-%s' % (int(tsm + (dt.microsecond / 1000)), seq)


class Message(object):
    __slots__ = ('stream', 'timestamp', 'sequence', 'data', 'message_id')

    def __init__(self, stream, message_id, data):
        self.stream = stream
        self.message_id = decode(message_id)
        self.data = decode_dict(data)
        self.timestamp, self.sequence = id_to_datetime(message_id)

    def __repr__(self):
        return '<Message %s %s: %s>' % (self.stream, self.message_id,
                                        self.data)


def normalize_id(message_id):
    if isinstance(message_id, basestring_type):
        return message_id
    elif isinstance(message_id, datetime.datetime):
        return datetime_to_id(message_id)
    elif isinstance(message_id, tuple):
        return datetime_to_id(*message_id)
    elif isinstance(message_id, Message):
        return message_id.message_id
    return message_id


def xread_to_messages(resp):
    if resp is None: return
    accum = []
    for stream, messages in resp.items():
        accum.extend(xrange_to_messages(stream, messages))
    # If multiple streams are present, sort them by timestamp.
    if len(resp) > 1:
        accum.sort(key=operator.attrgetter('message_id'))
    return accum


def xrange_to_messages(stream, resp):
    return [Message(stream, message_id, data) for message_id, data in resp]


class _TimeSeriesKey(object):
    __slots__ = ('database', 'group', 'key', 'consumer')

    def __init__(self, database, group, key, consumer):
        self.database = database
        self.group = group
        self.key = key
        self.consumer = consumer

    def ack(self, *id_list):
        id_list = [normalize_id(id) for id in id_list]
        return self.database.xack(self.key, self.group, *id_list)

    def add(self, data, id='*', maxlen=None, approximate=True):
        id = normalize_id(id)
        db_id = self.database.xadd(self.key, data, id, maxlen, approximate)
        return id_to_datetime(db_id)

    def claim(self, *id_list, **kwargs):
        id_list = [normalize_id(id) for id in id_list]
        min_idle_time = kwargs.pop('min_idle_time', None) or 0
        if kwargs: raise ValueError('incorrect arguments for claim()')
        resp = self.database.xclaim(self.key, self.group, self.consumer,
                                    min_idle_time, *id_list)
        return xrange_to_messages(self.key, resp)

    def delete(self, *id_list):
        id_list = [normalize_id(id) for id in id_list]
        return self.database.xdel(self.key, *id_list)

    def __getitem__(self, item):
        if isinstance(item, slice):
            return self.range(item.start or '-', item.stop or '+', item.step)
        else:
            return self.get(item)

    def get(self, id):
        id = normalize_id(id)
        items = self.range(id, id, 1)
        if items:
            return items[0]

    def __len__(self):
        """
        Return the total number of messages in the stream.
        """
        return self.database.xlen(self.key)

    def pending(self, start='-', stop='+', count=-1, consumer=None):
        start = normalize_id(start)
        stop = normalize_id(stop)
        resp = self.database.xpending(self.key, self.group, start, stop,
                                      count, consumer)
        return [(id_to_datetime(id), decode(c), idle, n)
                for id, c, idle, n in resp]

    def read(self, count=None, timeout=None):
        resp = self.database.xreadgroup(self.group, self.consumer, self.key,
                                        count, timeout)
        return xread_to_messages(resp)

    def range(self, start='-', stop='+', count=None):
        start = normalize_id(start)
        stop = normalize_id(stop)
        resp = self.database.xrange(self.key, start, stop, count)
        return xrange_to_messages(self.key, resp)

    def set_id(self, id='$'):
        id = normalize_id(id)
        return self.database.xgroup_setid(self.key, self.group, id)

    def trim(self, count, approximate=True):
        return self.database.xtrim(self.key, count, approximate)


class TimeSeries(object):
    """
    :py:class:`TimeSeries` is a consumer-group that provides a higher level of
    abstraction, reading and writing message ids as datetimes, and returning
    messages using a convenient, lightweight :py:class:`Message` class.

    Rather than creating this class directly, use the
    :py:meth:`Database.time_series` method.

    Each registered stream within the group is exposed as a special attribute
    that provides stream-specific APIs within the context of the group. For
    more information see :py:class:`_TimeSeriesKey`.

    Example::

        ts = db.time_series('groupname', ['stream-1', 'stream-2'])
        ts.stream_1  # _TimeSeriesKey for "stream-1"
        ts.stream_2  # _TimeSeriesKey for "stream-2"

    :param Database database: Redis client
    :param group: name of consumer group
    :param keys: stream identifier(s) to monitor. May be a single stream
        key, a list of stream keys, or a key-to-minimum id mapping. The
        minimum id for each stream should be considered an exclusive
        lower-bound. The '$' value can also be used to only read values
        added *after* our command started blocking.
    :param consumer: name for consumer within group
    :returns: a :py:class:`TimeSeries` instance
    """
    def __init__(self, database, group, keys, consumer=None):
        self.database = database
        self.group = group
        self.keys = database._normalize_stream_keys(keys)
        self._consumer = consumer or (self.group + '.c')

        # Add attributes for each stream exposed as part of the group.
        for key in self.keys:
            attr = make_python_attr(key)
            setattr(self, attr, _TimeSeriesKey(self.database, group, key,
                                               self._consumer))

    def consumer(self, name):
        """
        Create a new consumer for the :py:class:`TimeSeries`.

        :param name: name of consumer
        :returns: a :py:class:`TimeSeries` using the given consumer name.
        """
        return TimeSeries(self.database, self.group, self.keys, name)

    def create(self):
        """
        Create the consumer group and register it with the group's stream keys.
        """
        resp = {}
        for key, value in self.keys.items():
            resp[key] = self.database.xgroup_create(key, self.group, value)
        return resp

    def reset(self):
        """
        Reset the consumer group, clearing the last-read status for each
        stream so it will read from the beginning of each stream.
        """
        return self.set_id('0-0')

    def destroy(self):
        """
        Destroy the consumer group.
        """
        resp = {}
        for key in self.keys:
            resp[key] = self.database.xgroup_destroy(key, self.group)
        return resp

    def read(self, count=None, timeout=None):
        """
        Read unseen messages from all streams in the consumer group. Wrapper
        for :py:class:`Database.xreadgroup` method.

        :param int count: limit number of messages returned
        :param int timeout: milliseconds to block, 0 for indefinitely.
        :returns: a list of :py:class:`Message` objects or ``None`` if no data
            is available.
        """
        resp = self.database.xreadgroup(self.group, self._consumer,
                                        list(self.keys), count, timeout)
        return xread_to_messages(resp)

    def set_id(self, id='$'):
        """
        Set the last-read message id for each stream in the consumer group. By
        default, this will be the special "$" identifier, meaning all messages
        are marked as having been read.

        :param id: id of last-read message (or "$").
        """
        accum = {}
        id = normalize_id(id)
        for key in self.keys:
            accum[key] = self.database.xgroup_setid(key, self.group, id)
        return accum
