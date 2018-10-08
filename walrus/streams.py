import datetime
import operator
import time

from walrus.utils import basestring_type
from walrus.utils import decode
from walrus.utils import decode_dict


def id_to_datetime(ts):
    tsm, seq = ts.split(b'-', 1)
    return datetime.datetime.fromtimestamp(int(tsm) / 1000.), int(seq)

def datetime_to_id(dt, seq=0):
    tsm = time.mktime(dt.timetuple()) * 1000
    return '%s-%s' % (int(tsm + (dt.microsecond / 1000)), seq)


class Record(object):
    __slots__ = ('stream', 'timestamp', 'sequence', 'data', 'record_id')

    def __init__(self, stream, record_id, data):
        self.stream = stream
        self.record_id = decode(record_id)
        self.data = decode_dict(data)
        self.timestamp, self.sequence = id_to_datetime(record_id)

    def __repr__(self):
        return '<Record %s %s: %s>' % (self.stream, self.record_id, self.data)


def normalize_id(record_id):
    if isinstance(record_id, basestring_type):
        return record_id
    elif isinstance(record_id, datetime.datetime):
        return datetime_to_id(record_id)
    elif isinstance(record_id, tuple):
        return datetime_to_id(*record_id)
    elif isinstance(record_id, Record):
        return record_id.record_id
    return record_id


def xread_to_records(resp):
    if resp is None: return
    accum = []
    for stream, records in resp.items():
        accum.extend(xrange_to_records(stream, records))
    # If multiple streams are present, sort them by timestamp.
    if len(resp) > 1:
        accum.sort(key=operator.attrgetter('record_id'))
    return accum


def xrange_to_records(stream, resp):
    return [Record(stream, record_id, data) for record_id, data in resp]


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
        return xrange_to_records(self.key, resp)

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
        return xread_to_records(resp)

    def range(self, start='-', stop='+', count=None):
        start = normalize_id(start)
        stop = normalize_id(stop)
        resp = self.database.xrange(self.key, start, stop, count)
        return xrange_to_records(self.key, resp)

    def set_id(self, id='$'):
        id = normalize_id(id)
        return self.database.xgroup_setid(self.key, self.group, id)

    def trim(self, count, approximate=True):
        return self.database.xtrim(self.key, count, approximate)


class TimeSeries(object):
    def __init__(self, database, group, keys, consumer=None):
        self.database = database
        self.group = group
        self.keys = database._normalize_stream_keys(keys)
        self._consumer = consumer or (self.group + '.c')

        # Add attributes for each stream exposed as part of the group.
        for key in self.keys:
            setattr(self, key, _TimeSeriesKey(self.database, group, key,
                                              self._consumer))

    def consumer(self, name):
        return TimeSeries(self.database, self.group, self.keys, name)

    def create(self):
        resp = {}
        for key, value in self.keys.items():
            resp[key] = self.database.xgroup_create(key, self.group, value)
        return resp

    def reset(self, id='0-0'):
        return self.set_id(id)

    def destroy(self):
        resp = {}
        for key in self.keys:
            resp[key] = self.database.xgroup_destroy(key, self.group)
        return resp

    def read(self, count=None, timeout=None):
        resp = self.database.xreadgroup(self.group, self._consumer,
                                        list(self.keys), count, timeout)
        return xread_to_records(resp)

    def set_id(self, id='$'):
        accum = {}
        id = normalize_id(id)
        for key in self.keys:
            accum[key] = self.database.xgroup_setid(key, self.group, id)
        return accum
