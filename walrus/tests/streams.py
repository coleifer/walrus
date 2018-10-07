import datetime
import os
import unittest

from walrus.streams import TimeSeries
from walrus.tests.base import WalrusTestCase
from walrus.tests.base import db


def stream_test(fn):
    test_stream = os.environ.get('TEST_STREAM')
    return unittest.skipIf(not test_stream, 'skipping stream tests')(fn)


class TestTimeSeries(WalrusTestCase):
    def setUp(self):
        super(TestTimeSeries, self).setUp()
        for key in ('sa', 'sb', 'sc'):
            db.delete('key')

        self.ts = TimeSeries(db, 'cgabc', {'sa': '0', 'sb': '0', 'sc': '0'})

    def _create_test_data(self):
        start = datetime.datetime(2018, 1, 1)
        id_list = []
        keys = ('sa', 'sb', 'sc')
        for i in range(0, 10):
            tskey = getattr(self.ts, keys[i % 3])
            ts = start + datetime.timedelta(days=i)
            id_list.append(tskey.add({'k': '%s-%s' % (keys[i % 3], i)}, id=ts))
        return id_list

    def assertRecords(self, results, expected_ids):
        rdata = [(r.stream, r.timestamp, r.data) for r in results]
        streams = ('sa', 'sb', 'sc')
        edata = [(streams[i % 3], datetime.datetime(2018, 1, i + 1),
                  {'k': '%s-%s' % (streams[i % 3], i)}) for i in expected_ids]
        self.assertEqual(rdata, edata)

    @stream_test
    def test_timeseries_ranges(self):
        docids = self._create_test_data()

        self.ts.create()
        self.assertRecords(self.ts.sa.range(), [0, 3, 6, 9])
        self.assertRecords(self.ts.sb.range(), [1, 4, 7])
        self.assertRecords(self.ts.sc.range(), [2, 5, 8])
        self.assertRecords(self.ts.sc.range(count=2), [2, 5])

        self.assertRecords(self.ts.sa[:docids[4]], [0, 3])
        self.assertRecords(self.ts.sb[:docids[4]], [1, 4])
        self.assertRecords(self.ts.sa[docids[4]:], [6, 9])
        self.assertRecords(self.ts.sb[docids[4]:], [4, 7])

        self.assertRecords([self.ts.sa.get(docids[6])], [6])
        self.assertRecords([self.ts.sa.get(docids[9])], [9])
        self.assertRecords([self.ts.sc.get(docids[5])], [5])
        self.assertTrue(self.ts.sa.get(docids[5]) is None)
        self.assertTrue(self.ts.sb.get(docids[5]) is None)

    @stream_test
    def test_timeseries_stream_read(self):
        docids = self._create_test_data()
        self.ts.create()
        self.assertRecords(self.ts.read(count=1), [0, 1, 2])
        self.assertRecords(self.ts.read(count=1), [3, 4, 5])
        self.assertRecords(self.ts.read(count=2), [6, 7, 8, 9])
