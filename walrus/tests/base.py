import os
import unittest

from walrus import Database


HOST = os.environ.get('WALRUS_REDIS_HOST') or '127.0.0.1'
PORT = os.environ.get('WALRUS_REDIS_PORT') or 6379

db = Database(host=HOST, port=PORT, db=15)


REDIS_VERSION = None


class WalrusTestCase(unittest.TestCase):
    def setUp(self):
        db.flushdb()
        db._transaction_local.pipes = []

    def tearDown(self):
        db.flushdb()
        db._transaction_local.pipes = []

    def assertList(self, values, expected):
        values = list(values)
        self.assertEqual(len(values), len(expected))
        for value, item in zip(values, expected):
            self.assertEqual(value, item)
