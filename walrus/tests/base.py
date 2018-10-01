import unittest

from walrus import Database


db = Database(db=15)


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
