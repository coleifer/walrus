import sys
import unittest

from hirlite.hirlite import Rlite

from walrus import *
from walrus.tusks.helpers import TestHelper


class WalrusLite(Walrus):
    _invalid_callbacks = ('SET', 'MSET', 'LSET')

    def __init__(self, filename=':memory:', encoding='utf-8'):
        self._filename = filename
        self._encoding = encoding
        self._db = Rlite(path=filename, encoding=encoding)
        self.response_callbacks = self.__class__.RESPONSE_CALLBACKS.copy()
        for callback in self._invalid_callbacks:
            del self.response_callbacks[callback]

    def execute_command(self, *args, **options):
        command_name = args[0]
        result = self._db.command(*args)
        return self.parse_response(result, command_name, **options)

    def parse_response(self, result, command_name, **options):
        try:
            return self.response_callbacks[command_name.upper()](
                result, **options)
        except KeyError:
            return result

    def __repr__(self):
        if self._filename == ':memory:':
            db_file = 'in-memory database'
        else:
            db_file = self._filename
        return '<WalrusLite: %s>' % db_file

    def hscan_iter(self, key, *args, **kwargs):
        if args or kwargs:
            raise ValueError('Rlite does not support scanning with arguments.')
        return self.hgetall(key)

    def sscan_iter(self, key, *args, **kwargs):
        if args or kwargs:
            raise ValueError('Rlite does not support scanning with arguments.')
        return self.smembers(key)

    def zscan_iter(self, key, *args, **kwargs):
        if args or kwargs:
            raise ValueError('Rlite does not support scanning with arguments.')
        return self.zrange(key, 0, -1)


class TestWalrusLite(TestHelper, unittest.TestCase):
    def setUp(self):
        self.db = WalrusLite()

    def test_list_set_delete_item(self):
        l = self.db.List('list_obj')
        l.clear()
        l.extend(['i1', 'i2', 'i3', 'i4'])
        l[-1] = 'ix'
        l[1] = 'iy'
        self.assertEqual(list(l), ['i1', 'iy', 'i3', 'ix'])

        l.prepend('nuggie')
        for idx in [-1, 2, 9]:
            del l[idx]
        self.assertEqual([item for item in l], ['nuggie', 'i1', 'i3'])

    def test_set_random_and_pop(self):
        s = self.db.Set('s_obj')
        s.add('charlie', 'mickey')
        self.assertTrue(s.random() in ['charlie', 'mickey'])
        self.assertTrue(s.pop() in ['charlie', 'mickey'])

    def test_zset_iter(self):
        zs = self.db.ZSet('z_obj').clear()
        zs.add('zaizee', 3, 'mickey', 6, 'charlie', 31, 'huey', 3, 'nuggie', 0)

        items = [item for item in zs]
        self.assertEqual(
            items,
            ['nuggie', 'huey', 'zaizee', 'mickey', 'charlie'])


if __name__ == '__main__':
    unittest.main(argv=sys.argv)
