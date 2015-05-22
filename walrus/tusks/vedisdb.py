import sys
import unittest

from vedis import Vedis

from walrus import *


class VedisList(List):
    def extend(self, value):
        return self.database.lpush(self.key, *value)

    def pop(self):
        return self.database.lpop(self.key)


class WalrusVedis(Vedis, Database):
    def __init__(self, filename=':memory:'):
        self._filename = filename
        Vedis.__init__(self, filename)

    def __repr__(self):
        if self._filename == ':memory:':
            db_file = 'in-memory database'
        else:
            db_file = self._filename
        return '<WalrusVedis: %s>' % db_file

    def execute_command(self, *args, **options):
        raise ValueError('"%s" is not supported by Vedis.' % args[0])

    def parse_response(self, *args, **kwargs):
        raise RuntimeError('Error, parse_response should not be called.')

    def command(self, command_name, user_data=None):
        return self.register(command_name, user_data=user_data)

    # Compatibility with method names from redis-py.
    def getset(self, key, value):
        return self.get_set(key, value)

    def incrby(self, name, amount=1):
        return self.incr_by(name, amount)

    def decrby(self, name, amount=1):
        return self.decr_by(name, amount)

    # Provide "redis-like" names for the low-level KV-store functions.
    def kset(self, key, value):
        return self.kv_store(key, value)

    def kappend(self, key, value):
        return self.kv_append(key, value)

    def kget(self, key, buf_size=4096, determine_buffer_size=False):
        return self.kv_fetch(key, buf_size, determine_buffer_size)

    def kexists(self, key):
        return self.kv_exists(key)

    def kdel(self, key):
        return self.kv_delete(key)

    # Override certain methods to match either argument signature of Walrus,
    # or to consume a lazily-generated return value.
    def hmset(self, key, values):
        return super(WalrusVedis, self).hmset(key, **values)

    def smembers(self, key):
        return set(super(WalrusVedis, self).smembers(key))

    def sdiff(self, k1, k2):
        return set(super(WalrusVedis, self).sdiff(k1, k2))

    def sinter(self, k1, k2):
        return set(super(WalrusVedis, self).sinter(k1, k2))

    # Override the container types since Vedis provides its own using the
    # same method-names as Walrus, and we want the Walrus containers.
    def Hash(self, key):
        return Hash(self, key)

    def Set(self, key):
        return Set(self, key)

    def List(self, key):
        return VedisList(self, key)

    def not_supported(name):
        def decorator(self, *args, **kwargs):
            raise ValueError('%s is not supported by Vedis.' % name)
        return decorator

    ZSet = not_supported('ZSet')
    Array = not_supported('Array')
    HyperLogLog = not_supported('HyperLogLog')
    pipeline = not_supported('pipeline')
    lock = not_supported('lock')
    pubsub = not_supported('pubsub')


class TestWalrusVedis(unittest.TestCase):
    def setUp(self):
        self.db = WalrusVedis()

    def test_basic(self):
        self.db['foo'] = 'bar'
        self.assertEqual(self.db['foo'], 'bar')
        self.assertTrue('foo' in self.db)
        self.assertFalse('xx' in self.db)
        self.assertIsNone(self.db['xx'])

        self.db.mset(k1='v1', k2='v2', k3='v3')
        results = self.db.mget('k1', 'k2', 'k3', 'kx')
        self.assertEqual(list(results), ['v1', 'v2', 'v3', None])

        self.db.append('foo', 'baz')
        self.assertEqual(self.db.get('foo'), 'barbaz')

        self.db.incr_by('counter', 1)
        self.assertEqual(self.db.incr_by('counter', 5), 6)
        self.assertEqual(self.db.decr_by('counter', 2), 4)

        self.assertEqual(self.db.strlen('foo'), 6)
        self.assertEqual(self.db.getset('foo', 'nug'), 'barbaz')
        self.assertEqual(self.db['foo'], 'nug')

        self.assertFalse(self.db.setnx('foo', 'xxx'))
        self.assertTrue(self.db.setnx('bar', 'yyy'))
        self.assertEqual(self.db['bar'], 'yyy')

        del self.db['foo']
        self.assertFalse('foo' in self.db)

    def test_hash(self):
        h = self.db.Hash('hash_obj')
        h['k1'] = 'v1'
        h.update({'k2': 'v2', 'k3': 'v3'})
        self.assertEqual(h.as_dict(), {'k1': 'v1', 'k2': 'v2', 'k3': 'v3'})

        self.assertEqual(h['k2'], 'v2')
        self.assertIsNone(h['kx'])
        self.assertTrue('k2' in h)
        self.assertEqual(len(h), 3)

        del h['k2']
        del h['kxx']
        self.assertEqual(sorted(h.keys()), ['k1', 'k3'])
        self.assertEqual(sorted(h.values()), ['v1', 'v3'])

    def test_list(self):
        l = self.db.List('list_obj')
        l.prepend('charlie')
        l.extend(['mickey', 'huey', 'zaizee'])
        self.assertEqual(l[0], 'charlie')
        self.assertEqual(l[-1], 'zaizee')
        self.assertEqual(len(l), 4)
        self.assertEqual(l.pop(), 'charlie')

    def test_set(self):
        s = self.db.Set('set_obj')
        s.add('charlie')
        s.add('charlie', 'huey', 'mickey')
        self.assertEqual(len(s), 3)
        self.assertTrue('huey' in s)
        self.assertFalse('xx' in s)
        del s['huey']
        self.assertFalse('huey' in s)
        self.assertEqual(s.members(), set(['charlie', 'mickey']))

        s1 = self.db.Set('s1')
        s2 = self.db.Set('s2')
        s1.add(*range(5))
        s2.add(*range(3, 7))
        self.assertEqual(s1 - s2, set(['0', '1', '2']))
        self.assertEqual(s2 - s1, set(['5', '6']))
        self.assertEqual(s1 & s2, set(['3', '4']))

    def test_kv(self):
        self.db.kset('foo', 'bar')
        self.assertEqual(self.db.kget('foo'), 'bar')
        self.db.kappend('foo', 'xx')
        self.assertEqual(self.db.kget('foo'), 'barxx')
        self.assertTrue(self.db.kexists('foo'))
        self.assertFalse(self.db.kexists('xx'))
        self.db.kdel('foo')
        self.assertFalse(self.db.kexists('foo'))

    def test_unsupported(self):
        def assertUnsupported(cmd, *args):
            method = getattr(self.db, cmd)
            self.assertRaises(ValueError, method, *args)

        # Just check a handful of methods.
        assertUnsupported('zadd', 'zs', 'foo', 1)
        assertUnsupported('ZSet', 'zs')
        assertUnsupported('rpush', 'l_obj', 'val')
        assertUnsupported('rpop', 'l_obj')
        assertUnsupported('ltrim', 'l_obj', 0, 1)
        assertUnsupported('lrem', 'l_obj', 3, 1)

    def test_custom_commands(self):
        @self.db.command('KTITLE')
        def _ktitle_impl(context, key):
            value = context[key]
            if value:
                context[key] = value.title()
                return True
            return False

        self.db['n1'] = 'charlie'
        self.db['n2'] = 'huey'
        self.assertTrue(self.db.KTITLE('n1'))
        self.assertEqual(self.db['n1'], 'Charlie')

        self.assertTrue(self.db.KTITLE('n2'))
        self.assertEqual(self.db['n2'], 'Huey')

        self.assertFalse(self.db.KTITLE('nx'))
        self.assertIsNone(self.db['nx'])


if __name__ == '__main__':
    unittest.main(argv=sys.argv)
