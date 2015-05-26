import sys
import unittest

from ledis import Ledis
from ledis.client import Token

from walrus import *
from walrus.containers import chainable_method


class Scannable(object):
    def _scan(self, cmd, match=None, count=None, ordering=None, limit=None):
        parts = [self.key, '']
        if match:
            parts.extend([Token('MATCH'), match])
        if count:
            parts.extend([Token('COUNT'), count])
        if ordering:
            parts.append(Token(ordering.upper()))
        return self._execute_scan(self.database, cmd, parts, limit)

    def _execute_scan(self, database, cmd, parts, limit=None):
        idx = 0
        while True:
            cursor, rows = database.execute_command(cmd, *parts)
            for row in rows:
                idx += 1
                if limit and idx > limit:
                    cursor = None
                    break
                yield row
            if cursor:
                parts[1] = cursor
            else:
                break


class Sortable(object):
    def _sort(self, cmd, pattern=None, limit=None, offset=None,
              get_pattern=None, ordering=None, alpha=True, store=None):
        parts = [self.key]
        def add_kw(kw, param):
            if param is not None:
                parts.extend([Token(kw), param])
        add_kw('BY', pattern)
        if limit or offset:
            offset = offset or 0
            limit = limit or 'Inf'
            parts.extend([Token('LIMIT'), offset, limit])
        add_kw('GET', get_pattern)
        if ordering:
            parts.append(Token(ordering))
        if alpha:
            parts.append(Token('ALPHA'))
        add_kw('STORE', store)
        return self.database.execute_command(cmd, *parts)


class LedisHash(Scannable, Hash):
    @chainable_method
    def clear(self):
        self.database.hclear(self.key)

    @chainable_method
    def expire(self, ttl=None):
        if ttl is not None:
            self.database.hexpire(self.key, ttl)
        else:
            self.database.hpersist(self.key)

    def __iter__(self):
        return self._scan('XHSCAN')

    def scan(self, match=None, count=None, ordering=None, limit=None):
        if limit is not None:
            limit *= 2  # Hashes yield 2 values.
        return self._scan('XHSCAN', match, count, ordering, limit)


class LedisList(Sortable, List):
    @chainable_method
    def clear(self):
        self.database.lclear(self.key)

    def __setitem__(self, idx, value):
        raise TypeError('Ledis does not support setting values by index.')

    @chainable_method
    def expire(self, ttl=None):
        if ttl is not None:
            self.database.lexpire(self.key, ttl)
        else:
            self.database.lpersist(self.key)

    def sort(self, *args, **kwargs):
        return self._sort('XLSORT', *args, **kwargs)


class LedisSet(Scannable, Sortable, Set):
    @chainable_method
    def clear(self):
        self.database.sclear(self.key)

    @chainable_method
    def expire(self, ttl=None):
        if ttl is not None:
            self.database.sexpire(self.key, ttl)
        else:
            self.database.spersist(self.key)

    def __iter__(self):
        return self._scan('XSSCAN')

    def scan(self, match=None, count=None, ordering=None, limit=None):
        return self._scan('XSSCAN', match, count, ordering, limit)

    def sort(self, *args, **kwargs):
        return self._sort('XSSORT', *args, **kwargs)


class LedisZSet(Scannable, Sortable, ZSet):
    @chainable_method
    def clear(self):
        self.database.zclear(self.key)

    @chainable_method
    def expire(self, ttl=None):
        if ttl is not None:
            self.database.zexpire(self.key, ttl)
        else:
            self.database.zpersist(self.key)

    def __iter__(self):
        return self._scan('XZSCAN')

    def scan(self, match=None, count=None, ordering=None, limit=None):
        if limit:
            limit *= 2
        return self._scan('XZSCAN', match, count, ordering, limit)

    def sort(self, *args, **kwargs):
        return self._sort('XZSORT', *args, **kwargs)


class LedisBitSet(Container):
    def clear(self):
        self.database.delete(self.key)

    def __getitem__(self, idx):
        return self.database.execute_command('GETBIT', self.key, idx)

    def __setitem__(self, idx, value):
        return self.database.execute_command('SETBIT', self.key, idx, value)

    def pos(self, bit, start=None, end=None):
        pieces = ['BITPOS', self.key, bit]
        if start or end:
            pieces.append(start or 0)
        if end:
            pieces.append(end)
        return self.database.execute_command(*pieces)

    def __iand__(self, other):
        self.database.execute_command(
            'BITOP',
            'AND',
            self.key,
            self.key,
            other.key)
        return self

    def __ior__(self, other):
        self.database.execute_command(
            'BITOP',
            'OR',
            self.key,
            self.key,
            other.key)
        return self

    def __ixor__(self, other):
        self.database.execute_command(
            'BITOP',
            'XOR',
            self.key,
            self.key,
            other.key)
        return self

    def __str__(self):
        return self.database[self.key]

    __unicode__ = __str__


class WalrusLedis(Ledis, Scannable, Walrus):
    def __init__(self, *args, **kwargs):
        super(WalrusLedis, self).__init__(*args, **kwargs)

    def __setitem__(self, key, value):
        self.set(key, value)

    def zadd(self, key, *args, **kwargs):
        if not isinstance(args[0], (int, float)):
            reordered = []
            for idx in range(0, len(args), 2):
                reordered.append(args[idx + 1])
                reordered.append(args[idx])
        else:
            reordered = args
        return super(WalrusLedis, self).zadd(key, *reordered, **kwargs)

    def hash_exists(self, key):
        return self.execute_command('HKEYEXISTS', key)

    def __iter__(self):
        return self.scan()

    def scan(self, *args, **kwargs):
        return self._scan('XSCAN', *args, **kwargs)

    def _scan(self, cmd, match=None, count=None, ordering=None, limit=None):
        parts = ['KV', '']
        if match:
            parts.extend([Token('MATCH'), match])
        if count:
            parts.extend([Token('COUNT'), count])
        if ordering:
            parts.append(Token(ordering.upper()))
        return self._execute_scan(self, cmd, parts, limit)

    def update(self, values):
        return self.mset(values)

    def BitSet(self, key):
        return LedisBitSet(self, key)

    def Hash(self, key):
        return LedisHash(self, key)

    def List(self, key):
        return LedisList(self, key)

    def Set(self, key):
        return LedisSet(self, key)

    def ZSet(self, key):
        return LedisZSet(self, key)


class TestWalrusLedis(unittest.TestCase):
    def setUp(self):
        self.db = WalrusLedis()
        self.db.flushall()

    def test_scan(self):
        values = {
            'k1': 'v1',
            'k2': 'v2',
            'k3': 'v3',
            'charlie': 31,
            'mickey': 7,
            'huey': 5}
        self.db.update(values)
        results = self.db.scan()
        expected = ['charlie', 'huey', 'k1', 'k2', 'k3', 'mickey']
        self.assertEqual(list(results), expected)
        self.assertEqual([item for item in self.db], expected)

    def test_string_operations(self):
        self.assertTrue(self.db.set('name', 'charlie'))
        self.assertEqual(self.db.get('name'), 'charlie')
        self.assertIsNone(self.db.get('not-exist'))

        self.assertFalse(self.db.setnx('name', 'huey'))
        self.db.setnx('friend', 'zaizee')
        self.assertEqual(self.db['name'], 'charlie')
        self.assertEqual(self.db['friend'], 'zaizee')

        self.assertTrue(self.db.mset({'k1': 'v1', 'k2': 'v2'}))
        res = self.db.mget('k1', 'k2')
        self.assertEqual(res, ['v1', 'v2'])

        self.db.append('k1', 'xx')
        self.assertEqual(self.db['k1'], 'v1xx')

        del self.db['counter']
        self.assertEqual(self.db.incr('counter'), 1)
        self.assertEqual(self.db.incr('counter', 5), 6)
        self.assertEqual(self.db.decr('counter', 2), 4)

        self.assertEqual(self.db.getrange('name', 3, 5), 'rli')
        self.assertEqual(self.db.getset('k2', 'baze'), 'v2')
        self.assertEqual(self.db['k2'], 'baze')
        self.assertEqual(self.db.strlen('name'), 7)

        self.db['data'] = '\x07'
        self.assertEqual(self.db.bitcount('data'), 3)

        del self.db['name']
        self.assertIsNone(self.db.get('name'))
        self.assertRaises(KeyError, lambda: self.db['name'])

        self.assertFalse('name' in self.db)
        self.assertTrue('k1' in self.db)

    def test_hash(self):
        h = self.db.Hash('hash_obj')
        h.clear()

        h['k1'] = 'v1'
        h.update({'k2': 'v2', 'k3': 'v3'})
        self.assertEqual(h.as_dict(), {'k1': 'v1', 'k2': 'v2', 'k3': 'v3'})

        items = [item for item in h]
        self.assertEqual(items, ['k1', 'v1', 'k2', 'v2', 'k3', 'v3'])
        items = [item for item in h.scan(limit=2)]
        self.assertEqual(items, ['k1', 'v1', 'k2', 'v2'])

        self.assertEqual(h['k2'], 'v2')
        self.assertIsNone(h['k4'])
        self.assertTrue('k2' in h)
        self.assertFalse('k4' in h)

        del h['k2']
        del h['k4']
        self.assertEqual(sorted(h.keys()), ['k1', 'k3'])
        self.assertEqual(sorted(h.values()), ['v1', 'v3'])
        self.assertEqual(len(h), 2)

        self.assertEqual(h['k1', 'k2', 'k3'], ['v1', None, 'v3'])

        self.assertEqual(h.incr('counter'), 1)
        self.assertEqual(h.incr('counter', 3), 4)

    def test_list(self):
        l = self.db.List('list_obj')
        l.clear()

        l.append('charlie')
        l.extend(['mickey', 'huey', 'zaizee'])
        self.assertEqual(l[1], 'mickey')
        self.assertEqual(l[-1], 'zaizee')

        self.assertEqual(l[:], ['charlie', 'mickey', 'huey', 'zaizee'])
        self.assertEqual(l[1:-1], ['mickey', 'huey'])
        self.assertEqual(l[2:], ['huey', 'zaizee'])
        self.assertEqual(len(l), 4)

        l.prepend('nuggie')
        l.popright()
        l.popright()
        self.assertEqual([item for item in l], ['nuggie', 'charlie', 'mickey'])

        self.assertEqual(l.popleft(), 'nuggie')
        self.assertEqual(l.popright(), 'mickey')

        l.clear()
        self.assertEqual(list(l), [])
        self.assertIsNone(l.popleft())

    def test_set(self):
        s = self.db.Set('set_obj')
        s.clear()

        self.assertTrue(s.add('charlie'))
        self.assertFalse(s.add('charlie'))
        s.add('huey', 'mickey')
        self.assertEqual(len(s), 3)
        self.assertTrue('huey' in s)
        self.assertFalse('xx' in s)
        self.assertEqual(s.members(), set(['charlie', 'huey', 'mickey']))

        items = [item for item in s]
        self.assertEqual(sorted(items), ['charlie', 'huey', 'mickey'])
        items = [item for item in s.scan(limit=2, ordering='DESC')]
        self.assertEqual(items, ['mickey', 'huey'])

        del s['huey']
        del s['xx']
        self.assertEqual(s.members(), set(['charlie', 'mickey']))

        n1 = self.db.Set('n1')
        n2 = self.db.Set('n2')
        n1.add(*range(5))
        n2.add(*range(3, 7))

        self.assertEqual(n1 - n2, set(['0', '1', '2']))
        self.assertEqual(n2 - n1, set(['5', '6']))
        self.assertEqual(n1 | n2, set(map(str, range(7))))
        self.assertEqual(n1 & n2, set(['3', '4']))

        n1.diffstore('ndiff', n2)
        ndiff = self.db.Set('ndiff')
        self.assertEqual(ndiff.members(), set(['0', '1', '2']))

        n1.interstore('ninter', n2)
        ninter = self.db.Set('ninter')
        self.assertEqual(ninter.members(), set(['3', '4']))

    def test_zset(self):
        zs = self.db.ZSet('zset_obj')
        zs.clear()

        zs.add('charlie', 31, 'huey', 3, 'mickey', 6, 'zaizee', 3, 'nuggie', 0)
        self.assertEqual(zs[1], ['huey'])
        self.assertEqual(zs[1, True], [('huey', 3)])

        self.assertEqual(
            zs[:],
            ['nuggie', 'huey', 'zaizee', 'mickey', 'charlie'])
        self.assertEqual(zs[:2], ['nuggie', 'huey'])
        self.assertEqual(zs[1:3, True], [('huey', 3), ('zaizee', 3)])
        self.assertEqual(zs['huey':'charlie'], ['huey', 'zaizee', 'mickey'])

        self.assertEqual(len(zs), 5)
        self.assertTrue('charlie' in zs)
        self.assertFalse('xx' in zs)

        items = [item for item in zs]
        self.assertEqual(items, [
            'charlie', '31',
            'huey', '3',
            'mickey', '6',
            'nuggie', '0',
            'zaizee', '3',
        ])
        items = [item for item in zs.scan(limit=3, ordering='DESC')]
        self.assertEqual(items, [
            'zaizee', '3',
            'nuggie', '0',
            'mickey', '6',
        ])

        self.assertEqual(zs.score('charlie'), 31.)
        self.assertIsNone(zs.score('xx'))

        self.assertEqual(zs.rank('mickey'), 3)
        self.assertIsNone(zs.rank('xx'))

        self.assertEqual(zs.count(0, 5), 3)
        self.assertEqual(zs.count(6, 31), 2)
        self.assertEqual(zs.count(6, 30), 1)

        zs.incr('mickey')
        self.assertEqual(zs.score('mickey'), 7.)

        self.assertEqual(zs.range_by_score(0, 5), ['nuggie', 'huey', 'zaizee'])

        zs.remove('nuggie')
        self.assertEqual(zs[:2], ['huey', 'zaizee'])

        del zs['mickey']
        self.assertEqual(zs[:], ['huey', 'zaizee', 'charlie'])
        self.assertEqual(len(zs), 3)

        zs.remove_by_score(2, 3)
        self.assertEqual(zs[:], ['charlie'])

        zs.add('huey', 4, 'zaizee', 3, 'beanie', 8)
        zs.remove_by_rank(2)
        self.assertEqual(zs[:], ['zaizee', 'huey', 'charlie'])

        self.assertRaises(KeyError, lambda: zs['xx':])

        z1 = self.db.ZSet('z1')
        z2 = self.db.ZSet('z2')
        z1.add(1, 1, 2, 2, 3, 3)
        z2.add(3, 3, 4, 4, 5, 5)
        z3 = z1.unionstore('z3', z2)
        self.assertEqual(z3[:], ['1', '2', '4', '5', '3'])

        z3 = z1.interstore('z3', z2)
        self.assertEqual(z3[:], ['3'])

    def test_bit_set(self):
        b = self.db.BitSet('bitset_obj')
        b.clear()
        b[0] = 1
        b[1] = 1
        b[2] = 0
        b[3] = 1
        self.assertEqual(self.db[b.key], '\xd0')

        b[4] = 1
        self.assertEqual(self.db[b.key], '\xd8')
        self.assertEqual(b[0], 1)
        self.assertEqual(b[2], 0)

        self.db['b1'] = 'foobar'
        self.db['b2'] = 'abcdef'
        b = self.db.BitSet('b1')
        b2 = self.db.BitSet('b2')
        b &= b2
        self.assertEqual(self.db[b.key], '`bc`ab')
        self.assertEqual(str(b), '`bc`ab')

        self.db['b1'] = '\x00\xff\xf0'
        self.assertEqual(b.pos(1, 0), 8)
        self.assertEqual(b.pos(1, 2), 16)

        self.db['b1'] = '\x00\x00\x00'
        self.assertEqual(b.pos(1), -1)

    def test_sorting(self):
        items = ['charlie', 'zaizee', 'mickey', 'huey']
        sorted_items = sorted(items)

        l = self.db.List('l_obj').clear()
        l.extend(items)
        results = l.sort()
        self.assertEqual(results, sorted_items)

        dest = self.db.List('l_dest')
        l.sort(ordering='DESC', limit=3, store=dest.key)
        results = list(dest)
        self.assertEqual(results, ['zaizee', 'mickey', 'huey'])

        s = self.db.Set('s_obj').clear()
        s.add(*items)
        results = s.sort()
        self.assertEqual(results, sorted_items)

        results = s.sort(ordering='DESC', limit=3)
        self.assertEqual(results, ['zaizee', 'mickey', 'huey'])

        z = self.db.ZSet('z_obj').clear()
        z.add('charlie', 10, 'zaizee', 10, 'mickey', 3, 'huey', 4)
        results = z.sort()
        self.assertEqual(results, sorted_items)

        results = z.sort(ordering='DESC', limit=3)
        self.assertEqual(results, ['zaizee', 'mickey', 'huey'])

    def test_models(self):
        class User(Model):
            database = self.db
            username = TextField(primary_key=True)
            value = IntegerField(index=True)

        for i, username in enumerate(('charlie', 'huey', 'zaizee', 'mickey')):
            User.create(username=username, value=i)

        charlie = User.load('charlie')
        self.assertEqual(charlie.username, 'charlie')
        self.assertEqual(charlie.value, 0)

        query = User.query(
            (User.username == 'charlie') |
            (User.username == 'huey'))
        users = [user.username for user in query]
        self.assertEqual(sorted(users), ['charlie', 'huey'])


if __name__ == '__main__':
    unittest.main(argv=sys.argv)
