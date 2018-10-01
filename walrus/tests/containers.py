import os
import unittest

from walrus.tests.base import WalrusTestCase
from walrus.tests.base import db
from walrus.utils import decode
from walrus.utils import encode


class TestHash(WalrusTestCase):
    def setUp(self):
        super(TestHash, self).setUp()
        self.hsh = db.Hash('my-hash')

    def test_item_api(self):
        self.hsh['k1'] = 'v1'
        self.assertEqual(self.hsh['k1'], b'v1')
        self.assertTrue(self.hsh['kx'] is None)

        self.hsh['k2'] = 'v2'
        self.hsh['k3'] = 'v3'
        self.assertEqual(self.hsh.as_dict(), {
            b'k1': b'v1',
            b'k2': b'v2',
            b'k3': b'v3'})

        del self.hsh['k2']
        self.assertEqual(self.hsh.as_dict(), {b'k1': b'v1', b'k3': b'v3'})

    def test_dict_apis(self):
        self.hsh.update({'k1': 'v1', 'k2': 'v2'})
        self.hsh.update(k3='v3', k4='v4')
        self.assertEqual(sorted(self.hsh.items()), [
            (b'k1', b'v1'),
            (b'k2', b'v2'),
            (b'k3', b'v3'),
            (b'k4', b'v4')])
        self.assertEqual(sorted(self.hsh.keys()), [b'k1', b'k2', b'k3', b'k4'])
        self.assertEqual(sorted(self.hsh.values()),
                         [b'v1', b'v2', b'v3', b'v4'])

        self.assertEqual(len(self.hsh), 4)
        self.assertTrue('k1' in self.hsh)
        self.assertFalse('kx' in self.hsh)

    def test_search_iter(self):
        self.hsh.update(foo='v1', bar='v2', baz='v3')
        self.assertEqual(sorted(self.hsh), [
            (b'bar', b'v2'),
            (b'baz', b'v3'),
            (b'foo', b'v1')])
        self.assertEqual(sorted(self.hsh.search('b*')), [
            (b'bar', b'v2'),
            (b'baz', b'v3')])

    def test_as_dict(self):
        self.hsh.update(k1='v1', k2='v2')
        self.assertEqual(self.hsh.as_dict(True), {'k1': 'v1', 'k2': 'v2'})
        self.assertEqual(db.Hash('test').as_dict(), {})


class TestSet(WalrusTestCase):
    def setUp(self):
        super(TestSet, self).setUp()
        self.set = db.Set('my-set')

    def test_basic_apis(self):
        self.set.add('i1', 'i2', 'i3', 'i2', 'i1')
        self.assertEqual(sorted(self.set), [b'i1', b'i2', b'i3'])

        self.set.remove('i2')
        self.assertEqual(sorted(self.set), [b'i1', b'i3'])

        self.set.remove('ix')
        self.assertEqual(sorted(self.set), [b'i1', b'i3'])

        # Test __contains__
        self.assertTrue('i1' in self.set)
        self.assertFalse('ix' in self.set)

        # Test __iter__.
        self.assertEqual(sorted(self.set), [b'i1', b'i3'])

        del self.set['i3']
        self.assertEqual(sorted(self.set), [b'i1'])

    def test_combining(self):
        self.set2 = db.Set('my-set2')
        self.set.add(1, 2, 3, 4)
        self.set2.add(3, 4, 5, 6)

        self.assertEqual(
            self.set | self.set2,
            set([b'1', b'2', b'3', b'4', b'5', b'6']))
        self.assertEqual(self.set & self.set2, set([b'3', b'4']))
        self.assertEqual(self.set - self.set2, set([b'1', b'2']))
        self.assertEqual(self.set2 - self.set, set([b'5', b'6']))

    def test_combine_store(self):
        self.set2 = db.Set('my-set2')
        self.set.add(1, 2, 3, 4)
        self.set2.add(3, 4, 5, 6)

        s3 = self.set.unionstore('my-set3', self.set2)
        self.assertEqual(s3.members(),
                         set([b'1', b'2', b'3', b'4', b'5', b'6']))

        s3 = self.set.interstore('my-set3', self.set2)
        self.assertEqual(s3.members(), set([b'3', b'4']))

        s3 = self.set.diffstore('my-set3', self.set2)
        self.assertEqual(s3.members(), set([b'1', b'2']))

        self.set |= self.set2
        self.assertEqual(sorted(self.set),
                         [b'1', b'2', b'3', b'4', b'5', b'6'])

        s4 = db.Set('my-set4')
        s4.add('1', '3')
        s3 &= s4
        self.assertEqual(s3.members(), set([b'1']))

    def test_search(self):
        self.set.add('foo', 'bar', 'baz', 'nug')
        self.assertEqual(sorted(self.set.search('b*')), [b'bar', b'baz'])

    def test_sort(self):
        values = ['charlie', 'zaizee', 'mickey', 'huey']
        self.set.add(*values)
        self.assertEqual(self.set.sort(),
                         [b'charlie', b'huey', b'mickey', b'zaizee'])

        self.set.sort(ordering='DESC', limit=3, store='s_dest')
        self.assertList(db.List('s_dest'), [b'zaizee', b'mickey', b'huey'])

    def test_as_set(self):
        self.set.add('foo', 'bar', 'baz')
        self.assertEqual(self.set.as_set(True), set(('foo', 'bar', 'baz')))
        self.assertEqual(db.Set('test').as_set(), set())


class TestZSet(WalrusTestCase):
    def setUp(self):
        super(TestZSet, self).setUp()
        self.zs = db.ZSet('my-zset')

    def assertZSet(self, expected):
        self.assertEqual(list(self.zs), expected)

    def test_basic_apis(self):
        self.zs.add('i1', 1, 'i2', 2)
        self.assertZSet([(b'i1', 1), (b'i2', 2)])

        self.zs.add('i0', 0)
        self.zs.add('i3', 3)
        self.assertZSet([(b'i0', 0), (b'i1', 1), (b'i2', 2), (b'i3', 3)])

        self.zs.remove('i1')
        self.zs.remove_by_score(3)
        self.zs.add('i2', -2)
        self.zs.add('i9', 9)
        self.assertZSet([(b'i2', -2.), (b'i0', 0.), (b'i9', 9.)])

        # __len__
        self.assertEqual(len(self.zs), 3)

        # __contains__
        self.assertTrue('i0' in self.zs)
        self.assertFalse('i1' in self.zs)

        self.assertEqual(self.zs.score('i2'), -2)
        self.assertEqual(self.zs.score('ix'), None)

        self.assertEqual(self.zs.rank('i0'), 1)
        self.assertEqual(self.zs.rank('i1'), None)

        self.assertEqual(self.zs.count(0, 10), 2)
        self.assertEqual(self.zs.count(-3, 11), 3)

        self.zs.incr('i2')
        self.zs.incr('i0', -2)
        self.assertZSet([(b'i0', -2.), (b'i2', -1.), (b'i9', 9.)])

        self.assertEqual(self.zs.range_by_score(0, 9), [b'i9'])
        self.assertEqual(self.zs.range_by_score(-3, 0), [b'i0', b'i2'])

        self.assertEqual(self.zs.popmin_compat(), [(b'i0', -2.)])
        self.assertEqual(len(self.zs), 2)
        self.assertEqual(self.zs.popmax_compat(3),
                              [(b'i9', 9.), (b'i2', -1.)])
        self.assertEqual(self.zs.popmin_compat(), [])
        self.assertEqual(self.zs.popmax_compat(), [])
        self.assertEqual(len(self.zs), 0)

    @unittest.skipIf(not os.environ.get('TEST_ZPOP'), 'skipping zpop tests')
    def test_popmin_popmax(self):
        for i in range(10):
            self.zs.add('i%s' % i, i)

        # a list of item/score tuples is returned.
        self.assertEqual(self.zs.popmin(2), [(b'i0', 0.), (b'i1', 1.)])
        self.assertEqual(self.zs.popmax(2), [(b'i9', 9.), (b'i8', 8.)])

        # when called with no args, a list is still returned.
        self.assertEqual(self.zs.popmin(), [(b'i2', 2.)])
        self.assertEqual(self.zs.popmax(), [(b'i7', 7.)])

        # blocking pop returns single item.
        self.assertEqual(self.zs.bpopmin(), (b'i3', 3.))
        self.assertEqual(self.zs.bpopmax(), (b'i6', 6.))

        # blocking-pop with timeout.
        self.assertEqual(self.zs.bpopmin(2), (b'i4', 4.))
        self.assertEqual(self.zs.bpopmax(2), (b'i5', 5.))

        # empty list is returned when zset is empty.
        self.assertEqual(self.zs.popmin(), [])
        self.assertEqual(self.zs.popmax(), [])

    def test_item_apis(self):
        self.zs['i1'] = 1
        self.zs['i0'] = 0
        self.zs['i3'] = 3
        self.zs['i2'] = 2

        self.assertEqual(self.zs[0, False], [b'i0'])
        self.assertEqual(self.zs[0, True], [(b'i0', 0)])
        self.assertEqual(self.zs[2, False], [b'i2'])
        self.assertEqual(self.zs[2, True], [(b'i2', 2)])
        self.assertEqual(self.zs[-1, True], [(b'i3', 3)])
        self.assertEqual(self.zs[9, True], [])

        self.assertEqual(self.zs[0], [b'i0'])
        self.assertEqual(self.zs[2], [b'i2'])
        self.assertEqual(self.zs[9], [])

        del self.zs['i1']
        del self.zs['i3']
        self.zs['i2'] = -2
        self.zs['i9'] = 9
        self.assertZSet([(b'i2', -2.), (b'i0', 0.), (b'i9', 9.)])

    def test_slicing(self):
        self.zs.add('i1', 1, 'i2', 2, 'i3', 3, 'i0', 0)
        self.assertEqual(self.zs[:1, True], [(b'i0', 0)])
        self.assertEqual(self.zs[1:3, False], [b'i1', b'i2'])
        self.assertEqual(self.zs[1:-1, True], [(b'i1', 1), (b'i2', 2)])

        self.assertEqual(self.zs['i1':, False], [b'i1', b'i2', b'i3'])
        self.assertEqual(self.zs[:'i2', False], [b'i0', b'i1'])
        self.assertEqual(
            self.zs['i0':'i3', True],
            [(b'i0', 0), (b'i1', 1), (b'i2', 2)])
        self.assertRaises(KeyError, self.zs.__getitem__, (slice('i9'), False))
        self.assertEqual(self.zs[99:, False], [])

        del self.zs[:'i2']
        self.assertZSet([(b'i2', 2.), (b'i3', 3.)])
        del self.zs[1:]
        self.assertZSet([(b'i2', 2.)])

    def test_combine_store(self):
        zs2 = db.ZSet('my-zset2')
        self.zs.add(1, 1, 2, 2, 3, 3)
        zs2.add(3, 3, 4, 4, 5, 5)

        zs3 = self.zs.unionstore('my-zset3', zs2)
        self.assertEqual(
            list(zs3),
            [(b'1', 1.), (b'2', 2.), (b'4', 4.), (b'5', 5.), (b'3', 6.)])

        zs3 = self.zs.interstore('my-zset3', zs2)
        self.assertEqual(list(zs3), [(b'3', 6.)])

        self.zs |= zs2
        self.assertZSet([
            (b'1', 1.), (b'2', 2.), (b'4', 4.), (b'5', 5.), (b'3', 6.)])

        zs3 &= zs2
        self.assertEqual(list(zs3), [(b'3', 9.)])

    def test_search(self):
        self.zs.add('foo', 1, 'bar', 2, 'baz', 1, 'nug', 3)
        self.assertEqual(
            list(self.zs.search('b*')),
            [(b'baz', 1.), (b'bar', 2.)])

    def test_sort(self):
        values = ['charlie', 3, 'zaizee', 2, 'mickey', 6, 'huey', 3]
        self.zs.add(*values)
        self.assertEqual(
            self.zs.sort(),
            [b'charlie', b'huey', b'mickey', b'zaizee'])

        self.zs.sort(ordering='DESC', limit=3, store='z_dest')
        res = db.List('z_dest')
        self.assertEqual(list(res), [b'zaizee', b'mickey', b'huey'])

    def test_as_items(self):
        self.zs.add('foo', 3, 'bar', 1, 'baz', 2)
        self.assertEqual(self.zs.as_items(True),
                         [('bar', 1.), ('baz', 2.), ('foo', 3.)])
        self.assertEqual(db.ZSet('test').as_items(), [])


class TestList(WalrusTestCase):
    def setUp(self):
        super(TestList, self).setUp()
        self.lst = db.List('my-list')

    def test_basic_apis(self):
        self.lst.append('i1')
        self.lst.extend(['i2', 'i3'])
        self.lst.prepend('ix')
        self.assertList(self.lst, [b'ix', b'i1', b'i2', b'i3'])

        self.lst.insert('iy', 'i2', 'before')
        self.lst.insert('iz', 'i2', 'after')
        self.assertList(self.lst, [b'ix', b'i1', b'iy', b'i2', b'iz', b'i3'])

        self.assertEqual(self.lst.pop(), b'i3')
        self.assertEqual(self.lst.popleft(), b'ix')
        self.assertEqual(len(self.lst), 4)

    def test_item_apis(self):
        self.lst.append('i0')
        self.assertEqual(self.lst[0], b'i0')

        self.lst.extend(['i1', 'i2'])
        del self.lst['i1']
        self.assertList(self.lst, [b'i0', b'i2'])

        self.lst[1] = 'i2x'
        self.assertList(self.lst, [b'i0', b'i2x'])

        del self.lst[0]
        self.assertList(self.lst, [b'i2x'])

        del self.lst[99]
        self.assertList(self.lst, [b'i2x'])

        del self.lst['ixxx']
        self.assertList(self.lst, [b'i2x'])

    def test_slicing(self):
        self.lst.extend(['i1', 'i2', 'i3', 'i4'])
        self.assertEqual(self.lst[:1], [b'i1'])
        self.assertEqual(self.lst[:2], [b'i1', b'i2'])
        self.assertEqual(self.lst[:-1], [b'i1', b'i2', b'i3'])
        self.assertEqual(self.lst[1:2], [b'i2'])
        self.assertEqual(self.lst[1:], [b'i2', b'i3', b'i4'])

        l = db.List('l1')
        l.extend(range(10))

        # LTRIM, preserve the 1st to last (removes the 0th element).
        del l[1:-1]
        self.assertEqual([int(decode(i)) for i in l],
                         [1, 2, 3, 4, 5, 6, 7, 8, 9])

        # Trim the list so that it contains only the values within the
        # specified range.
        del l[:3]
        self.assertEqual([int(decode(i)) for i in l], [1, 2, 3])

    def test_sort(self):
        values = ['charlie', 'zaizee', 'mickey', 'huey']
        self.lst.extend(values)
        self.assertEqual(self.lst.sort(),
                         [b'charlie', b'huey', b'mickey', b'zaizee'])

        self.lst.sort(ordering='DESC', limit=3, store='l_dest')
        self.assertList(db.List('l_dest'), [b'zaizee', b'mickey', b'huey'])

    def test_as_list(self):
        self.lst.extend(['foo', 'bar'])
        self.assertEqual(self.lst.as_list(True), ['foo', 'bar'])
        self.assertEqual(db.List('test').as_list(), [])


class TestArray(WalrusTestCase):
    def setUp(self):
        super(TestArray, self).setUp()
        self.arr = db.Array('my-arr')

    def test_basic_apis(self):
        self.arr.append('i1')
        self.arr.append('i2')
        self.arr.append('i3')
        self.arr.append('i4')
        self.assertEqual(len(self.arr), 4)

        # Indexing works. Invalid indices return None.
        self.assertEqual(self.arr[0], b'i1')
        self.assertEqual(self.arr[3], b'i4')
        self.assertTrue(self.arr[4] is None)

        # Negative indexing works and includes bounds-checking.
        self.assertEqual(self.arr[-1], b'i4')
        self.assertEqual(self.arr[-4], b'i1')
        self.assertTrue(self.arr[-5] is None)

        self.assertEqual(self.arr.pop(1), b'i2')
        self.assertList(self.arr, [b'i1', b'i3', b'i4'])

        self.assertEqual(self.arr.pop(), b'i4')
        self.assertList(self.arr, [b'i1', b'i3'])

        self.arr[-1] = 'iy'
        self.arr[0] = 'ix'
        self.assertList(self.arr, [b'ix', b'iy'])

        self.assertTrue('iy' in self.arr)
        self.assertFalse('i1' in self.arr)

        self.arr.extend(['foo', 'bar', 'baz'])
        self.assertList(self.arr, [b'ix', b'iy', b'foo', b'bar', b'baz'])

    def test_as_list(self):
        self.arr.extend(['foo', 'bar'])
        self.assertEqual(self.arr.as_list(True), ['foo', 'bar'])
        self.assertEqual(db.Array('test').as_list(), [])
