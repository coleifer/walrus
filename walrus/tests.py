import datetime
import random
import sys
import threading
import time
import unittest

from walrus import *
from walrus.query import OP_AND
from walrus.query import OP_OR
try:
    from walrus.tusks.ledisdb import TestWalrusLedis
except ImportError:
    pass
try:
    from walrus.tusks.rlite import TestWalrusLite
except ImportError:
    pass
try:
    from walrus.tusks.vedisdb import TestWalrusVedis
except ImportError:
    pass
from walrus.utils import encode


if sys.version_info[0] == 3:
    unicode = str


db = Database(db=15)


class BaseModel(Model):
    __database__ = db
    __namespace__ = 'test'

class User(BaseModel):
    username = TextField(primary_key=True)

class Note(BaseModel):
    user = TextField(index=True)
    text = TextField()
    timestamp = DateTimeField(default=datetime.datetime.now, index=True)
    tags = JSONField()

class Message(BaseModel):
    content = TextField(fts=True)
    status = IntegerField(default=1, index=True)

class FTSOptions(BaseModel):
    content = TextField(fts=True, stemmer=True)
    metaphone = TextField(fts=True, stemmer=True, metaphone=True)

class Stat(BaseModel):
    key = AutoIncrementField()
    stat_type = ByteField(index=True)
    value = IntegerField(index=True)

class DefaultOption(BaseModel):
    default_empty = JSONField()
    txt = TextField(default='')
    num = IntegerField(default=0)

cache = db.cache(name='test.cache')


@cache.cached(timeout=60)
def now(seed=None):
    return datetime.datetime.now()

class Clock(object):
    @cache.cached_property()
    def now(self):
        return datetime.datetime.now()


class WalrusTestCase(unittest.TestCase):
    def setUp(self):
        db.flushdb()
        db._transaction_local.pipes = []

    def tearDown(self):
        db.flushdb()
        db._transaction_local.pipes = []

    def assertBytes(self, value, expected):
        if isinstance(expected, unicode) and isinstance(value, bytes):
            expected = expected.encode('utf-8')
        return self.assertEqual(value, expected)

    def assertList(self, values, expected):
        for value, item in zip(values, expected):
            self.assertEquivalent(value, item)

        assert len(values) == len(expected)

    def assertDict(self, values, expected):
        for vkey, ekey in zip(sorted(values), sorted(expected)):
            value = values[vkey]
            item = expected[ekey]
            self.assertEquivalent(vkey, ekey)
            self.assertEquivalent(value, item)

    def assertSet(self, values, expected):
        self.assertList(sorted(values), sorted(expected))

    def assertEquivalent(self, lhs, rhs):
        if isinstance(rhs, dict):
            self.assertDict(lhs, rhs)
        elif isinstance(rhs, (list, tuple)):
            self.assertList(lhs, rhs)
        elif isinstance(rhs, set):
            self.assertSet(lhs, rhs)
        else:
            self.assertBytes(lhs, rhs)


class TestModels(WalrusTestCase):
    def create_objects(self):
        for i in range(3):
            u = User.create(username='u%s' % (i + 1))
            for j in range(3):
                Note.create(
                    user=u.username,
                    text='n%s-%s' % (i + 1, j + 1),
                    tags=['t%s' % (k + 1) for k in range(j)])

    def test_store_none(self):
        class Simple(BaseModel):
            text = TextField()
            number = IntegerField()
            normalized = FloatField()

        s = Simple.create(text=None, number=None, normalized=None)
        s_db = Simple.load(s._id)
        self.assertEqual(s_db.text, '')
        self.assertEqual(s_db.number, 0)
        self.assertEqual(s_db.normalized, 0.)

    def test_create(self):
        self.create_objects()
        self.assertEquivalent(
            sorted(user.username for user in User.all()),
            ['u1', 'u2', 'u3'])

        notes = Note.query(Note.user == 'u1')
        self.assertEquivalent(
            sorted(note.text for note in notes),
            ['n1-1', 'n1-2', 'n1-3'])

        notes = sorted(
            Note.query(Note.user == 'u2'),
            key = lambda note: note._id)
        note = notes[2]
        self.assertEquivalent(note.tags, ['t1', 't2'])

    def test_exceptions(self):
        self.assertRaises(KeyError, User.load, 'charlie')
        User.create(username='charlie')
        user = User.load('charlie')
        self.assertEquivalent(user.username, 'charlie')

    def test_query(self):
        self.create_objects()
        notes = Note.query(Note.user == 'u2')
        self.assertEquivalent(
            sorted(note.text for note in notes),
            ['n2-1', 'n2-2', 'n2-3'])

        user = User.get(User.username == 'u3')
        self.assertEquivalent(user._data, {'username': 'u3'})

        self.assertRaises(ValueError, User.get, User.username == 'ux')

    def test_query_with_update(self):
        stat = Stat.create(stat_type='s1', value=1)
        vq = list(Stat.query(Stat.value == 1))
        self.assertEqual(len(vq), 1)
        stat_db = vq[0]
        self.assertEquivalent(stat_db.stat_type, 's1')
        self.assertEqual(stat_db.value, 1)

        stat.value = 2
        stat.save()

        def assertCount(expr, count):
            self.assertEqual(len(list(Stat.query(expr))), count)

        assertCount(Stat.value == 1, 0)
        assertCount(Stat.value == 2, 1)
        assertCount(Stat.stat_type == 's1', 1)

        stat.stat_type = 's2'
        stat.save()

        assertCount(Stat.value == 1, 0)
        assertCount(Stat.value == 2, 1)
        assertCount(Stat.stat_type == 's1', 0)
        assertCount(Stat.stat_type == 's2', 1)

    def test_sorting(self):
        self.create_objects()
        all_notes = [
            'n1-1', 'n1-2', 'n1-3', 'n2-1', 'n2-2', 'n2-3', 'n3-1', 'n3-2',
            'n3-3']

        notes = Note.query(order_by=Note.text)
        self.assertEquivalent([note.text for note in notes], all_notes)

        notes = Note.query(order_by=Note.text.desc())
        self.assertEquivalent(
            [note.text for note in notes],
            all_notes[::-1])

        notes = Note.query(Note.user == 'u2', Note.text)
        self.assertEquivalent(
            [note.text for note in notes],
            ['n2-1', 'n2-2', 'n2-3'])

        notes = Note.query(Note.user == 'u2', Note.text.desc())
        self.assertEquivalent(
            [note.text for note in notes],
            ['n2-3', 'n2-2', 'n2-1'])

    def test_complex_query(self):
        usernames = ['charlie', 'huey', 'mickey', 'zaizee']
        for username in usernames:
            User.create(username=username)

        def assertUsers(expr, expected):
            users = User.query(expr)
            self.assertEquivalent(
                sorted(user.username for user in users),
                sorted(expected))

        assertUsers(User.username == 'charlie', ['charlie'])
        assertUsers(User.username != 'huey', ['charlie', 'mickey', 'zaizee'])
        assertUsers(
            ((User.username == 'charlie') | (User.username == 'mickey')),
            ['charlie', 'mickey'])
        assertUsers(
            (User.username == 'charlie') | (User.username != 'mickey'),
            ['charlie', 'huey', 'zaizee'])
        expr = (
            ((User.username != 'huey') & (User.username != 'zaizee')) |
            (User.username == 'charlie'))
        assertUsers(expr, ['charlie', 'mickey'])

    def test_scalar_query(self):
        data = [
            ('t1', 1),
            ('t1', 2),
            ('t1', 3),
            ('t2', 10),
            ('t2', 11),
            ('t2', 12),
            ('t3', 0),
        ]
        for stat_type, value in data:
            Stat.create(stat_type=stat_type, value=value)

        stat_objects = sorted(
            (stat for stat in Stat.all()),
            key=lambda stat: stat.key)
        self.assertEquivalent([stat._data for stat in stat_objects], [
            {'key': 1, 'stat_type': 't1', 'value': 1},
            {'key': 2, 'stat_type': 't1', 'value': 2},
            {'key': 3, 'stat_type': 't1', 'value': 3},
            {'key': 4, 'stat_type': 't2', 'value': 10},
            {'key': 5, 'stat_type': 't2', 'value': 11},
            {'key': 6, 'stat_type': 't2', 'value': 12},
            {'key': 7, 'stat_type': 't3', 'value': 0},
        ])

        def assertStats(expr, expected):
            stats = Stat.query(expr)
            self.assertEquivalent(
                sorted(stat.key for stat in stats),
                sorted(expected))

        assertStats(Stat.value <= 3, [1, 2, 3, 7])
        assertStats(Stat.value >= 10, [4, 5, 6])
        assertStats(Stat.value < 3, [1, 2, 7])
        assertStats(Stat.value > 10, [5, 6])

        assertStats(Stat.value == 3, [3])
        assertStats(Stat.value >= 13, [])
        assertStats(
            (Stat.value <= 2) | (Stat.key >= 7),
            [1, 2, 7])
        assertStats(
            ((Stat.value <= 2) & (Stat.key >= 7)) | (Stat.value >= 11),
            [5, 6, 7])
        assertStats(
            ((Stat.value <= 2) | (Stat.key >= 7)) & (Stat.stat_type == 't1'),
            [1, 2])

        assertStats(Stat.value.between(2, 11), [2, 3, 4, 5])
        assertStats(Stat.value.between(4, 12), [4, 5, 6])

    def test_full_text_search(self):
        phrases = [
            ('A faith is a necessity to a man. Woe to him who believes in '
             'nothing.'),
            ('All who call on God in true faith, earnestly from the heart, '
             'will certainly be heard, and will receive what they have asked '
             'and desired.'),
            ('Be faithful in small things because it is in them that your '
             'strength lies.'),
            ('Faith consists in believing when it is beyond the power of '
             'reason to believe.'),
            ('Faith has to do with things that are not seen and hope with '
             'things that are not at hand.')]

        for idx, message in enumerate(phrases):
            Message.create(content=message, status=1 + (idx % 2))

        def assertMatches(query, indexes):
            results = [message.content for message in query]
            self.assertEquivalent(results, [phrases[i] for i in indexes])

        def assertSearch(search, indexes):
            assertMatches(
                Message.query(Message.content.match(search)),
                indexes)

        assertSearch('faith', [4, 3, 2, 0, 1])
        assertSearch('faith man', [0])
        assertSearch('things', [4, 2])
        assertSearch('blah', [])

        query = Message.query(
            Message.content.match('faith') & (Message.status == 1))
        assertMatches(query, [4, 2, 0])

    def test_full_text_combined(self):
        phrases = [
            'little bunny foo foo',  # 0, s=1
            'a little green owl',  # 1, s=2
            'the owl was named foo',  # 2, s=1
            'he had a nicotine patch on his wing',  # 3, s=2
            'he was trying to quit smoking',  # 4, s=1
            'the owl was little and green and sweet',  # 5, s=2
            'he dropped presents on my porch',  # 6, s=1
        ]
        index_to_phrase = {}
        for idx, message in enumerate(phrases):
            msg = Message.create(content=message, status=1 + (idx % 2))
            index_to_phrase[idx] = message

        def assertSearch(search, indexes):
            self.assertEquivalent(
                sorted(message.content for message in query),
                sorted(index_to_phrase[idx] for idx in indexes))

        query = Message.query(Message.content.match('little owl'))
        assertSearch(query, [1, 5, 2])  # "little" is ignored (stop word).

        query = Message.query(
            Message.content.match('little owl') &
            Message.content.match('foo'))
        assertSearch(query, [2])

        query = Message.query(
            (Message.content.match('owl') & (Message.status == 1)) |
            (Message.content.match('foo') & (Message.status == 2)))
        assertSearch(query, [2])

        query = Message.query(
            (Message.content.match('green') & (Message.status == 2)) |
            (Message.status == 1))
        assertSearch(query, [0, 2, 4, 6, 1, 5])

        query = Message.query(
            ((Message.status == 2) & Message.content.match('green')) |
            (Message.status == 1))
        assertSearch(query, [0, 2, 4, 6, 1, 5])

    def test_full_text_options(self):
        phrases = [
            'building web applications with python and flask',
            'modern web development with python',
            'unit testing with python',
            'writing better tests for your application',
            'applications for the web',
        ]

        for phrase in phrases:
            FTSOptions.create(content=phrase, metaphone=phrase)

        def assertMatches(search, indexes, use_metaphone=False):
            if use_metaphone:
                field = FTSOptions.metaphone
            else:
                field = FTSOptions.content
            query = FTSOptions.query(field.match(search))
            results = [message.content for message in query]
            self.assertEquivalent(results, [phrases[i] for i in indexes])

        assertMatches('web application', [4, 0])
        assertMatches('web application', [4, 0], True)

        assertMatches('python', [2, 1, 0])
        assertMatches('python', [2, 1, 0], True)

        assertMatches('testing', [3, 2])
        assertMatches('testing', [3, 2], True)

        # Test behavior of the metaphone algorithm.
        assertMatches('python flasck', [0], True)
        assertMatches('pithon devellepment', [], False)
        assertMatches('pithon devellepment', [1], True)
        assertMatches('younit tessts', [2], True)

    def test_fts_query_parser(self):
        messages = [
            'foo green',
            'bar green',
            'baz blue',
            'nug blue',
            'nize yellow',
            'huey greener',
            'mickey greens',
            'zaizee',
        ]
        for message in messages:
            Message.create(content=message)

        def assertMatches(query, expected, default_conjunction=OP_AND):
            expression = Message.content.search(query, default_conjunction)
            messages = Message.query(expression, order_by=Message.content)
            results = [msg.content for msg in messages]
            self.assertEquivalent(results, expected)

        assertMatches('foo', ['foo green'])
        assertMatches('foo OR baz', ['baz blue', 'foo green'])
        assertMatches('green OR blue', [
            'bar green',
            'baz blue',
            'foo green',
            'mickey greens',
            'nug blue',
        ])
        assertMatches('green AND (bar OR mickey OR nize)', [
            'bar green',
            'mickey greens',
        ])
        assertMatches('zaizee OR (blue AND nug) OR (green AND bar)', [
            'bar green',
            'nug blue',
            'zaizee',
        ])
        assertMatches('(blue AND (baz OR (nug OR huey OR mickey))', [
            'baz blue',
            'nug blue',
        ])
        assertMatches(
            '(blue OR foo) AND (green OR (huey OR (baz AND mickey)))',
            ['foo green'])

        assertMatches('(green AND nug) OR (blue AND bar)', [])
        assertMatches('nuglet', [])
        assertMatches('foobar', [])
        assertMatches('', sorted(messages))

    def test_load(self):
        User.create(username='charlie')
        u = User.load('charlie')
        self.assertEquivalent(u._data, {'username': 'charlie'})

    def test_save_delete(self):
        charlie = User.create(username='charlie')
        huey = User.create(username='huey')
        note = Note.create(user='huey', text='n1')
        note.text = 'n1-edited'
        note.save()

        self.assertEquivalent(
            sorted(user.username for user in User.all()),
            ['charlie', 'huey'])

        notes = Note.all()
        self.assertEquivalent([note.text for note in notes], ['n1-edited'])

        charlie.delete()
        self.assertEquivalent([user.username for user in User.all()], ['huey'])

    def test_delete_indexes(self):
        self.assertEquivalent(set(db.keys()), set())

        Message.create(content='charlie message', status=1)
        Message.create(content='huey message', status=2)

        keys = set(db.keys())
        charlie = Message.load(1)
        charlie.delete()

        huey_keys = set(db.keys())
        diff = keys - huey_keys

        make_key = Message._query.make_key
        self.assertEquivalent(diff, set([
            make_key('_id', 'absolute', 1),
            make_key('content', 'absolute', 'charlie message'),
            make_key('content', 'fts', 'charli'),
            make_key('id', 1),
            make_key('status', 'absolute', 1),
        ]))

        # Ensure we cannot query for Charlie, but that we can query for Huey.
        expressions = [
            (Message.status == 1),
            (Message.status != 2),
            (Message._id == 1),
            (Message._id != 2),
            (Message.content == 'charlie message'),
            (Message.content != 'huey message'),
            (Message.content.match('charlie')),
        ]
        for expression in expressions:
            self.assertRaises(ValueError, Message.get, expression)

        expressions = [
            (Message.status == 2),
            (Message.status > 1),
            (Message._id == 2),
            (Message._id != 1),
            (Message.content == 'huey message'),
            (Message.content != 'charlie'),
            (Message.content.match('huey')),
            (Message.content.match('message')),
        ]
        for expression in expressions:
            obj = Message.get(expression)
            self.assertEquivalent(obj._data, {
                '_id': 2,
                'content': 'huey message',
                'status': 2,
            })

        after_filter_keys = set(db.keys())
        symm_diff = huey_keys ^ after_filter_keys
        self.assertTrue(all(decode(key).startswith('temp')
                            for key in symm_diff))

        huey = Message.load(2)
        huey.delete()

        final_keys = set(key for key in db.keys()
                         if not decode(key).startswith('temp'))
        self.assertEquivalent(final_keys, set([make_key('_id', '_sequence')]))

    def test_get_regression(self):
        Message.create(content='huey', status=1)
        Message.create(content='charlie', status=2)

        def assertMessage(msg, data):
            self.assertEqual(msg._data, data)

        huey = {'_id': 1, 'content': 'huey', 'status': 1}
        charlie = {'_id': 2, 'content': 'charlie', 'status': 2}
        assertMessage(Message.load(1), huey)
        assertMessage(Message.load(2), charlie)

        for i in range(3):
            assertMessage(Message.get(Message._id == 1), huey)
            assertMessage(Message.get(Message._id == 2), charlie)

            assertMessage(Message.get(Message.status == 1), huey)
            assertMessage(Message.get(Message.status == 2), charlie)
            assertMessage(Message.get(Message.status != 1), charlie)
            assertMessage(Message.get(Message.status != 2), huey)

            messages = list(Message.query(Message.status == 1))
            self.assertEqual(len(messages), 1)
            assertMessage(messages[0], huey)
            messages = list(Message.query(Message.status != 1))
            self.assertEqual(len(messages), 1)
            assertMessage(messages[0], charlie)

    def test_index_separator(self):
        class CustomSeparator(BaseModel):
            index_separator = '$'
            name = TextField(primary_key=True)
            data = IntegerField(index=True)

        CustomSeparator.create(name='huey.zai', data=3)
        CustomSeparator.create(name='michael.nuggie', data=5)

        keys = sorted(db.keys())
        self.assertEquivalent(keys, [
            # namespace | model : $-delimited indexed data
            'test|customseparator:all',
            'test|customseparator:data$absolute$3',
            'test|customseparator:data$absolute$5',
            'test|customseparator:data$continuous',
            'test|customseparator:id$huey.zai',
            'test|customseparator:id$michael.nuggie',
            'test|customseparator:name$absolute$huey.zai',
            'test|customseparator:name$absolute$michael.nuggie'])

        huey = CustomSeparator.get(CustomSeparator.data < 5)
        self.assertEquivalent(huey.name, 'huey.zai')

        mickey = CustomSeparator.load('michael.nuggie')
        self.assertEquivalent(mickey.data, 5)

    def test_incr(self):
        for i in range(3):
            Stat.create(stat_type='test', value=i)

        s1 = Stat.get(Stat.value == 1)
        res = s1.incr(Stat.value, 5)
        self.assertEqual(res, 6)
        self.assertEqual(s1.value, 6)

        self.assertRaises(ValueError, Stat.get, Stat.value == 1)
        s6 = Stat.get(Stat.value == 6)
        self.assertEqual(s1.key, s6.key)

    def test_count(self):
        self.assertEqual(User.count(), 0)

        for username in ['charlie', 'leslie', 'connor']:
            User.create(username=username)

        self.assertEqual(User.count(), 3)

    def test_query_delete(self):
        for i in range(5):
            u = User.create(username='u%s' % (i + 1))

        User.query_delete((User.username == 'u1') | (User.username == 'u4'))
        usernames = [user.username for user in User.all()]
        self.assertEqual(sorted(usernames), ['u2', 'u3', 'u5'])

        User.query_delete()
        self.assertEqual([user for user in User.all()], [])

    def test_container_field_persistence(self):
        class HashModel(BaseModel):
            data = HashField()
            name = TextField()

        hm1 = HashModel.create(name='hm1')
        hm1.data.update(k1='v1', k2='v2')

        hm2 = HashModel.create(name='hm2')
        hm2.data.update(k3='v3', k4='v4')

        hm1.name = 'hm1-e'
        hm1.save()

        hm1_db = HashModel.load(hm1._id)
        self.assertEquivalent(hm1_db.name, 'hm1-e')
        self.assertEquivalent(hm1.data.as_dict(), {'k1': 'v1', 'k2': 'v2'})

    def test_delete_container_fields(self):
        class HashModel(BaseModel):
            data = HashField()
            name = TextField()

        hm1 = HashModel.create(name='hm1')
        hm1.data.update(k1='v1', k2='v2')

        hm2 = HashModel.create(name='hm2')
        hm2.data.update(k3='v3', k4='v4')

        hm1.delete()
        self.assertEquivalent(hm1.data.as_dict(), {})
        self.assertEquivalent(hm2.data.as_dict(), {'k3': 'v3', 'k4': 'v4'})

    def test_default_is_an_empty_dict(self):
        instance = DefaultOption()
        self.assertTrue(instance.default_empty is None)
        self.assertEqual(instance.num, 0)
        self.assertEqual(instance.txt, '')

    def test_json_storage(self):
        class APIResponse(BaseModel):
            data = JSONField()

        ar = APIResponse(data={'k1': 'v1', 'k2': 'v2'})
        ar.save()

        ar_db = APIResponse.load(ar._id)
        self.assertEqual(ar_db.data, {'k1': 'v1', 'k2': 'v2'})

    def test_pickled_storage(self):
        class PythonData(BaseModel):
            data = PickledField()

        pd = PythonData(data={'k1': ['v1', None, 'v3']})
        pd.save()

        pd_db = PythonData.load(pd._id)
        self.assertEqual(pd_db.data, {'k1': ['v1', None, 'v3']})

        pd2 = PythonData.create(data=None)
        pd2_db = PythonData.load(pd2._id)
        self.assertTrue(pd2_db.data is None)

    def test_boolean_field(self):
        class Account(BaseModel):
            name = TextField(primary_key=True)
            active = BooleanField()
            admin = BooleanField(default=False)

        charlie = Account(name='charlie', active=True, admin=True)
        huey = Account(name='huey', active=False)
        charlie.save()
        huey.save()

        charlie_db = Account.get(Account.name == 'charlie')
        self.assertTrue(charlie_db.active)
        self.assertTrue(charlie_db.admin)

        huey_db = Account.get(Account.name == 'huey')
        self.assertFalse(huey_db.active)
        self.assertFalse(huey_db.admin)

        huey_db.active = True
        huey_db.admin = True
        huey_db.save()

        huey_db2 = Account.get(Account.name == 'huey')
        self.assertTrue(huey_db2.active)
        self.assertTrue(huey_db2.admin)

    def test_uuid(self):
        class Beacon(BaseModel):
            name = TextField(primary_key=True)
            data = UUIDField()

        b1 = Beacon.create(name='alpha', data=uuid.uuid4())
        b2 = Beacon.create(name='bravo', data=uuid.uuid4())
        b3 = Beacon.create(name='charlie')
        b3_db = Beacon.load('charlie')
        b2_db = Beacon.load('bravo')
        b1_db = Beacon.load('alpha')
        self.assertEqual(b1.data, b1_db.data)
        self.assertEqual(b2.data, b2_db.data)
        self.assertTrue(b3.data is None)

    def _test_date_field(self, field_class, dt_func):
        class Event(BaseModel):
            timestamp = field_class(index=True)
            value = TextField()

        events = [
            Event.create(timestamp=dt_func(i), value='e%s' % i)
            for i in range(1, 11)]

        e_db = Event.get(Event._id == events[-1]._id)
        self.assertEqual(e_db.timestamp, dt_func(10))
        self.assertEqual(e_db.value, 'e10')

        events = Event.query(
            (Event.timestamp >= dt_func(3)) &
            (Event.timestamp < dt_func(7)), Event.timestamp)
        ts2value = [(e.timestamp, e.value) for e in events]
        self.assertEqual(ts2value, [
            (dt_func(3), 'e3'),
            (dt_func(4), 'e4'),
            (dt_func(5), 'e5'),
            (dt_func(6), 'e6')])

        e = Event.create(value='ex')
        e_db = Event.load(e._id)
        self.assertTrue(e_db.timestamp is None)
        self.assertEqual(e_db.value, 'ex')

    def test_datetime_field(self):
        dt = lambda day: datetime.datetime(2018, 1, day, 3, 13, 37)
        self._test_date_field(DateTimeField, dt)

    def test_date_field(self):
        dt = lambda day: datetime.date(2018, 1, day)
        self._test_date_field(DateField, dt)


class TestCache(WalrusTestCase):
    def test_cache_apis(self):
        self.assertEqual(cache.get('foo'), None)
        cache.set('foo', 'bar', 60)
        self.assertEqual(cache.get('foo'), 'bar')
        cache.delete('foo')
        self.assertEqual(cache.get('foo'), None)

    def test_cache_decorator(self):
        n1 = now()
        n2 = now(1)
        self.assertNotEqual(n1, n2)
        self.assertEqual(now(), n1)
        self.assertEqual(now(1), n2)

        now.bust(1)
        self.assertNotEqual(now(1), n2)
        self.assertEqual(now(1), now(1))

    def test_cached_property(self):
        c = Clock()
        n1 = c.now
        n2 = c.now
        self.assertEqual(n1, n2)

        del c.now
        n3 = c.now
        self.assertTrue(n1 != n3)
        self.assertEqual(c.now, n3)

    def test_cached_async(self):
        @cache.cache_async()
        def double_value(value):
            return value * 2

        res = double_value(3)
        self.assertEqual(res(), 6)
        self.assertEqual(res(), 6)

        self.assertEqual(double_value(3)(), 6)
        self.assertEqual(double_value(4)(), 8)

    def test_flush_empty_cache(self):
        cache.set('foo', 'bar', 10)
        self.assertEqual(list(cache.keys()), ['test.cache:foo'])
        cache.flush()
        self.assertEqual(list(cache.keys()), [])
        cache.flush()


class TestHash(WalrusTestCase):
    def setUp(self):
        super(TestHash, self).setUp()
        self.hsh = db.Hash('my-hash')

    def test_item_api(self):
        self.hsh['k1'] = 'v1'
        self.assertEquivalent(self.hsh['k1'], 'v1')
        self.assertEquivalent(self.hsh['kx'], None)

        self.hsh['k2'] = 'v2'
        self.hsh['k3'] = 'v3'
        self.assertEquivalent(self.hsh.as_dict(), {
            'k1': 'v1',
            'k2': 'v2',
            'k3': 'v3'})

        del self.hsh['k2']
        self.assertEquivalent(self.hsh.as_dict(), {'k1': 'v1', 'k3': 'v3'})

    def test_dict_apis(self):
        self.hsh.update({'k1': 'v1', 'k2': 'v2'})
        self.hsh.update(k3='v3', k4='v4')
        self.assertEquivalent(sorted(self.hsh.items()), [
            ('k1', 'v1'),
            ('k2', 'v2'),
            ('k3', 'v3'),
            ('k4', 'v4')])
        self.assertEquivalent(sorted(self.hsh.keys()), ['k1', 'k2', 'k3', 'k4'])
        self.assertEquivalent(sorted(self.hsh.values()), ['v1', 'v2', 'v3', 'v4'])

        self.assertEqual(len(self.hsh), 4)
        self.assertTrue('k1' in self.hsh)
        self.assertFalse('kx' in self.hsh)

    def test_search_iter(self):
        self.hsh.update(foo='v1', bar='v2', baz='v3')
        self.assertEquivalent(sorted(self.hsh), [
            ('bar', 'v2'),
            ('baz', 'v3'),
            ('foo', 'v1'),
        ])
        self.assertEquivalent(sorted(self.hsh.search('b*')), [
            ('bar', 'v2'),
            ('baz', 'v3'),
        ])


class TestSet(WalrusTestCase):
    def setUp(self):
        super(TestSet, self).setUp()
        self.set = db.Set('my-set')

    def test_basic_apis(self):
        self.set.add('i1', 'i2', 'i3', 'i2', 'i1')
        self.assertSet(self.set, ['i1', 'i2', 'i3'])

        self.set.remove('i2')
        self.assertSet(self.set, ['i1', 'i3'])

        self.set.remove('ix')
        self.assertSet(self.set, ['i1', 'i3'])

        # Test __contains__
        self.assertTrue('i1' in self.set)
        self.assertFalse('ix' in self.set)

        # Test __iter__.
        self.assertEquivalent(sorted(self.set), ['i1', 'i3'])

        del self.set['i3']
        self.assertSet(self.set, ['i1'])

    def test_combining(self):
        self.set2 = db.Set('my-set2')
        self.set.add(1, 2, 3, 4)
        self.set2.add(3, 4, 5, 6)

        self.assertEquivalent(
            self.set | self.set2,
            set(['1', '2', '3', '4', '5', '6']))

        self.assertEquivalent(
            self.set & self.set2,
            set(['3', '4']))

        self.assertEquivalent(
            self.set - self.set2,
            set(['1', '2']))
        self.assertEquivalent(
            self.set2 - self.set,
            set(['5', '6']))

    def test_combine_store(self):
        self.set2 = db.Set('my-set2')
        self.set.add(1, 2, 3, 4)
        self.set2.add(3, 4, 5, 6)

        s3 = self.set.unionstore('my-set3', self.set2)
        self.assertSet(s3.members(), set(['1', '2', '3', '4', '5', '6']))

        s3 = self.set.interstore('my-set3', self.set2)
        self.assertSet(s3.members(), set(['3', '4']))

        s3 = self.set.diffstore('my-set3', self.set2)
        self.assertSet(s3.members(), set(['1', '2']))

        self.set |= self.set2
        self.assertSet(self.set, ['1', '2', '3', '4', '5', '6'])

        s4 = db.Set('my-set4')
        s4.add('1', '3')
        s3 &= s4
        self.assertSet(s3.members(), set(['1']))

    def test_search(self):
        self.set.add('foo', 'bar', 'baz', 'nug')
        self.assertEquivalent(sorted(self.set.search('b*')), ['bar', 'baz'])

    def test_sort(self):
        values = ['charlie', 'zaizee', 'mickey', 'huey']
        expected = sorted(values)
        self.set.add(*values)
        self.assertEquivalent(self.set.sort(), expected)

        self.set.sort(ordering='DESC', limit=3, store='s_dest')
        res = db.List('s_dest')
        self.assertEquivalent(list(res), ['zaizee', 'mickey', 'huey'])


class TestZSet(WalrusTestCase):
    def setUp(self):
        super(TestZSet, self).setUp()
        self.zs = db.ZSet('my-zset')

    def assertZSet(self, expected):
        self.assertEquivalent(list(self.zs), expected)

    def test_basic_apis(self):
        self.zs.add('i1', 1, 'i2', 2)
        self.assertZSet([('i1', 1), ('i2', 2)])

        self.zs.add('i0', 0)
        self.zs.add('i3', 3)
        self.assertZSet([('i0', 0), ('i1', 1), ('i2', 2), ('i3', 3)])

        self.zs.remove('i1')
        self.zs.remove_by_score(3)
        self.zs.add('i2', -2)
        self.zs.add('i9', 9)
        self.assertZSet([('i2', -2.), ('i0', 0.), ('i9', 9.)])

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
        self.assertZSet([('i0', -2.), ('i2', -1.), ('i9', 9.)])

        self.assertEquivalent(self.zs.range_by_score(0, 9), ['i9'])
        self.assertEquivalent(self.zs.range_by_score(-3, 0), ['i0', 'i2'])

    def test_item_apis(self):
        self.zs['i1'] = 1
        self.zs['i0'] = 0
        self.zs['i3'] = 3
        self.zs['i2'] = 2

        self.assertEquivalent(self.zs[0, False], ['i0'])
        self.assertEquivalent(self.zs[0, True], [('i0', 0)])
        self.assertEquivalent(self.zs[2, False], ['i2'])
        self.assertEquivalent(self.zs[2, True], [('i2', 2)])
        self.assertEquivalent(self.zs[-1, True], [('i3', 3)])
        self.assertEquivalent(self.zs[9, True], [])

        self.assertEquivalent(self.zs[0], ['i0'])
        self.assertEquivalent(self.zs[2], ['i2'])
        self.assertEquivalent(self.zs[9], [])

        del self.zs['i1']
        del self.zs['i3']
        self.zs['i2'] = -2
        self.zs['i9'] = 9
        self.assertZSet([('i2', -2.), ('i0', 0.), ('i9', 9.)])

    def test_slicing(self):
        self.zs.add('i1', 1, 'i2', 2, 'i3', 3, 'i0', 0)
        self.assertEquivalent(self.zs[:1, True], [('i0', 0)])
        self.assertEquivalent(self.zs[1:3, False], ['i1', 'i2'])
        self.assertEquivalent(self.zs[1:-1, True], [('i1', 1), ('i2', 2)])

        self.assertEquivalent(self.zs['i1':, False], ['i1', 'i2', 'i3'])
        self.assertEquivalent(self.zs[:'i2', False], ['i0', 'i1'])
        self.assertEquivalent(
            self.zs['i0':'i3', True],
            [('i0', 0), ('i1', 1), ('i2', 2)])
        self.assertRaises(KeyError, self.zs.__getitem__, (slice('i9'), False))
        self.assertEquivalent(self.zs[99:, False], [])

        del self.zs[:'i2']
        self.assertZSet([('i2', 2.), ('i3', 3.)])
        del self.zs[1:]
        self.assertZSet([('i2', 2.)])

    def test_combine_store(self):
        zs2 = db.ZSet('my-zset2')
        self.zs.add(1, 1, 2, 2, 3, 3)
        zs2.add(3, 3, 4, 4, 5, 5)

        zs3 = self.zs.unionstore('my-zset3', zs2)
        self.assertEquivalent(
            list(zs3),
            [('1', 1.), ('2', 2.), ('4', 4.), ('5', 5.), ('3', 6.)])

        zs3 = self.zs.interstore('my-zset3', zs2)
        self.assertEquivalent(list(zs3), [('3', 6.)])

        self.zs |= zs2
        self.assertZSet([
            ('1', 1.), ('2', 2.), ('4', 4.), ('5', 5.), ('3', 6.)])

        zs3 &= zs2
        self.assertEquivalent(list(zs3), [('3', 9.)])

    def test_search(self):
        self.zs.add('foo', 1, 'bar', 2, 'baz', 1, 'nug', 3)
        self.assertEquivalent(
            list(self.zs.search('b*')),
            [('baz', 1.), ('bar', 2.)])

    def test_sort(self):
        values = ['charlie', 3, 'zaizee', 2, 'mickey', 6, 'huey', 3]
        self.zs.add(*values)
        self.assertEquivalent(
            self.zs.sort(),
            ['charlie', 'huey', 'mickey', 'zaizee'])

        self.zs.sort(ordering='DESC', limit=3, store='z_dest')
        res = db.List('z_dest')
        self.assertEquivalent(list(res), ['zaizee', 'mickey', 'huey'])


class TestList(WalrusTestCase):
    def setUp(self):
        super(TestList, self).setUp()
        self.lst = db.List('my-list')

    def test_basic_apis(self):
        self.lst.append('i1')
        self.lst.extend(['i2', 'i3'])
        self.lst.prepend('ix')
        self.assertEquivalent(self.lst, ['ix', 'i1', 'i2', 'i3'])

        self.lst.insert('iy', 'i2', 'before')
        self.lst.insert('iz', 'i2', 'after')
        self.assertEquivalent(self.lst, ['ix', 'i1', 'iy', 'i2', 'iz', 'i3'])

        self.assertEquivalent(self.lst.pop(), 'i3')
        self.assertEquivalent(self.lst.popleft(), 'ix')
        self.assertEqual(len(self.lst), 4)

    def test_item_apis(self):
        self.lst.append('i0')
        self.assertEquivalent(self.lst[0], 'i0')

        self.lst.extend(['i1', 'i2'])
        del self.lst['i1']
        self.assertEquivalent(self.lst, ['i0', 'i2'])

        self.lst[1] = 'i2x'
        self.assertEquivalent(self.lst, ['i0', 'i2x'])

        del self.lst[0]
        self.assertEquivalent(self.lst, ['i2x'])

        del self.lst[99]
        self.assertEquivalent(self.lst, ['i2x'])

        del self.lst['ixxx']
        self.assertEquivalent(self.lst, ['i2x'])

    def test_slicing(self):
        self.lst.extend(['i1', 'i2', 'i3', 'i4'])
        self.assertEquivalent(self.lst[:1], ['i1'])
        self.assertEquivalent(self.lst[:2], ['i1', 'i2'])
        self.assertEquivalent(self.lst[:-1], ['i1', 'i2', 'i3'])
        self.assertEquivalent(self.lst[1:2], ['i2'])
        self.assertEquivalent(self.lst[1:], ['i2', 'i3', 'i4'])

        l = db.List('l1')
        l.extend(range(10))
        del l[1:-1]
        self.assertEquivalent([int(i) for i in l], [1, 2, 3, 4, 5, 6, 7, 8, 9])

        del l[:3]
        self.assertEquivalent([int(i) for i in l], [1, 2, 3])

    def test_sort(self):
        values = ['charlie', 'zaizee', 'mickey', 'huey']
        expected = sorted(values)
        self.lst.extend(values)
        self.assertEquivalent(self.lst.sort(), expected)

        self.lst.sort(ordering='DESC', limit=3, store='l_dest')
        res = db.List('l_dest')
        self.assertEquivalent(list(res), ['zaizee', 'mickey', 'huey'])


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
        self.assertEquivalent(self.arr[0], 'i1')
        self.assertEquivalent(self.arr[3], 'i4')
        self.assertEqual(self.arr[4], None)

        # Negative indexing works and includes bounds-checking.
        self.assertEquivalent(self.arr[-1], 'i4')
        self.assertEquivalent(self.arr[-4], 'i1')
        self.assertEqual(self.arr[-5], None)

        self.assertEquivalent(self.arr.pop(1), 'i2')
        self.assertEquivalent(list(self.arr), ['i1', 'i3', 'i4'])

        self.assertEquivalent(self.arr.pop(), 'i4')
        self.assertEquivalent(list(self.arr), ['i1', 'i3'])

        self.arr[-1] = 'iy'
        self.arr[0] = 'ix'
        self.assertEquivalent(list(self.arr), ['ix', 'iy'])

        self.assertTrue(encode('iy') in self.arr)
        self.assertFalse(encode('i1') in self.arr)

        self.arr.extend(['foo', 'bar', 'baz'])
        self.assertEquivalent(list(self.arr), ['ix', 'iy', 'foo', 'bar', 'baz'])


class TestWalrus(WalrusTestCase):
    def test_atomic(self):
        def assertDepth(depth):
            self.assertEqual(len(db._transaction_local.pipes), depth)

        assertDepth(0)
        with db.atomic() as p1:
            assertDepth(1)
            with db.atomic() as p2:
                assertDepth(2)
                with db.atomic() as p3:
                    assertDepth(3)
                    p3.pipe.set('k3', 'v3')

                assertDepth(2)
                self.assertEquivalent(db['k3'], 'v3')

                p2.pipe.set('k2', 'v2')

            assertDepth(1)
            self.assertEquivalent(db['k3'], 'v3')
            self.assertEquivalent(db['k2'], 'v2')
            p1.pipe.set('k1', 'v1')

        assertDepth(0)
        self.assertEquivalent(db['k1'], 'v1')
        self.assertEquivalent(db['k2'], 'v2')
        self.assertEquivalent(db['k3'], 'v3')

    def test_atomic_exception(self):
        def do_atomic(k, v, exc=False):
            with db.atomic() as a:
                a.pipe.set(k, v)
                if exc:
                    raise TypeError('foo')

        do_atomic('k', 'v')
        self.assertEquivalent(db['k'], 'v')

        self.assertRaises(TypeError, do_atomic, 'k2', 'v2', True)
        self.assertRaises(KeyError, lambda: db['k2'])
        self.assertEqual(db._transaction_local.pipe, None)

        # Try nested failure.
        with db.atomic() as outer:
            outer.pipe.set('k2', 'v2')
            self.assertRaises(TypeError, do_atomic, 'k3', 'v3', True)

            # Only this will be set.
            outer.pipe.set('k4', 'v4')

        self.assertEquivalent(db._transaction_local.pipe, None)
        self.assertEquivalent(db['k2'], 'v2')
        self.assertRaises(KeyError, lambda: db['k3'])
        self.assertEquivalent(db['k4'], 'v4')

    def test_clear_transaction(self):
        with db.atomic() as a1:
            a1.pipe.set('k1', 'v1')
            with db.atomic() as a2:
                a2.pipe.set('k2', 'v2')
                a2.clear()

        self.assertEquivalent(db['k1'], 'v1')
        self.assertRaises(KeyError, lambda: db['k2'])

        with db.atomic() as a1:
            a1.pipe.set('k3', 'v3')
            with db.atomic() as a2:
                self.assertRaises(KeyError, lambda: db['k3'])

                a2.pipe.set('k4', 'v4')
                a2.clear()

            a1.pipe.set('k5', 'v5')

        self.assertEquivalent(db['k3'], 'v3')
        self.assertRaises(KeyError, lambda: db['k4'])
        self.assertEquivalent(db['k5'], 'v5')

        self.assertEquivalent(db._transaction_local.pipe, None)


class TestCounter(WalrusTestCase):
    def test_counter(self):
        counter_a = db.counter('counter-a')
        counter_b = db.counter('counter-b')

        self.assertEqual(counter_a.value(), 0)
        self.assertEqual(counter_a.incr(), 1)
        self.assertEqual(counter_a.incr(3), 4)
        self.assertEqual(counter_a.value(), 4)

        self.assertEqual(counter_b.value(), 0)
        counter_b += 3
        self.assertEqual(counter_b.value(), 3)
        counter_b = counter_b + counter_a
        self.assertEqual(counter_b.value(), 7)
        counter_b = counter_b - 5
        self.assertEqual(counter_b.value(), 2)


class TestLock(WalrusTestCase):
    def test_lock(self):
        lock_a = db.lock('lock-a')
        lock_b = db.lock('lock-b')
        self.assertTrue(lock_a.acquire())
        self.assertTrue(lock_b.acquire())

        lock_a2 = db.lock('lock-a')
        self.assertFalse(lock_a2.acquire(block=False))
        self.assertFalse(lock_a2.release())
        self.assertNotEqual(lock_a._lock_id, lock_a2._lock_id)

        self.assertFalse(lock_a.acquire(block=False))
        self.assertFalse(lock_b.acquire(block=False))

        t_waiting = threading.Event()
        t_acquired = threading.Event()
        t_acknowledged = threading.Event()

        def wait_for_lock():
            lock_a = db.lock('lock-a')
            t_waiting.set()
            lock_a.acquire()
            t_acquired.set()
            t_acknowledged.wait()
            lock_a.release()

        waiter_t = threading.Thread(target=wait_for_lock)
        waiter_t.start()
        t_waiting.wait()  # Wait until the thread is up and running.

        lock_a.release()
        t_acquired.wait()
        self.assertFalse(lock_a.acquire(block=False))
        t_acknowledged.set()
        waiter_t.join()
        self.assertTrue(lock_a.acquire(block=False))
        lock_a.release()

    def test_lock_ctx_mgr(self):
        lock_a = db.lock('lock-a')
        lock_a2 = db.lock('lock-a')
        with lock_a:
            self.assertFalse(lock_a2.acquire(block=False))
        self.assertTrue(lock_a2.acquire(block=False))

    def test_lock_decorator(self):
        lock = db.lock('lock-a')

        @lock
        def locked():
            lock2 = db.lock('lock-a')
            self.assertFalse(lock2.acquire(block=False))

        locked()

        @lock
        def raise_exception():
            raise ValueError()

        self.assertRaises(ValueError, raise_exception)

        # In the event of an exception, the lock will still be released.
        self.assertTrue(lock.acquire(block=False))


class TestAutocomplete(WalrusTestCase):
    test_data = (
        (1, 'testing python'),
        (2, 'testing python code'),
        (3, 'web testing python code'),
        (4, 'unit tests with python'))

    def setUp(self):
        super(TestAutocomplete, self).setUp()
        self.autocomplete = db.autocomplete()

    def store_test_data(self, id_to_store=None):
        for obj_id, title in self.test_data:
            if id_to_store is None or obj_id == id_to_store:
                self.autocomplete.store(obj_id, title, {
                    'obj_id': obj_id,
                    'title': title,
                    'value': obj_id % 2 == 0 and 'even' or 'odd'})

    def sort_results(self, results):
        return sorted(results, key=lambda item: item['obj_id'])

    def assertResults(self, results, expected):
        self.assertEquivalent(
            [result['obj_id'] for result in results], expected)

    def assertSearch(self, results, expected):
        self.assertEquivalent(list(results), expected)

    def test_search(self):
        self.store_test_data()

        results = self.autocomplete.search('testing python')
        self.assertSearch(results, [
            {'obj_id': 1, 'title': 'testing python', 'value': 'odd'},
            {'obj_id': 2, 'title': 'testing python code', 'value': 'even'},
            {'obj_id': 3, 'title': 'web testing python code', 'value': 'odd'},
        ])

        results = self.autocomplete.search('test')
        self.assertResults(results, [1, 2, 4, 3])

        results = self.autocomplete.search('uni')
        self.assertResults(results, [4])

        self.assertSearch(self.autocomplete.search(''), [])
        self.assertSearch(self.autocomplete.search('missing'), [])
        self.assertSearch(self.autocomplete.search('the'), [])

    def test_boosting(self):
        letters = ('alpha', 'beta', 'gamma', 'delta')
        n = len(letters)
        test_data = []
        for i in range(n * 3):
            obj_id = i + 1
            obj_type = 't%d' % ((i / n) + 1)
            title = 'test %s' % letters[i % n]
            self.autocomplete.store(
                obj_id,
                title,
                {'obj_id': obj_id, 'title': title},
                obj_type)

        def assertBoosts(query, boosts, expected):
            results = self.autocomplete.search(query, boosts=boosts)
            self.assertEqual(
                [result['obj_id'] for result in results],
                expected)

        assertBoosts('alp', None, [1, 5, 9])
        assertBoosts('alp', {'t2': 1.1}, [5, 1, 9])
        assertBoosts('test', {'t3': 1.5, 't2': 1.1}, [
            9, 10, 12, 11, 5, 6, 8, 7, 1, 2, 4, 3])
        assertBoosts('alp', {'t1': 0.5}, [5, 9, 1])
        assertBoosts('alp', {'t1': 1.5, 't3': 1.6}, [9, 1, 5])
        assertBoosts('alp', {'t3': 1.5, '5': 1.6}, [5, 9, 1])

    def test_stored_boosts(self):
        id_to_type = {
            'aaa': 1,
            'aab': 2,
            'aac': 3,
            'aaab': 4,
            'bbbb': 4}
        for obj_id, obj_type in id_to_type.items():
            self.autocomplete.store(obj_id, obj_type=obj_type)

        results = self.autocomplete.search('aa')
        self.assertSearch(results, [
            'aaa',
            'aaab',
            'aab',
            'aac'])

        self.autocomplete.boost_object(obj_type=2, multiplier=2)
        results = self.autocomplete.search('aa')
        self.assertSearch(results, [
            'aab',
            'aaa',
            'aaab',
            'aac'])

        self.autocomplete.boost_object('aac', multiplier=3)
        results = self.autocomplete.search('aa')
        self.assertSearch(results, [
            'aac',
            'aab',
            'aaa',
            'aaab'])

        results = self.autocomplete.search('aa', boosts={'aac': 1.5})
        self.assertSearch(results, [
            'aab',
            'aac',
            'aaa',
            'aaab'])

    def test_limit(self):
        self.store_test_data()
        results = self.autocomplete.search('testing', limit=1)
        self.assertResults(results, [1])

        results = self.autocomplete.search('testing', limit=2)
        self.assertResults(results, [1, 2])

    def test_search_empty(self):
        gen_exp = self.autocomplete.search('')
        self.assertSearch(gen_exp, [])

    def test_chunked(self):
        for i in range(25):
            self.autocomplete.store('foo %s' % (chr(i + ord('a')) * 2))

        ge = self.autocomplete.search('foo', limit=21, chunk_size=5)
        results = list(ge)
        self.assertEqual(len(results), 21)
        self.assertEqual(results[0], 'foo aa')
        self.assertEqual(results[-1], 'foo uu')

    def test_scoring_error(self):
        self.autocomplete.store('aa bb cc')
        self.autocomplete.store('tt cc')

        results = self.autocomplete.search('cc')
        self.assertSearch(results, ['tt cc', 'aa bb cc'])

        self.autocomplete.store('aa b cc')
        results = self.autocomplete.search('cc')
        self.assertSearch(results, ['tt cc', 'aa b cc', 'aa bb cc'])

    def test_simple(self):
        for _, title in self.test_data:
            self.autocomplete.store(title)

        self.assertSearch(self.autocomplete.search('testing'), [
            'testing python',
            'testing python code',
            'web testing python code'])
        self.assertSearch(self.autocomplete.search('code'), [
            'testing python code',
            'web testing python code'])

        self.autocomplete.store('z python code')
        self.assertSearch(self.autocomplete.search('cod'), [
            'testing python code',
            'z python code',
            'web testing python code'])

    def test_sorting(self):
        strings = []
        for i in range(26):
            strings.append('aaaa%s' % chr(i + ord('a')))
            if i > 0:
                strings.append('aaa%sa' % chr(i + ord('a')))

        random.shuffle(strings)
        for s in strings:
            self.autocomplete.store(s)

        results = self.autocomplete.search('aaa')
        self.assertSearch(results, sorted(strings))

        results = self.autocomplete.search('aaa', limit=30)
        self.assertSearch(results, sorted(strings)[:30])

    def test_removing_objects(self):
        self.store_test_data()
        self.autocomplete.remove(1)

        results = self.autocomplete.search('testing')
        self.assertResults(results, [2, 3])

        self.store_test_data(1)
        self.autocomplete.remove(2)

        results = self.autocomplete.search('testing')
        self.assertResults(results, [1, 3])

        # `2` has already been removed
        with self.assertRaises(KeyError):
            self.autocomplete.remove(2)

    def test_tokenize_title(self):
        self.assertEqual(
            self.autocomplete.tokenize_title('abc def ghi'),
            ['abc', 'def', 'ghi'])

        self.assertEqual(self.autocomplete.tokenize_title('a A tHe an a'), [])
        self.assertEqual(self.autocomplete.tokenize_title(''), [])

        self.assertEqual(self.autocomplete.tokenize_title(
            'The Best of times, the blurst of times'),
            ['times', 'blurst', 'times'])

    def test_exists(self):
        self.assertFalse(self.autocomplete.exists('test'))
        self.autocomplete.store('test')
        self.assertTrue(self.autocomplete.exists('test'))

    def test_key_leaks(self):
        initial_key_count = len(db.keys())

        # store the blog "testing python"
        self.store_test_data(1)

        # see how many keys we have in the db - check again in a bit
        key_len = len(db.keys())

        self.store_test_data(2)
        key_len2 = len(db.keys())

        self.assertTrue(key_len != key_len2)
        self.autocomplete.remove(2)

        # back to the original amount of keys
        self.assertEqual(len(db.keys()), key_len)

        self.autocomplete.remove(1)
        self.assertEqual(len(db.keys()), initial_key_count)

    def test_updating(self):
        self.autocomplete.store('id1', 'title baze', 'd1', 't1')
        self.autocomplete.store('id2', 'title nugget', 'd2', 't2')
        self.autocomplete.store('id3', 'title foo', 'd3', 't3')

        results = self.autocomplete.search('tit')
        self.assertSearch(results, ['d1', 'd3', 'd2'])

        # overwrite the data for id1
        self.autocomplete.store('id1', 'title foo', 'D1', 't1')

        results = self.autocomplete.search('tit')
        self.assertSearch(results, ['D1', 'd3', 'd2'])

        # overwrite the data with a new title, will remove the title one refs
        self.autocomplete.store('id1', 'Herple', 'done', 't1')

        results = self.autocomplete.search('tit')
        self.assertSearch(results, ['d3', 'd2'])

        results = self.autocomplete.search('herp')
        self.assertSearch(results, ['done'])

        self.autocomplete.store('id1', 'title baze', 'Done', 't1')
        results = self.autocomplete.search('tit')
        self.assertSearch(results, ['Done', 'd3', 'd2'])

        # this shows that when we don't clean up crap gets left around
        results = self.autocomplete.search('herp')
        self.assertSearch(results, [])

    def test_word_position_ordering(self):
        self.autocomplete.store('aaaa bbbb')
        self.autocomplete.store('bbbb cccc')
        self.autocomplete.store('bbbb aaaa')
        self.autocomplete.store('aaaa bbbb')

        results = self.autocomplete.search('bb')
        self.assertSearch(results, ['bbbb aaaa', 'bbbb cccc', 'aaaa bbbb'])

        results = self.autocomplete.search('aa')
        self.assertSearch(results, ['aaaa bbbb', 'bbbb aaaa'])

        self.autocomplete.store('aabb bbbb')

        results = self.autocomplete.search('bb')
        self.assertSearch(results, [
            'bbbb aaaa',
            'bbbb cccc',
            'aaaa bbbb',
            'aabb bbbb'])

        results = self.autocomplete.search('aa')
        self.assertSearch(results, [
            'aaaa bbbb',
            'aabb bbbb',
            'bbbb aaaa'])

        # Verify issue 9 is fixed.
        self.autocomplete.store('foo one')
        self.autocomplete.store('bar foo one')

        results = self.autocomplete.search('foo')
        self.assertSearch(results, ['foo one', 'bar foo one'])

    def test_return_all_results(self):
        phrases = ('aa bb', 'aa cc', 'bb aa cc', 'bb cc', 'cc aa bb')
        for phrase in phrases:
            self.autocomplete.store(phrase)

        data = self.autocomplete.list_data()
        self.assertEqual(sorted(data), list(phrases))

        titles = self.autocomplete.list_titles()
        self.assertEqual(sorted(data), list(phrases))


class TestRateLimit(WalrusTestCase):
    def setUp(self):
        super(TestRateLimit, self).setUp()
        # Limit to 5 events per second.
        self.rl = db.rate_limit('test-rl', 5, 1)

    def test_rate_limit(self):
        for i in range(5):
            self.assertFalse(self.rl.limit('k1'))

        for i in range(3):
            self.assertTrue(self.rl.limit('k1'))

        self.assertFalse(self.rl.limit('k2'))

    def test_rate_limit_rollover(self):
        rl = db.rate_limit('test-rl2', 3, 100)
        container = db.List('test-rl2:k1')

        now = time.time()
        past = now - 101

        # Simulate two events.
        container.extend([now, now])

        # Third event goes through OK.
        self.assertFalse(rl.limit('k1'))

        # Fourth event is rate-limited.
        self.assertTrue(rl.limit('k1'))

        # There are three timestamps in the container.
        self.assertEqual(len(container), 3)

        # Hand modify the oldest timestamp to appear as if it happened over
        # 100 seconds ago.
        container[-1] = past

        # We can again perform an action.
        self.assertFalse(rl.limit('k1'))

        # We once again have 3 items all within the last 100 seconds, so we
        # are rate-limited.
        self.assertTrue(rl.limit('k1'))

        # There are only 3 items in the container.
        self.assertEqual(len(container), 3)

        # The oldest item is the 2nd we added at the beginning of the test.
        self.assertEqual(float(container[-1]), now)

        # Remove an item and make the 2nd timestamp (oldest) in the past. This
        # gives us 2 actions.
        container.popright()
        container[-1] = past

        self.assertFalse(rl.limit('k1'))
        self.assertFalse(rl.limit('k1'))
        self.assertTrue(rl.limit('k1'))

    def test_decorator(self):
        rl = db.rate_limit('test-rl2', 3, 100)
        container = db.List('test-rl2:fake-key')

        def key_fn(*args, **kwargs):
            return 'fake-key'

        @rl.rate_limited(key_function=key_fn)
        def do_test():
            return 'OK'

        now = time.time()
        container.extend([now, now])

        self.assertEqual(do_test(), 'OK')
        self.assertRaises(RateLimitException, do_test)

        container.popright()
        container[-1] = now - 101

        self.assertEqual(do_test(), 'OK')
        self.assertEqual(do_test(), 'OK')
        self.assertRaises(RateLimitException, do_test)


class TestGraph(WalrusTestCase):
    def setUp(self):
        super(TestGraph, self).setUp()
        # Limit to 5 events per second.
        self.g = db.graph('test-graph')

    def create_graph_data(self):
        data = (
            ('charlie', 'likes', 'huey'),
            ('charlie', 'likes', 'mickey'),
            ('charlie', 'likes', 'zaizee'),
            ('charlie', 'is', 'human'),
            ('connor', 'likes', 'huey'),
            ('connor', 'likes', 'mickey'),
            ('huey', 'eats', 'catfood'),
            ('huey', 'is', 'cat'),
            ('mickey', 'eats', 'anything'),
            ('mickey', 'is', 'dog'),
            ('zaizee', 'eats', 'catfood'),
            ('zaizee', 'is', 'cat'),
        )
        self.g.store_many(data)

    def create_friends(self):
        data = (
            ('charlie', 'friend', 'huey'),
            ('huey', 'friend', 'charlie'),
            ('huey', 'friend', 'mickey'),
            ('zaizee', 'friend', 'charlie'),
            ('zaizee', 'friend', 'mickey'),
            ('mickey', 'friend', 'nuggie'),
        )
        for item in data:
            self.g.store(*item)

    def test_search_extended(self):
        self.create_graph_data()
        X = self.g.v.x
        Y = self.g.v.y
        Z = self.g.v.z
        result = self.g.search(
            (X, 'likes', Y),
            (Y, 'is', 'cat'),
            (Z, 'likes', Y))
        self.assertEqual(result['x'], set(['charlie', 'connor']))
        self.assertEqual(result['y'], set(['huey', 'zaizee']))
        self.assertEqual(result['z'], set(['charlie', 'connor']))

        self.g.store_many((
            ('charlie', 'likes', 'connor'),
            ('connor', 'likes', 'charlie'),
            ('connor', 'is', 'baby'),
            ('connor', 'is', 'human'),
            ('nash', 'is', 'baby'),
            ('nash', 'is', 'human'),
            ('connor', 'lives', 'ks'),
            ('nash', 'lives', 'nv'),
            ('charlie', 'lives', 'ks')))

        result = self.g.search(
            ('charlie', 'likes', X),
            (X, 'is', 'baby'),
            (X, 'lives', 'ks'))
        self.assertEqual(result, {'x': set(['connor'])})

        result = self.g.search(
            (X, 'is', 'baby'),
            (X, 'likes', Y),
            (Y, 'lives', 'ks'))
        self.assertEqual(result, {
            'x': set(['connor']),
            'y': set(['charlie']),
        })

    def assertTriples(self, result, expected):
        result = list(result)
        self.assertEqual(len(result), len(expected))
        for i1, i2 in zip(result, expected):
            self.assertEqual(
                (i1['s'], i1['p'], i1['o']), i2)

    def test_query(self):
        self.create_graph_data()
        res = self.g.query('charlie', 'likes')
        self.assertTriples(res, (
            ('charlie', 'likes', 'huey'),
            ('charlie', 'likes', 'mickey'),
            ('charlie', 'likes', 'zaizee'),
        ))

        res = self.g.query(p='is', o='cat')
        self.assertTriples(res, (
            ('huey', 'is', 'cat'),
            ('zaizee', 'is', 'cat'),
        ))

        res = self.g.query(s='huey')
        self.assertTriples(res, (
            ('huey', 'eats', 'catfood'),
            ('huey', 'is', 'cat'),
        ))

        res = self.g.query(o='huey')
        self.assertTriples(res, (
            ('charlie', 'likes', 'huey'),
            ('connor', 'likes', 'huey'),
        ))

    def test_search(self):
        self.create_graph_data()
        X = self.g.v('x')
        result = self.g.search(
            {'s': 'charlie', 'p': 'likes', 'o': X},
            {'s': X, 'p': 'eats', 'o': 'catfood'},
            {'s': X, 'p': 'is', 'o': 'cat'})
        self.assertEqual(result, {'x': set(['huey', 'zaizee'])})

    def test_search_simple(self):
        self.create_friends()
        X = self.g.v('x')
        result = self.g.search({'s': X, 'p': 'friend', 'o': 'charlie'})
        self.assertEqual(result, {'x': set(['huey', 'zaizee'])})

    def test_search_2var(self):
        self.create_friends()
        X = self.g.v('x')
        Y = self.g.v('y')

        result = self.g.search(
            {'s': X, 'p': 'friend', 'o': 'charlie'},
            {'s': Y, 'p': 'friend', 'o': X})
        self.assertEqual(result, {
            'x': set(['huey']),
            'y': set(['charlie']),
        })

        result = self.g.search(
            ('charlie', 'friend', X),
            (X, 'friend', Y),
            (Y, 'friend', 'nuggie'))
        self.assertEqual(result, {
            'x': set(['huey']),
            'y': set(['mickey']),
        })

        result = self.g.search(
            ('huey', 'friend', X),
            (X, 'friend', Y))
        self.assertEqual(result['y'], set(['huey', 'nuggie']))

    def test_search_mutual(self):
        self.create_friends()
        X = self.g.v('x')
        Y = self.g.v('y')

        result = self.g.search(
            {'s': X, 'p': 'friend', 'o': Y},
            {'s': Y, 'p': 'friend', 'o': X})
        self.assertEqual(result['y'], set(['charlie', 'huey']))


if __name__ == '__main__':
    unittest.main(argv=sys.argv)
