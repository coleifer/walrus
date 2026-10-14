# Hexastore.
import itertools
import json

from walrus.utils import decode
from walrus.utils import encode


def _indexes(s, p, o):
    return (('spo', (s, p, o)), ('pos', (p, o, s)), ('osp', (o, s, p)))


def _check_values(*values):
    for value in values:
        if value is not None and ('::' in value or value.startswith(':') or
                                  value.endswith(':')):
            raise ValueError('Graph values cannot contain "::" or start or '
                             'end with ":": %r' % value)


class _VariableGenerator(object):
    def __getattr__(self, name):
        return Variable(name)

    def __call__(self, name):
        return Variable(name)


class Graph(object):
    """
    Simple hexastore built using Redis ZSets. The basic idea is that we have
    a collection of relationships of the form subject-predicate-object. For
    example:

    * charlie -- friends -- huey
    * charlie -- lives -- Kansas
    * huey -- lives -- Kansas

    We might wish to ask questions of our data-store like "which of charlie's
    friends live in Kansas?" To do this we will store every permutation of
    the S-P-O triples, then we can do efficient queries using the parts of
    the relationship we know:

    * query the "object" portion of the "charlie -- friends" subject
      and predicate.
    * for each object returned, turn it into the subject of a second query
      whose predicate is "lives" and whose object is "Kansas"

    So we would return the subjects that satisfy the following expression::

        ("charlie -- friends") -- lives -- Kansas.

    To accomplish this in Python we could write:

    .. code-block:: python

        db = Database()
        graph = db.graph('people')

        # Store my friends.
        graph.store_many([
            ('charlie', 'friends', 'huey'),
            ('charlie', 'friends', 'zaizee'),
            ('charlie', 'friends', 'nuggie')])

        # Store where people live.
        graph.store_many([
            ('huey', 'lives', 'Kansas'),
            ('zaizee', 'lives', 'Missouri'),
            ('nuggie', 'lives', 'Kansas'),
            ('mickey', 'lives', 'Kansas')])

        # Perform our search. We will use a variable (X) to indicate the
        # value we're interested in.
        X = graph.v.X  # Create a variable placeholder.

        # In the first clause we indicate we are searching for my friends.
        # In the second clause, we only want those friends who also live in
        # Kansas.
        results = graph.search(
            {'s': 'charlie', 'p': 'friends', 'o': X},
            {'s': X, 'p': 'lives', 'o': 'Kansas'})
        print(results)

        # Prints: {'X': {'huey', 'nuggie'}}

    See: http://redis.io/topics/indexes#representing-and-querying-graphs-using-an-hexastore
    """
    #: Create named variables for :py:meth:`Graph.search`, written as
    #: ``graph.v.X`` or ``graph.v('X')``.
    v = _VariableGenerator()

    def __init__(self, walrus, namespace):
        self.walrus = walrus
        self.namespace = namespace
        self._z = self.walrus.ZSet(self.namespace)

    def store(self, s, p, o):
        """
        Store a subject-predicate-object triple in the database.
        """
        self.store_many([(s, p, o)])

    def store_many(self, items):
        """
        Store multiple subject-predicate-object triples in the database.

        :param items: A list of (subj, pred, obj) 3-tuples.
        """
        # One ZADD writes every index key atomically.
        keys = [key for item in items for key in self.keys_for_values(*item)]
        self._z.add(dict.fromkeys(keys, 0))

    def delete(self, s, p, o):
        """Remove the given subj-pred-obj triple from the database."""
        self._z.remove(*self.keys_for_values(s, p, o))

    def keys_for_values(self, s, p, o):
        _check_values(s, p, o)
        for index, values in _indexes(s, p, o):
            yield '::'.join((index,) + values)

    def keys_for_query(self, s=None, p=None, o=None):
        _check_values(s, p, o)
        # Use the index whose leading parts are exactly the ones given.
        for index, values in _indexes(s, p, o):
            known = list(itertools.takewhile(lambda v: v is not None, values))
            if all(v is None for v in values[len(known):]):
                break

        key = '::'.join([index] + known)
        if len(known) == 3:
            return key, None
        # The byte 0xff never occurs in UTF-8, so it sorts after every key
        # with this prefix.
        start = key + '::'
        return start, encode(start) + b'\xff'

    def query(self, s=None, p=None, o=None):
        """
        Return all triples that satisfy the given expression. You may specify
        all or none of the fields (s, p, and o). For instance, if I wanted
        to query for all the people who live in Kansas, I might write:

        .. code-block:: python

            for triple in graph.query(p='lives', o='Kansas'):
                print(triple['s'], 'lives in Kansas!')
        """
        start, end = self.keys_for_query(s, p, o)
        if end is None:
            if start in self._z:
                yield {'s': s, 'p': p, 'o': o}
        else:
            for key in self._z.range_by_lex('[' + start, b'[' + end):
                keys, p1, p2, p3 = decode(key).split('::')
                yield dict(zip(keys, (p1, p2, p3)))

    def search(self, *conditions):
        """
        Given a set of conditions, return all values that satisfy the
        conditions for a given set of variables.

        For example, suppose I wanted to find all of my friends who live in
        Kansas:

        .. code-block:: python

            X = graph.v.X
            results = graph.search(
                {'s': 'charlie', 'p': 'friends', 'o': X},
                {'s': X, 'p': 'lives', 'o': 'Kansas'})

        The return value consists of a dictionary keyed by variable, whose
        values are ``set`` objects containing the values that satisfy the
        query clauses, e.g.:

        .. code-block:: python

            print(results)

            # Result has one key, for our "X" variable. The value is the set
            # of my friends that live in Kansas.
            # {'X': {'huey', 'nuggie'}}

            # We can assume the following triples exist:
            # ('charlie', 'friends', 'huey')
            # ('charlie', 'friends', 'nuggie')
            # ('huey', 'lives', 'Kansas')
            # ('nuggie', 'lives', 'Kansas')
        """
        conditions = [dict(zip('spo', c)) if isinstance(c, tuple) else c
                      for c in conditions]
        names = set(v.name for c in conditions for v in c.values()
                    if isinstance(v, Variable))

        # Each row binds variable names to values. A condition extends every
        # row with the triples that match the values the row already binds.
        rows = [{}]
        for condition in conditions:
            next_rows = []
            for row in rows:
                query, free = {}, []
                for part, value in condition.items():
                    if not isinstance(value, Variable):
                        query[part] = value
                    elif value.name in row:
                        query[part] = row[value.name]
                    else:
                        free.append((part, value.name))

                for triple in self.query(**query):
                    bound = dict(row)
                    # A variable used twice in one condition must match itself.
                    for part, name in free:
                        value = bound.setdefault(name, triple[part])
                        if value != triple[part]:
                            break
                    else:
                        next_rows.append(bound)
            rows = next_rows

        results = dict((name, set()) for name in names)
        for row in rows:
            for name, value in row.items():
                results[name].add(value)
        return results


class Variable(object):
    __slots__ = ['name']

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return '<Variable: %s>' % (self.name)
