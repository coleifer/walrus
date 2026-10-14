import hashlib
import os
import pickle
import re


def encode(s):
    return s.encode('utf-8') if isinstance(s, str) else s


def decode(s):
    return s.decode('utf-8') if isinstance(s, bytes) else s


def decode_dict(d):
    accum = {}
    for key in d:
        accum[decode(key)] = decode(d[key])
    return accum


def safe_decode_list(l):
    return [i.decode('utf-8', 'backslashreplace') if isinstance(i, bytes)
            else i for i in l]


def _sort_set(value):
    # Set iteration order varies between processes, so sort the members.
    if isinstance(value, (set, frozenset)):
        return type(value), sorted(value, key=repr)
    return value


def hash_args(args, kwargs):
    """
    Hash function arguments by pickling them. Keyword arguments and set
    members are sorted first, so equal calls hash the same in any order and
    in any process.
    """
    data = (tuple(_sort_set(a) for a in args),
            sorted((k, _sort_set(v)) for k, v in kwargs.items()))
    return hashlib.md5(pickle.dumps(data)).hexdigest()


def make_python_attr(s):
    if isinstance(s, bytes):
        s = decode(s)
    s = re.sub(r'[^\w]+', '_', s)
    if not s:
        raise ValueError('cannot construct python identifer from "%s"' % s)
    if s[0].isdigit():
        s = '_' + s
    return s.lower()


class memoize(dict):
    def __init__(self, fn):
        self._fn = fn

    def __call__(self, *args):
        return self[args]

    def __missing__(self, key):
        result = self[key] = self._fn(*key)
        return result


@memoize
def load_stopwords(stopwords_file):
    # Prefer a file bundled with walrus, then the path as given.
    bundled = os.path.join(os.path.dirname(__file__), stopwords_file)
    if os.path.exists(bundled):
        stopwords_file = bundled
    with open(stopwords_file) as fh:
        return fh.read()
