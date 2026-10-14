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


def _canonical(value):
    # Set and dict order can vary, so sort them at any depth.
    if isinstance(value, (set, frozenset)):
        return type(value), sorted(map(_canonical, value), key=repr)
    elif isinstance(value, dict):
        items = ((_canonical(k), _canonical(v)) for k, v in value.items())
        return dict, sorted(items, key=repr)
    elif isinstance(value, (list, tuple)):
        return type(value), [_canonical(v) for v in value]
    return value


def hash_args(args, kwargs):
    """
    Hash function arguments by pickling them. Sets and dicts are sorted at
    any depth first, so equal calls hash the same in any process.
    """
    return hashlib.md5(pickle.dumps(_canonical((args, kwargs)))).hexdigest()


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
