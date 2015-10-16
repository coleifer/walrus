import hashlib
import pickle
import time
from functools import wraps


class RateLimitException(Exception):
    pass


class RateLimit(object):
    """
    Rate limit implementation. Allows up to `number` of events every `per`
    seconds.
    """
    def __init__(self, database, name='rate-limit', limit=5, per=60,
                 debug=False):
        """
        :param database: :py:class:`Database` instance.
        :param name: Namespace for this cache.
        :param int limit: Number of events allowed during a given time period.
        :param int per: Time period the ``limit` applies to, in seconds.
        :param debug: Disable rate-limit for debugging purposes. All events
                      will appear to be allowed and valid.
        """
        self.database = database
        self.name = name
        self._limit = limit
        self._per = per
        self._debug = debug

    def limit(self, key):
        if self._debug:
            return False

        counter = self.database.List(self.name + ':' + key)
        n = len(counter)
        is_limited = False
        if n < self._limit:
            counter.prepend(str(time.time()))
        else:
            oldest = float(counter[-1])
            if time.time() - oldest < self._per:
                is_limited = True
            else:
                counter.prepend(str(time.time()))
            del counter[:self._limit]
        counter.pexpire(int(self._per * 2000))
        return is_limited

    def rate_limited(self, key_function=None):
        if key_function is None:
            def key_function(*args, **kwargs):
                data = pickle.dumps((args, sorted(kwargs.items())))
                return hashlib.md5(data).hexdigest()

        def decorator(fn):
            @wraps(fn)
            def inner(*args, **kwargs):
                key = key_function(*args, **kwargs)
                if self.limit(key):
                    raise RateLimitException(
                        'Call to %s exceeded %s events in %s seconds.' % (
                            fn.__name__, self._limit, self._per))
                return fn(*args, **kwargs)
            return inner
        return decorator
