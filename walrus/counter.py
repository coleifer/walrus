class Counter(object):
    """
    Simple counter.
    """
    def __init__(self, database, name):
        """
        :param database: A walrus ``Database`` instance.
        :param str name: The name for the counter.
        """
        self.database = database
        self.name = name
        self.key = 'counter:%s' % self.name
        self.database.setnx(self.key, 0)

    def decr(self, decr_by=1):
        """Decrement the counter and return the new value."""
        return self.database.decr(self.key, decr_by)

    def incr(self, incr_by=1):
        """Increment the counter and return the new value."""
        return self.database.incr(self.key, incr_by)

    def value(self):
        """Return the current value of the counter."""
        return int(self.database[self.key])

    def _op(self, method, other):
        if isinstance(other, Counter):
            other = other.value()
        if not isinstance(other, int):
            raise TypeError('Counter operand must be an integer: %r' % other)
        method(other)
        return self

    def __iadd__(self, other):
        return self._op(self.incr, other)

    def __isub__(self, other):
        return self._op(self.decr, other)
