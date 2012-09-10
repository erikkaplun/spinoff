class DoubleDict(dict):
    def __init__(self, *args, **kwargs):
        dict.__init__(self, *args, **kwargs)
        self._rev = {v: k for k, v in self.items()}

    def values(self):
        return self._rev.keys()

    def hasvalue(self, v):
        return v in self._rev

    def byvalue(self, v):
        return self._rev[v]

    def byvalue_get(self, v):
        return self._rev.get(v)

    def __setitem__(self, k, v):
        if v in self._rev and self._rev[v] != k:
            raise ValueAlreadyAssociated("Value %r already associated with another key %r" % (v, k))
        dict.__setitem__(self, k, v)
        self._rev[v] = k

    def __delitem__(self, k):
        v = dict.pop(self, k)
        del self._rev[v]
        return v

    def pop(self, k):
        return self.__delitem__(k)

    def popvalue(self, v):
        k = self._rev.pop(v)
        del self[k]
        return k


class MultiDict(dict):
    """A dictionary that allows duplicate keys (but not duplicate pairs)."""
    def __init__(self, *args, **kwargs):
        pairs = args and args[0]
        if pairs and hasattr(pairs, '__iter__'):
            dict.__init__(self)
            for k, v in pairs:
                MultiDict.__setitem__(self, k, v)
        else:
            dict.__init__(self, *args, **kwargs)
            for k, v in dict.iteritems(self):
                dict.__setitem__(self, k, set([v]))

    def __setitem__(self, key, value):
        self.setdefault(key, set()).add(value)

    def iteritems(self):
        for k, vs in dict.iteritems(self):
            for v in vs:
                yield k, v

    def itervalues(self):
        for vs in dict.itervalues(self):
            for v in vs:
                yield v

    def values(self):
        return list(self.itervalues())

    def items(self):
        return list(self.iteritems())

    def get(self, k):
        ret = dict.get(self, k)
        return set() if ret is None else ret

    def pop(self, key):
        ret = self[key]
        del self[key]
        return ret

    def popitem(self):
        for k in self:
            values = self[k]
            v = iter(values).next()
            self.rempair(k, v)
            return (k, v)
        else:
            raise KeyError("MultiDict is empty")

    def update(self, d):
        for k, v in d.iteritems():
            self[k] = v

    def rempair(self, k, v):
        vs = self[k]
        try:
            vs.remove(v)
        except KeyError:
            raise KeyValueError((k, v))
        if not vs:
            del self[k]

    def __repr__(self):
        return "{%s}" % (", ".join("%r: [%s]" % (k, ", ".join(repr(x) for x in self[k])) for k in self),)

    def __str__(self):
        return self.__repr__()


class DoubleMultiDict(MultiDict):
    def __init__(self, *args, **kwargs):
        super(DoubleMultiDict, self).__init__(*args, **kwargs)
        self._rev = MultiDict((v, k) for k, v in self.iteritems())

    def __setitem__(self, k, v):
        super(DoubleMultiDict, self).__setitem__(k, v)
        self._rev[v] = k

    def __delitem__(self, k):
        vs = self.get(k)
        super(DoubleMultiDict, self).__delitem__(k)
        for v in vs:
            self._rev.rempair(v, k)
            # self._rev[v].remove(k)

    def one(self, k):
        ret = self[k]
        if len(ret) == 1:
            return ret.pop()
        else:
            raise Exception("More than 1 value associated with key")

    def hasvalue(self, v):
        return v in self._rev

    def byvalue(self, v):
        return self._rev[v]

    def byvalue_get(self, v):
        return self._rev.get(v)

    def byvalue_one(self, v):
        ret = self.byvalue(v)
        if len(ret) == 1:
            return iter(ret).next()
        else:
            raise Exception("More than 1 key associated with value")

    def delvalue(self, v):
        ks = self._rev.pop(v)
        for k in ks:
            try:
                self.rempair(k, v)
            except KeyValueError:
                pass
        return self

    def popvalue(self, v):
        ks = list(self._rev.pop(v))
        for k in ks:
            x = self[k]
            x.remove(v)
            if not x:
                dict.__delitem__(self, k)
        return ks

    def discardvalue(self, v):
        try:
            return self.popvalue(v)
        except KeyError:
            return None


class KeyValueError(Exception):
    pass


class ValueAlreadyAssociated(KeyValueError):
    pass
