class _Values(list):
    pass


def match(pattern, data, flatten=True):
    def _match(pattern, data, success):
        def _is_ignore(pattern):
            return not (isinstance(pattern, _Matcher) and not pattern.ignore)

        is_tuple = isinstance(pattern, tuple)
        if not is_tuple:
            return (pattern == data if success else False,
                    data if not _is_ignore(pattern) else NOTHING)

        values = NOTHING
        data_is_tuple = isinstance(data, tuple)

        for pi in pattern:
            success, subvalues = _match(pi, data[0] if data_is_tuple and data else None, success)
            if subvalues is not NOTHING:
                if values is NOTHING:
                    values = _Values()
                if flatten and isinstance(subvalues, _Values):
                    values += subvalues
                else:
                    values.append(tuple(subvalues) if isinstance(subvalues, _Values) else subvalues)
            data = data[1:] if data_is_tuple and data else None
        if data:
            success = False

        return success, values if values is not NOTHING else NOTHING

    ret = _match(pattern, data, True)
    if flatten and isinstance(ret[1], _Values):
        return (ret[0],) + tuple(ret[1])
    else:
        return ret[0] if ret[1] is NOTHING else (ret[0], tuple(ret[1]) if isinstance(ret[1], _Values) else ret[1])


class _Marker(object):
    def __repr__(self):
        return self.__str__()

    def __str__(self):
        return self.name

    def clone(self):
        return type(self)()


class _Matcher(_Marker):
    ignore = False

    def __req__(self, x):
        return self.__eq__(x)


class _ANY(_Matcher):
    name = 'ANY'

    def __eq__(self, x):
        return True
ANY = _ANY()


def IGNORE(x):
    if isinstance(x, _Matcher):
        x = x.clone()
        x.ignore = True
    return x


class IS_INSTANCE(_Matcher):
    def __init__(self, t):
        self.t = t

    def __eq__(self, x):
        return isinstance(x, self.t)

    def __str__(self):
        return 'IS_INSTANCE(%s)' % self.t

    def clone(self):
        return type(self)(self.t)


class _NOTHING(_Marker):
    name = 'NOTHING'
NOTHING = _NOTHING()
