class _Values(list):
    pass


def match(pattern, data, flatten=True):
    def _match(pattern, data, success):
        def _is_ignore(pattern):
            return pattern is not ANY

        is_tuple = isinstance(pattern, tuple)
        if not is_tuple:
            return ((pattern == data or pattern is ANY or pattern is IGNORE) if success else False,
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
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return self.name

    def __str__(self):
        return self.name


ANY = _Marker('ANY')
IGNORE = _Marker('IGNORE')


NOTHING = _Marker('NOTHING')
