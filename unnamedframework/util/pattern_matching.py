def match(pattern, data):
    def _match(pattern, data, success):
        values = NOTHING

        def _is_ignore(pattern):
            return pattern is not ANY

        is_tuple = isinstance(pattern, tuple)
        if not is_tuple:
            return ((pattern == data or pattern is ANY or pattern is IGNORE) if success else False,
                    data if not _is_ignore(pattern) else NOTHING)

        else:
            data_is_tuple = isinstance(data, tuple)
            for pi in pattern:
                success, subvalues = _match(pi, data[0] if data_is_tuple and data else None, success)
                if subvalues is not NOTHING:
                    if values is NOTHING:
                        values = []
                    values.append(subvalues)
                data = data[1:] if data_is_tuple and data else None
            if data:
                success = False
        return success, tuple(values) if values is not NOTHING else NOTHING

    ret = _match(pattern, data, True)
    return ret[0] if ret[1] is NOTHING else ret


ANY = object()
IGNORE = object()


NOTHING = object()
