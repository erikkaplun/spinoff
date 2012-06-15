_ = object()


def match(pattern, value):
    if not isinstance(value, tuple) or len(pattern) != len(value):
        return False, None
    ret = []
    for pi, vi in zip(pattern, value):
        if pi == vi:
            continue
        if pi is _:
            ret.append(vi)
        else:
            return False, None
    return True, tuple(ret)
