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


class Any(object):
    __eq__ = staticmethod(lambda self_: True)
Any = Any()


class InstanceOf(object):
    def __init__(self, cls):
        self.cls = cls

    def __eq__(self, x):
        return isinstance(x, self.cls)
