from twisted.python.failure import Failure


_old_getstate = Failure.__getstate__


def __getstate__(self):
    tb = self.tb
    ret = _old_getstate(self)
    ret['tb'] = tb
    return ret


# TODO: only do this in DEBUG mode
Failure.__getstate__ = __getstate__
