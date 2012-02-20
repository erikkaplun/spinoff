import inspect
import time
from functools import wraps

from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.python import log


def profile(func):
    """
    Simple profile decorator, monitors method execution time
    """
    @inlineCallbacks
    def callme(*args, **kwargs):
        start = time.time()
        ret = yield func(*args, **kwargs)
        time_to_execute = time.time() - start
        log.msg('%s executed in %.3f seconds' % (func.__name__, time_to_execute))
        returnValue(ret)
    return callme


def monkeypatch(method, check):
    def ret(new_impl):
        assert check(), "Check failed, can't monkeypatch"
        overriden = wraps(method)(new_impl)
        setattr(method.im_class, method.im_func.func_name, overriden)
    return ret


def selfdocumenting(f):
    f.__doc__ = inspect.getsource(f)
    return f
