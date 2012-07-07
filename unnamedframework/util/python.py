import functools


# thanks to:
# http://stackoverflow.com/questions/2589690/creating-a-method-that-is-simultaneously-an-instance-and-class-method

class combomethod(object):

    def __init__(self, method):
        self.method = method

    def __get__(self, obj=None, objtype=None):
        @functools.wraps(self.method)
        def _wrapper(*args, **kwargs):
            if obj is not None:
                return self.method(obj, *args, **kwargs)
            else:
                return self.method(objtype, *args, **kwargs)
        return _wrapper
