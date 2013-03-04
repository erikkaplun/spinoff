# coding: utf-8
from __future__ import print_function

import inspect


# TODO: rename to _UnspawnedActor
class Props(object):
    def __init__(self, cls, *args, **kwargs):
        if hasattr(inspect, 'getcallargs'):
            inspect.getcallargs(cls.__init__, None, *args, **kwargs)
        self.cls, self.args, self.kwargs = cls, args, kwargs

    def __call__(self):
        return self.cls(*self.args, **self.kwargs)

    def using(self, *args, **kwargs):
        args += args
        kwargs.update(self.kwargs)
        return Props(self.cls, *args, **kwargs)

    def __repr__(self):
        args = ', '.join(repr(x) for x in self.args)
        kwargs = ', '.join('%s=%r' % x for x in self.kwargs.items())
        return '<props:%s(%s%s)>' % (self.cls.__name__, args, ((', ' + kwargs) if args else kwargs) if kwargs else '')
