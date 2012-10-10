import functools


__all__ = ['combomethod', 'EnumValue', 'enums', 'enumrange', 'noreturn', 'clean_tb', 'clean_tb_twisted']


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


class EnumValue(object):
    """Named value in an enumeration which can be ordered."""

    def __init__(self, name, order=None):
        self.name = name
        self.order = order

    def __lt__(self, other):
        if self.order is None or other.order is None:
            raise TypeError
        else:
            return self.order < other.order

    def __str__(self):
        return str(self.name)

    def __repr__(self):
        return '<EnumValue %s%s>' % (self.name, ':%s' % self.order if self.order is not None else '')


def enums(*names):
    """Returns a set of `EnumValue` objects with specified names and optionally orders.

    Values in an enumeration must have unique names and be either all ordered or all unordered.

    """
    if len(names) != len(list(set(names))):
        raise TypeError("Names in an enumeration must be unique")

    item_types = set(True if isinstance(x, tuple) else False for x in names)
    if len(item_types) == 2:
        raise TypeError("Mixing of ordered and unordered enumeration items is not allowed")
    else:
        is_ordered = item_types.pop() is True
        if not is_ordered:
            names = [(None, x) for x in names]
        return [EnumValue(name, order) for order, name in names]


def enumrange(*names):
    """Returns an implicitly ordered enumeration.

    Shorthand for `enums((0, 'A'), (1, 'B'), (2, 'C'), ...)`

    """
    return enums(*[(order, name) for order, name in enumerate(names)])


class _NoReturn(BaseException):
    """Uused internally in the cooperation between noreturn and the customized inlineCallbacks in util._defer."""
    def __init__(self, gen):
        self._gen = gen


def noreturn(gen):
    """Marks a function call that does not return to the current caller.

    Can only be used within generators wrapped with `@inlineCallbacks`. Supports calling of regular functions, and
    functions that either return a generator or a Deferred.

    When used with a function `foo` that returns a `Deferred`, it is functionally equivalent to but more memory
    efficient than `returnValue((yield foo()))`.

    See `spinoff.tests.noreturn_test` for usage examples.

    """
    raise _NoReturn(gen)


def clean_tb(tb_lines, excludes):
    for line in tb_lines:
        if not any(all(exclusion in line for exclusion in exclude) for exclude in excludes):
            yield line


def clean_tb_twisted(tb_lines):
    excludes = [
        ('txcoroutine/__init__.py', '_inlineCallbacks'),
        ('twisted/internet/defer.py', '_inlineCallbacks'),
        ('twisted/python/failure.py', 'throwExceptionIntoGenerator'),
    ]
    return clean_tb(tb_lines, excludes)


def dump_method_call(name, args, kwargs):
    return "%s(%s%s)" % (
        name,
        ", ".join(map(repr, args)),
        "" if not kwargs else ", ".join("%s=%r" % kv for kv in kwargs.items())
    )


def dump_dict(d):
    return '{' + ', '.join('%r: %r' % (k, v) for k, v in d.items()) + '}'
