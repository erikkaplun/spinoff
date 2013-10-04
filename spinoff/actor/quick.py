from spinoff.actor import Actor


def actor(fn):
    class Ret(Actor):
        def receive(self, message):
            return fn(self, message)

    # Ret.__doc__ = fn.__doc__  # XXX: not allowed for some reason
    Ret.__module__ = fn.__module__
    Ret.__name__ = fn.__name__

    return Ret


def process(fn):
    class Ret(Actor):
        def run(self, *args, **kwargs):
            return fn(self, *args, **kwargs)

    # Ret.__doc__ = fn.__doc__  # XXX: not allowed for some reason
    Ret.__module__ = fn.__module__
    Ret.__name__ = fn.__name__

    return Ret
