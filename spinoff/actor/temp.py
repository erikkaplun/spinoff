from twisted.internet.defer import Deferred

from spinoff.actor.process import Process
from spinoff.util.async import with_timeout, call_when_idle


def TempActorFactory(context, timeout=None):
    """Creates a class of temp actors all sharing a common containing actor
    spawned from the `context` node, and (optionally) all sharing a common timeout.

    >>> TempActor = TempActorFactory(our_node)
    """

    class _TempActor(object):
        @classmethod
        def make(cls, **kwargs):
            kwargs.update(timeout=timeout)
            return TempActor.make(context, **kwargs)

    return _TempActor


class TempActor(Process):
    """Create a temporary actor which will fire a deferred when it receives its first reply.
    The temporary actors will be stopped once they receive the first message (usually a reply)
    or when the (optional) timeout occurs.

    >>> temp, d = TempActor.make(timeout=2.0)
    >>> target << ('message', temp)
    >>> res = yield d
    """

    @classmethod
    def make(cls, context, timeout=None):
        d = Deferred()
        return context.spawn(cls.using(d, timeout)), d

    def run(self, d, timeout):
        try:
            msg = yield with_timeout(timeout, self.get())
            call_when_idle(d.errback if isinstance(msg, BaseException) else d.callback, msg)
        except Exception as e:
            call_when_idle(d.errback, e)
