from __future__ import print_function

from weakref import WeakValueDictionary

from twisted.internet.defer import Deferred, CancelledError

from spinoff.util.pattern_matching import IN
from spinoff.util.logging import log, dbg


class Container(object):
    """Holds and helps manage a set of subordinate actors.

    Can spawn new subordinates, and keep track of them when requested.

    """
    def __init__(self, factory=None):
        self.factory = factory
        self._items = set()
        self._waiting = {}  # WeakValueDictionary()

    def spawn(self, owner, *args, **kwargs):
        """Spawns a new subordinate actor of `owner` and stores it in this container.

        jobs = Container()
        ...
        jobs.spawn(self, Job)
        jobs.spawn(self, Job, some_param=123)

        jobs = Container(Job)
        ...
        jobs.spawn(self)
        jobs.spawn(self, some_param=123)

        jobs = Container(Job.using('abc', some_kwarg=321))
        ...
        jobs.spawn(self, extra_kwarg=123)
        jobs.spawn(self, some_kwarg=123, extra_kwarg=123)
        jobs.spawn(self, 'xyz', some_kwarg=345, extra_kwarg=567)

        """
        return (self._spawn(owner, self.factory, *args, **kwargs)
                if self.factory else
                self._spawn(owner, *args, **kwargs))

    def watch(self, owner, *args, **kwargs):
        return owner.watch(self.spawn(owner, *args, **kwargs))

    def track(self, owner):
        while True:
            msg = owner.get(('terminated', IN(self._items)))
            if isinstance(msg, Deferred):
                msg.addErrback(lambda f: f.trap(CancelledError))
                msg.cancel()
                break
            self._terminated(msg[1])

    def wait_one(self, owner, child=None):
        if child and child not in self._items:
            raise TypeError("Actor %r not contained here" % (child,))

        d = owner.get(('terminated', child or IN(self)))
        d.addCallback(lambda (_, child): self._terminated(child))
        return d

    __doc_wait__ = """Semantic alias for `wait_one`"""
    wait = wait_one

    def _spawn(self, owner, factory=None, *args, **kwargs):
        if not factory:
            raise TypeError("Container.spawn should be called with an actor factory if none was defined for the container")
        actor = owner.watch(owner.spawn(factory.using(*args, **kwargs)))
        self._items.add(actor)
        d = _Deferred()
        self._waiting[actor] = d
        return d

    def _terminated(self, actor):
        try:
            d = self._waiting.pop(actor)
        except KeyError:
            pass
        else:
            d.callback(None)
        self._items.remove(actor)

    def __len__(self):
        return len(self._items)

    def __contains__(self, actor):
        return actor in self._items

    def __iter__(self):
        return iter(self._items)

    def __repr__(self):
        return "<container:%r>" % (self.factory,) if self.factory else "<container>"


class _Deferred(Deferred):
    def when_terminated(self, fn, *args, **kwargs):
        self.addCallback(lambda _: fn(*args, **kwargs))
        self.addErrback(self._report_error)

    def _report_error(self, f):
        f.printBriefTraceback()
