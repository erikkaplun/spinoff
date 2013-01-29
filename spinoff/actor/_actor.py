# coding: utf-8
from __future__ import print_function

from spinoff.actor.exceptions import Unhandled
from spinoff.actor.props import Props


TESTING = False


class Actor(object):
    """Description here.

    __init__ should not attempt to access `self.ref` or `self.parent` as this are available only on an already
    initialized actor instance. If your initialization routine depends on either of those, use `pre_start` instead.

    """
    SPAWNING_IS_ASYNC = _DEFAULT_SPAWNING_IS_ASYNC = True
    SENDING_IS_ASYNC = _DEFAULT_SENDING_IS_ASYNC = False

    @classmethod
    def using(cls, *args, **kwargs):
        return Props(cls, *args, **kwargs)

    @classmethod
    def reset_flags(cls, debug=False):
        cls.SPAWNING_IS_ASYNC = False if debug else cls._DEFAULT_SPAWNING_IS_ASYNC
        cls.SENDING_IS_ASYNC = cls._DEFAULT_SENDING_IS_ASYNC

    __cell = None  # make it really private so it's hard and unpleasant to access the cell
    _startargs = ((), {})

    def __init__(self, *args, **kwargs):
        self._startargs = args, kwargs

    def receive(self, message):
        raise Unhandled

    def spawn(self, factory, name=None):
        return self.__cell.spawn(factory, name)

    @property
    def children(self):
        return self.__cell.children

    def watch(self, actor, *actors, **kwargs):
        return self.__cell.watch(actor, *actors, **kwargs)

    def unwatch(self, actor, *actors):
        self.__cell.unwatch(actor, *actors)

    @property
    def ref(self):
        return self.__cell.ref

    @property
    def root(self):
        return self.__cell.root

    @property
    def node(self):  # pragma: no cover
        """Returns a reference to the `Node` whose hierarchy this `Actor` is part of."""
        return self.root.node

    def _set_cell(self, cell):
        self.__cell = cell

    # TODO: this is a potential spot for optimisation: we could put the message directly in the Cell's inbox without
    # without invoking the receive logic which will not handle the message immediately anyway because we're processing
    # a message already--otherwise this method would not get called.
    def send(self, *args, **kwargs):
        """Alias for self.ref.send"""
        self.ref.send(*args, **kwargs)

    def __lshift__(self, message):  # pragma: no cover
        self.ref.send(message)
        return self

    def stop(self):
        self.ref.stop()

    def __eq__(self, other):
        return other is self or self.ref == other

    def __ne__(self, other):
        return not (self == other)

    def __repr__(self):
        return "<actor:%s@%s>" % (type(self).__name__, self.ref.uri)
