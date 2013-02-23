# coding: utf-8
from __future__ import print_function

import types
from pickle import PicklingError

import gevent
from spinoff.actor.cell import _BaseCell
from spinoff.actor.events import Events, UnhandledMessage, Terminated, UnhandledError
from spinoff.actor.ref import _BaseRef
from spinoff.actor.supervision import Ignore, Restart, Stop
from spinoff.util.pattern_matching import IS_INSTANCE, ANY


class Guardian(_BaseCell, _BaseRef):
    """The root of an actor hierarchy.

    `Guardian` is a pseudo-actor in the sense that it's implemented in a way that makes it both an actor reference (but
    not a subclass of `Ref`) and an actor (but not a subclass of `Actor`). It only handles spawning of top-level
    actors, supervising them with the default guardian strategy, and taking care of stopping the entire system when
    told so.

    Obviously, unlike a normal actor, any other actor can directly spawn from under the/a `Guardian`.

    """
    is_local = True  # imitate Ref
    is_stopped = False  # imitate Ref

    root = None
    node = None
    uri = None
    cell = None

    def __init__(self, uri, node, supervision=Stop):
        if supervision not in (Stop, Restart, Ignore):
            raise TypeError("Invalid supervision specified for Guardian")
        self.uri = uri
        self.node = node
        self.root = self
        self._cell = self  # for _BaseCell
        self.supervision = supervision
        self.all_children_stopped = None

    @property
    def ref(self):
        return self

    def send(self, message):
        if ('_error', ANY, IS_INSTANCE(Exception), IS_INSTANCE(types.TracebackType) | IS_INSTANCE(basestring)) == message:
            _, sender, exc, tb = message
            Events.log(UnhandledError(sender, exc, tb))
            if self.supervision == Stop:
                sender.stop()
            elif self.supervision == Restart:
                sender << '_restart'
            elif self.supervision == Ignore:
                sender << '_resume'
            else:
                assert False
        elif ('_child_terminated', ANY) == message:
            _, sender = message
            self._child_gone(sender)
            if not self._children and self.all_children_stopped:
                self.all_children_stopped.set(None)
            # XXX: find a better way to avoid Terminated messages for TempActors,
            # possibly by using a /tmp container for them
            if not str(sender.uri).startswith('/tempactor'):
                Events.log(Terminated(sender))
        elif '_stop' == message:
            return self._do_stop()
        else:
            Events.log(UnhandledMessage(self, message))

    receive = send

    def _do_stop(self):
        if self.children:
            self.all_children_stopped = gevent.event.AsyncResult()
            for actor in self.children:
                actor.stop()
            try:
                self.all_children_stopped.get(timeout=3.0)
            except gevent.Timeout:  # pragma: no cover
                raise RuntimeError("actors %r refused to stop" % (self.children,))
                # TODO: force-stop

    def __getstate__(self):  # pragma: no cover
        raise PicklingError("Guardian cannot be serialized")

    def __repr__(self):
        return "<guardian:%s>" % (self.uri,)
