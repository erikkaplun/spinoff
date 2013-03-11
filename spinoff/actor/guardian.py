# coding: utf-8
from __future__ import print_function

from pickle import PicklingError

import gevent
from spinoff.actor.cell import _BaseCell
from spinoff.actor.events import Events, UnhandledMessage
from spinoff.actor.ref import _BaseRef
from spinoff.actor.context import get_context
from spinoff.util.pattern_matching import ANY


class Guardian(_BaseCell, _BaseRef):
    """The root of an actor hierarchy.

    `Guardian` is a pseudo-actor in the sense that it's implemented in a way that makes it both an actor reference (but
    not a subclass of `Ref`) and an actor (but not a subclass of `Actor`). It only handles spawning of top-level
    actors, and taking care of stopping the entire system when told so.

    Unlike a normal actor, any other actor can directly spawn from under the/a `Guardian`.

    """
    is_local = True  # imitate Ref
    is_stopped = False  # imitate Ref

    root = None
    node = None
    uri = None
    cell = None

    def __init__(self, uri, node):
        self.uri = uri
        self.node = node
        self.root = self
        self._cell = self  # for _BaseCell
        self.all_children_stopped = None

    @property
    def ref(self):
        return self

    def send(self, message, _sender=None):
        if ('_child_terminated', ANY) == message:
            _, sender = message
            self._child_gone(sender)
            if not self._children and self.all_children_stopped:
                self.all_children_stopped.set(None)
            # # XXX: find a better way to avoid Terminated messages for TempActors,
            # # possibly by using a /tmp container for them
            # if not str(sender.uri).startswith('/tempactor'):
            #     Events.log(Terminated(sender))
        elif message == '_stop':
            return self._do_stop()
        elif message == '_kill':
            return self._do_stop(kill=True)
        else:
            if not _sender:
                context = get_context()
                if context:
                    _sender = context.ref
            Events.log(UnhandledMessage(self, message, _sender))

    receive = send

    def _do_stop(self, kill=False):
        if self.children:
            self.all_children_stopped = gevent.event.AsyncResult()
            for actor in self.children:
                (actor.stop if not kill else actor.kill)()
            try:
                self.all_children_stopped.get(timeout=1.0)
            except gevent.Timeout:  # pragma: no cover
                assert not kill
                for actor in self.children:
                    actor.kill()

    def __getstate__(self):  # pragma: no cover
        raise PicklingError("Guardian cannot be serialized")

    def __repr__(self):
        return "<guardian:%s>" % (self.uri,)
