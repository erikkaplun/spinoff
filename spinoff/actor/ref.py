# coding: utf-8
from __future__ import print_function

import abc

from gevent import getcurrent, spawn_later

from spinoff.actor.events import Events, DeadLetter
from spinoff.actor.uri import Uri
from spinoff.actor.misc import TempActor
from spinoff.actor.context import get_context
from spinoff.util.pattern_matching import ANY, IN, Matcher
from spinoff.util.logging import dbg


class _BaseRef(object):
    """Internal abstract class for all objects that behave like actor references."""
    __metaclass__ = abc.ABCMeta

    @abc.abstractproperty
    def is_local(self):
        raise NotImplementedError

    @abc.abstractproperty
    def is_stopped(self):
        raise NotImplementedError

    @abc.abstractproperty
    def uri(self):
        raise NotImplementedError

    def __div__(self, next):
        """Looks up a descendant of this actor.

        Raises `LookupFailed` if this is a local ref and the descendant was not found.

        """
        # local and not dead
        if self.is_local and self._cell:
            return self._cell.lookup_ref(next)
        # non-local or dead
        else:
            return Ref(cell=None, uri=self.uri / next, is_local=self.is_local,
                       node=None if self.is_local else self.node)

    def __lshift__(self, message):
        """A fancy looking alias to `_BaseRef.stop`, which in addition also supports chaining.

            someactor.send(msg1); someactor.send(msg2)
            someactor << msg1 << msg2

        """
        self.send(message)
        return self

    def ask(self, msg):
        tmp, d = TempActor.make()
        self.send(msg, _sender=tmp)
        return d.get()

    def forward(self, msg):
        sender = get_context().sender
        assert sender
        self.send(msg, _sender=sender)

    def send_later(self, delay, message, _sender=None):
        spawn_later(delay, self.send, message, _sender=_sender or get_context().ref)

    def stop(self):
        """Sends '_stop' to this actor"""
        self.send('_stop')

    def kill(self):
        """Sends '_kill' to this actor"""
        self.send('_kill')


class Ref(_BaseRef):
    """A serializable, location-transparent, encapsulating reference to an actor.

    `Ref`s can be obtained in several different ways.

    Refs to already existing actors:

    * `self.ref`: a ref to the actor itself; this is meant to be the *only* channel an actor should ever be communicated
       through--an actor should *never* send out messages containing `self` and should instead *always* insert
       `self.ref` in messages sent out to other actors if it wants them to reach it back;

    * `Node.guardian`: a ref to the default `Guardian`
       (shouldn't be used fron inside an actor hierarchy if networkless, in-process testability is a goal, which it should be);

    * `self.node.guardian`: a ref to the `Guardian` of the actor hierarchy the actor belongs to; useful during testing

    By spawning new actors:

    * `self.spawn(...)`: a ref to a newly created subordinate actor of the spawning actor;

    * `spawn(...)`: a ref to a newly spawned top-level actor in the default hierarchy, however, top-level actors should
       in general be avoided, instead, have only one top-level actor under which your entire application is laid out;
       (this is an alias for `Node.spawn`, in turn alias for `Node.guardian.spawn`)

    * `self.node.spawn(...)`: a ref to a newly created top-level actor of the actor hierarchy that `self` is part of;
       useful during testing where the default `Node` should not be used, or cannot be used because multiple `Node`s
       might be residing in the same process;

    By looking up existing actors:

    * `self.ref / <name>`: looks up a child actor
    * `self.ref / <path/to/descendant>`: looks up a descendant actor
    * `self.ref / <name> / <name> / ...`: looks up a descendant actor

    * `Node.lookup(<uri or path>)`: looks up a local or remote actor in the default hierarchy starting from the root

    * `self.node.lookup(<uri or path>)`: looks up a local or remote actor in the hierarchy that `self` belongs to
       starting from the guardian;

    Note: refs to `Guardian`s are not true `Ref`s but merely objects that by all practical means have the same
    interface as Ref.

    """

    _cell = None

    # XXX: should be is_resolved with perhaps is_local being None while is_resolved is False
    # Ref constructor should set is_resolved=False by default, but that requires is_dead for creating dead refs, because
    # currently dead refs are just Refs with no cell and is_local=True
    is_local = True
    uri = None
    node = None

    def __init__(self, cell, uri, node, is_local=True):
        assert is_local or not cell
        assert uri is None or isinstance(uri, Uri)
        self._cell = cell
        self.uri = uri
        self.node = node
        self.is_local = is_local

    def send(self, message, _sender=None):
        """Sends a message to the actor represented by this `Ref`."""
        if not _sender:
            context = get_context()
            if context:
                _sender = context.ref
        if self._cell:
            if not self._cell.stopped:
                self._cell.receive(message, _sender)
                return
            else:
                self._cell = None
        if not self.is_local:
            if self.uri.node != self.node.nid:
                self.node.send_message(message, remote_ref=self, sender=_sender)
            else:
                self._cell = self.node.guardian.lookup_cell(self.uri)
                self.is_local = True
                self._cell.receive(message, _sender)
        else:
            if self.node and self.node.guardian:
                cell = self.node.guardian.lookup_cell(self.uri)
                if cell:
                    cell.receive(message, _sender)  # do NOT set self._cell--it will never be unset and will cause a memleak
                    return
            if ('_watched', ANY) == message:
                message[1].send(('terminated', self))
            elif (message == ('terminated', ANY) or message == ('_unwatched', ANY) or message == ('_node_down', ANY) or
                  message == '_stop' or message == '_kill' or message == '__done'):
                pass
            else:
                Events.log(DeadLetter(self, message, _sender))

    @property
    def is_stopped(self):
        """Returns `True` if this actor is guaranteed to have stopped.

        If it returns `False`, it is not guaranteed that the actor isn't still running.

        """
        return self.is_local and not self._cell

    def join(self):
        # XXX: will break if somebody tries to do lookups on the future or inspect its `Uri`, which it doesn't have:
        from spinoff.actor.misc import Future
        future = Future()
        self << ('_watched', future)
        return future

    def __eq__(self, other):
        """Returns `True` if the `other` `Ref` points to the same actor.

        This method is cooperative with the `pattern_matching` module.

        """
        return (isinstance(other, Ref) and self.uri == other.uri
                or isinstance(other, Matcher) and other == self)

    def __ne__(self, other):
        return not (self == other)

    def __hash__(self):
        return hash(self.uri)

    def __repr__(self):  # TODO: distinguish local and remote
        return '<%s>' % (str(self.uri),)

    def __getstate__(self):
        # assert self._cell or not self.is_local, "TODO: if there is no cell and we're local, we should be returning a state that indicates a dead ref"
        # assert self.uri.node, "does not make sense to serialize a ref with no node: %r" % (self,)
        return str(self.uri)  # if self.is_local else (str(self.uri), self.node)

    def __setstate__(self, uri):
        # if it's a tuple, it's a remote `Ref` and the tuple origates from IncomingMessageUnpickler,
        # otherwise it must be just a local `Ref` being pickled and unpickled for whatever reason:
        if isinstance(uri, tuple):
            self.is_local = False
            uri, self.node = uri
        self.uri = Uri.parse(uri)
