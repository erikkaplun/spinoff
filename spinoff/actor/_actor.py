# coding: utf-8
from __future__ import print_function

import abc
import inspect
import re
import sys
import types
import traceback
import warnings
import weakref
from functools import wraps
from pickle import PicklingError
from collections import deque
from itertools import count, chain

from twisted.internet.defer import inlineCallbacks, Deferred
from txcoroutine import coroutine

from spinoff.actor.events import (
    Events, UnhandledMessage, DeadLetter, ErrorIgnored, TopLevelActorTerminated, ErrorReportingFailure, Error, UnhandledError)
from spinoff.actor.supervision import Decision, Resume, Restart, Stop, Escalate, Default
from spinoff.actor.exceptions import (
    NameConflict, LookupFailed, Unhandled, CreateFailed, UnhandledTermination, BadSupervision, WrappingException)
from spinoff.util.pattern_matching import IS_INSTANCE, ANY, IN
from spinoff.util.async import call_when_idle_unless_already, with_timeout, Timeout, sleep
from spinoff.util.pattern_matching import Matcher
from spinoff.util.logging import logstring, dbg, fail, panic, err
from spinoff.util.python import clean_tb_twisted


TESTING = False
_NODE = None  # set in __init__.py


_VALID_IP_RE = '(?:(?:[0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}(?:[0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])'
_VALID_HOSTNAME_RE = '(?:(?:[a-zA-Z]|[a-zA-Z][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)*(?:[A-Za-z]|[A-Za-z][A-Za-z0-9\-]*[A-Za-z0-9])'
_VALID_NODEID_RE = re.compile('(?:%s|%s):(?P<port>[0-9]+)$' % (_VALID_HOSTNAME_RE, _VALID_IP_RE))


# these messages get special handling from the framework and never reach Actor.receive
_SYSTEM_MESSAGES = ('_start', '_stop', '_restart', '_suspend', '_resume', ('_child_terminated', ANY))


class Uri(object):
    """Represents the identity and location of an actor.

    Attention: for the sake of consistency, the root `Uri` is represented by an empty string, **not** `'/'`. The `'/'` is
    used **only** as a path separator. Thus, both of the name and path path of the root `Uri` are `''`, and the steps
    are `['']`. The root `Uri` is also only `__eq__` to `''` and not `'/'`.

    """
    _node = None

    def __init__(self, name, parent, node=None):
        if name and node:
            raise TypeError("node specified for a non-root Uri")  # pragma: no cover
        self.name, self.parent = name, parent
        if node:
            _validate_nodeid(node)
            self._node = node

    @property
    def root(self):
        """Returns the topmost `Uri` this `Uri` is part of."""
        return self.parent.root if self.parent else self

    @property
    def node(self):
        """Returns the node ID this `Uri` points to."""
        return self.root._node

    def __div__(self, child):
        """Builds a new child `Uri` of this `Uri` with the given `name`."""
        if not child or not isinstance(child, str):
            raise TypeError("Uri traversal expected a non-empty str but got %r" % (child,))  # pragma: no cover
        if child in ('.', '..'):
            raise TypeError("Traversing using . and .. is not supported (yet)")  # pragma: no cover
        elif '/' in child:
            raise TypeError("Traversing more than 1 level at a time is not supported (yet)")  # pragma: no cover
        return Uri(name=child, parent=self)

    @property
    def path(self):
        """Returns the `Uri` without the `node` part as a `str`."""
        return '/'.join(self.steps)

    @property
    def steps(self):
        """Returns an iterable containing the steps to this `Uri` from the root `Uri`, including the root `Uri`."""
        def _iter(uri, acc):
            acc.appendleft(uri.name if uri.name else '')
            return _iter(uri.parent, acc) if uri.parent else acc
        return _iter(self, acc=deque())

    def __str__(self):
        return (self.node or '') + self.path

    def __repr__(self):
        return '<@%s>' % (str(self),)

    @property
    def url(self):
        return 'tcp://' + str(self) if self.node else None

    @classmethod
    def parse(cls, addr):
        """Parses a new `Uri` instance from a string representation of a URI.

        >>> u1 = Uri.parse('/foo/bar')
        >>> u1.node, u1.steps, u1.path, u1.name
        (None, ['', 'foo', 'bar'], '/foo/bar', 'bar')
        >>> u2 = Uri.parse('somenode:123/foo/bar')
        >>> u2.node, u1.steps, u2.path, ur2.name
        ('somenode:123', ['', 'foo', 'bar'], '/foo/bar', 'bar')
        >>> u1 = Uri.parse('foo/bar')
        >>> u1.node, u1.steps, u1.path, u1.name
        (None, ['foo', 'bar'], 'foo/bar', 'bar')

        """
        if addr.endswith('/'):
            raise ValueError("Uris must not end in '/'")  # pragma: no cover
        parts = addr.split('/')
        if ':' in parts[0]:
            node, parts[0] = parts[0], ''
        else:
            node = None

        ret = None  # Uri(name=None, parent=None, node=node) if node else None
        for step in parts:
            ret = Uri(name=step, parent=ret, node=node)
            node = None  # only set the node on the root Uri
        return ret

    @property
    def local(self):
        """Returns a copy of the `Uri` without the node. If the `Uri` has no node, returns the `Uri`."""
        if not self.node:
            return self
        else:
            return Uri.parse(self.path)

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        """Returns `True` if `other` points to the same actor.

        This method is cooperative with the `pattern_matching` module.

        """
        if isinstance(other, str):
            other = Uri.parse(other)
        return str(self) == str(other) or isinstance(other, Matcher) and other == self

    def __ne__(self, other):
        return not (self == other)


class _BaseRef(object):
    """Internal abstract class for all objects that behave like actor references."""
    __metaclass__ = abc.ABCMeta

    @abc.abstractproperty
    def is_local(self):
        raise NotImplementedError

    @abc.abstractproperty
    def is_stopped(self):
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
                       hub=None if self.is_local else self.hub)

    def __lshift__(self, message):
        """A fancy looking alias to `_BaseRef.stop`, which in addition also supports chaining.

            someactor.send(msg1); someactor.send(msg2)
            someactor << msg1 << msg2

        """
        self.send(message)
        return self

    def stop(self):
        """Shortcut for `send('_stop')`"""
        self.send('_stop')


class _HubBound(object):
    """Internal abstract class for objects that depend on {being bound to/having available} a `Hub` instance."""
    _hub = None

    def __init__(self, hub, is_local=True):
        assert hub or is_local if TESTING else not hub
        if hub:
            self._hub = hub

    @property
    def hub(self):
        """Returns the hub that this object should use for remote messaging and lookup.

        During `TESTING`, the `Node`, and from there on, all `_HubBound` objects, propagate the hub they are bound to
        down to any new `_HubBound` objects created. Otherwise, `Node.hub` is used.

        """
        ret = self._hub or (_NODE.hub if _NODE else None)
        assert ret or self.is_local
        return ret


class Ref(_BaseRef, _HubBound):
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

    * `self.spawn(...)`: a ref to a newly created subordinate actor of the spawning actor (a supervisor);

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

    def __init__(self, cell, uri, is_local=True, hub=None):
        super(Ref, self).__init__(hub=hub, is_local=is_local)
        assert uri is None or isinstance(uri, Uri)
        if cell:
            assert not hub
            assert isinstance(cell, Cell)
            self._cell = cell
        self.uri = uri
        if not is_local:
            assert not cell
            self.is_local = False

    def send(self, message, force_async=False):
        """Sends a message to the actor represented by this `Ref`.

        The send could but might not be asynchronous, depending on how the system has been configured and where the
        recipient is located.

        By default, sends to local actors are eager for better performance. This can be on a set per-call basis by
        passing `force_async=True` to this method, or overridden globally by setting `actor.SENDING_IS_ASYNC = True`;
        globally changing this is not recommended however unless you know what you're doing (e.g. during testing).

        """
        if self._cell:
            self._cell.receive(message, force_async=force_async)
        elif not self.is_local:
            self.hub.send(message, to_remote_actor_pointed_to_by=self)
        else:
            if ('_watched', ANY) == message:
                message[1].send(('terminated', self))
            elif message not in ('_stop', '_suspend', '_resume', '_restart', (IN(['terminated', '_watched', '_unwatched']), ANY)):
                Events.log(DeadLetter(self, message))

    @property
    def is_stopped(self):
        """Returns `True` if this actor is guaranteed to have stopped.

        If it returns `False`, it is not guaranteed that the actor isn't still running.

        """
        return self.is_local and not self._cell

    def join(self):
        # XXX: will break if somebody tries to do lookups on the future or inspect its `Uri`, which it doesn't have:
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
        return str(self.uri)

    def __setstate__(self, uri):
        # if it's a tuple, it's a remote `Ref`, otherwise it must be just a local `Ref`
        # being pickled and unpickled for whatever reason:
        if isinstance(uri, tuple):
            self.is_local = False
            uri, self._hub = uri
        self.uri = Uri.parse(uri)


class _BaseCell(_HubBound):
    __metaclass__ = abc.ABCMeta

    _children = {}  # XXX: should be a read-only dict
    _child_name_gen = None

    @abc.abstractproperty
    def root(self):
        raise NotImplementedError

    @abc.abstractproperty
    def ref(self):
        raise NotImplementedError

    @abc.abstractproperty
    def uri(self):
        raise NotImplementedError

    def spawn(self, factory, name=None):
        """Spawns an actor using the given `factory` with the specified `name`.

        Returns an immediately usable `Ref` to the newly created actor, regardless of the location of the new actor, or
        when the actual spawning will take place.

        """
        if name and '/' in name:  # pragma: no cover
            raise TypeError("Actor names cannot contain slashes")
        if not self._children:
            self._children = {}
        uri = self.uri / name if name else None
        if name:
            if name.startswith('$'):
                raise ValueError("Unable to spawn actor at path %s; name cannot start with '$', it is reserved for auto-generated names" % (uri.path,))
            if name in self._children:
                raise NameConflict("Unable to spawn actor at path %s; actor %r already sits there" % (uri.path, self._children[name]))
        if not uri:
            name = self._generate_name(factory)
            uri = self.uri / name

        assert name not in self._children  # XXX: ordering??
        self._children[name] = None
        child = _do_spawn(parent=self.ref, factory=factory, uri=uri, hub=self.hub if TESTING else None)
        if name in self._children:  # it might have been removed already
            self._children[name] = child

        return child

    @abc.abstractmethod
    def receive(self, message, force_async=None):
        pass

    def _generate_name(self, factory):
        # TODO: the factory should provide that as a property
        basename = factory.__name__.lower() if isinstance(factory, type) else factory.cls.__name__
        if not self._child_name_gen:
            self._child_name_gen = ('$%d' % i for i in count(1))
        return basename.lower() + self._child_name_gen.next()

    @property
    def children(self):
        return self._children.values()

    def _child_gone(self, child):
        name = child.uri.name  # rsplit('/', 1)[-1]
        del self._children[name]

    def get_child(self, name):
        if not (name and isinstance(name, str)):
            raise TypeError("get_child takes a non-emtpy string")  # pragma: no cover
        return self._children.get(name, None)

    def lookup_cell(self, uri):
        """Looks up a local actor by its location relative to this actor."""
        steps = uri.steps

        if steps[0] == '':
            found = self.root
            steps.popleft()
        else:
            found = self

        for step in steps:
            assert step != ''
            found = found.get_child(step)
            if not found:
                break
            found = found._cell
        return found

    def lookup_ref(self, uri):
        if not isinstance(uri, (Uri, str)):
            raise TypeError("%s.lookup_ref expects a str or Uri" % type(self).__name__)  # pragma: no cover

        uri = uri if isinstance(uri, Uri) else Uri.parse(uri)

        assert not uri.node or uri.node == self.uri.node
        cell = self.lookup_cell(uri)
        if not cell:
            raise LookupFailed("Look-up of local actor failed: %s" % (uri,))
        return cell.ref


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

    def __init__(self, uri, node, hub, supervision=Stop):
        super(Guardian, self).__init__(hub=hub)
        if supervision not in (Stop, Restart, Resume):
            raise TypeError("Invalid supervision specified for Guardian")
        self.uri = uri
        self.node = node
        self.root = self
        self._cell = self  # for _BaseCell
        self.supervision = supervision

    @property
    def ref(self):
        return self

    def send(self, message, force_async=False):
        if ('_error', ANY, IS_INSTANCE(Exception), IS_INSTANCE(types.TracebackType) | IS_INSTANCE(basestring)) == message:
            _, sender, exc, tb = message
            Events.log(UnhandledError(sender, exc, tb))
            if self.supervision == Stop:
                sender.stop()
            elif self.supervision == Restart:
                sender << '_restart'
            elif self.supervision == Resume:
                sender << '_resume'
            else:
                assert False
        elif ('_child_terminated', ANY) == message:
            _, sender = message
            self._child_gone(sender)
            Events.log(TopLevelActorTerminated(sender))
        elif '_stop' == message:
            return self._do_stop()
        else:
            Events.log(UnhandledMessage(self, message))

    receive = send

    @inlineCallbacks
    def _do_stop(self):
        # dbg("GUARDIAN: stopping")
        for actor in self.children:
            # dbg("GUARDIAN: stopping", actor)
            actor.stop()
            # dbg("GUARDIAN: joining...", actor, actor._cell)
            try:
                yield with_timeout(.01, actor.join())
            except Timeout:  # pragma: no cover
                # dbg("GUARDIAN: actor %r refused to stop" % (actor,))
                assert False, "actor %r refused to stop" % (actor,)
                # TODO: force-stop
            # dbg("GUARDIAN: ...OK", actor)

    def __getstate__(self):  # pragma: no cover
        raise PicklingError("Guardian cannot be serialized")

    def __repr__(self):
        return "<guardian:%s>" % (self.uri,)


class Node(object):
    """`Node` is both a singleton instance and a class lookalike, there is thus always available a default global
    `Node` instance but it is possible to create more, non-default, instances of `Node` by simply calling it as if it
    were a class, i.e. using it as a class. This is mainly useful for testing multi-node scenarios without any network
    involved by setting a custom `actor.remoting.Hub` to the `Node`.

    """
    hub = None
    _all = []

    @classmethod
    @inlineCallbacks
    def stop_all(cls):
        for node in cls._all:
            try:
                yield node.stop()
            except Exception:
                err("Failed to stop %s:\n%s" % (node, traceback.format_exc()))
        del cls._all[:]

    @classmethod
    def make_local(cls):
        from .remoting import HubWithNoRemoting
        return cls(hub=HubWithNoRemoting())

    def __init__(self, hub, root_supervision=Stop):
        if not hub:  # pragma: no cover
            raise TypeError("Node instances must be bound to a Hub")
        self._uri = Uri(name=None, parent=None, node=hub.nodeid if hub else None)
        self.guardian = Guardian(uri=self._uri, node=self, hub=hub if TESTING else None, supervision=root_supervision)
        self.set_hub(hub)
        Node._all.append(self)

    def set_hub(self, hub):
        if hub:
            from .remoting import Hub, HubWithNoRemoting
            if not isinstance(hub, (Hub, HubWithNoRemoting)):  # pragma: no cover
                raise TypeError("hub parameter to Guardian must be a %s.%s or a %s.%s" %
                                (Hub.__module__, Hub.__name__,
                                 HubWithNoRemoting.__module__, HubWithNoRemoting.__name__))
            if hub.guardian:  # pragma: no cover
                raise RuntimeError("Can't bind Guardian to a Hub that is already bound to another Guardian")
            hub.guardian = self.guardian
            self.hub = hub

    def lookup(self, uri_or_addr):
        if not isinstance(uri_or_addr, (Uri, str)):  # pragma: no cover
            raise TypeError("Node.lookup expects a non-empty str or Uri")

        uri = uri_or_addr if isinstance(uri_or_addr, Uri) else Uri.parse(uri_or_addr)

        if not uri.node or uri.node == self._uri.node:
            try:
                return self.guardian.lookup_ref(uri)
            except LookupFailed if uri.node else None:
                # for remote lookups that actually try to look up something on the local node, we don't want to raise an
                # exception because the caller might not be statically aware of the localness of the `Uri`, thus we
                # return a mere dead ref:
                return Ref(cell=None, uri=uri, is_local=True)
        elif not uri.root:  # pragma: no cover
            raise TypeError("Node can't look up a relative Uri; did you mean Node.guardian.lookup(%r)?" % (uri_or_addr,))
        else:
            return Ref(cell=None, uri=uri, is_local=False, hub=self.hub if TESTING else None)

    def spawn(self, *args, **kwargs):
        return self.guardian.spawn(*args, **kwargs)

    @inlineCallbacks
    def stop(self):
        yield self.guardian.stop()
        # TODO: just make the node send a special notification to other nodes, so as to avoid needless sending of many
        # small termination messages:
        if not TESTING:  # XXX: this might break some (future) tests that test termination messaging
            yield sleep(.1)  # let actors send termination messages
        yield self.hub.stop()

    def __getstate__(self):  # pragma: no cover
        raise PicklingError("Node cannot be serialized")

    def __repr__(self):
        return '<node:%s>' % (self._uri if self.hub else 'local')

    def __call__(self, hub=None):
        """Spawns new, non-default instances of the guardian; useful for testing."""
        return type(self)(hub)


class ActorType(abc.ABCMeta):  # ABCMeta to enable Process.run to be @abstractmethod
    def __new__(self, *args, **kwargs):
        """Automatically wraps any receive methods that are reported to be generators by `inspect` with
        `txcoroutine.coroutine`.

        """
        ret = super(ActorType, self).__new__(self, *args, **kwargs)
        if inspect.isgeneratorfunction(ret.receive):
            ret.receive = coroutine(ret.receive)
        if hasattr(ret, 'pre_start') and inspect.isgeneratorfunction(ret.pre_start):
            ret.pre_start = coroutine(ret.pre_start)
        if hasattr(ret, 'post_stop') and inspect.isgeneratorfunction(ret.post_stop):
            ret.post_stop = coroutine(ret.post_stop)
        return ret


class Actor(object):
    """Description here.

    __init__ should not attempt to access `self.ref` or `self.parent` as this are available only on an already
    initialized actor instance. If your initialization routine depends on either of those, use `pre_start` instead.

    """
    __metaclass__ = ActorType

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
    __startargs = ((), {})

    def __init__(self, *args, **kwargs):
        self.__startargs = args, kwargs

    def receive(self, message):
        raise Unhandled

    def spawn(self, factory, name=None):
        return self.__cell.spawn(factory, name)

    @property
    def children(self):
        return self.__cell.children

    def watch(self, actor, *actors):
        return self.__cell.watch(actor, *actors)

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
        return "<actor-impl:%s@%s>" % (type(self).__name__, self.ref.uri)


# TODO: rename to _UnspawnedActor
class Props(object):
    def __init__(self, cls, *args, **kwargs):
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


def _do_spawn(parent, factory, uri, hub):
    cell = Cell(parent=parent, factory=factory, uri=uri, hub=hub)
    cell.receive('_start', force_async=Actor.SPAWNING_IS_ASYNC)
    return cell.ref


def default_supervise(exc):
    if isinstance(exc, CreateFailed):
        return Stop
    elif isinstance(exc, AssertionError):
        return Escalate
    elif isinstance(exc, Exception):
        return Restart
    else:
        assert False, "don't know how to supervise this exception"
    #     return Escalate  # TODO: needed for BaseException


class Cell(_BaseCell):
    constructed = False
    started = False
    actor = None
    inbox = None
    priority_inbox = None

    # actor has begun shutting itself down but is waiting for all its children to stop first, and its own post_stop;
    # in the shutting-down state, an actor only accepts '_child_terminated' messages (and '_force_stop' in the future)
    shutting_down = False

    stopped = False

    suspended = False
    tainted = False  # true when init or pre_start failed and the actor is waiting for supervisor decision
    processing_messages = False
    _ongoing = None

    _ref = None
    _child_name_gen = None
    uri = None

    watchers = None
    watchees = None

    def __init__(self, parent, factory, uri, hub):
        super(Cell, self).__init__(hub=hub)

        if not callable(factory):  # pragma: no cover
            raise TypeError("Provide a callable (such as a class, function or Props) as the factory of the new actor")
        self.parent = parent
        self.uri = uri

        self.factory = factory

        self.inbox = deque()
        self.priority_inbox = deque()

    @property
    def root(self):
        return self.parent if isinstance(self.parent, Guardian) else self.parent._cell.root

    # TODO: benchmark the message methods and optimise
    def has_message(self):
        return self.inbox or self.priority_inbox

    def consume_message(self):
        try:
            return self.priority_inbox.popleft()
        except IndexError:
            try:
                return self.inbox.popleft()
            except IndexError:  # pragma: no cover
                assert False, "should not reach here"

    def peek_message(self):
        try:
            return self.priority_inbox[0]
        except IndexError:
            try:
                return self.inbox[0]
            except IndexError:
                return None

    def logstate(self):  # pragma: no cover
        return {'--\\': self.shutting_down, '+': self.stopped, 'N': not self.started,
                '_': self.suspended, '?': self.tainted, 'X': self.processing_messages, }

    def logcomment(self):  # pragma: no cover
        if self.priority_inbox and self.inbox:
            def g():
                for i, msg in enumerate(chain(self.priority_inbox, self.inbox)):
                    yield msg if isinstance(msg, str) else repr(msg)
                    if i == 2:
                        yield '...'
                        return
            q = ', '.join(g())
            return '<<<[%s]' % (q,)
        else:
            return ''

    # TODO: change `force_async` to `async` and make it completely override the global setting, not just when it's `True`.
    @logstring(u'←')
    def receive(self, message, force_async=False):
        # dbg(message if isinstance(message, str) else repr(message),)
        assert not self.stopped, "should not reach here"

        if self.shutting_down:
            # the shutting_down procedure is waiting for all children to terminate so we make an exception here
            # and handle the message directly, bypassing the standard message handling logic:
            # NB! DO NOT do this with a running actor--it changes the visible state of the actor
            if message == ('_child_terminated', ANY):
                _, child = message
                self._do_child_terminated(child)
            # don't care about any system message if we're already stopping:
            elif message not in _SYSTEM_MESSAGES:
                # so that it could be sent to dead letters when the stopping is complete:
                self.inbox.append(message)
            # XXX: untested
            elif self.priority_inbox:
                self.priority_inbox.append(message)
            return

        # XXX: should ('terminated', child) also be prioritised?

        # '_watched' is something that is safe to handle immediately as it doesn't change the visible state of the actor;
        # NB: DO NOT do the same with '_suspend', '_resume' or any other message that changes the visible state of the actor!
        if message == ('_watched', ANY):
            self._do_watched(message[1])
        elif message == ('_unwatched', ANY):
            self._do_unwatched(message[1])
        elif ('terminated', ANY) == message and not (self.watchees and message[1] in self.watchees):
            pass  # ignore unwanted termination message
        elif message == ('_node_down', ANY):
            self._do_node_down(message[1])
        # ...except in case of an ongoing receive in which case the suspend-resume event will be seem atomic to the actor
        elif self._ongoing and message in ('_suspend', '_resume'):
            if message == '_suspend':
                self._do_suspend()
            else:
                self._do_resume()
        else:
            if message in _SYSTEM_MESSAGES:
                self.priority_inbox.append(message)
            else:
                self.inbox.append(message)
            self.process_messages(force_async=force_async)

    @logstring(u'↻')
    def process_messages(self, force_async=False):
        next_message = self.peek_message()
        # dbg(next_message)

        is_startstop = next_message in ('_start', '_stop')
        is_untaint = next_message in ('_resume', '_restart')

        if not self.processing_messages and (self.started or is_startstop or self.tainted and is_untaint):
            if Actor.SENDING_IS_ASYNC or force_async:
                # dbg(u'⇝')
                # dbg("PROCESS-MSGS: async (Actor.SENDING_IS_ASYNC? %s  force_async? %s" % (Actor.SENDING_IS_ASYNC, force_async))
                call_when_idle_unless_already(self._process_messages)  # TODO: check if there's an already scheduled call to avoid redundant calls
            else:
                # dbg(u'↯')
                self._process_messages()
        # else:
        #     dbg(u'  →X')

    @logstring(u'↻ ↻')
    @inlineCallbacks
    def _process_messages(self):
        # XXX: this method can be called after the actor is stopped--add idle call cancelling
        # dbg(self.peek_message())
        # first = True
        try:
            while not self.stopped and (not self.shutting_down) and self.has_message() and (not self.suspended or self.peek_message() in ('_stop', '_restart', '_resume', '_suspend')) or (self.shutting_down and ('_child_terminated', ANY) == self.peek_message()):
                message = self.consume_message()
                self.processing_messages = repr(message)
                # if not first:
                #     dbg(u"↪ %r" % (message,))
                # first = False
                try:
                    yield self._process_one_message(message)
                    # if isinstance(ret, Deferred) and not self.receive_is_coroutine:
                    #     warnings.warn(ConsistencyWarning("prefer yielding Deferreds from Actor.receive rather than returning them"))
                    # yield d
                except Exception:
                    # dbg("☹")
                    self.report_to_parent()
                # else:
                #     dbg(message, u"✓")
                finally:
                    self.processing_messages = False
            # dbg("☺")
        except Exception:  # pragma: no cover
            panic(u"!!BUG!!\n", traceback.format_exc())
            self.report_to_parent()

    @logstring(u"↻ ↻ ↻")
    @inlineCallbacks
    def _process_one_message(self, message):
        # dbg(message)
        if '_start' == message:
            yield self._do_start()
        elif ('_error', ANY, ANY, ANY) == message:
            _, sender, exc, tb = message
            yield self._do_supervise(sender, exc, tb)
        elif '_stop' == message:
            self._do_stop()
        elif '_restart' == message:
            yield self._do_restart()
        elif '_resume' == message:
            yield self._do_resume()
        elif '_suspend' == message:
            self._do_suspend()
        elif ('_child_terminated', ANY) == message:
            _, child = message
            self._do_child_terminated(child)
        else:
            if ('terminated', ANY) == message:
                _, watchee = message
                # XXX: it's possible to have two termination messages in the queue; this will be solved when actors of
                # nodes going down do not send individual messages but instead the node sends a single 'going-down'
                # message:
                if watchee not in self.watchees:
                    return
                self.watchees.remove(watchee)
                self._unwatch(watchee, silent=True)

            receive = self.actor.receive
            try:
                self._ongoing = receive(message)
                # dbg("PROCESS-ONE: receive returned...", self._ongoing, self)
                # dbg("PROCESS-ONE: still processing?", self.processing_messages, self)
                yield self._ongoing
                # dbg("PROCESS-ONE: ...receive terminated", self)
                del self._ongoing
            except Unhandled:
                self._unhandled(message)
            except Exception:
                raise

    def _unhandled(self, message):
        if ('terminated', ANY) == message:
            raise UnhandledTermination(watcher=self.ref, watchee=message[1])
        else:
            Events.log(UnhandledMessage(self.ref, message))

    @logstring("ctor:")
    @inlineCallbacks
    def _construct(self):
        # dbg()
        factory = self.factory

        try:
            actor = factory()
        except Exception:
            # dbg(u"☹")
            raise CreateFailed("Constructing actor failed", factory)
        else:
            self.actor = actor

        actor._parent = self.parent
        actor._set_cell(self)

        if hasattr(actor, 'pre_start'):
            pre_start = actor.pre_start
            args, kwargs = actor._Actor__startargs
            try:
                self._ongoing = pre_start(*args, **kwargs)
                yield self._ongoing
                del self._ongoing
            except Exception:
                # dbg(u"☹")
                raise CreateFailed("Actor failed to start", actor)

        self.constructed = True
        # dbg(u"✓")

    @logstring(u"►►")
    @inlineCallbacks
    def _do_start(self):
        # dbg()
        try:
            yield self._construct()
        except Exception:
            self.tainted = True
            raise
        else:
            self.started = True
            # dbg(u"✓")

    @logstring("sup:")
    def _do_supervise(self, child, exc, tb):
        dbg(u"%r ← %r" % (exc, child))
        if child not in self.children:
            Events.log(ErrorIgnored(child, exc, tb))
            return

        supervise = getattr(self.actor, 'supervise', None)

        if supervise:
            decision = supervise(exc)
        if not supervise or decision == Default:
            # dbg("...fallback to default...")
            decision = default_supervise(exc)

        dbg(u"→ %r → %r" % (decision, child))

        if not isinstance(decision, Decision):
            raise BadSupervision("Bad supervisor decision: %s" % (decision,), exc, tb)

        # TODO: make these always async?
        if Resume == decision:
            child.send('_resume')
        elif Restart(ANY, ANY) == decision:
            child.send('_restart')
        elif Stop == decision:
            child.send('_stop')
        else:
            raise exc, None, tb

    @logstring(u"||")
    def _do_suspend(self):
        # dbg()
        self.suspended = True
        if self._ongoing:
            # dbg("ongoing.pause")
            self._ongoing.pause()
        if hasattr(self.actor, '_coroutine') and self.actor._coroutine:
            # dbg("calling coroutine.pause on", self.actor._coroutine)
            self.actor._coroutine.pause()

        for child in self.children:
            child.send('_suspend')

    @logstring(u"||►")
    def _do_resume(self):
        # dbg()
        if self.tainted:
            # dbg("...tainted → restarting...")
            warnings.warn("Attempted to resume an actor that failed to start; falling back to restarting:\n%s" % (''.join(traceback.format_stack()),))
            self.tainted = False
            return self._do_restart()
        else:
            # dbg("resuming...")
            self.suspended = False
            if self._ongoing:
                self._ongoing.unpause()
            if hasattr(self.actor, '_coroutine'):
                self.actor._coroutine.unpause()

            for child in self.children:
                child.send('_resume')
        # dbg(u"✓")

    @logstring("stop:")
    def _do_stop(self):
        # dbg()
        if self._ongoing:
            del self._ongoing
        # del self.watchers
        self._shutdown().addCallback(self._finish_stop).addErrback(panic)

    @logstring("finish-stop:")
    def _finish_stop(self, _):
        # dbg()
        try:
            ref = self.ref

            # TODO: test that system messages are not deadlettered
            for message in self.inbox:
                if ('_error', ANY, ANY, ANY) == message:
                    _, sender, exc, tb = message
                    Events.log(ErrorIgnored(sender, exc, tb))
                elif ('_watched', ANY) == message:
                    _, watcher = message
                    watcher.send(('terminated', ref))
                elif (IN(['terminated', '_unwatched']), ANY) != message:
                    Events.log(DeadLetter(ref, message))

            assert not self.actor
            del self.inbox
            # dbg("self.priority_inbox:", self.priority_inbox)
            del self.priority_inbox  # don't want no more, just release the memory

            # dbg("unlinking reference")
            del ref._cell
            self.stopped = True

            # XXX: which order should the following two operations be done?

            self.parent.send(('_child_terminated', ref))

            if self.watchers:
                for watcher in self.watchers:
                    watcher.send(('terminated', ref))
        except Exception:  # pragma: no cover
            panic(u"!!BUG!!\n", traceback.format_exc())
            _, exc, tb = sys.exc_info()
            Events.log(ErrorIgnored(ref, exc, tb))
        # dbg(u"✓")

    @logstring(u"► ↻")
    @inlineCallbacks
    def _do_restart(self):
        # dbg()
        self.suspended = True
        yield self._shutdown()
        yield self._construct()
        self.suspended = False
        # dbg(u"✓")

    @logstring("child-term:")
    def _do_child_terminated(self, child):
        # probably a child that we already stopped as part of a restart
        if child.uri.name not in self._children:
            # dbg("ignored child termination")
            # Events.log(TerminationIgnored(self, child))
            return
        self._child_gone(child)
        # itms = self._children.items()
        # ix = itms.index((ANY, child))
        # del self._children[itms[ix][0]]
        if self.shutting_down and not self._children:
            # dbg("CHILD-TERM: signalling all children stopped")
            self._all_children_stopped.callback(None)

    @logstring("shutdown:")
    @inlineCallbacks
    def _shutdown(self):
        # dbg()
        self.shutting_down = True

        if self._children:  # we don't want to do the Deferred magic if there're no babies
            self._all_children_stopped = Deferred()
            # dbg("SHUTDOWN: shutting down children:", self.children)
            for child in self.children:
                child.stop()
            # dbg("SHUTDOWN: waiting for all children to stop", self)
            yield self._all_children_stopped
            # dbg("SHUTDOWN: ...children stopped", self)

        self._unwatch_all()

        if self.constructed and hasattr(self.actor, 'post_stop'):
            try:
                yield self.actor.post_stop()  # XXX: possibly add `yield` here
            except Exception:
                _ignore_error(self.actor)
        if hasattr(self.actor, '_coroutine'):
            try:
                self.actor._Process__shutdown()
            except Exception:
                _ignore_error(self.actor)

        self.actor = None
        self.shutting_down = False
        # dbg(u"✓")

    @logstring("failure report")
    def report_to_parent(self, exc_and_tb=None):
        if not exc_and_tb:
            _, exc, tb = sys.exc_info()
        else:
            exc, tb = exc_and_tb
        try:
            if not isinstance(exc, UnhandledTermination):
                if isinstance(tb, str):
                    exc_fmt = tb
                else:
                    exc_fmt = traceback.format_exception(type(exc), exc, tb)
                    exc_fmt = list(clean_tb_twisted(exc_fmt))
                    exc_fmt = ''.join(exc_fmt)
                    if isinstance(exc, WrappingException):
                        inner_exc_fm = traceback.format_exception(type(exc.cause), exc.cause, exc.tb)
                        inner_exc_fm = list(clean_tb_twisted(inner_exc_fm))
                        inner_exc_fm = ''.join('      ' + line for line in inner_exc_fm)
                        exc_fmt += "\n-------\nCAUSE:\n\n" + inner_exc_fm
                fail('\n\n', exc_fmt)
            else:
                fail("Died because a watched actor (%r) died" % (exc.watchee,))
            Events.log(Error(self.ref, exc, tb)),
            self._do_suspend()
            # XXX: might make sense to make it async by default for better latency
            self.parent.send(('_error', self.ref, exc, tb), force_async=True)
        except Exception:  # pragma: no cover
            try:
                panic("failed to report:\n", traceback.format_exc())
                Events.log(ErrorIgnored(self.ref, exc, tb))
                _, sys_exc, sys_tb = sys.exc_info()
                Events.log(ErrorReportingFailure(self.ref, sys_exc, sys_tb))
            except Exception:
                panic("failed to log:\n", traceback.format_exc())

    def watch(self, actor, *actors):
        actors = (actor,) + actors
        actors = [self.spawn(x) if isinstance(x, (type, Props)) else x for x in actors]
        for other in actors:
            node = self.uri.node
            if other != self.ref:
                assert not other.is_local if other.uri.node and other.uri.node != node else True, (other.uri.node, node)
                if not self.watchees:
                    self.watchees = set()
                if other not in self.watchees:
                    self.watchees.add(other)
                    if not other.is_local:
                        self.hub.watch_node(other.uri.node, report_to=self.ref)
                    other << ('_watched', self.ref)
        return actors[0] if len(actors) == 1 else actors

    def unwatch(self, actor, *actors):
        actors = (actor,) + actors
        if self.watchees:
            for actor in actors:
                try:
                    self.watchees.remove(actor)
                except KeyError:
                    pass
                else:
                    self._unwatch(actor)

    def _unwatch(self, other, silent=False):
        # we're intentionally not purging 'terminated' messages in self.inbox
        # --we'll do it as they are processed because it performs more smoothly
        if not silent:
            other << ('_unwatched', self.ref)
        if not other.is_local:
            self.hub.unwatch_node(other.uri.node, report_to=self.ref)

    def _unwatch_all(self):
        # called by shutdown
        if self.watchees:
            w = self.watchees
            while w:
                other = self.watchees.pop()
                self._unwatch(other)

    @logstring("watched:")
    def _do_watched(self, other):
        assert not self.stopped
        if not self.watchers:
            self.watchers = set()
        self.watchers.add(other)

    @logstring("unwatched")
    def _do_unwatched(self, other):
        if self.watchers:
            self.watchers.discard(other)

    @logstring("node-down")
    def _do_node_down(self, node):
        if self.watchees:
            terminated_watchees = [x for x in self.watchees if x.uri.node == node]
            for x in terminated_watchees:
                self.receive(('terminated', x))
                # if the message was unhandled
                if self.stopped:
                    break

    @property
    def ref(self):
        if self.stopped:
            return Ref(cell=None, uri=self.uri)

        if not self._ref or not self._ref():
            ref = Ref(self, self.uri)  # must store in a temporary variable to avoid immediate collection
            self._ref = weakref.ref(ref)
        return self._ref()


    def __repr__(self):
        return "<cell:%s@%s>" % (type(self.actor).__name__ if self.actor else (self.factory.__name__ if isinstance(self.factory, type) else self.factory.cls.__name__),
                                 self.uri.path,)


# TODO: replace with serializable temporary actors
class Future(Deferred):  # TODO: inherit from _BaseRef or similar
    def send(self, message):
        self.callback(message)


@wraps(_BaseCell.spawn)
def spawn(*args, **kwargs):
    if not _NODE:  # pragma: no cover
        raise TypeError("No active node set")
    else:
        return _NODE.spawn(*args, **kwargs)


def lookup(uri):  # pragma: no cover
    if not _NODE:
        raise TypeError("No active node set")
    else:
        return _NODE.lookup(uri)


def _ignore_error(actor):
    _, exc, tb = sys.exc_info()
    Events.log(ErrorIgnored(actor, exc, tb))


def _validate_nodeid(nodeid):
    # call from app code
    m = _VALID_NODEID_RE.match(nodeid)
    if not m:  # pragma: no cover
        raise ValueError("Node IDs should be in the format '<ip-or-hostname>:<port>': %s" % (nodeid,))
    port = int(m.group(1))
    if not (0 <= port <= 65535):  # pragma: no cover
        raise ValueError("Ports should be in the range 0-65535: %d" % (port,))
