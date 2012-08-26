from __future__ import print_function

import abc
import inspect
import sys
import types
import traceback
import warnings
import weakref
from collections import deque
from itertools import count

from twisted.internet.defer import inlineCallbacks, returnValue
from txcoroutine import coroutine

from unnamedframework.util.async import call_when_idle
from unnamedframework.util.pattern_matching import IS_INSTANCE, ANY
from unnamedframework.actor.events import UnhandledError, Events, UnhandledMessage, DeadLetter, ErrorIgnored, TopLevelActorTerminated
from unnamedframework.actor.supervision import Decision, Resume, Restart, Stop, Escalate, Default


def critical(fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs)
    except Exception:
        print("*** CRITICAL FAILURE:", file=sys.stderr)
        traceback.print_exc(sys.stderr)
        sys.exit(1)


class NameConflict(Exception):
    pass


class Unhandled(Exception):
    pass


class UnhandledTermination(Exception):
    pass


class WrappingException(Exception):
    def raise_original(self):
        raise self.cause, None, self.tb


class CreateFailed(WrappingException):
    def __init__(self, message):
        indented_original = '\n'.join('    ' + line for line in traceback.format_exc().split('\n') if line)
        Exception.__init__(self, "%s:\n%s" % (message, indented_original))
        _, self.cause, self.tb = sys.exc_info()


class BadSupervision(WrappingException):
    def __init__(self, message, exc, tb):
        WrappingException.__init__(self, message)
        self.cause, self.tb = exc, tb


class ActorRef(object):
    # XXX: should be protected/private
    target = None  # so that .target could be deleted to save memory

    def __init__(self, target, path):
        self.target = target
        self.path = path

    def send(self, message, force_async=False):
        """Sends a message to this actor.

        The send could but might not be asynchronous, depending on how the system has been configured and where the
        recipient is located.

        By default, sends to local actors are eager for better performance. This can be on a set per-call basis by
        passing `force_async=True` to this method, or overridden globally by setting `actor.SENDING_IS_ASYNC = True`;
        globally changing this is not recommended however unless you know what you're doing (e.g. during testing).

        """
        if self.target:
            self.target.receive(message, force_async=force_async)
        elif message == ('_watched', ANY):
            message[1].send(('terminated', self))
        elif message not in ('_stop', '_suspend', '_resume', '_restart', ('terminated', ANY)):
            Events.log(DeadLetter(self, message))

    def __lshift__(self, message):
        """See ActorRef.send"""
        self.send(message)
        return self

    def __repr__(self):
        return '<actor@%s>' % self.path

    def stop(self):
        """Shortcut for `ActorRef.send('_stop')`"""
        self.send('_stop')

    def _stop_noevent(self):
        Events.consume_one(TopLevelActorTerminated)
        self.stop()

    @property
    def is_stopped(self):
        """Returns `True` if this actor is known to have stopped.

        If it returns `False`, the actor still might be running.

        """
        return not self.target


class _ActorContainer(object):
    _children = {}  # XXX: should be a read-only dict
    _child_name_gen = None

    def spawn(self, factory, name=None):
        if not self._children:
            self._children = {}
        path = self.path + ('' if self.path[-1] == '/' else '/') + name if name else None
        if name:
            if name.startswith('$'):
                raise ValueError("Unable to spawn actor at path %s; name cannot start with '$', it is reserved for auto-generated names" % (path,))
            if name in self._children:
                raise NameConflict("Unable to spawn actor at path %s; actor %r already sits there" % (path, self._children[name].target.actor))
        if not path:
            name = self._generate_name()
            path = '%s%s%s' % (self.path, ('' if self.path[-1] == '/' else '/'), name)
        ret = _do_spawn(parent=self.ref(), factory=factory, path=path)
        assert name not in self._children
        self._children[name] = ret
        return ret

    def _generate_name(self):
        if not self._child_name_gen:
            self._child_name_gen = ('$%d' % i for i in count(1))
        return self._child_name_gen.next()

    def _reset(self):
        # XXX: not sure why but Python thinks these attributes don't exist sometimes
        try:
            del self._children
        except AttributeError:
            pass
        try:
            del self._child_name_gen
        except AttributeError:
            pass


class Guardian(_ActorContainer):
    path = '/'

    def ref(self):
        return self

    def send(self, message, force_async=False):
        if message == ('_error', ANY, IS_INSTANCE(Exception), IS_INSTANCE(types.TracebackType) | IS_INSTANCE(basestring)):
            _, sender, exc, tb = message
            Events.log(UnhandledError(sender, exc, tb))
        elif message == ('_child_terminated', ANY):
            _, sender = message
            Events.log(TopLevelActorTerminated(sender))
        else:
            Events.log(UnhandledMessage(self, message))

    def reset(self):
        self._reset()
Guardian = Guardian()


spawn = Guardian.spawn


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
    def reset_flags(cls, debug=False):
        cls.SPAWNING_IS_ASYNC = False if debug else cls._DEFAULT_SPAWNING_IS_ASYNC
        cls.SENDING_IS_ASYNC = cls._DEFAULT_SENDING_IS_ASYNC

    __cell = None  # make it really private so it's hard and unpleasant to access the cell

    def receive(self, message):
        raise Unhandled

    def spawn(self, factory, name=None):
        return self.__cell.spawn(factory, name)

    @property
    def children(self):
        return self.__cell._children.values()

    def watch(self, other, self_ok=False):
        if other == self.ref:
            if not self_ok:
                warnings.warn("Portential problem: actor %s started watching itself; pass self_ok=True to mark as safe")
        else:
            other << ('_watched', self.ref)
        return other

    @property
    def ref(self):
        return self.__cell.ref()

    def _set_cell(self, cell):
        self.__cell = cell

    def send(self, *args, **kwargs):
        """Alias for self.ref.send"""
        self.ref.send(*args, **kwargs)

    def __lshift__(self, message):
        self.ref.send(message)
        return self

    def stop(self):
        self.ref.stop()


class Props(object):
    def __init__(self, cls, *args, **kwargs):
        self.cls, self.args, self.kwargs = cls, args, kwargs

    def __call__(self):
        return self.cls(*self.args, **self.kwargs)


def _do_spawn(parent, factory, path):
    cell = Cell(parent=parent, factory=factory, path=path)
    cell.receive('_start', force_async=Actor.SPAWNING_IS_ASYNC)
    return cell.ref()


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


class Cell(_ActorContainer):
    started = False
    actor = None
    inbox = None
    stopped = False

    suspended = False
    tainted = False  # true when init or pre_start failed and the actor is waiting for supervisor decision
    processing_messages = False
    _ongoing_receive_d = None

    _ref = None
    _child_name_gen = None

    watchers = []

    def __init__(self, parent, factory, path):
        self.parent = parent
        self.path = path

        self.factory = factory

        self.inbox = deque()
        self.priority_inbox = deque()

    # TODO: benchmark the message methods and optimise
    def has_message(self):
        return self.inbox or self.priority_inbox

    def consume_message(self):
        try:
            return self.priority_inbox.popleft()
        except IndexError:
            try:
                return self.inbox.popleft()
            except IndexError:
                return None

    def peek_message(self):
        try:
            return self.priority_inbox[0]
        except IndexError:
            try:
                return self.inbox[0]
            except IndexError:
                return None

    def receive(self, message, force_async=False):
        # print("RECV: %r => %r" % (message, self.ref()), file=sys.stderr)
        if self.stopped:
            return

        # XXX: should ('terminated', child) also be prioritised?

        # '_watched' is something that should be safe to handle immediately as it doesn't change the state of the actor
        if message == ('_watched', ANY):
            self._do_watched(message[1])
        else:
            if message in ('_start', '_stop', '_restart', '_suspend', '_resume', ('_child_terminated', ANY)):
                self.priority_inbox.append(message)
            else:
                self.inbox.append(message)
            self.process_messages(force_async=force_async)

    def process_messages(self, force_async=False):
        next_message = self.peek_message()

        is_startstop = next_message in ('_start', '_stop')
        is_untaint = next_message in ('_resume', '_restart')

        if not self.processing_messages and (self.started or is_startstop or self.tainted and is_untaint):
            if Actor.SENDING_IS_ASYNC or force_async:
                call_when_idle(self._process_messages)  # TODO: check if there's an already scheduled call to avoid redundant calls
            else:
                self._process_messages()

    @inlineCallbacks
    def _process_messages(self):
        while self.has_message() and (not self.suspended or self.peek_message() in ('_stop', '_restart', '_resume')):
            self.processing_messages = True
            message = self.consume_message()
            try:
                d = self._process_one_message(message)
                # if isinstance(ret, Deferred) and not self.receive_is_coroutine:
                #     warnings.warn(ConsistencyWarning("prefer yielding Deferreds from Actor.receive rather than returning them"))
                yield d
            except Exception:
                self.report_to_parent(d)
            finally:
                self.processing_messages = False

    @inlineCallbacks
    def _process_one_message(self, message):
        # print("PROCESS-ONE: %r => %r" % (message, self), file=sys.stderr)
        if message == '_start':
            yield self._do_start()
        elif message == ('_error', ANY, ANY, ANY):
            _, sender, exc, tb = message
            yield self._do_supervise(sender, exc, tb)
        elif message == '_stop':
            self._do_stop()
        elif message == '_restart':
            yield self._do_restart()
        elif message == '_resume':
            yield self._do_resume()
        elif message == '_suspend':
            self._do_suspend()
        elif message == ('_child_terminated', ANY):
            _, child = message
            self._do_child_terminated(child)
        else:
            receive = self.actor.receive
            try:
                yield receive(message)
            except Unhandled:
                self._unhandled(message)
            except Exception:
                raise

    def _unhandled(self, message):
        if message == ('terminated', ANY):
            raise UnhandledTermination
        else:
            Events.log(UnhandledMessage(self.ref(), message))

    @inlineCallbacks
    def _construct(self):
        factory = self.factory

        try:
            actor = factory()
        except Exception:
            raise CreateFailed("Constructing actor with %s failed" % (factory,))

        actor._parent = self.parent
        actor._set_cell(self)

        if hasattr(actor, 'pre_start'):
            pre_start = actor.pre_start
            try:
                yield pre_start()
            except Exception:
                raise CreateFailed("Actor.pre_start of %s failed" % actor)

        returnValue(actor)

    @inlineCallbacks
    def _do_start(self):
        try:
            actor = yield self._construct()
        except Exception:
            self.tainted = True
            raise
        else:
            self.actor = actor
            self.started = True
            if self.has_message():
                self.process_messages()

    def _do_supervise(self, child, exc, tb):
        if child not in self._children.values():  # TODO: use a denormalized set
            Events.log(ErrorIgnored(child, exc, tb))
            return

        supervise = getattr(self.actor, 'supervise', None)

        if supervise:
            decision = supervise(exc)
        if not supervise or decision == Default:
            # print("SUP: fallback to default @ %r= > %r" % (self.ref(), child), file=sys.stderr)
            decision = default_supervise(exc)

        # print("SUP: %r => %r @ %r => %r" % (exc, decision, self.ref(), child), file=sys.stderr)

        if not isinstance(decision, Decision):
            raise BadSupervision("Bad supervisor decision: %s" % (decision,), exc, tb)

        if decision == Resume:
            child.send('_resume')
        elif decision == Restart(ANY, ANY):
            child.send('_restart')
        elif decision == Stop:
            child.send('_stop')
        else:
            raise exc, None, tb

    def _do_suspend(self):
        self.suspended = True

        for child in self._children.values():
            child.send('_suspend')

    def _do_resume(self):
        if self.tainted:
            warnings.warn("Attempted to resume an actor that failed to start; falling back to restarting:\n%s" % ''.join(traceback.format_stack()))
            self.tainted = False
            return self._do_restart()
        else:
            self.suspended = False
            for child in self._children.values():
                child.send('_resume')

    def _do_stop(self):
        self._shutdown()
        self.stopped = True

        ref = self._ref()
        if ref:
            del ref.target

        self.parent.send(('_child_terminated', ref))

        for watcher in self.watchers:
            watcher.send(('terminated', ref))

        for message in self.inbox:
            if message != ('terminated', ANY):
                Events.log(DeadLetter(ref, message))
        self.inbox = None
        self.priority_inbox = None

    def _do_watched(self, other):
        if not self.watchers:
            self.watchers = []
        self.watchers.append(other)

    @inlineCallbacks
    def _do_restart(self):
        self.suspended = True
        self._shutdown()
        self.actor = yield self._construct()
        self.suspended = False

    def _do_child_terminated(self, child):
        # TODO: PLEASE OPTIMISE
        # probably a child that we already stopped as part of a restart
        if child not in self._children.values():
            # LOGEVENT(TerminationIgnored(self, child))
            return
        itms = self._children.items()
        ix = itms.index((ANY, child))
        del self._children[itms[ix][0]]

    def _shutdown(self):
        if self.actor and hasattr(self.actor, 'post_stop'):
            try:
                self.actor.post_stop()  # XXX: possibly add `yield` here
            except Exception:
                _, exc, tb = sys.exc_info()
                Events.log(ErrorIgnored(self.actor, exc, tb))
        self.actor = None

        for child in self._children.values():
            child.stop()
        self._children = {}

    def report_to_parent(self, df=None):
        _, exc, tb = sys.exc_info()
        try:
            # Events.log(Error(self, e, sys.exc_info()[2])),
            self._do_suspend()
            # XXX: might make sense to make it async by default for better latency
            self.parent.send(('_error', self.ref(), exc, tb))
        except Exception:
            print("*** FAILED TO SUPERVISE:", file=sys.stderr)
            traceback.print_exc(sys.stderr)

    def ref(self):
        if self.stopped:
            return ActorRef(None, path=self.path)

        if not self._ref or not self._ref():
            ref = ActorRef(self, self.path)  # must store in a temporary variable to avoid immediate collection
            self._ref = weakref.ref(ref)
        return self._ref()

    def _generate_name(self):
        """Overrides ActorContext._generate_name"""
        if not self._child_name_gen:
            self._child_name_gen = ('$%d' % i for i in count(1))
        return self._child_name_gen.next()

    def __repr__(self):
        return "<Cell:%s>" % (type(self.actor).__name__ if self.actor else (self.factory.__name__ if isinstance(self.factory, type) else repr(self.factory)))
