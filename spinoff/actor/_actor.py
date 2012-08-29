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

from twisted.internet.defer import inlineCallbacks, returnValue, Deferred
from txcoroutine import coroutine

from spinoff.util.async import call_when_idle
from spinoff.util.pattern_matching import IS_INSTANCE, ANY
from spinoff.actor.events import UnhandledError, Events, UnhandledMessage, DeadLetter, ErrorIgnored, TopLevelActorTerminated
from spinoff.actor.supervision import Decision, Resume, Restart, Stop, Escalate, Default
from spinoff.actor.events import SupervisionFailure
from spinoff.util.async import call_when_idle_unless_already
from spinoff.util.async import with_timeout
from spinoff.util.async import Timeout
from spinoff.util.pattern_matching import Matcher


# these messages get special handling from the framework and never reach Actor.receive
_SYSTEM_MESSAGES = ('_start', '_stop', '_restart', '_suspend', '_resume', ('_child_terminated', ANY))


def dbg(*args):
    print(file=sys.stderr, *args)


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

    @classmethod
    def remote(cls, path, node, the_dude):
        proxy = the_dude.make_proxy(path, node)
        return ActorRef(proxy, path, node)

    def __init__(self, target, path, node=None):
        self.target = target
        self.path = path
        self.name = path.rsplit('/', 1)[-1]
        self.node = node

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
        elif ('_watched', ANY) == message:
            message[1].send(('terminated', self))
        elif message not in ('_stop', '_suspend', '_resume', '_restart', ('terminated', ANY)):
            Events.log(DeadLetter(self, message))

    def __lshift__(self, message):
        """See ActorRef.send"""
        self.send(message)
        return self

    def __repr__(self):
        return '<actor%s:%s>' % ((':%s' % type(self.target.actor).__name__ if self.target else '<dead>'), self.path)

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

    def join(self):
        future = Future()
        self << ('_watched', future)
        return future

    def __eq__(self, other):
        return (isinstance(other, ActorRef) and self.path == other.path and self.node == other.node
                or isinstance(other, Matcher) and other == self)

    def __getstate__(self):
        return {'path': self.path, 'node': self.node}


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
        assert name not in self._children  # XXX: ordering??
        self._children[name] = None
        child = _do_spawn(parent=self.ref(), factory=factory, path=path)
        if name in self._children:  # it might have been removed already
            self._children[name] = child
        return child

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

    @property
    def children(self, child):
        return self._children.values()

    def _child_gone(self, child):
        name = child.path.rsplit('/', 1)[-1]
        del self._children[name]


class Guardian(_ActorContainer):
    path = '/'

    def ref(self):
        return self

    def send(self, message, force_async=False):
        if ('_error', ANY, IS_INSTANCE(Exception), IS_INSTANCE(types.TracebackType) | IS_INSTANCE(basestring)) == message:
            _, sender, exc, tb = message
            Events.log(UnhandledError(sender, exc, tb))
        elif ('_child_terminated', ANY) == message:
            _, sender = message
            self._child_gone(sender)
            Events.log(TopLevelActorTerminated(sender))
        else:
            Events.log(UnhandledMessage(self, message))

    @inlineCallbacks
    def stop(self):
        # dbg("GUARDIAN: stopping")
        for actor in Guardian._children.values():
            # dbg("GUARDIAN: stopping", actor)
            actor.stop()
            # dbg("GUARDIAN: joining...", actor, actor.target)
            try:
                yield with_timeout(.01, actor.join())
            except Timeout:
                # dbg("GUARDIAN: actor %r refused to stop" % (actor,))
                assert False, "actor %r refused to stop" % (actor,)
                # TODO: force-stop
            # dbg("GUARDIAN: ...OK", actor)

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

    # TODO: this is a potential spot for optimisation: we could put the message directly in the Cell's inbox without
    # without invoking the receive logic which will not handle the message immediately anyway because we're processing
    # a message already--otherwise this method would not get called.
    def send(self, *args, **kwargs):
        """Alias for self.ref.send"""
        self.ref.send(*args, **kwargs)

    def __lshift__(self, message):
        self.ref.send(message)
        return self

    def stop(self):
        self.ref.stop()

    def __repr__(self):
        return "<actor-impl:%s@%s>" % (type(self).__name__, self.ref.path)


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

    def _debug_queue(self):
        return (list(self.priority_inbox) + list(self.inbox)
                if self.priority_inbox is not None and self.inbox is not None
                else '(not yet started or already stopped)')

    def receive(self, message, force_async=False):
        # dbg("RECV: %r => %r%s%s  queue: %s" % (message, self, ' (stopped)' if self.stopped else '', ' (shutting down)' if self.shutting_down else '', self._debug_queue()))
        if self.stopped:
            return

        if self.shutting_down:
            # dbg("RECV: already shutting down...")
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
            elif self.priority_inbox:
                self.priority_inbox.append(message)
            return

        # XXX: should ('terminated', child) also be prioritised?

        # '_watched' is something that is safe to handle immediately as it doesn't change the visible state of the actor;
        # NB: DO NOT do the same with '_suspend', '_resume' or any other message that changes the visible state of the actor!
        if message == ('_watched', ANY):
            self._do_watched(message[1])
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

    def process_messages(self, force_async=False):
        # dbg("PROCESS-MSGS: %r already processing? %r" % (self, self.processing_messages,))
        next_message = self.peek_message()

        is_startstop = next_message in ('_start', '_stop')
        is_untaint = next_message in ('_resume', '_restart')

        if not self.processing_messages and (self.started or is_startstop or self.tainted and is_untaint):
            if Actor.SENDING_IS_ASYNC or force_async:
                # dbg("PROCESS-MSGS: async (Actor.SENDING_IS_ASYNC? %s  force_async? %s" % (Actor.SENDING_IS_ASYNC, force_async))
                call_when_idle_unless_already(self._process_messages)  # TODO: check if there's an already scheduled call to avoid redundant calls
            else:
                # dbg("PROCESS-MSGS: immediate")
                self._process_messages()
        # else:
        #     dbg("PROCESS-MSGS: ...returning", self)

    @inlineCallbacks
    def _process_messages(self):
        # dbg("-PROCESS-MSGS: %r suspended? %r  has-message? %r" % (self, self.suspended, self.peek_message()))
        # first = True
        try:
            while not self.stopped and (not self.shutting_down) and self.has_message() and (not self.suspended or self.peek_message() in ('_stop', '_restart', '_resume', '_suspend')) or (self.shutting_down and ('_child_terminated', ANY) == self.peek_message()):
                message = self.consume_message()
                self.processing_messages = repr(message)
                # if not first:
                #     dbg("-PROCESS-MSGS: taking next msg %r => %r  queue: %r" % (message, self, self._debug_queue()))
                # first = False
                try:
                    d = self._process_one_message(message)
                    # if isinstance(ret, Deferred) and not self.receive_is_coroutine:
                    #     warnings.warn(ConsistencyWarning("prefer yielding Deferreds from Actor.receive rather than returning them"))
                    yield d
                except Exception:
                    self.report_to_parent()
                # else:
                #     dbg("-PROCESS-MSGS: ...process_one(%r) terminated %r" % (message, self))
                finally:
                    self.processing_messages = False
            # dbg("-PROCESS-MSGS: %r ...no more messages (queue: %r)" % (self, self._debug_queue()))
        except Exception:
            self.report_to_parent()

    @inlineCallbacks
    def _process_one_message(self, message):
        # dbg("PROCESS-ONE: %r => %r" % (message, self))
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
            raise UnhandledTermination
        else:
            Events.log(UnhandledMessage(self.ref(), message))

    @inlineCallbacks
    def _construct(self):
        # dbg("CONSTRUCT:", self)
        factory = self.factory

        try:
            actor = factory()
        except Exception:
            raise CreateFailed("Constructing actor with %s failed" % (factory,))
        else:
            self.actor = actor

        actor._parent = self.parent
        actor._set_cell(self)

        if hasattr(actor, 'pre_start'):
            pre_start = actor.pre_start
            try:
                self._ongoing = pre_start()
                yield self._ongoing
                del self._ongoing
            except Exception:
                raise CreateFailed("Actor failed to start: %s" % (actor,))

        self.constructed = True
        # dbg("CONSTRUCT: ...ok", self)

    @inlineCallbacks
    def _do_start(self):
        try:
            yield self._construct()
        except Exception:
            self.tainted = True
            raise
        else:
            self.started = True
            # dbg("STARTED:", self)

    def _do_supervise(self, child, exc, tb):
        # dbg("SUPERVISE: %r => %r @ %r" % (child, exc, self.ref()))
        if child not in self._children.values():  # TODO: use a denormalized set
            Events.log(ErrorIgnored(child, exc, tb))
            return

        supervise = getattr(self.actor, 'supervise', None)

        if supervise:
            decision = supervise(exc)
        if not supervise or decision == Default:
            # dbg("SUP: fallback to default @ %r= > %r" % (self.ref(), child))
            decision = default_supervise(exc)

        # dbg("SUP: %r => %r @ %r => %r" % (exc, decision, self.ref(), child))

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

    def _do_suspend(self):
        # dbg("SUSPEND:")
        self.suspended = True
        if self._ongoing:
            # dbg("SUSPEND: ongoing.pause")
            self._ongoing.pause()
        if hasattr(self.actor, '_coroutine') and self.actor._coroutine:
            # dbg("SUSPEND: calling coroutine.pause on", self.actor._coroutine)
            self.actor._coroutine.pause()

        for child in self._children.values():
            child.send('_suspend')

    def _do_resume(self):
        if self.tainted:
            # dbg("RESUME: tainted => restarting...")
            warnings.warn("Attempted to resume an actor that failed to start; falling back to restarting:\n%s" % (''.join(traceback.format_stack()),))
            self.tainted = False
            return self._do_restart()
        else:
            # dbg("RESUME: resuming...", self)
            self.suspended = False
            if self._ongoing:
                self._ongoing.unpause()
            if hasattr(self.actor, '_coroutine'):
                self.actor._coroutine.unpause()

            for child in self._children.values():
                child.send('_resume')
        # dbg("RESUME: ...ok", self)

    def _do_stop(self):
        # dbg("STOP: %r" % (self,))
        if self._ongoing:
            del self._ongoing
        # del self.watchers
        self._shutdown().addCallback(self._finish_stop)

    def _finish_stop(self, _):
        # dbg("FINISH-STOP:", self)
        try:
            ref = self.ref()

            # TODO: test that system messages are not deadlettered
            for message in self.inbox:
                if ('_error', ANY, ANY, ANY) == message:
                    _, sender, exc, tb = message
                    Events.log(ErrorIgnored(sender, exc, tb))
                elif ('_watched', ANY) == message:
                    _, watcher = message
                    watcher.send(('terminated', ref))
                elif ('terminated', ANY) != message:
                    Events.log(DeadLetter(ref, message))

            if self.actor:
                self.actor = None
            del self.inbox
            del self.priority_inbox  # don't want no more, just release the memory

            # dbg("FINISH-STOP: unlinking reference")
            del ref.target
            self.stopped = True

            # XXX: which order should the following two operations be done?

            self.parent.send(('_child_terminated', ref))

            for watcher in self.watchers:
                watcher.send(('terminated', ref))
        except Exception:
            _, exc, tb = sys.exc_info()
            Events.log(ErrorIgnored(ref, exc, tb))
        # dbg("FINISH-STOP: ...stopped!", self)

    def _do_watched(self, other):
        # dbg("WATCHED: %r <== %r" % (self, other))
        if self.stopped:
            other << ('terminated', self.ref())
            return
        if not self.watchers:
            self.watchers = []
        self.watchers.append(other)

    @inlineCallbacks
    def _do_restart(self):
        # dbg("RESTART:", self)
        self.suspended = True
        yield self._shutdown()
        yield self._construct()
        self.suspended = False
        # dbg("RESTART: ...ok", self)

    def _do_child_terminated(self, child):
        # TODO: PLEASE OPTIMISE
        # probably a child that we already stopped as part of a restart
        if child not in self._children.values():
            # LOGEVENT(TerminationIgnored(self, child))
            return
        self._child_gone(child)
        # itms = self._children.items()
        # ix = itms.index((ANY, child))
        # del self._children[itms[ix][0]]
        if self.shutting_down and not self._children:
            # dbg("CHILD-TERM: signalling all children stopped")
            self._all_children_stopped.callback(None)

    @inlineCallbacks
    def _shutdown(self):
        # dbg("SHUTDOWN: %r shutting down..." % (self,))
        self.shutting_down = True

        if self._children:  # we don't want to do the Deferred magic if there're no babies
            self._all_children_stopped = Deferred()
            # dbg("SHUTDOWN: shutting down children:", self._children.values())
            for child in self._children.values():
                child.stop()
            # dbg("SHUTDOWN: waiting for all children to stop", self)
            yield self._all_children_stopped
            # dbg("SHUTDOWN: ...children stopped", self)

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
        # dbg("SHUTDOWN: ...OK: %r" % (self,))

    def report_to_parent(self, exc_and_tb=None):
        if not exc_and_tb:
            _, exc, tb = sys.exc_info()
        else:
            exc, tb = exc_and_tb
        try:
            # Events.log(Error(self, e, sys.exc_info()[2])),
            self._do_suspend()
            # XXX: might make sense to make it async by default for better latency
            self.parent.send(('_error', self.ref(), exc, tb), force_async=True)
        except Exception:
            try:
                Events.log(ErrorIgnored(self.ref(), exc, tb))
                _, sys_exc, sys_tb = sys.exc_info()
                Events.log(SupervisionFailure(self.ref(), sys_exc, sys_tb))
            except Exception:
                print("*** PANIC", file=sys.stderr)
                traceback.print_exc(file=sys.stderr)

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
        return "<cell:%s@%s%s>" % (type(self.actor).__name__ if self.actor else (self.factory.__name__ if isinstance(self.factory, type) else repr(self.factory)),
                                   self.path,
                                   (' (%s)' % ('dead' if self.stopped else ('stopping' if self.shutting_down else 'new'))) if not self.actor else '',)


# TODO: replace with serializable temporary actors
class Future(Deferred):  # TODO: ActorRefBase or IActorRef or smth
    def send(self, message):
        self.callback(message)


def _ignore_error(actor):
    _, exc, tb = sys.exc_info()
    Events.log(ErrorIgnored(actor, exc, tb))
