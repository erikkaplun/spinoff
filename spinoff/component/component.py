import warnings
from collections import defaultdict

from twisted.application import service
from twisted.application.service import Service
from twisted.internet.defer import Deferred, inlineCallbacks, returnValue, DeferredQueue
from zope.interface import Interface, implements

from spinoff.util.meta import selfdocumenting


__all__ = ['IComponent', 'IProducer', 'IConsumer', 'Component', 'Pipeline', 'Application', 'NoRoute', 'RoutingException', 'InterfaceException']


class NoRoute(Exception):
    pass


class RoutingException(Exception):
    pass


class InterfaceException(Exception):
    pass


class IProducer(Interface):

    def connect(outbox, (inbox, component)):
        """Connects the `outbox` of this component to one of the `inbox`es of another `component`.

        It is legal to pass in `self` as the value of `component` if needed.

        """


class IConsumer(Interface):

    def deliver(message, inbox, routing_key=None):
        """Delivers an incoming `message` into one of the `inbox`es of this component with an optional `routing_key`.

        The `routing_key` argument is intended for writing routers to be able to have branching in the flow of
        messages so as to avoid having to resort to using a non-static number of outboxes.

        Returns a `Deferred` which will be fired when this component has received the `message`.

        """

    def plugged(inbox, component):
        """Called when something has been plugged into the specified `inbox` of this `IConsumer`.

        (Optional).

        """


class IComponent(IProducer, IConsumer):
    pass


class Component(object, Service):
    implements(IComponent)

    def __init__(self, connections=None, *args, **kwargs):
        super(Component, self).__init__(*args, **kwargs)
        self._inboxes = defaultdict(lambda: DeferredQueue(backlog=1))
        self._waiting = {}
        self._outboxes = {}
        if connections:
            for connection in connections.items():
                self.connect(*connection)

    def deliver(self, message, inbox, routing_key=None):
        d = Deferred()
        self._inboxes[inbox].put((message, d, routing_key))
        return d

    def connect(self, outbox=None, to=None):
        """%(parent_doc)s

        The connection (`to`) can be either a tuple of `(<inbox>, <receiver>)` or just `receiver`, in which case `<inbox>` is
        taken to be the same as `outbox`.

        If no `outbox` is specified, it is taken to be `'default'`, thus:

            `comp_a.connect(to=...)`

        is equivalent to:

            `comp_a.connect('default', ...)`

        and

            `comp_a.connect(to=comp_b)`

        is equivalent to:

            `a.connect('default', ('default', b))`

        """
        if hasattr(outbox, '__iter__'):
            for o in outbox:
                self._connect(o, to)
        else:
            self._connect(outbox, to)
    connect.__doc__ %= {'parent_doc': IComponent.getDescriptionFor('connect').getDoc()}

    def _connect(self, outbox, to):
        inbox, receiver = (to if isinstance(to, tuple) else (outbox, to))
        self._outboxes.setdefault(outbox, []).append((inbox, receiver))
        if hasattr(receiver, 'plugged'):
            receiver.plugged(inbox, self)
        if hasattr(self, 'connected'):
            self.connected(outbox, receiver)

    def plugged(self, inbox, component):
        self._inboxes[inbox]  # leverage defaultdict behaviour

    @selfdocumenting
    def short_circuit(self, outbox, inbox=None):
        if inbox is None:
            inbox = outbox
        self.connect(outbox, (inbox, self))

    @inlineCallbacks
    def get(self, inbox='default'):
        """Retrieves a message from the specified `inbox`.

        Returns a `Deferred` which will be fired when a message is available in the specified `inbox` to be returned.

        This method will not complain if nothing has been connected to the requested `inbox`.

        """
        routing_key, message = yield self._get(inbox, routed=False)
        returnValue(message)

    @inlineCallbacks
    def get_routed(self, inbox='default'):
        routing_key, message = yield self._get(inbox, routed=True)
        returnValue((routing_key, message))

    @inlineCallbacks
    def _get(self, inbox, routed):
        if inbox not in self._inboxes:
            warnings.warn("Component %s attempted to get from a non-existent inbox %s" % (repr(self), repr(inbox)))
        message, d, routing_key = yield self._inboxes[inbox].get()
        if bool(routed) != (routing_key is not None):
            raise InterfaceException("Routing key was%s expected but was%s found" % (' not' if not routed else '', ' not' if routing_key is None else ''))
        d.callback(None)
        returnValue((routing_key, message))

    def put(self, message, outbox='default', routing_key=None):
        """Puts a `message` into one of the `outbox`es of this component with an optional `routing_key`.

        If the specified `outbox` has not been previously connected to anywhere (see `Component.connect`), a
        `NoRoute` will be raised, i.e. outgoing messages cannot be queued locally and must immediately be delivered
        to an inbox of another component and be queued there (if/as needed).

        Returns a `Deferred` which will be fired when the messages has been delivered to all connected components.

        """
        if outbox not in self._outboxes:
            raise NoRoute("Component %s has no connection from outbox %s" % (repr(self), repr(outbox)))

        connections = self._outboxes[outbox]
        for inbox, component in connections:
            component.deliver(message, inbox, routing_key)

    # `startService` and `stopService` are ugly name because they 1) repeat the class
    # name and 2) not all `Service`s want to be labelled as "services".
    # Thus, we effectively rename `startService`/`stopService` to `start`/`stop` for subclasses
    # to override.
    def startService(self):
        self.start()

    def stopService(self):
        self.stop()

    def start(self):
        pass

    def stop(self):
        pass

    def debug_state(self, name=None):
        for inbox, queue in self._inboxes.items():
            print '*** %s.INBOX %s:' % (name or '', inbox)
            for message, _ in queue.pending:
                print '*** \t%s' % message


def _normalize_pipe(pipe):
    if not isinstance(pipe, tuple):
        pipe = (pipe, )
    assert len(pipe) <= 3, "A pipe definition is should be a 3-tuple"

    is_box = lambda x: isinstance(x, basestring)

    if len(pipe) == 3:
        assert is_box(pipe[0]), "Left item of a pipe definition should be an inbox name"
        assert is_box(pipe[2]), "Right item of a pipe definition should be an outbox name"
    elif len(pipe) == 1:
        pipe = ('default', pipe[0], 'default')
    else:
        pipe = ('default', ) + pipe if is_box(pipe[1]) else pipe + ('default', )

    assert is_box(pipe[0]) or is_box(pipe[2]), "Left and right item of a pipe definition shuld be box names"
    return pipe


def Pipeline(*pipes):
    """Returns a `Pipeline` that can be used as part of an `Application`.

    A `Pipeline` consists of one ore more pipes.

    A pipe is a connection/link in the pipeline; a pipe connects a
    component to its neighbouring components via inboxes and outboxes;
    the normalized form of a pipe definition is a 3-tuple of the form:

        `(<inbox-name>, <component>, <outbox-name>)`

    where `inbox-name`
    and `outbox-name` should be strings; a pipe definition can
    optionally be shortened to following forms:

        `(<inbox-name>, <component>)`
        `(<component>, <outbox-name>)`
        `(<component>, )`
        `<component>`

    each of which will be normalized, unspecified box names defaulting
    to `'default'`.

    """
    pipes = [_normalize_pipe(pipe) for pipe in pipes]

    for sender, receiver in zip(pipes[:-1], pipes[1:]):
        _, sender, outbox = sender
        inbox, receiver, _ = receiver
        sender.connect(outbox, (inbox, receiver))

    return [pipe[1] for pipe in pipes]


def Application(*pipelines):
    """Returns an application object that can be run using `twistd`.

    An `Application` consists of one or more pipelines.

    """
    services = []
    for pipeline in pipelines:
        # components = [connection[1] for stage in pipeline for connection in stage]
        services.extend(pipeline)

    application = service.Application("DTS Server")
    for s in services:
        s.setServiceParent(application)

    return application
