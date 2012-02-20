from twisted.internet.defer import DeferredList, inlineCallbacks
from zope.interface import implements

from spinoff.component.component import IComponent


class CompositeComponentBase(object):
    implements(IComponent)

    def __init__(self, *members):
        self._connections = []
        self._parent = None
        for member in members:
            self.add(member)

    def add(self, component):
        self._connect(component)
        self._members.append(component)
        self._connect([component], self._connections)
        self._set_parent([component])

    def connect(self, *args, **kwargs):
        connection = (args, kwargs)
        self._connections.append(connection)
        self._connect(self._members, [connection])

    def setServiceParent(self, parent):
        assert not self._parent
        self._parent = parent
        self._set_parent(self._members)

    def _connect(self, components, connections):
        for connection in connections:
            args, kwargs = connection
            for component in components:
                component.connect(*args, **kwargs)

    def _set_parent(self, components):
        if self._parent:
            for component in components:
                component.setServiceParent(self._parent)


class Filter(CompositeComponentBase):
    """Filters messages based on `routing_key`.

    The function that maps members to routing key values needs to be provided in the constructor.

    """
    def __init__(self, key_fun, *args, **kwargs):
        super(Filter, self).__init__(*args, **kwargs)
        self._key_fun = key_fun

    @inlineCallbacks
    def deliver(self, inbox, message, routing_key):
        ds = []
        for member in self._members:
            if self._key_fun(member) == routing_key:
                ds.append(member.deliver(inbox, message))
        yield DeferredList(ds, consumeErrors=True, fireOnOneErrback=True)


class LoadBalancer(CompositeComponentBase):
    """Round-robin balancer.

    Not really needed--load should be balanced by directing it to another process or node in the network.

    """
    _next_index = 0

    def deliver(self, message, inbox, routing_key):
        component = self._members[self._next_index]
        self._next_index = (self._next_index + 1) % len(self._members)
        return component.deliver(message, inbox, routing_key)


class Publisher(CompositeComponentBase):

    @inlineCallbacks
    def deliver(self, *args, **kwargs):
        ds = []
        for component in self._members:
            ds.append(component.deliver(*args, **kwargs))
        for d in ds: yield d
