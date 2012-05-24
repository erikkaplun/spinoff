from twisted.internet.defer import inlineCallbacks
from zope.interface import implements

from spinoff.actor.actor import IActor


class CompositeComponentBase(object):
    implements(IActor)

    def __init__(self, *members):
        self._members = []
        self._connections = []
        self._parent = None
        for member in members:
            self.add(member)

    def add(self, component):
        self._members.append(component)
        self._connect([component], self._connections)
        self._set_parent([component])

    def remove(self, component):
        self._members.remove(component)

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


class LoadBalancer(CompositeComponentBase):
    """Round-robin balancer.

    Not really needed--load should be balanced by directing it to another process or node in the network.

    """
    _next_index = 0

    def deliver(self, message, inbox):
        component = self._members[self._next_index]
        self._next_index = (self._next_index + 1) % len(self._members)
        return component.deliver(message, inbox)


class Publisher(CompositeComponentBase):

    @inlineCallbacks
    def deliver(self, *args, **kwargs):
        ds = []
        for component in self._members:
            ds.append(component.deliver(*args, **kwargs))
        for d in ds:
            yield d
