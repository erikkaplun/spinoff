from twisted.internet.defer import inlineCallbacks, DeferredList
from zope.interface import implements

from spinoff.actor.actor import IActor


class ComponentCollection(object):
    implements(IActor)

    def __init__(self, *members):
        self._connections = []
        self._members = []
        self._parent = None
        for member in members:
            self.add(member)

    def add(self, component):
        self._members.append(component)
        self._connect([component], self._connections)

    def connect(self, *args, **kwargs):
        connection = (args, kwargs)
        self._connections.append(connection)
        self._connect(self._members, [connection])

    def _connect(self, components, connections):
        for connection in connections:
            args, kwargs = connection
            for component in components:
                component.connect(*args, **kwargs)
