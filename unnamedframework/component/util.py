from twisted.internet.defer import inlineCallbacks, DeferredList
from zope.interface import implements

from unnamedframework.component.component import IComponent


class ComponentCollection(object):
    implements(IComponent)

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


class Filter(ComponentCollection):
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
                ds.append(member.deliver(inbox, message, routing_key))
        yield DeferredList(ds, consumeErrors=True, fireOnOneErrback=True)
