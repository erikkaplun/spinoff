# coding: utf-8
from __future__ import print_function

import traceback
from pickle import PicklingError

import gevent
from spinoff.actor import _actor
from spinoff.actor.exceptions import LookupFailed
from spinoff.actor.guardian import Guardian
from spinoff.actor.ref import Ref
from spinoff.actor.supervision import Stop
from spinoff.actor.uri import Uri
from spinoff.util.logging import err


class Node(object):
    """`Node` is both a singleton instance and a class lookalike, there is thus always available a default global
    `Node` instance but it is possible to create more, non-default, instances of `Node` by simply calling it as if it
    were a class, i.e. using it as a class. This is mainly useful for testing multi-node scenarios without any network
    involved by setting a custom `actor.remoting.Hub` to the `Node`.

    """
    hub = None
    _all = []

    @classmethod
    def stop_all(cls):
        for node in cls._all:
            try:
                node.stop()
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
        self.guardian = Guardian(uri=self._uri, node=self, hub=hub, supervision=root_supervision)
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
            return Ref(cell=None, uri=uri, is_local=False, hub=self.hub)

    def spawn(self, *args, **kwargs):
        return self.guardian.spawn(*args, **kwargs)

    def stop(self):
        self.guardian.stop()
        # TODO: just make the node send a special notification to other nodes, so as to avoid needless sending of many
        # small termination messages:
        if not _actor.TESTING:  # XXX: this might break some (future) tests that test termination messaging
            gevent.sleep(.1)  # let actors send termination messages
        self.hub.stop()

    def __getstate__(self):  # pragma: no cover
        raise PicklingError("Node cannot be serialized")

    def __repr__(self):
        return '<node:%s>' % (self._uri if self.hub else 'local')

    def __call__(self, hub=None):
        """Spawns new, non-default instances of the guardian; useful for testing."""
        return type(self)(hub)
