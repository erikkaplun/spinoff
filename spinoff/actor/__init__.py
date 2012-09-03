import _actor
from _actor import *
# from . import comm


def _build_node_instance():
    from .remoting import HubWithNoRemoting
    # TODO: import configuration and set up a real Hub
    return Node(hub=HubWithNoRemoting())

# XXX: this should be lazy, e.g. using a proxy
Node = _actor._NODE = _build_node_instance()
