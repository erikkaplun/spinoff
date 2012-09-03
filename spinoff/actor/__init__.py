import _actor
from _actor import *
# from . import comm


def _build_node_instance():
    # TODO: import configuration and set up a real Hub
    return Node()

# XXX: this should be lazy, e.g. using a proxy
Node = _actor._NODE = _build_node_instance()
