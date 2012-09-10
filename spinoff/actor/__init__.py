import _actor
from _actor import *
# from . import comm


# CurrentNode = None


def set_default_node(node=None):
    global CurrentNode
    CurrentNode = _actor._NODE = node or Node.make_local()
    return CurrentNode
