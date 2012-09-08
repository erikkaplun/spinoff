import _actor
from _actor import *
# from . import comm


CurrentNode = None


def set_default_node(node):
    global Node
    CurrentNode = _actor._NODE = node
