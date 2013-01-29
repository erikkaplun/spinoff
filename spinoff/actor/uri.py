from collections import deque

from spinoff.util.pattern_matching import Matcher
from spinoff.actor.validate import _validate_nodeid


class Uri(object):
    """Represents the identity and location of an actor.

    Attention: for the sake of consistency, the root `Uri` is represented by an empty string, **not** `'/'`. The `'/'` is
    used **only** as a path separator. Thus, both of the name and path path of the root `Uri` are `''`, and the steps
    are `['']`. The root `Uri` is also only `__eq__` to `''` and not `'/'`.

    """
    _node = None

    def __init__(self, name, parent, node=None):
        if name and node:
            raise TypeError("node specified for a non-root Uri")  # pragma: no cover
        self.name, self.parent = name, parent
        if node:
            _validate_nodeid(node)
            self._node = node

    @property
    def root(self):
        """Returns the topmost `Uri` this `Uri` is part of."""
        return self.parent.root if self.parent else self

    @property
    def node(self):
        """Returns the node ID this `Uri` points to."""
        return self.root._node

    def __div__(self, child):
        """Builds a new child `Uri` of this `Uri` with the given `name`."""
        if not child or not isinstance(child, str):
            raise TypeError("Uri traversal expected a non-empty str but got %r" % (child,))  # pragma: no cover
        if child in ('.', '..'):
            raise TypeError("Traversing using . and .. is not supported (yet)")  # pragma: no cover
        elif '/' in child:
            raise TypeError("Traversing more than 1 level at a time is not supported (yet)")  # pragma: no cover
        return Uri(name=child, parent=self)

    @property
    def path(self):
        """Returns the `Uri` without the `node` part as a `str`."""
        return '/'.join(self.steps)

    @property
    def steps(self):
        """Returns an iterable containing the steps to this `Uri` from the root `Uri`, including the root `Uri`."""
        def _iter(uri, acc):
            acc.appendleft(uri.name if uri.name else '')
            return _iter(uri.parent, acc) if uri.parent else acc
        return _iter(self, acc=deque())

    def __str__(self):
        return (self.node or '') + self.path

    def __repr__(self):
        return '<@%s>' % (str(self),)

    @property
    def url(self):
        return 'tcp://' + str(self) if self.node else None

    @classmethod
    def parse(cls, addr):
        """Parses a new `Uri` instance from a string representation of a URI.

        >>> u1 = Uri.parse('/foo/bar')
        >>> u1.node, u1.steps, u1.path, u1.name
        (None, ['', 'foo', 'bar'], '/foo/bar', 'bar')
        >>> u2 = Uri.parse('somenode:123/foo/bar')
        >>> u2.node, u1.steps, u2.path, ur2.name
        ('somenode:123', ['', 'foo', 'bar'], '/foo/bar', 'bar')
        >>> u1 = Uri.parse('foo/bar')
        >>> u1.node, u1.steps, u1.path, u1.name
        (None, ['foo', 'bar'], 'foo/bar', 'bar')

        """
        if addr.endswith('/'):
            raise ValueError("Uris must not end in '/'")  # pragma: no cover
        parts = addr.split('/')
        if ':' in parts[0]:
            node, parts[0] = parts[0], ''
        else:
            node = None

        ret = None  # Uri(name=None, parent=None, node=node) if node else None
        for step in parts:
            ret = Uri(name=step, parent=ret, node=node)
            node = None  # only set the node on the root Uri
        return ret

    @property
    def local(self):
        """Returns a copy of the `Uri` without the node. If the `Uri` has no node, returns the `Uri`."""
        if not self.node:
            return self
        else:
            return Uri.parse(self.path)

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        """Returns `True` if `other` points to the same actor.

        This method is cooperative with the `pattern_matching` module.

        """
        if isinstance(other, str):
            other = Uri.parse(other)
        return str(self) == str(other) or isinstance(other, Matcher) and other == self

    def __ne__(self, other):
        return not (self == other)
