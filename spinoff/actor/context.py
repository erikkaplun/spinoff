from gevent import getcurrent


def get_context():
    curr = getcurrent()
    try:
        cell = curr._cell
    except AttributeError:
        if hasattr(curr, 'spawn_actor'):
            cell = curr
        else:
            return None
    # give out the method, not the cell object itself, to avoid exposing the internals
    return Context(cell)


class Context(object):
    def __init__(self, cell):
        self.spawn = cell.spawn_actor
        self.sender = cell.actor.sender if cell.actor else None
        self.node = cell.node
        self.ref = cell.ref
