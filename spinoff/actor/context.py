from gevent import getcurrent


def _get_cell():
    curr = getcurrent()
    try:
        cell = curr._cell
    except AttributeError:
        if hasattr(curr, 'spawn_actor'):
            cell = curr
        else:
            return None
    # give out the method, not the cell object itself, to avoid exposing the internals
    return cell


def get_context():
    cell = _get_cell()
    return Context(cell) if cell else None


class Context(object):
    def __init__(self, cell):
        self.spawn = cell.spawn_actor
        self.sender = cell.actor.sender if cell.actor else None
        self.node = cell.node
        self.ref = cell.ref


def spawn(*args, **kwargs):
    return get_context().spawn(*args, **kwargs)
