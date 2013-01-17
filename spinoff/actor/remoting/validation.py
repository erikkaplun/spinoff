import re

from spinoff.actor._actor import _VALID_NODEID_RE, _validate_nodeid


_VALID_ADDR_RE = re.compile('tcp://%s' % (_VALID_NODEID_RE.pattern,))
_PROTO_ADDR_RE = re.compile('(tcp://)(%s)' % (_VALID_NODEID_RE.pattern,))


def _validate_addr(addr):
    # call from app code
    m = _VALID_ADDR_RE.match(addr)
    if not m:  # pragma: no cover
        raise ValueError("Addresses should be in the format 'tcp://<ip-or-hostname>:<port>': %s" % (addr,))
    port = int(m.group(1))
    if not (0 <= port <= 65535):  # pragma: no cover
        raise ValueError("Ports should be in the range 0-65535: %d" % (port,))


def _assert_valid_nodeid(nodeid):  # pragma: no cover
    try:
        _validate_nodeid(nodeid)
    except ValueError as e:
        raise AssertionError(e.message)


def _assert_valid_addr(addr):  # pragma: no cover
    try:
        _validate_addr(addr)
    except ValueError as e:
        raise AssertionError(e.message)


# semantic alias for prefixing with `assert`
def _valid_addr(addr):  # pragma: no cover
    _assert_valid_addr(addr)
    return True
