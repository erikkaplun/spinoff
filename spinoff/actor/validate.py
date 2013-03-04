import re


_VALID_IP_RE = '(?:(?:[0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}(?:[0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])'
_VALID_HOSTNAME_RE = '(?:(?:[a-zA-Z]|[a-zA-Z][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)*(?:[A-Za-z]|[A-Za-z][A-Za-z0-9\-]*[A-Za-z0-9])'
_VALID_NODEID_RE = re.compile('(?:%s|%s):(?P<port>[0-9]+)$' % (_VALID_HOSTNAME_RE, _VALID_IP_RE))


def _validate_nodeid(nodeid):
    if '|' in nodeid:
        nodeid = nodeid.split('|', 1)[0]
    # call from app code
    m = _VALID_NODEID_RE.match(nodeid)
    if not m:  # pragma: no cover
        raise ValueError("Node IDs should be in the format '<ip-or-hostname>:<port>': %s" % (nodeid,))
    port = int(m.group(1))
    if not (0 <= port <= 65535):  # pragma: no cover
        raise ValueError("Ports should be in the range 0-65535: %d" % (port,))
