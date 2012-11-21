import socket

from spinoff.actor import _actor


def resolve(hostname):
    if not _actor.TESTING:
        return socket.gethostbyname(hostname)
    else:
        return hostname
