import sys


def main():
    from twisted.scripts import twistd
    from pyutils import autoreload
    from twisted.internet import defer

    defer.setDebugging(True)

    # XXX: hack.
    if sys.argv[1] != '-n':
        sys.argv[1:] = ['-n'] + sys.argv[2:] + ['runactor', '-a', sys.argv[1]]
    autoreload.main(twistd.run)
