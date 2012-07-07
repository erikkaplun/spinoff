import sys


def main():
    from twisted.scripts import twistd
    from pyutils import autoreload
    from twisted.internet import defer

    defer.setDebugging(True)

    if len(sys.argv) == 1 or sys.argv[1] != '-n' and len(sys.argv) != 2:
        print "usage: runactor path.to.Actor"
        sys.exit(1)

    # XXX: hack.
    if sys.argv[1] != '-n':
        sys.argv[1:] = ['-n'] + ['runactor', '-a', sys.argv[1]]
    autoreload.main(twistd.run)
