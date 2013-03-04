from __future__ import absolute_import

from contextlib import contextmanager

from gevent import sleep
from lockfile import LockFile, AlreadyLocked


@contextmanager
def lock_file(path, maxdelay=.1, lock_cls=LockFile):
    """Cooperative file lock. Uses `lockfile.LockFile` polling under the hood.

    `maxdelay` defines the interval between individual polls.

    """
    lock = lock_cls(path)
    while True:
        try:
            lock.acquire(timeout=0)
        except AlreadyLocked:
            sleep(maxdelay)
        else:
            try:
                yield
                break
            finally:
                lock.release()
