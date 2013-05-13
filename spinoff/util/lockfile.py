from __future__ import absolute_import

import time
from contextlib import contextmanager

from gevent import sleep
from lockfile import LockFile, AlreadyLocked, LockTimeout


@contextmanager
def lock_file(path, maxdelay=.1, lock_cls=LockFile, timeout=10.0):
    """Cooperative file lock. Uses `lockfile.LockFile` polling under the hood.

    `maxdelay` defines the interval between individual polls.

    """
    lock = lock_cls(path)
    max_t = time.time() + timeout
    while True:
        if time.time() >= max_t:
            raise LockTimeout("Timeout waiting to acquire lock for %s" % (path,))  # same exception messages as in lockfile
        try:
            lock.acquire(timeout=0)
        except AlreadyLocked:
            sleep(maxdelay)
        else:
            try:
                yield lock
                break
            finally:
                lock.release()
