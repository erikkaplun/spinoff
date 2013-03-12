from __future__ import print_function

import errno
import os


def read_file_async(threadpool, fhandle, limit=None):
    return threadpool.apply(_do_read_file_async, args=(fhandle, limit))


def _do_read_file_async(fhandle, limit):
    return fhandle.read(limit) if limit is not None else fhandle.read()


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


def reasonable_get_mtime(fname):
    mtime = os.stat(fname).st_mtime
    return round(mtime, 2)
