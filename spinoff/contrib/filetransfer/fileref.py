import os
import shutil
import tempfile
import uuid
from contextlib import contextmanager

from spinoff.actor.context import get_context
from spinoff.contrib.filetransfer.request import Request
from spinoff.contrib.filetransfer.server import Server
from spinoff.contrib.filetransfer.util import mkdir_p, reasonable_get_mtime
from spinoff.util.lockfile import lock_file
from spinoff.util.pattern_matching import ANY


__all__ = ['serve_file']


_NULL_CTX = contextmanager(lambda: (yield))


ERRNO_INVALID_CROSS_DEVICE_LINK = 18


def serve_file(path, abstract_path=None, node=None):
    mtime = reasonable_get_mtime(path)
    size = os.path.getsize(path)
    file_id = uuid.uuid4().get_hex()

    node = node or get_context().node
    server = Server._instances.get(node)
    if not server:
        server = Server._instances[node] = node.spawn(Server)

    server << ('serve', path, file_id)
    if abstract_path is None:
        abstract_path = os.path.basename(path)
    return FileRef(file_id, server, abstract_path=abstract_path, mtime=mtime, size=size)


class FileRef(object):
    def __init__(self, file_id, server, abstract_path, mtime, size):
        self.file_id = file_id
        self.server = server
        self.abstract_path = abstract_path
        self.mtime = mtime
        self.size = size

    def fetch(self, dst_path=None):
        if dst_path is not None and os.path.isdir(dst_path):
            raise TransferFailed("%r is a directory" % (dst_path,))
        if dst_path:
            mkdir_p(os.path.dirname(dst_path))
        with lock_file(dst_path) if dst_path is not None else _NULL_CTX():
            if (dst_path is not None and
                    os.path.exists(dst_path) and
                    reasonable_get_mtime(dst_path) == self.mtime and
                    os.path.getsize(dst_path) == self.size):
                return dst_path

            ret = None
            # local
            if self.server.uri.node == get_context().ref.uri.node:
                _, src_path = self.server.ask(('request-local', self.file_id))
                assert _ == 'local-file'
                # store to specific path
                if dst_path is not None:
                    if src_path != dst_path:
                        if os.path.exists(dst_path):
                            os.unlink(dst_path)
                        shutil.copy(src_path, dst_path)
                    ret = dst_path
                # store to temp
                else:
                    fd, tmppath = tempfile.mkstemp()
                    os.close(fd)
                    shutil.copy(src_path, tmppath)
                    ret = tmppath
            # remote
            else:
                fd, tmppath = tempfile.mkstemp()
                os.close(fd)
                with open(tmppath, 'wb') as tmpfile:
                    transferred_size = self._transfer(tmpfile)
                if transferred_size != self.size:
                    os.unlink(tmppath)
                    raise TransferFailed("fetched file size %db does not match remote size %db" % (transferred_size, self.size))
                # store to specific path
                if dst_path:
                    if os.path.exists(dst_path):
                        os.unlink(dst_path)
                    move_or_copy(tmppath, dst_path)
                    ret = dst_path
                # store to temp
                else:
                    ret = tmppath
            os.utime(ret, (self.mtime, self.mtime))
            return ret

    def _transfer(self, fh):
        request = get_context().spawn(Request.using(server=self.server, file_id=self.file_id, size=self.size, abstract_path=self.abstract_path))
        more = True
        ret = 0
        while more:
            msg = request.ask('next')
            if msg == 'stop':
                break
            elif msg == ('failure', ANY):
                _, cause = msg
                assert cause in ('response-died', 'inconsistent', 'timeout')
                raise TransferFailed("Other side died prematurely" if cause == 'response-died' else
                                     "Inconsistent stream" if cause == 'inconsistent' else
                                     "Timed out")
            _, chunk, more = msg
            assert _ == 'chunk'
            ret += len(chunk)
            fh.write(chunk)
        return ret

    def __repr__(self):
        return "<file '%s' @ %r>" % (self.abstract_path, self.server)


class TransferFailed(Exception):
    pass


def move_or_copy(src, dst):
    try:
        os.rename(src, dst)
    except OSError as e:
        if e.errno == ERRNO_INVALID_CROSS_DEVICE_LINK:
            try:
                shutil.copy(src, dst)
            finally:
                os.unlink(src)
        else:
            raise
