import os
import tempfile
import uuid
from contextlib import contextmanager

from spinoff.actor.context import get_context
from spinoff.contrib.filetransfer.request import Request
from spinoff.contrib.filetransfer.server import Server
from spinoff.contrib.filetransfer.util import mkdir_p, reasonable_get_mtime
from spinoff.util.lockfile import lock_file
from spinoff.util.logging import dbg
from spinoff.util.pattern_matching import ANY


__all__ = ['serve_file']


_NULL_CTX = contextmanager(lambda: (yield))


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

    def fetch(self, path=None):
        if path is not None and os.path.isdir(path):
            raise TransferFailed("%r is a directory" % (path,))
        if path:
            mkdir_p(os.path.dirname(path))
        with lock_file(path) if path is not None else _NULL_CTX():
            if (path is not None and
                    os.path.exists(path) and
                    reasonable_get_mtime(path) == self.mtime and
                    os.path.getsize(path) == self.size):
                return
            if self.server.uri.node == get_context().ref.uri.node:
                _, local_path = self.server.ask(('request-local', self.file_id))
                assert _ == 'local-file'
                if path is not None:
                    if local_path != path:
                        if os.path.exists(path):
                            os.unlink(path)
                        os.link(local_path, path)
                    return path
                else:
                    return local_path
            else:
                fd, tmppath = tempfile.mkstemp()
                os.close(fd)
                with open(tmppath, 'wb') as tmpfile:
                    transferred_size = self._transfer(tmpfile)
                if transferred_size != self.size:
                    os.unlink(tmppath)
                    raise TransferFailed("fetched file size %db does not match remote size %db" % (transferred_size, self.size))
                os.utime(tmppath, (self.mtime, self.mtime))
                if path:
                    os.rename(tmppath, path)
                    return path
                else:
                    return tmppath

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
