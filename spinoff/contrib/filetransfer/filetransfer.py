import datetime
import errno
import uuid
import os
import warnings
from cStringIO import StringIO

from twisted.internet.defer import Deferred, inlineCallbacks, returnValue
from txcoroutine import coroutine

from spinoff.actor import Actor, Ref
from spinoff.actor.exceptions import Unhandled
from spinoff.actor.process import Process
from spinoff.contrib.filetransfer.util import read_file_async
from spinoff.util.async import with_timeout, Timeout, after
from spinoff.util.logging import dbg, err
from spinoff.util.pattern_matching import ANY, IN


DEFAULT_CHUNK_SIZE = 10 * 1024 * 1024
OPEN_FILE_TIMEOUT = 30 * 60
FILE_MAX_LIFETIME = 60 * 60
# MAX_FILE_SIZE_IN_MEMORY = 10 * 1024 * 1024


class FileAlreadyPublished(Exception):
    pass


class _Sender(Process):
    def run(self, service, pub_id, file, send_to):
        self.watch(send_to)

        seek_ptr = 0

        while True:
            try:
                msg = yield with_timeout(OPEN_FILE_TIMEOUT, self.get())
            except Timeout:
                err("Sending of file at %r timed out" % (file,))
                break

            if ('next-chunk', ANY) == msg:
                service << ('touch-file', pub_id)
                _, chunk_size = msg

                chunk = yield read_file_async(file, start=seek_ptr, end=seek_ptr + chunk_size)
                seek_ptr += len(chunk)

                more_coming = len(chunk) > 0 if chunk_size > 0 else True

                send_to << ('chunk', chunk, more_coming)

                if not more_coming:
                    break

            elif ('terminated', send_to) == msg:
                break

            else:
                self.unhandled(msg)


class FilePublisher(Actor):
    _instances = {}

    @classmethod
    def get(cls, node=None):
        if node not in cls._instances:
            cls._instances[node] = node.spawn(cls.using())
        return cls._instances[node]

    def pre_start(self):
        self.published = {}  # <pub_id> => (<local file path>, <time added>)
        self.senders = {}  # <sender> => <pub_id>

        self << 'purge-old'

    def _touch_file(self, pub_id):
        file_path, time_added = self.published[pub_id]
        self.published[pub_id] = (file_path, datetime.datetime.now())

    def receive(self, msg):
        if ('publish', ANY, ANY) == msg:
            _, file_path, pub_id = msg
            if not os.path.exists(file_path) and not os.path.isdir(file_path):
                err("attempt to publish a file that does not exist")
                return

            if pub_id in self.published:
                raise FileAlreadyPublished("Attempt to publish %r with ID %r but a file already exists with that ID" % (file_path, pub_id))
            else:
                self.published[pub_id] = (file_path, datetime.datetime.now())

        elif ('get-file', ANY, ANY) == msg:
            _, pub_id, send_to = msg

            if pub_id not in self.published:
                err("attempt to get a file with ID %r which has not been published or is not available anymore" % (pub_id,))
                return

            self._touch_file(pub_id)

            file_path, time_added = self.published[pub_id]
            sender = self.watch(_Sender.using(service=self.ref, pub_id=pub_id, file=file_path, send_to=send_to))
            self.senders[sender] = pub_id
            # self.senders[sender] = pub_id
            send_to << ('take-file', sender)

        elif ('touch-file', ANY) == msg:
            _, pub_id = msg
            if pub_id not in self.published:
                err("attempt to touch a file with ID %r which has not been published or is not available anymore" % (pub_id,))
                return
            _, pub_id = msg
            self._touch_file(pub_id)

        elif 'purge-old' == msg:
            after(60.0).do(self.send, 'purge-old')

            t = datetime.datetime.now()

            for pub_id, (file_path, time_added) in self.published.items():
                if (t - time_added).total_seconds() > FILE_MAX_LIFETIME and pub_id not in self.senders.values():
                    dbg("purging file %r at %r" % (pub_id, file_path))
                    del self.published[pub_id]

        elif ('terminated', IN(self.senders)) == msg:
            _, sender = msg
            del self.senders[sender]

    def post_stop(self):
        del self._instances[self.node]


class _Receiver(Process):
    def run(self, pub_id, file_service):
        self.watch(file_service)

        file_service << ('get-file', pub_id, self.ref)

        msg = yield self.get(('take-file', ANY), ('terminated', file_service))

        if ('take-file', ANY) == msg:
            _, sender = msg
        elif ('terminated', file_service) == msg:
            _, _, d = yield self.get(('next-chunk', ANY, ANY))
            d.errback(Exception("file sender died prematurely"))
            return
        else:
            raise Unhandled(msg)

        self.watch(sender)

        while True:
            msg = yield self.get(('next-chunk', ANY, ANY), ('terminated', sender))

            if ('next-chunk', ANY, ANY) == msg:
                _, size, d = msg
                sender << ('next-chunk', size)
                _, chunk, more_coming = yield self.get(('chunk', ANY, ANY))
                # if chunk:
                #     dbg("receive %r at %r" % (hashlib.md5(chunk[:100]).hexdigest()[:8], str(time.time())[8:]))
                d.callback((chunk, more_coming))

            elif ('terminated', sender) == msg:
                break

            else:
                assert False


class FileRef(object):
    _fetching = {}

    def __init__(self, pub_id, file_service, abstract_path, mtime):
        """Private; see File.publish instead"""
        self.pub_id = pub_id
        self.file_service = file_service
        self.abstract_path = abstract_path
        self.mtime = mtime

    @classmethod
    def publish(cls, path, node, abstract_path=None):
        if not os.path.exists(path):
            raise IOError("File not found: %s" % (path,))
        pub_id = str(uuid.uuid4())
        file_service = FilePublisher.get(node=node)
        file_service << ('publish', path, pub_id)
        return cls(pub_id, file_service, abstract_path=abstract_path or os.path.basename(path), mtime=os.stat(path).st_mtime)

    def open(self, context=None):
        ret = FileHandle(self.pub_id, self.file_service, context=context, abstract_path=self.abstract_path)
        return ret._open().addCallback(lambda _: ret)

    @coroutine
    def fetch(self, path, context=None):
        if path in self._fetching:
            yield self._fetching[path]
        else:
            if os.path.exists(path) and os.stat(path).st_mtime != self.mtime:
                os.remove(path)
            self._fetching[path] = Deferred()
            mkdir_p(os.path.dirname(path))
            with (yield self.open(context=context)) as f:
                yield f.read_into(path)
            os.utime(path, (self.mtime, self.mtime))
            self._fetching[path].callback(None)
            del self._fetching[path]

    # @classmethod
    # def at_url(cls, url):
    #     raise NotImplementedError

    def __repr__(self):
        return "<file '%s' @ %r>" % (self.abstract_path, self.file_service)


class FileHandle(object):
    """A distributed file handle that represents a physical file sitting somewhere on the network, possibly the same
    machine.

    """
    opened = False
    closed = False

    receiver = None

    _reading = {}

    def __init__(self, pub_id, file_service, context, abstract_path):
        """Private; see File.publish or File.at_url instead"""
        self.pub_id = pub_id
        self.file_service = file_service
        self.context = context
        self.abstract_path = abstract_path

    def read(self, size=None):
        """Asynchronously reads `size` number of bytes.

        If `size` is not specified, returns the content of the entire file.

        """
        d = Deferred()
        if size is None or size > DEFAULT_CHUNK_SIZE:
            return self._read_multipart(total_size=size)
        else:
            self.receiver << ('next-chunk', size, d)
            d.addCallback(lambda (chunk, more_coming): ((self.close() if not more_coming else None), chunk)[-1])
            d.addErrback(err)
            return d

    @inlineCallbacks
    def read_into(self, file):
        if file in self._reading:
            yield self._reading[file]
        else:
            d = self._reading[file] = Deferred()
            try:
                if os.path.exists(file):
                    os.unlink(file)
                with open(file, 'wb') as f:
                    yield self._read_multipart(read_into=f)
            finally:
                d.callback(None)

    @inlineCallbacks
    def _read_multipart(self, total_size=None, read_into=None):
        if self.closed:
            raise Exception("Can't read from a File that's been closed")
        assert self.opened

        if read_into:
            ret = None
        else:
            read_into = ret = StringIO()

        while total_size is None or total_size > 0:
            if total_size is not None:
                chunk_size = min(DEFAULT_CHUNK_SIZE, total_size)
                total_size -= chunk_size
            else:
                chunk_size = DEFAULT_CHUNK_SIZE

            d = Deferred()
            self.receiver << ('next-chunk', chunk_size, d)
            chunk, more_coming = yield d

            read_into.write(chunk)

            if not more_coming:
                break

        returnValue(ret.getvalue() if ret else None)

    def _open(self):
        self.opened = True
        self.receiver = self.context.spawn(_Receiver.using(self.pub_id, self.file_service))
        d = Deferred()
        self.receiver << ('next-chunk', 0, d)
        return d

    def close(self):
        self.closed = True
        if self.opened:
            if self.receiver:
                self.receiver.stop()
                self.receiver = None

    def __del__(self):
        self.close()

    def __getstate__(self):
        raise Exception("file handles cannot be pickled")

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.close()

    def __repr__(self):
        return "<open file '%s' @ %r>" % (self.abstract_path, self.file_service)


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise
