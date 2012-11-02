import hashlib
import time

from spinoff.actor.process import Process
from spinoff.actor.supervision import Stop
from spinoff.contrib.filetransfer.filetransfer import FileRef
from spinoff.util.logging import dbg
from spinoff.util.pattern_matching import IN
from spinoff.actor._actor import lookup


CHUNK_SIZE = 10 * 1024 * 1024


def get_checksum(filename):
    md5 = hashlib.md5()
    with open(filename) as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            md5.update(chunk)
            if len(chunk) < CHUNK_SIZE:
                break
    return md5.hexdigest()


class App(Process):
    supervise = lambda *args: Stop

    def run(self, filename):
        md5 = get_checksum(filename)
        dbg("file checksum: ", md5)

        receivers = set()

        for _ in range(2):
            receiver = self.spawn(Receiver.using(self.ref))
            self.spawn(Sender.using(filename=filename, receiver=receiver, checksum=md5))
            self.watch(receiver)
            receivers.add(receiver)

        while receivers:
            _, receiver = yield self.get(('terminated', IN(receivers)))
            receivers.remove(receiver)
            dbg("done:", receiver)


class Sender(Process):
    def run(self, filename, receiver, checksum=None):
        if checksum is None:
            checksum = get_checksum(filename)
            dbg("checksum:", checksum)

        if isinstance(receiver, basestring):
            receiver = lookup(receiver)

        file = FileRef.publish(filename)
        receiver << checksum << file

        import os
        bytes = os.stat(filename).st_size
        t1 = time.time()

        self.watch(receiver)
        yield self.get(('terminated', receiver))

        t2 = time.time()

        dt = t2 - t1
        dbg("%s bytes transferred in %ss -- speed: %r MB/s" % (bytes, dt, round((bytes / dt) / 1024 / 1024, 2)))


class Receiver(Process):
    def run(self):
        checksum_orig = yield self.get()
        fileref = yield self.get()
        fh = fileref.get_handle()
        dbg("RECEIVED", fileref, "with checksum", checksum_orig)
        md5 = hashlib.md5()
        while True:
            chunk = yield fh.read(CHUNK_SIZE)
            dbg("READ CHUNK %r AT %r" % (hashlib.md5(chunk[:100]).hexdigest()[:8], str(time.time())[8:]))
            md5.update(chunk)
            if len(chunk) < CHUNK_SIZE:
                break
        checksum = md5.hexdigest()
        dbg("CHECKSUM", checksum)
        assert checksum_orig == checksum, (checksum, checksum_orig)
