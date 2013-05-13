from gevent.queue import Empty

from spinoff.actor import Actor
from spinoff.contrib.filetransfer.util import read_file_async
from spinoff.contrib.filetransfer import constants
from spinoff.util.pattern_matching import OR, ANY


class Response(Actor):
    def run(self, file, request, threadpool, chunk_size=constants.DEFAULT_CHUNK_SIZE, send_ahead=constants.SEND_AHEAD):
        self.watch(request)
        seek_ptr = 0
        chunks_sent = 0
        other_received = 0
        with open(file, 'rb') as f:
            while True:
                chunk = read_file_async(threadpool, f, limit=chunk_size)
                more_coming = len(chunk) > 0
                request << ('chunk', chunk, more_coming, chunks_sent)
                seek_ptr += len(chunk)
                chunks_sent += 1
                if not more_coming:
                    break
                try:
                    timeout = (0 if seek_ptr - other_received < send_ahead else None)
                    msg = self.get(OR(('terminated', request), ('received', ANY)), timeout=timeout)
                except Empty:
                    continue
                if msg == ('received', ANY):
                    _, other_received = msg
                else:
                    break
        while True:
            msg = self.get(OR(('received', ANY), ('terminated', request)), timeout=10.0)
            if msg[0] == 'terminated':
                break
