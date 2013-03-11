from gevent.queue import Empty
from spinoff.actor import Actor
from spinoff.contrib.filetransfer.util import read_file_async
from spinoff.util.pattern_matching import OR
from spinoff.contrib.filetransfer import constants


class Response(Actor):
    def run(self, file, request, threadpool, chunk_size=constants.DEFAULT_CHUNK_SIZE):
        self.watch(request)
        seek_ptr = 0
        with open(file, 'r') as f:
            while True:
                chunk = read_file_async(f, limit=chunk_size, threadpool=threadpool)
                seek_ptr += chunk_size
                more_coming = len(chunk) > 0
                request << ('chunk', chunk, more_coming)
                if not more_coming:
                    break
                try:
                    msg = self.get_nowait(OR('pause', ('terminated', request)))
                except Empty:
                    pass
                else:
                    if msg == 'pause':
                        self.get('unpause')
                    else:
                        break
