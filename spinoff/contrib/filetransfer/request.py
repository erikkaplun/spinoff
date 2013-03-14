from gevent.queue import Empty

from spinoff.actor import Actor
from spinoff.util.pattern_matching import ANY, OR
from spinoff.contrib.filetransfer import constants


class Request(Actor):
    def run(self, server, file_id, size, abstract_path, buffer_size=constants.DEFAULT_BUFFER_SIZE):
        response = None
        client = None
        buf = []
        more_coming = True
        have_next = False
        failure = None

        received = 0
        chunks_received = 0
        last_chunk_id = None

        server << ('request', file_id)
        while True:
            try:
                msg = self.get(OR(('chunk', ANY, ANY, ANY),
                                  'next',
                                  ('terminated', ANY)),
                               timeout=10)
            except Empty:
                client << ('failure', 'timeout')
                break

            if ('terminated', ANY) == msg:
                _, who = msg
                if who == client:
                    break
                else:
                    assert who == response
                    if more_coming:
                        failure = 'response-died'
            elif msg == 'next':
                assert buf or more_coming, "should have stopped already"
                client = self.sender

                if failure:
                    client << ('failure', failure)
                    break

                if buf:
                    chunk = buf.pop(0)
                    client << ('chunk', chunk, bool(buf) or more_coming)
                    if not more_coming and not buf:
                        client << 'stop'
                        break
                else:
                    have_next = True
            else:
                _, chunk, more_coming, chunk_id = msg
                received += len(chunk)
                chunks_received += 1

                if not (last_chunk_id is None or chunk_id == last_chunk_id + 1):
                    failure = 'inconsistent'
                    continue
                last_chunk_id = chunk_id

                if not response:
                    response = self.sender
                    self.watch(response)
                else:
                    response << ('received', received)

                if have_next:
                    assert not buf
                    have_next = False
                    client << ('chunk', chunk, more_coming)
                    if not more_coming:
                        client << 'stop'
                        break
                else:
                    buf.append(chunk)
