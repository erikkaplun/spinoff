from twisted.internet.threads import deferToThread


def read_file_async(filename, start=0, end=None):
    return deferToThread(_do_read_file_async, filename, start, end)


def _do_read_file_async(filename, start, end):
    with open(filename, 'rb') as f:
        f.seek(start)
        return f.read(end - start) if end is not None else f.read()
