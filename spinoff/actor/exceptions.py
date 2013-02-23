class NameConflict(Exception):
    pass


class Unhandled(Exception):
    pass


class UnhandledTermination(Exception):
    def __init__(self, watcher, watchee):
        self.watcher = watcher
        self.watchee = watchee


class LookupFailed(RuntimeError):
    pass
