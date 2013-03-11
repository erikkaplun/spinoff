from spinoff.actor.ref import _BaseRef


class TypedRef(_BaseRef):
    """Base class for typed references.

    Typed references allow providing of a strongly typed interface to actor refs by exposing (a subset of) the list of
    the messages an actor responds to as methods.

    This class provides the same interface as regular, untyped references. Methods of subclasses of this class should
    be implemented by (directly or indirectly) sending messages to `self.ref`.

    To turn an untyped reference into a typed reference, wrap it with the `TypedRef` subclass.

    Example:

        class Adder(Actor):
            def receive(self, msg):
            if ('add', ANY, ANY) == msg:
                _, operands, r = msg
                r << sum(operands)
            else:
                raise Unhandled

            class Ref(TypedRef):
                def add(self, *operands):
                    deferred, tmp = TempActor.spawn()
                    self.ref << ('add', operands, tmp)
                    return deferred

        adder = Adder.Ref(self.spawn(Adder))
        sum_ = adder.add(1, 2)

    """

    def __init__(self, ref):
        self.ref = ref

    @property
    def is_local(self):
        return self.ref.is_local

    @property
    def is_stopped(self):
        return self.ref.is_stopped

    @property
    def uri(self):
        return self.ref.uri

    def __div__(self, next):
        return self.ref.__div__(next)

    def __lshift__(self, message):
        return self.ref.__lshift__(message)

    def stop(self):
        return self.ref.stop()

    def __repr__(self):
        return repr(self.ref)
