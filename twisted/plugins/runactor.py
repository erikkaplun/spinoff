import sys

from zope.interface import implements
from twisted.python import usage, failure
from twisted.plugin import IPlugin
from twisted.application.service import IServiceMaker

from spinoff.actor.runner import ActorRunner


_EMPTY = object()


class Options(usage.Options):

    optParameters = [
        ['actor', 'a', None, 'The actor to spawn.'],
        ['message', 'm', _EMPTY, 'Message to send to the actor'],
        ]


class ActorRunnerMaker(object):
    implements(IServiceMaker, IPlugin)

    tapname = 'runactor'
    description = 'Runs an actor'
    options = Options

    def makeService(self, options):
        actor = options['actor']
        if not actor:
            print >> sys.stderr, "error: no actor specified"
            sys.exit(1)

        try:
            module_path, actor_cls_name = actor.rsplit('.', 1)
        except ValueError:
            print >> sys.stderr, "error: invalid path to actor %s" % actor
            sys.exit(1)

        try:
            mod = __import__(module_path, globals(), locals(), [actor_cls_name], -1)
        except ImportError:
            print >> sys.stderr, "error: could not import %s" % actor
            # failure.Failure().printTraceback(file=sys.stderr)
            sys.exit(1)

        try:
            actor_cls = getattr(mod, actor_cls_name)
        except AttributeError:
            print >> sys.stderr, "error: no such actor %s" % actor
            sys.exit(1)

        if options['message'] is not _EMPTY:
            initial_message = options['message']
            try:
                initial_message = eval(initial_message)
            except (SyntaxError, NameError):
                print >> sys.stderr, "error: could not parse initial message"
                sys.exit(1)
            else:
                kwargs = {'initial_message': initial_message}
        else:
            kwargs = {}
        return ActorRunner(actor_cls, **kwargs)


serviceMaker = ActorRunnerMaker()
