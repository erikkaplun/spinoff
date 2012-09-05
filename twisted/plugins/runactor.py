from __future__ import print_function

import sys

from twisted.application.service import IServiceMaker
from twisted.plugin import IPlugin
from twisted.python import usage
from zope.interface import implements

from spinoff.actor.runner import ActorRunner


class _EMPTY(object):
    def __repr__(self):
        return '<empty>'

    def __nonzero__(self):
        return False
_EMPTY = _EMPTY()


class Options(usage.Options):

    optParameters = [
        ['actor', 'a', None, "The actor to spawn."],
        ['params', 'p', _EMPTY, (u"Parameters to initialize the actor with.\n\n"
                                 u"Parsed as `dict(<params>)` and passed as **kwargs.\n\n"
                                 u"\b")],  # so that Twisted wouldn't strip off the line endings before [default: <empty>]
        ['message', 'm', _EMPTY, "Message to send to the actor"],
    ]


class ActorRunnerMaker(object):
    implements(IServiceMaker, IPlugin)

    tapname = 'runactor'
    description = 'Runs an actor'
    options = Options

    def makeService(self, options):
        actor = options['actor']
        if not actor:
            print("error: no actor specified", file=sys.stderr)
            sys.exit(1)

        try:
            module_path, actor_cls_name = actor.rsplit('.', 1)
        except ValueError:
            print("error: invalid path to actor %s" % actor, file=sys.stderr)
            sys.exit(1)

        try:
            mod = __import__(module_path, globals(), locals(), [actor_cls_name], -1)
        except ImportError:
            print("error: could not import %s" % actor, file=sys.stderr)
            # failure.Failure().printTraceback(file=sys.stderr)
            sys.exit(1)

        try:
            actor_cls = getattr(mod, actor_cls_name)
        except AttributeError:
            print("error: no such actor %s" % actor, file=sys.stderr)
            sys.exit(1)

        kwargs = {}

        if options['params'] is not _EMPTY:
            params = 'dict(%s)' % (options['params'],)
            try:
                params = eval(params)
            except:
                print("error: could not parse parameters", file=sys.stderr)
                sys.exit(1)
            else:
                kwargs['init_params'] = params

        if options['message'] is not _EMPTY:
            initial_message = options['message']
            try:
                initial_message = eval(initial_message)
            except:
                print("error: could not parse initial message", file=sys.stderr)
                sys.exit(1)
            else:
                kwargs = {'initial_message': initial_message}

        return ActorRunner(actor_cls, **kwargs)


serviceMaker = ActorRunnerMaker()
