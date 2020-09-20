import logging

from .overmind import Overmind

_lg = logging.getLogger("zergswarm")

if "__main__" == __name__:
    overmind_ = Overmind()
    overmind_.run()
