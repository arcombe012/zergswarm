zergswarm module
================

use the module as
    python3 -m zergswarm

help and information on defaults are provided with
    python3 -m zergswarm -h


Required files
--------------

Actual swarming requires a python file providing
a Hatchling class. The default name of the file
(unless a custom one is specified on the command
line) is hatchling.py located in the current directory.

|   @Colony
|   class Hatchling(Hatchery, ...):
|       def __init__(self, ...):
|           pass
|
|       @Colony.task("random")
|       async def some_random_task(self):
|           pass

The Hatchling class must be decorated with a
@Colony decorator and must inherit the Hatchery mixin.

Tasks performed by the Hatchling must be member
coroutines (async def ...) decorated with the
@Colony.task() decorator, specifying the type of task
and associated parameters. Task types can be:

* random - with an optional parameter "weight" (default 1)
* ordered - with an optional parameter "index"
  (default -1, meaning in the order they are registered,
  or before all the explicitly indexed ones) and an
  optional parameter "count" (default 1) for the
  number of repetitions
* parallel - no optional parameters, will be set to
  execute in parallel
* setup - no optional parameters, will be run before
  the regular {random, ordered, parallel} tasks
* shutdown - no optional parameters, will be run either
  after the regular tasks or if an exception is caught

The default behavior of task running is implemented
as a coroutine member of Hatchery

| class Hatchery:
|   ...
|   async def run_tasks(self):
|       ...

To customize the way tasks are run, reimplement it
in the derived Hatchling class.

The main process reads a settings file for information
about hatchling management and for hatchling settings
to be passed along to the spawned colonies. Unless a filename
is explicitly passed on the command line, the default
is to look for a file called settings.ini in the
current directory. The file should contain two sections:
an [OVERMIND] section with (fixed) settings for the
swarm coordinator, and a [HATCHLING] section with
arbitrary settings for the initialition of the
swarm hatchling class (will be passed as **kwargs to
the class' __init__() method). A sample structure of this file is
provided inside the package as settings.ini_template