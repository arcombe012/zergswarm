import logging
import asyncio
import typing
import inspect
import itertools
import random

import toolz


from .comm_center import ZeroCommClient

_lg = logging.getLogger("zergswarm")


class Colony:
    task_types = {
        "random": {"weight"},
        "ordered": {"index", "count"},
        "parallel": {"count"},
        "setup": None,
        "shutdown": None}

    def __init__(self, cls):
        _lg.debug("building colony")
        self._zero_client = None        # type: typing.Optional[ZeroCommClient]
        self._wrapped_class = cls
        self._hatchery = None
        self._lock = asyncio.Lock()
        self._wrapped_class.tasks_random = []
        self._wrapped_class.tasks_ordered = []
        self._wrapped_class.tasks_parallel = []
        self._wrapped_class.tasks_setup = None
        self._wrapped_class.tasks_shutdown = None
        for v_ in cls.__dict__.values():
            if isinstance(v_, typing.Callable):                     # pragma: no branch
                _lg.debug("%s: %s", v_.__name__, v_.__dict__)
                ttype_ = v_.__dict__.get("task", None)
                if "random" == ttype_:
                    self._wrapped_class.tasks_random.append(v_)
                elif "ordered" == ttype_:
                    self._wrapped_class.tasks_ordered.append(v_)
                elif "parallel" == ttype_:
                    self._wrapped_class.tasks_parallel.append(v_)
                elif "setup" == ttype_:
                    self._wrapped_class.tasks_setup = v_
                elif "shutdown" == ttype_:
                    self._wrapped_class.tasks_shutdown = v_
        if len(self._wrapped_class.tasks_ordered):
            self._wrapped_class.tasks_ordered.sort(key=lambda x_: getattr(x_, "index", -1))

    def __call__(self, server_address):
        _lg.debug(("wrapped class has:"
                   "\n\tsetup: {}"
                   "\n\trandom tasks: {}"
                   "\n\tordered tasks: {}"
                   "\n\tparallel tasks: {}"
                   "\n\tshutdown: {}").format(self._wrapped_class.tasks_setup,
                                              len(self._wrapped_class.tasks_random),
                                              len(self._wrapped_class.tasks_ordered),
                                              len(self._wrapped_class.tasks_parallel),
                                              self._wrapped_class.tasks_shutdown
                                              ))
        self._hatchery = []
        self._zero_client = ZeroCommClient(server_address)
        return self

    async def _get_colony_config(self):
        _lg.debug("getting hatchling configs")
        # await asyncio.sleep(0.3)
        try:
            _lg.debug("requesting colony config")
            cfg_ = await self._zero_client.call("get_colony_config", {"client_id": self._zero_client.client_id})
            _lg.debug("colony config: %s", cfg_)
            if not cfg_ or not isinstance(cfg_, typing.Mapping):
                raise RuntimeError("invalid colony config: {}".format(cfg_))
            n_ = int(cfg_.get("data", {}).get("hatchlings", 0))
            if n_ < 1:
                raise RuntimeError("no hatchlings configured")
            _lg.debug("requesting hatchlings' config")
            cfg_ = await self._zero_client.call("get_hatchlings_config", {"client_id": self._zero_client.client_id})
            if n_ != len(cfg_["data"]["configs"]):              # pragma: no branch
                raise RuntimeError("expected {} configs, received {}".format(n_, len(cfg_["data"]["configs"])))
            _lg.debug("received %s hatchling configs", n_)
            self._hatchery = [self._wrapped_class(c_) for c_ in cfg_["data"]["configs"]]
            _lg.debug("spawned %s hatchlings", len(self._hatchery))
        except Exception as e_:
            _lg.exception("failed to initialize colony: %s", e_)
            raise

    async def _run_tasks(self):
        async with self._lock:
            _lg.debug("running colony ...")
            try:
                await self._get_colony_config()
                if 0 == len(self._hatchery):
                    _lg.error("no hatchlings spawned, leaving")
                    return
                await asyncio.wait([h_.run_tasks() for h_ in self._hatchery])
            except Exception as e_:
                _lg.exception("failed to run tasks: %s", e_)

    async def _run_stats_reporter(self):
        try:
            while True:
                if self._lock.locked():
                    # tasks are running, wait before reporting stats
                    await asyncio.sleep(60)
                stats_ = self._wrapped_class.report_stats()
                if stats_:                                              # pragma: no branch
                    await asyncio.shield(self._zero_client.call("stats", {"data": stats_}))
                if not self._lock.locked():
                    break
        except asyncio.CancelledError:
            # cancellation happens if all hatchlings finished
            _lg.info("stats reporter cancelled")
        except Exception as e_:
            _lg.exception("exception while running the stats reporter: %s", e_)

    async def _run_async(self):
        async with self._zero_client:
            t1_ = asyncio.ensure_future(self._run_tasks())
            t2_ = asyncio.ensure_future(self._run_stats_reporter())
            done_, pending_ = await asyncio.wait({t1_, t2_}, return_when=asyncio.FIRST_COMPLETED)
            for t_ in pending_:
                # this should be t2_, unless something bad happened to the stats reporter
                # in which case cancel the hatchlings
                t_.cancel()
            # run the stats reporter one more time in case there are left-over stats
            # self._lock is released, so this should only run once
            await self._run_stats_reporter()

    def run(self):
        _lg.debug("running")
        loop_ = asyncio.new_event_loop()
        if loop_.is_running():                                      # pragma: no branch
            raise RuntimeError("loop already running")
        loop_.run_until_complete(self._run_async())

    @staticmethod
    def task(task_type: str, **kwargs):
        if task_type not in Colony.task_types.keys():                      # pragma: no branch
            raise RuntimeError("undefined task type {} - should be one of {}".format(task_type, Colony.task_types))

        def actual_decorator(fnc: typing.Coroutine):
            """
            decorator to add a task attribute to coroutine functions
            :param fnc: class method to decorate
            :return: decorated class method
            """
            if inspect.iscoroutinefunction(fnc):                    # pragma: no branch
                fnc.task = task_type
                if "random" == task_type:
                    fnc.weight = kwargs.get("weight", 1)
                elif "ordered" == task_type:
                    fnc.index = kwargs.get("index", -1)
                    fnc.weight = kwargs.get("count", 1)
                elif "parallel" == task_type:
                    fnc.weight = kwargs.get("count", 1)
            return fnc
        return actual_decorator


class Hatchery:
    async def run_parallel(self):
        if 0 < len(self.__class__.tasks_parallel):
            tasks_ = []
            for task_ in self.__class__.tasks_parallel:
                tasks_ += [task_(self) for _ in range(task_.__dict__.get("weight", 1))]
            if tasks_:
                await asyncio.wait(tasks_)

    async def run_ordered(self):
        for task_ in self.__class__.tasks_ordered:
            for _ in range(task_.__dict__.get("count", 1)):
                await task_(self)

    async def run_random(self):
        if 0 == len(self.__class__.tasks_random):
            return
        # im python3.6+ random has choices() which can take weights
        # let's emulate that
        cw_ = [0] + list(itertools.accumulate((v_.__dict__.get("weight", 1) for v_ in self.__class__.tasks_random)))
        try:
            while True:
                # generate random number in the range [0, cumweights_[-1])
                # then determine to which interval [cw_[i], cw_[i+1]) it belongs to
                i_ = random.randrange(cw_[-1])
                # _lg.debug("picked value %s out of %s", i_, cw_)
                # n_ = list(enumerate(itertools.takewhile(lambda x: x <= i_, cw_)))
                # _lg.debug("have to take the last element of %s", n_)
                n_ = toolz.last(enumerate(itertools.takewhile(lambda x: x <= i_, cw_)))[0]
                if not await self.__class__.tasks_random[n_](self):
                    _lg.debug("finishing random task loop by returning False")
                    break
        except StopIteration:
            # done running
            _lg.debug("finishing random task loop by raising StopIteration")
            pass
        except Exception as e_:
            _lg.error("exception encountered while running random tasks: {}".format(e_))
            raise

    async def run_tasks(self) -> typing.NoReturn:
        """
        default base class implementation of hatchling task running
        reimplement in the derived Hatchling class for more complex functionality
        :return: None
        """
        _lg.debug("running hatchery")
        # run setup if any
        if self.__class__.tasks_setup is not None:
            try:
                _lg.debug("running setup")
                await self.__class__.tasks_setup(self)
            except Exception as e_:
                _lg.error("failed to run setup: {}".format(e_))
                raise

        # then run tasks
        try:
            await asyncio.wait([self.run_ordered(), self.run_random(), self.run_parallel()])
        except Exception as e_:
            _lg.error("exception encountered while running tasks: {}".format(e_))

        # then run shutdown if needed
        if self.__class__.tasks_shutdown is not None:
            try:
                await self.__class__.tasks_shutdown(self)
            except Exception as e_:
                _lg.error("failed to run shutdown: {}".format(e_))
                raise
