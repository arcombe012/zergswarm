import logging
import asyncio
import sys
import os
import typing

_lg = logging.getLogger("zergswarm")


class ColonySpawner:
    def __init__(self, required_colonies: typing.Optional[int] = None):
        if not isinstance(required_colonies, int):
            self._colony_count = ColonySpawner.available_colony_slots()
        else:
            self._colony_count = min(ColonySpawner.available_colony_slots(), required_colonies)
        _lg.debug("ColonySpawner initialized for %s colonies", self._colony_count)
        self._lock = asyncio.Lock()

    @staticmethod
    def available_colony_slots() -> int:
        return len(os.sched_getaffinity(0))

    @property
    def colonies(self) -> int:
        return self._colony_count

    async def run_colonies(self, server_address: str, hatchery_file: str = None, debug: bool = False):
        if hatchery_file is None or not os.path.exists(hatchery_file):      # pragma: no branch
            _lg.warning("hatchery file [{}] not found, using default".format(hatchery_file))
            hatchery_file = "hatchery.py"
        if debug:               # pragma: no branch
            script_ = (
                """import importlib.util
import sys
spec = importlib.util.spec_from_file_location("hatchery", "{}")
hatchery = importlib.util.module_from_spec(spec)
sys.modules["hatchery"] = hatchery
spec.loader.exec_module(hatchery)
import logging
logging.basicConfig(level=logging.DEBUG)
hatchery.Hatchling("{}").run()""").format(hatchery_file, server_address)
        else:
            script_ = (
                """import importlib.util
import sys
spec = importlib.util.spec_from_file_location("hatchery", "{}")
hatchery = importlib.util.module_from_spec(spec)
sys.modules["hatchery"] = hatchery
spec.loader.exec_module(hatchery)
hatchery.Hatchling("{}").run()""").format(hatchery_file, server_address)
        _lg.debug("running hatchery script as:\n%s", script_)
        command_ = [sys.executable, "-c", script_]
        procs_ = [await asyncio.create_subprocess_exec(*command_) for _ in range(self._colony_count)]
        async with self._lock:
            pending_ = [p_.wait() for p_ in procs_]
            while len(pending_) > 0:
                done_, pending_ = await asyncio.wait(pending_, timeout=10)

    def running(self):
        return self._lock.locked()
