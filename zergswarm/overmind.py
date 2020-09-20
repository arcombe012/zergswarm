import asyncio
import argparse
import logging
import csv
import time
# from io import StringIO
from math import ceil
from datetime import datetime, timedelta
from collections import OrderedDict

from .comm_center import ZeroCommServer, ZeroCommClient
from .config_reader import ConfigReader
from .subprocess_manager import ColonySpawner
from .reports import Report

_lg = logging.getLogger("zergswarm")


class Overmind:
    # noinspection PyProtectedMember
    def __init__(self, report_class=Report):
        _lg.debug("raising the overmind")
        cmdline_ = Overmind._parse_cmdline()
        _lg.debug("command line parsed as %s", cmdline_)
        # sanity check
        if cmdline_["log_level"] not in logging._nameToLevel.keys():
            cmdline_["log_level"] = "INFO"
        logging.basicConfig()
        logging.root.setLevel(logging._nameToLevel[cmdline_["log_level"]])
        _lg.info("logging level is set to %s", logging.getLevelName(logging.root.getEffectiveLevel()))
        if isinstance(cmdline_["central_server"], str):
            self._comm_sender = ZeroCommClient(cmdline_["central_server"])
        else:
            self._comm_sender = None
        self._report_class = report_class
        self._stats_accumulator = report_class()
        self._config_reader = ConfigReader(cmdline_["settings_file"])
        self._configs = list(self._config_reader.get_hatchling_config())
        n_ = len(self._configs)
        self._comm_listener = ZeroCommServer(cmdline_["bind_address"])
        _lg.debug("initializing 0MQ listener on %s", self._comm_listener.server_address)
        self._comm_listener.register_callback("satellite_action", self._satellite_action)
        self._comm_listener.register_callback("stats", self._stats_accumulator_callback)
        self._comm_listener.register_callback("get_colony_config", self._colony_config)
        self._comm_listener.register_callback("get_hatchlings_config", self._hatchlings_config)
        req_cols_ = self.required_colony_count(n_, ColonySpawner.available_colony_slots())
        self._hatchery_file = cmdline_["hatchery_file"]
        self._spawner = ColonySpawner(req_cols_)
        _lg.info("spawning %s colonies with %s hatchlings", req_cols_, n_)
        n1_ = n_ % req_cols_
        x_ = n_ // req_cols_
        self._hatchlings_per_col = [x_ for _ in range(req_cols_ - n1_)] + [(x_ + 1) for _ in range(n1_)]
        self._colonies = dict()
        self._satellites = set()
        self._start_time = datetime.utcnow()

    async def _send_to_central(self, data: dict) -> dict:
        ans_ = {}
        if not self._comm_sender:
            return ans_
        async with self._comm_sender as sender:     # type: ZeroCommClient
            ans_ = await sender.call(message_type="stats", data=data)
        return ans_

    async def _notify_central(self, action: str) -> None:
        """
        notify a central overmind, if any
        :param action: str, either "register" or "unregister"
        :return: None
        """
        if not self._comm_sender:
            return
        async with self._comm_sender as sender:     # type: ZeroCommClient
            _ = await sender.call(message_type="satellite_action", data={"action": action})

    def _satellite_action(self, data: dict) -> dict:
        if not isinstance(data, dict) or 0 == len(data):
            return {}
        if not data.get("client_id", None):
            id_ = "unknown"
        else:
            id_ = data["client_id"]
        if id_ in self._satellites:
            if "unregister" == data["data"].get("action", "register"):
                self._satellites.remove(id_)
                return {"client_id": id_, "data": {"result": "ok"}}
            return {"client_id": id_, "data": {"result": "error", "error": "invalid request"}}
        if "register" == data["data"].get("action", "register"):
            self._satellites.add(id_)
            return {"client_id": id_, "data": {"result": "ok"}}
        return {"client_id": id_, "data": {"result": "error", "error": "invalid request"}}

    async def _stats_accumulator_callback(self, data: dict) -> dict:
        if not isinstance(data, dict) or 0 == len(data):
            return {}
        if self._comm_sender is not None:
            # pass this along to the central overmind
            try:
                ans_ = await asyncio.wait_for(self._send_to_central(data), 2)
            except asyncio.TimeoutError:
                err_ = "timeout while attempting to send stats to central overmind"
                _lg.error(err_)
                return {"client_id": data.get("client_id", "unknown"),
                        "data": {"stats": "error", "error": err_}}
            except Exception as e_:
                _lg.error("exception caught while attempting to send stats to central overmind: %s", str(e_))
                return {"client_id": data.get("client_id", "unknown"),
                    "data": {"stats": "error", "error": str(e_)}}
        try:
            rdata_ = self._report_class.from_dict(data["data"])
        except Exception as e_:
            err_ = "failed to construct report from stat message: [%s] %s", e_.__class__.__name__, e_
            _lg.error(err_)
            return {"client_id": data.get("client_id", "unknown"),
                "data": {"stats": "error", "error": err_}}
        if not data.get("client_id", None):
            id_ = "unknown"
        else:
            id_ = data["client_id"]
        now_ = datetime.utcnow()
        if not((now_ - self._start_time).seconds % (10 * 60)):
            # about once every ~ X minutes
            _lg.info("[%s] received data from %s: %s", now_, id_, rdata_)

        try:
            self._stats_accumulator += rdata_
            return {"data": {"result": "ok"}}
        except Exception as e_:
            _lg.error("caught exception while adding stats: %s", e_)
            return {"client_id": data.get("client_id", "unknown"),
                    "data": {"stats": "error", "error": str(e_)}}

    def _colony_config(self, data: dict):
        if not isinstance(data, dict) or data.get("client_id", None) is None:
            id_ = "unknown_{}".format(len(self._colonies))
        else:
            id_ = data["client_id"]
        _lg.debug("colony config requested for %s", id_)

        if id_ not in self._colonies.keys():
            if len(self._colonies) >= len(self._hatchlings_per_col):
                self._colonies[id_] = 0
            else:
                self._colonies[id_] = self._hatchlings_per_col[len(self._colonies)]
        _lg.debug("returning colony config: %s", self._colonies[id_])
        return {"client_id": data.get("client_id", "unknown"), "data": {"hatchlings": self._colonies[id_]}}

    def _hatchlings_config(self, data: dict):
        if not self._configs or not isinstance(data, dict):   # zero length array takes this branch
            ans_ = []
        else:
            id_ = data.get("client_id", None)
            if not id_ or id_ not in self._colonies.keys():
                ans_ = []
            else:
                n_ = self._colonies[id_]
                cnf_ = self._configs[:n_]
                self._configs = self._configs[n_:]
                ans_ = cnf_
        _lg.debug("returning %s hatchling configs", len(ans_))
        return {"client_id": data.get("client_id", "unknown"), "data": {"configs": ans_}}

    @staticmethod
    def _parse_cmdline() -> dict:
        ap_ = argparse.ArgumentParser()
        default_bind_ = "tcp://127.0.0.1:23176"
        default_settings_ = "settings.ini"
        default_hatchery_ = "hatchling.py"
        ap_.add_argument("--bind_address", "-a",
            help=("URI (tcp://IP:port) the overmind's 0MQ should bind to for listening "
                  "(default is {})").format(default_bind_),
            type=str, default=default_bind_, required=False)
        ap_.add_argument("--central_server", "-c",
            help="full URI of the central server coordinating all overminds, if any (tcp://IP:port) (default is None)",
            type=str, default=None, required=False)
        ap_.add_argument("--settings_file", "-s", help="custom settings file (default is {})".format(default_settings_),
            type=str, default=default_settings_, required=False)
        ap_.add_argument("--hatchery_file", "-x", help="custom hatchery file (default is {})".format(default_hatchery_),
            type=str, default=default_hatchery_, required=False)
        ap_.add_argument("--log_level", "-l", type=str, choices=["DEBUG", "INFO", "WARNING", "ERROR"],
            default="INFO", required=False, help="log level for the overmind coordinator")
        return ap_.parse_known_args()[0].__dict__

    def required_colony_count(self, hatchlings: int, colony_slots: int) -> int:
        max_hatchlings_per_colony = self._config_reader.max_hatchlings_per_colony
        min_hatchlings_per_colony = self._config_reader.min_hatchlings_per_colony
        if hatchlings / colony_slots > max_hatchlings_per_colony:
            ans_ = colony_slots
        elif hatchlings < colony_slots * min_hatchlings_per_colony:
            ans_ = ceil(hatchlings / min_hatchlings_per_colony)
        else:
            ans_ = ceil(hatchlings / max_hatchlings_per_colony)
        return ans_ if ans_ > 1 else 1

    async def _run_async(self):
        async with self._comm_listener as srv:
            # have to put this message out for external connections
            _lg.info("overmind listening to 0MQ connections on {}".format(srv.server_address))
            if self._comm_sender:
                await self._notify_central("register")
            await self._spawner.run_colonies(server_address=srv.server_address, hatchery_file=self._hatchery_file)
            if self._comm_sender:
                await self._notify_central("unregister")
            else:
                if len(self._satellites) > 0:
                    # wait a finite amount of time, to avoid cases where the satellites crash or
                    # otherwise stop communicating
                    wait_mins_ = 5
                    _lg.info("waiting %d minutes for %d satellites", wait_mins_, len(self._satellites))
                    timeout_end_ = datetime.utcnow() + timedelta(minutes=wait_mins_)
                    while datetime.utcnow() < timeout_end_:
                        await asyncio.sleep(1)
                        if len(self._satellites) == 0:
                            break
        self._stop_time = datetime.utcnow()

    def run(self):
        loop_ = asyncio.get_event_loop()
        loop_.run_until_complete(self._run_async())
        self.print_stats()

    def print_stats(self):
        # buff_ = StringIO()
        # for k_, v_ in self._stats_accumulator.items():
        #     print("{}: {}".format(k_, v_), file=buff_)
        # _lg.info("reported stats:\n%s", buff_.getvalue())
        # buff_.close()
        if len(self._stats_accumulator) == 0:
            _lg.warning("empty accumulator, no stats printed\n%s", self._stats_accumulator)

        time_ = self._stop_time - self._start_time   # type: timedelta
        _lg.info("reported stats over %d minutes:\n%s", time_.seconds / 60, self._stats_accumulator)

        # output_ = "stats_output.csv"
        # fields = set()
        # for v_ in self._stats_accumulator.values():
        #     fields.update(v_["success"].keys())
        # fields = ["time"] + sorted(fields)
        # with open(output_, "wt") as outf_:
        #     dwrt_ = csv.DictWriter(outf_, fieldnames=list(fields), restval=0)
        #     dwrt_.writeheader()
        #     for k_, v_ in self._stats_accumulator.items():
        #         row_ = {"time": k_.time()}
        #         row_.update({k1_: v1_["count"] for k1_, v1_ in v_["success"].items()})
        #         dwrt_.writerow(row_)