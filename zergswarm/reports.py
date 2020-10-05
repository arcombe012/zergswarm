import typing
import logging
from collections import defaultdict
import functools
from math import floor, ceil

_lg = logging.getLogger("reports")


class Stats:
    def __init__(self, data: list = list()):
        self._data = sorted(data)

    def median(self):
        return self._data[len(self._data) // 2]

    def perc_mid_50(self):
        x = len(self._data) / 4
        return (self._data[floor(x)], self._data[floor(3*x)])

    def perc_top_10(self):
        return self._data[floor(9 * len(self._data) / 10)]


class Report(dict):
    """
    dict-like class storing data abiut successful and failed requests
    the "success" key holds a dict of names and {count, time} dicts
    names can be endpoints or customized representations of them
    (for instance, to have a single name for endpoints that receive get parameters)
    while the count is the number of successful requests and the time
    is the aggregated duration of successful requests
    (so avg. request time is time / count)
    Three types of errors are stored, as names and counts only:
    request errors (requests that returned an error code),
    monitored errors (specific instances of request errors that the client monitors,
    such as when the error code returned is one of a pre-defined set of values)
    and other errors (such as network, DNS, underlying server errors and so on)
    """
    def __init__(self):
        super().__init__()
        self["success"] = defaultdict(list)
        self["request errors"] = defaultdict(int)
        self["monitored errors"] = defaultdict(int)
        self["other errors"] = defaultdict(int)
        self["statistics"] = defaultdict(int)

    def to_dict(self) -> dict:
        return {"success":      {k_: v_ for k_, v_ in self["success"].items()},
            "request errors":   {k_: v_ for k_, v_ in self["request errors"].items()},
            "monitored errors": {k_: v_ for k_, v_ in self["monitored errors"].items()},
            "other errors":     {k_: v_ for k_, v_ in self["other errors"].items()},
            "statistics": {k_: v_ for k_, v_ in self["statistics"].items()},
            }

    @staticmethod
    def from_dict(data: typing.Mapping) -> typing.Mapping:
        r_ = Report()
        if not isinstance(data, typing.Mapping):        # pragma: no branch
            return r_
        dv_ = data.pop("success", None)
        if isinstance(dv_, typing.Mapping):                # pragma: no branch
            r_["success"].update(dv_)
        dv_ = data.pop("request errors", None)
        if isinstance(dv_, typing.Mapping):                  # pragma: no branch
            r_["request errors"].update(dv_)
        dv_ = data.pop("monitored errors", None)
        if isinstance(dv_, typing.Mapping):                 # pragma: no branch
            r_["monitored errors"].update(dv_)
        dv_ = data.pop("other errors", None)
        if isinstance(dv_, typing.Mapping):                 # pragma: no branch
            r_["other errors"].update(dv_)
        dv_ = data.pop("statistics", None)
        if isinstance(dv_, typing.Mapping):                 # pragma: no branch
            r_["statistics"].update(dv_)
        return r_

    def __add__(self, data: typing.Mapping) -> typing.Mapping:
        _lg.debug("adding")
        r_ = Report()
        if not isinstance(data, typing.Mapping):  # pragma: no branch
            return r_
        dv_ = data.pop("success", None)
        if isinstance(dv_, typing.Mapping):  # pragma: no branch
            keys_ = set(self["success"].keys()).union(dv_.keys())
            for k_ in keys_:
                v_ = dv_.get(k_, [])
                if isinstance(v_, list):
                    r_["success"][k_] = self["success"][k_] + v_

        dv_ = data.pop("request errors", None)
        if isinstance(dv_, typing.Mapping):  # pragma: no branch
            keys_ = set(self["request errors"].keys()).union(dv_.keys())
            for k_ in keys_:
                r_["request errors"][k_] = self["request errors"][k_] + dv_[k_]

        dv_ = data.pop("monitored errors", None)
        if isinstance(dv_, typing.Mapping):  # pragma: no branch
            keys_ = set(self["monitored errors"].keys()).union(dv_.keys())
            for k_ in keys_:
                r_["monitored errors"][k_] = self["monitored errors"][k_] + dv_[k_]

        dv_ = data.pop("other errors", None)
        if isinstance(dv_, typing.Mapping):  # pragma: no branch
            keys_ = set(self["other errors"].keys()).union(dv_.keys())
            for k_ in keys_:
                r_["other errors"][k_] = self["other errors"][k_] + dv_[k_]

        dv_ = data.pop("statistics", None)
        if isinstance(dv_, typing.Mapping):  # pragma: no branch
            keys_ = set(self["statistics"].keys()).union(dv_.keys())
            for k_ in keys_:
                r_["statistics"][k_] = self["statistics"][k_] + dv_[k_]
        return r_

    def __sub__(self, data: typing.Mapping) -> typing.Mapping:
        r_ = Report()
        if not isinstance(data, typing.Mapping):  # pragma: no branch
            return r_
        if "success" in data.keys() and \
                isinstance(data["success"], typing.Mapping):  # pragma: no branch
            keys_ = set(self["success"].keys()).union(data["success"].keys())
            for k_ in keys_:
                v_ = data["success"].get(k_, [])
                if isinstance(v_, list):
                    r_["success"][k_] = [x for x in self["success"][k_] if x not in v_]

        if "request errors" in data.keys() and \
                isinstance(data["request errors"], typing.Mapping):  # pragma: no branch
            keys_ = set(self["request errors"].keys()).union(data["request errors"].keys())
            for k_ in keys_:
                r_["request errors"][k_] = self["request errors"][k_] - data["request errors"][k_]
        if "monitored errors" in data.keys() and \
                isinstance(data["monitored errors"], typing.Mapping):  # pragma: no branch
            keys_ = set(self["monitored errors"].keys()).union(data["monitored errors"].keys())
            for k_ in keys_:
                r_["monitored errors"][k_] = self["monitored errors"][k_] - data["monitored errors"][k_]
        if "other errors" in data.keys() and \
                isinstance(data["other errors"], typing.Mapping):  # pragma: no branch
            keys_ = set(self["other errors"].keys()).union(data["other errors"].keys())
            for k_ in keys_:
                r_["other errors"][k_] = self["other errors"][k_] - data["other errors"][k_]

        if "statistics" in data.keys() and \
                isinstance(data["statistics"], typing.Mapping):  # pragma: no branch
            keys_ = set(self["statistics"].keys()).union(data["statistics"].keys())
            for k_ in keys_:
                r_["statistics"][k_] = self["statistics"][k_] - data["statistics"][k_]

        return r_

    def __mul__(self, data: typing.Union[float, int]) -> typing.Mapping:
        r_ = Report()
        if not (isinstance(data, int) or isinstance(data, float)):  # pragma: no branch
            return r_
        for k_, v_ in self["success"].items():
            r_["success"][k_] = [x * data for x in v_]
        for k_, v_ in self["request errors"].items():
            r_["request errors"][k_] = int(data * v_)
        for k_, v_ in self["monitored errors"].items():
            r_["monitored errors"][k_] = int(data * v_)
        for k_, v_ in self["other errors"].items():
            r_["other errors"][k_] = int(data * v_)
        for k_, v_ in self["statistics"].items():
            r_["statistics"][k_] = int(data * v_)
        return r_

    def __rmul__(self, data: typing.Union[float, int]) -> typing.Mapping:
        return self.__mul__(data)

    def __str__(self):
        ans_ = "\nsuccess:\n"
        vals_ = []
        for k_, v_ in self["success"].items():
            s_ = Stats(v_)
            vals_.append((k_, len(v_), s_.median(), *s_.perc_mid_50(), s_.perc_top_10()))
        vals_.sort(key=lambda x: x[-1], reverse=True)
        for v_ in vals_:
            ans_ += "{:>45}: {:>6d}, median: {:.3f}s, mid 50%: {:.3f}s - {:.3f}s, top 10% above {:.3f}s\n".format(
                *v_)
        for i_ in ("request errors", "monitored errors", "other errors", "statistics"):
            ans_ += "\n{}:\n".format(i_)
            for k_, v_ in self[i_].items():
                ans_ += "{:>35}: {:>6d}\n".format(k_, v_)
        return ans_ + "\n"

    def add_success(self, name, call_time):
        self["success"][name].append(call_time)

    def add_error(self, name, error_type):
        if error_type == "success" or error_type not in self.keys():
            return
        self[error_type][name] += 1

    def add_statistics(self, name, value):
        self["statistics"][name] += value
