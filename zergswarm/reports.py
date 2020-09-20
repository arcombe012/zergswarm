import typing
from collections import defaultdict
import functools


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
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self["success"] = defaultdict(lambda: {"count": 0, "time": 0.})
        self["request errors"] = defaultdict(int)
        self["monitored errors"] = defaultdict(int)
        self["other errors"] = defaultdict(int)

    def to_dict(self) -> dict:
        return {"success":      {k_: v_ for k_, v_ in self["success"].items()},
            "request errors":   {k_: v_ for k_, v_ in self["request errors"].items()},
            "monitored errors": {k_: v_ for k_, v_ in self["monitored errors"].items()},
            "other errors":     {k_: v_ for k_, v_ in self["other errors"].items()},
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
        return r_

    def __add__(self, data: typing.Mapping) -> typing.Mapping:
        r_ = Report()
        if not isinstance(data, typing.Mapping):  # pragma: no branch
            return r_
        dv_ = data.pop("success", None)
        if isinstance(dv_, typing.Mapping):  # pragma: no branch
            keys_ = set(self["success"].keys()).union(dv_.keys())
            for k_ in keys_:
                v_ = dv_[k_]
                if isinstance(v_, typing.Mapping) and \
                        {"count", "time"}.issubset(v_.keys()):
                    v0_ = self["success"][k_]
                    r_["success"][k_]["count"] = v0_["count"] + v_["count"]
                    r_["success"][k_]["time"] = v0_["time"] + v_["time"]

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
        return r_

    def __sub__(self, data: typing.Mapping) -> typing.Mapping:
        r_ = Report()
        if not isinstance(data, typing.Mapping):  # pragma: no branch
            return r_
        if "success" in data.keys() and \
                isinstance(data["success"], typing.Mapping):  # pragma: no branch
            keys_ = set(self["success"].keys()).union(data["success"].keys())
            for k_ in keys_:
                v_ = data["success"][k_]
                if isinstance(v_, typing.Mapping) and \
                        {"count", "time"}.issubset(v_.keys()):
                    v0_ = self["success"][k_]
                    r_["success"][k_]["count"] = v0_["count"] - v_["count"]
                    r_["success"][k_]["time"] = v0_["time"] - v_["time"]

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
        return r_

    def __mul__(self, data: typing.Union[float, int]) -> typing.Mapping:
        r_ = Report()
        if not (isinstance(data, int) or isinstance(data, float)):  # pragma: no branch
            return r_
        for k_, v_ in self["success"].items():
            x_ = data * v_["count"]
            nx_ = int(data * v_["count"])
            r_["success"][k_]["count"] = nx_
            r_["success"][k_]["time"] = nx_ * v_["time"] / x_
        for k_, v_ in self["request errors"].items():
            r_["request errors"][k_] = int(data * v_)
        for k_, v_ in self["monitored errors"].items():
            r_["monitored errors"][k_] = int(data * v_)
        for k_, v_ in self["other errors"].items():
            r_["other errors"][k_] = int(data * v_)
        return r_

    def __rmul__(self, data: typing.Union[float, int]) -> typing.Mapping:
        return self.__mul__(data)

    def __str__(self):
        ans_ = "\n{}:\n\t{}".format("success",
            "\n\t".join(
                "{:>45}: {:>6d} [avg. {:.3f}s]".format(
                    k1_, v1_["count"], (v1_["time"] / v1_["count"] if v1_["count"] > 0 else 0))
                    for k1_, v1_ in self["success"].items())) + \
            "\n" + "\n".join("{}:\n\t{}".format(k_,
                "\n\t".join(
                    "{:>45}: {:>6d}".format(k1_, v1_) for k1_, v1_ in self[k_].items()
                ))
                for k_ in ("request errors", "monitored errors", "other errors")
            ) + "\n"
        return ans_

    def add_success(self, name, call_time):
        self["success"][name]["count"] += 1
        self["success"][name]["time"] += call_time

    def add_error(self, name, error_type):
        if error_type == "success" or error_type not in self.keys():
            return
        self[error_type][name] += 1
