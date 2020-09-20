import configparser as cp
import csv
import logging
import typing

_lg = logging.getLogger("zergswarm")


class ConfigReader:
    def __init__(self, settings_file: typing.Optional[str] = None):
        if settings_file is None:   # pragma: no branch
            settings_file = "settings.ini"
        try:
            parser_ = cp.ConfigParser()
            _lg.debug("reading file %s" % settings_file)
            parser_.read(settings_file)
            if "OVERMIND" not in parser_.keys():
                raise RuntimeError("settings file {} is missing the [DEFAULT] section")
            sec_ = parser_["OVERMIND"]       # type: dict
            self._hatchling_config_file = sec_.get("hatchling_config_file", None)
            self._hatchling_offset = int(sec_.get("hatchling_offset", 0))
            self._min_hatchlings_per_colony = int(sec_.get("min_hatchlings_per_colony", 100))
            self._max_hatchlings_per_colony = int(sec_.get("max_hatchlings_per_colony", 200))
            if self._hatchling_offset < 0:
                self._hatchling_offset = 0
            self._hatchling_count = int(sec_.get("hatchling_count", -1))
            cfg_section_ = "HATCHLING"
            if cfg_section_ in parser_.sections():
                self._hatchling_settings = {k: v for k, v in parser_[cfg_section_].items()}
            else:
                self._hatchling_settings = dict()
            if self._hatchling_count < 1:
                self._hatchling_count = len(list(self.get_hatchling_config()))

        except Exception as e_:
            _lg.error("failed to read the config file %s: %s", settings_file, e_)
            raise

    def get_hatchling_config(self) -> typing.Generator:
        if not self._hatchling_config_file:
            _lg.warning("no config file for hatchlings, will only use the section in the settings file")
            if self._hatchling_count < 1:
                return
            for _ in range(self._hatchling_count):
                yield self._hatchling_settings
        else:
            try:
                with open(self._hatchling_config_file) as f_:
                    settings_ = csv.DictReader(f_)
                    start_ = self._hatchling_offset
                    stop_ = start_ + self._hatchling_count if self._hatchling_count > 0 else 0
                    for i_, d_ in enumerate(settings_):     # type: int, dict
                        if i_ < start_:
                            continue
                        elif 0 < stop_ <= i_:
                            break
                        d_.update(self._hatchling_settings)
                        yield d_
            except Exception as e_:
                _lg.error("failed to parse hatchling config file %s: %s", self._hatchling_config_file, e_)
                raise

    @property
    def hatchling_count(self):
        return self._hatchling_count

    @property
    def min_hatchlings_per_colony(self) -> int:
        return self._min_hatchlings_per_colony

    @property
    def max_hatchlings_per_colony(self) -> int:
        return self._max_hatchlings_per_colony
