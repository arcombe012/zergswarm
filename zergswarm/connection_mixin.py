import logging
import asyncio
import typing
import types
import json

import aiohttp

from .reports import Report


_lg = logging.getLogger("zergswarm")


async def _on_request_start(session: aiohttp.ClientSession, trace_config_ctx, params):
    trace_config_ctx.start = asyncio.get_event_loop().time()


async def _on_request_end(session: aiohttp.ClientSession, trace_config_ctx, params):
    trace_config_ctx.end = asyncio.get_event_loop().time()


class ConnectionMixin:
    # making this a class attribute means for this process
    # all statistics are already aggregated
    # no locks means pretty please do not use thread executors
    # to avoid a need for locks :-)
    reports = None

    @staticmethod
    def reset_reports(report_cls=None):
        if not report_cls:
            report_cls = type(ConnectionMixin.reports)
            if report_cls == type(None):
                report_cls = Report
        ConnectionMixin.reports = report_cls()

    @staticmethod
    def report_stats() -> dict:
        ans_ = ConnectionMixin.reports
        ConnectionMixin.reset_reports()
        _lg.debug("report:\n\n%s\n\n", ans_)
        return ans_

    def __init__(self, base_url: str, max_retries: int = 10, retry_delay: float = 1., report_cls = Report):
        self.__report_cls = report_cls
        if not ConnectionMixin.reports:
            ConnectionMixin.reset_reports(report_cls)
        self.__session = None               # type: typing.Optional[aiohttp.ClientSession]
        self.__headers = None
        self.__auth_headers = None          # type: typing.Optional[dict]
        self.__base_url = base_url          # type: str
        self.__max_retries = max_retries    # type: int
        self.__retry_delay = retry_delay    # type: float
        self._context = types.SimpleNamespace()     # type: types.SimpleNamespace
        self._tracer = aiohttp.TraceConfig(trace_config_ctx_factory=self.context)
        self._tracer.on_request_start.append(_on_request_start)
        self._tracer.on_request_end.append(_on_request_end)

    def context(self, *args, **kwargs):
        return self._context

    async def setup_session(self):
        self.__session = aiohttp.ClientSession(headers=self.__headers,
           trace_configs=[self._tracer])

    async def close_session(self):
        await self.__session.close()
        self.__session = None

    async def __aenter__(self):
        if self.__session is not None:
            _lg.debug("closing existing session")
            await self.__session.close()
        await self.setup_session()
        _lg.debug("new session opened")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.__session is not None and not self.__session.closed:
            await self.close_session()
            _lg.debug("session closed")

    def has_session(self) -> bool:
        return self.__session is not None

    @property
    def auth_headers(self) -> typing.Optional[dict]:
        return self.__auth_headers

    @auth_headers.setter
    def auth_headers(self, new_headers):
        self.__auth_headers = new_headers

    @property
    def headers(self) -> typing.Optional[dict]:
        return self.__headers

    @headers.setter
    def headers(self, new_headers):
        self.__headers = new_headers

    async def _wait_and_increment_delay(self):
        _lg.debug("waiting ...")
        await asyncio.sleep(self.__retry_delay)
        if self.__retry_delay < 60:
            self.__retry_delay *= 1.5
        elif self.__retry_delay < 120:
            self.__retry_delay += 5

    async def do_request(self, *, url: str, name: str = None, request_type: str = "POST",
            json_data: dict = None, needs_auth: bool = True,
            error_status: set = None) -> typing.Optional[str]:
        """
        perform a request
        :param url: route to append to the base url
        :param name: name for logging the request outcome (same as the url if None)
                    specify an explicit name if the route has variable parameters
        :param request_type: type of request: POST, GET, OPTIONS, HEAD, PUT, PATCH, DELETE
                    default: POST
        :param json_data: json data to be sent in the post request
        :param needs_auth: if True (default) include the auth headers
        :param error_status: set or error status values to monitor (for monitored errors
                    the response is returned instead of None)
        :return: the call response, or None for unmonitored or connection errors
        """
        if name is None:
            name = url
        if self.__session is None or self.__session.closed:
            _lg.error("no active session")
            self.reports["request errors"][name] += 1
            return None
        if needs_auth and (self.__auth_headers is None
                or "authorization" not in self.__auth_headers.keys()):
            _lg.error("authorization needed and not present in headers")
            self.reports["request errors"][name] += 1
            return None

        counter_ = 0
        full_url_ = self.__base_url + url
        if "POST" == request_type:
            _lg.debug("performing a POST to %s", full_url_)
            method_ = self.__session.post
        elif "GET" == request_type:
            _lg.debug("performing a GET to %s", full_url_)
            method_ = self.__session.get
        elif "OPTIONS" == request_type:
            _lg.debug("performing an OPTIONS to %s", full_url_)
            method_ = self.__session.options
        elif "PUT" == request_type:
            _lg.debug("performing a PUT to %s", full_url_)
            method_ = self.__session.put
        elif "PATCH" == request_type:
            _lg.debug("performing a PATCH to %s", full_url_)
            method_ = self.__session.patch
        elif "DELETE" == request_type:
            _lg.debug("performing a DELETE to %s", full_url_)
            method_ = self.__session.delete
        elif "HEAD" == request_type:
            _lg.debug("performing a HEAD to %s", full_url_)
            method_ = self.__session.head

        while counter_ < self.__max_retries:
            try:
                async with method_(url=full_url_, json=json_data,
                        headers=self.__auth_headers if needs_auth else None) as resp:
                    txt_ = await resp.text()
                    if 200 == resp.status:
                        duration_ = self._context.end - self._context.start
                        _lg.debug("response status: 200, duration %.4fs", duration_)
                        self.reports.add_success(name, duration_)
                        return txt_
                    else:
                        _lg.error(("error received while calling %s: status=%s "
                            "[attempt %s of %s]"), url, resp.status, counter_ + 1,
                            self.__max_retries)
                        if error_status and resp.status in error_status:
                            self.reports.add_error(name, "monitored errors")
                            return txt_
                        elif 500 <= resp.status:
                            self.reports.add_error(name, "request errors")
                            counter_ += 1
                            await self._wait_and_increment_delay()
                            continue
            except aiohttp.client.ClientError as e_:
                counter_ += 1
                _lg.error("caught a %s while accessing %s: %s [attempt %s of %s]",
                    type(e_).__name__, url, e_, counter_, self.__max_retries)
                self.reports.add_error(name, "other errors")
                continue
            break
        return None

    async def do_request_json(self, *, url: str, name: str = None, request_type: str = "POST",
            json_data: dict = None, needs_auth: bool = True,
            error_status: set = None) -> typing.Optional[dict]:
        ans_ = await self.do_request(url=url, name=name, request_type=request_type, json_data=json_data,
            needs_auth=needs_auth, error_status=error_status)
        if not ans_:
            return None
        return json.loads(ans_)
