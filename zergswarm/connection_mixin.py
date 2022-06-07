import logging
import asyncio
import typing
import types
import json
import socket

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
    __reports = None
    __connector: aiohttp.BaseConnector = None

    @staticmethod
    def reset_reports(report_cls=None):
        if not report_cls:
            report_cls = type(ConnectionMixin.__reports)
            if type(None) == report_cls:
                report_cls = Report
        ConnectionMixin.__reports = report_cls()

    @staticmethod
    def report_stats() -> dict:
        ans_ = ConnectionMixin.__reports
        ConnectionMixin.reset_reports()
        _lg.debug("report:\n\n%s\n\n", ans_)
        return ans_

    def __init__(self, base_url: str, max_retries: int = 10, retry_delay: float = 1.,
            report_cls=Report):
        self.__report_cls = report_cls
        if not ConnectionMixin.__reports:
            ConnectionMixin.reset_reports(report_cls)
        self.__session = None               # type: typing.Optional[aiohttp.ClientSession]
        self.__headers = None
        self.__auth_headers = None          # type: typing.Optional[dict]
        self.__base_url = base_url          # type: str
        self.__max_retries = max_retries    # type: int
        self.__retry_delay = retry_delay    # type: float
        self._context = types.SimpleNamespace()     # type: types.SimpleNamespace
        # noinspection PyTypeChecker
        self._tracer = aiohttp.TraceConfig(trace_config_ctx_factory=self.context)
        self._tracer.on_request_start.append(_on_request_start)
        self._tracer.on_request_end.append(_on_request_end)

    def context(self, *args, **kwargs) -> types.SimpleNamespace:
        _lg.debug("context params: %s, %s", args, kwargs)
        return self._context

    async def setup_session(self, custom_connector: bool = False):
        """
        set up a client session; can request a custom connector class
        if the object is not used as a context manager.

        :param custom_connector: object of type derived from aiohttp.BaseConnector,
            typically aiohttp.TCPConnector
        :return:
        """
        _lg.info("setting up session")
        try:
            if custom_connector:
                if not ConnectionMixin.__connector:
                    ConnectionMixin.__connector = aiohttp.TCPConnector(
                        verify_ssl=True, use_dns_cache=True, ttl_dns_cache=600,
                        family=socket.AF_INET, limit=10000, enable_cleanup_closed=True,
                        force_close=True
                    )
                self.__session = aiohttp.ClientSession(headers=self.__headers,
                    connector=ConnectionMixin.__connector, connector_owner=False,
                    trace_configs=[self._tracer], auto_decompress=True)
            else:
                self.__session = aiohttp.ClientSession(headers=self.__headers,
                    trace_configs=[self._tracer], auto_decompress=True)
        except Exception as e_:
            _lg.error(e_)
            raise

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

    @property
    def base_url(self) -> str:
        return self.__base_url

    async def _wait_and_increment_delay(self):
        _lg.debug("waiting ...")
        await asyncio.sleep(self.__retry_delay)
        if self.__retry_delay < 60:
            self.__retry_delay *= 1.5
        elif self.__retry_delay < 120:
            self.__retry_delay += 5

    async def do_request(self, *, url: str, name: str = None, request_type: str = "POST",
            data: dict = None, json_data: dict = None, needs_auth: bool = False, cookies=None,
            error_status: set = None, detailed_response: bool = False) \
            -> typing.Optional[typing.Union[str, tuple]]:
        """
        perform a request
        :param url: route to append to the base url
        :param name: name for logging the request outcome (same as the url if None)
                    specify an explicit name if the route has variable parameters
        :param request_type: type of request: POST, GET, OPTIONS, HEAD, PUT, PATCH, DELETE
                    default: POST
        :param data: data to be sent in the post request if not json encoded
        :param json_data: json data to be sent in the post request if json encoded
                    (data and json_data should not simultaneously be not None)
        :param needs_auth: if True (default  False) include the auth headers
        :param cookies: optional request cookies, to be merged with the session ones
        :param error_status: set or error status values to monitor (for monitored errors
                    the response is returned instead of None)
        :param detailed_response: if False (default) return only the response, otherwise
                    return additional metadata information
        :return: if not detailed_response, return the call response,
                    or (call response, content_type, response_headers, cookies) otherwise
                    for various kinds of errors return None
        """
        if name is None:
            name = url
        if self.__session is None or self.__session.closed:
            _lg.error("no active session")
            ConnectionMixin.__reports["request errors"][name] += 1
            return None
        if needs_auth and (not self.__auth_headers):
            _lg.error("authorization needed and not present in headers [%s]", self.__auth_headers)
            ConnectionMixin.__reports["request errors"][name] += 1
            return None

        if data and json_data:
            _lg.error("set either data or json_data, but not both")
            return None

        if data is None and json_data is None:
            data = ""

        counter_ = 0
        if "/" == url[0]:
            full_url_ = self.__base_url + url
        else:
            full_url_ = url

        while counter_ < self.__max_retries:
            try:
                compress_ = True
                _lg.debug("performing a %s request to %s:\n%s", request_type, full_url_,
                    dict(method=request_type, url=full_url_,
                        data=data, json=json_data, cookies=cookies, compress=compress_,
                        headers=self.__auth_headers if needs_auth else None))
                async with self.__session.request(method=request_type, url=full_url_,
                        data=data, json=json_data, cookies=cookies, compress=compress_,
                        headers=self.__auth_headers if needs_auth else None) as resp:
                    ans_ = await resp.read()
                    try:
                        # try to decode as text
                        txt_ = ans_.decode(errors="strict")
                    except UnicodeDecodeError:
                        # pass along binary result
                        txt_ = ans_
                    if 400 > resp.status:
                        # 2xx and 3xx responses are considered success
                        duration_ = self._context.end - self._context.start
                        _lg.debug("response status: %d, duration %.4fs", resp.status, duration_)
                        ConnectionMixin.__reports.add_success(name, duration_)
                        if not detailed_response:
                            return txt_
                        else:
                            return txt_, resp.content_type, resp.headers, resp.cookies
                    else:
                        if 1 == resp.status // 400:
                            _lg.error("error received while calling %s: status=%s reason=%s",
                                url, resp.status, resp.reason)
                            # 4xx errors are fatal unless monitored
                            if error_status and resp.status in error_status:
                                ConnectionMixin.__reports.add_error(name, "monitored errors")
                                if not detailed_response:
                                    return txt_
                                else:
                                    return txt_, resp.content_type, resp.headers, resp.cookies
                            ConnectionMixin.__reports.add_error(name, "other errors")
                            break
                        _lg.error(("error received while calling %s: status=%s reason=%s"
                            "[attempt %s of %s]"), url, resp.status, resp.reason, counter_ + 1,
                            self.__max_retries)
                        if error_status and resp.status in error_status:
                            ConnectionMixin.__reports.add_error(name, "monitored errors")
                            if not detailed_response:
                                return txt_
                            else:
                                return txt_, resp.content_type, resp.headers, resp.cookies
                        elif 1 == resp.status // 500:
                            # 5xx responses
                            ConnectionMixin.__reports.add_error(name, "request errors")
                            counter_ += 1
                            await self._wait_and_increment_delay()
                            continue
            except aiohttp.client.ClientError as e_:
                counter_ += 1
                _lg.error("caught a %s while accessing %s: %s [attempt %s of %s]",
                    type(e_).__name__, url, e_, counter_, self.__max_retries)
                ConnectionMixin.__reports.add_error(name, "other errors")
                continue
            break
        return None

    async def do_request_json(self, *, url: str, name: str = None, request_type: str = "POST",
            data: dict = None, json_data: dict = None, needs_auth: bool = False, cookies=None,
            error_status: set = None, detailed_response: bool = False) \
            -> typing.Optional[typing.Union[dict, tuple]]:
        ans_ = await self.do_request(url=url, name=name, request_type=request_type,
            data=data, json_data=json_data, needs_auth=needs_auth, cookies=cookies,
            error_status=error_status, detailed_response=detailed_response)
        if not ans_ or ans_[1] != "application/json":
            return None
        if not detailed_response:
            return json.loads(ans_[0])
        else:
            return json.loads(ans_[0]), *ans_[1:]
