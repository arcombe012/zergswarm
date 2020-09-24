import logging
import typing
import asyncio
from collections import defaultdict
import json
from uuid import uuid1

import zmq
import zmq.asyncio as zmqaio

_lg = logging.getLogger("zergswarm")


class Message:
    __slots__ = ["message_type", "payload"]

    def __init__(self, message_type: typing.Optional[str] = None,
            payload: typing.Optional[dict] = None):
        self.message_type = message_type
        self.payload = payload

    def __str__(self) -> str:
        return '{{"message_type": {}, "payload": {} }}'.format(
            self.message_type, self.payload)

    def encode(self) -> bytes:
        return json.dumps({"message_type": self.message_type, "payload": self.payload},
            ensure_ascii=False).encode("utf-8")

    @classmethod
    def decode(cls, raw_message):
        try:
            return cls(**json.loads(raw_message))
        except Exception as e_:
            _lg.error("failed to decode message %s: [%s] %s", raw_message, e_.__class__.__name__, e_)
            return None


class ZeroCommServer:
    def __init__(self, server: str = "127.0.0.1"):
        if not "://" in server:
            self._server_ip = server
            self._server_address = None
            self._protocol = "tcp"
        else:
            self._server_address = server
            s_ = server.split(":")
            self._server_ip = s_[1][2:]
            self._protocol = s_[0]
        self._listener = None
        self._message_handlers = {
            "register": self._register_connection,
            "unregister": self._unregister_connection,
            }
        self._registered_connections = defaultdict(int)
        self._ctx = zmqaio.Context()
        self._listener = self._ctx.socket(zmq.REP)      # type: zmq.Socket
        self._running_task = None

    def _register_connection(self, data: dict):
        _lg.debug("registering client with data %s", data)
        if data is None or data.get("client_id", None) is None:
            id_ = "unknown"
        else:
            id_ = data["client_id"]
        self._registered_connections[id_] += 1
        _lg.debug(self._registered_connections)
        return {"client_id": id_, "data": {"registered": "ok"}}

    def _unregister_connection(self, data: dict):
        _lg.debug("unregistering client with data %s", data)
        if data is None or data.get("client_id", None) is None:
            id_ = "unknown"
        else:
            id_ = data["client_id"]
        self._registered_connections[id_] -= 1
        _lg.debug(self._registered_connections)
        return {"client_id": id_, "data": {"unregistered": "ok"}}

    async def __aenter__(self):
        _lg.debug("starting server")
        if not self._server_address:
            port_ = self._listener.bind_to_random_port("tcp://{}".format(self._server_ip))
            self._server_address = "tcp://{}:{}".format(self._server_ip, port_)
        else:
            self._listener.bind(self._server_address)
        await asyncio.sleep(0.02)
        _lg.debug("server bound to %s", self.server_address)
        self._running_task = asyncio.get_event_loop().create_task(self.request_processor())
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._running_task:
            self._running_task.cancel()
        self._listener.unbind(self._server_address)
        self._listener.close()
        self._server_address = None

    @property
    def server_address(self) -> typing.Optional[str]:
        return self._server_address

    @property
    def server_connections(self) -> int:
        _lg.debug(self._registered_connections)
        return sum(self._registered_connections.values())

    async def request_processor(self):
        _lg.debug("starting request processor")
        while True:
            try:
                # wait for a second for messages
                msg_raw_ = await self._listener.recv_multipart()
                _lg.debug("received message %s", msg_raw_)
                msg_ = Message.decode(b''.join(msg_raw_).decode('utf-8'))
                _lg.debug("decoded message as %s", msg_)
                message_type = msg_.message_type
                client_id = msg_.payload.get("client_id", None)
                if message_type not in self._message_handlers.keys():       # pragma: no branch
                    _lg.error("server received invalid message of type %s [payload = %s]",
                        message_type, msg_.payload)
                    ans_ = Message(message_type="error", payload={"client_id": client_id, "error": "invalid message type"})
                    self._listener.send_multipart([ans_.encode()])
                    continue
                try:
                    fnc_ = self._message_handlers[message_type]
                    # _lg.debug("calling function %s", fnc_.__name__)
                    ans_ = fnc_(msg_.payload)
                    if isinstance(ans_, typing.Awaitable):
                        ans_ = await ans_
                    ans_["client_id"] = client_id
                    ans_ = Message(message_type=message_type+"_reply", payload=ans_)
                    _lg.debug("replying with %s", ans_)
                    await self._listener.send_multipart([ans_.encode()])
                    # _lg.debug("reply sent")
                except Exception as e_:
                    _lg.error("exception while calling handler: %s", e_)
                    ans_ = Message(payload={"client_id": client_id, "error": str(e_)})
                    await self._listener.send_multipart([ans_.encode()])
                continue
            except asyncio.CancelledError:
                _lg.info("stopping server")
                break
            except Exception as e_:
                _lg.error("exception %s while reading message\n%s", e_.__class__.__name__, e_)
                continue
        _lg.debug("stopping request processor")

    def register_callback(self, message_type: str,
            callback: typing.Callable[[typing.Any], typing.Optional[dict]]):
        self._message_handlers[message_type] = callback
        _lg.debug("registered callback\n\t%s: %s", message_type, callback)


class ZeroCommClient:
    def __init__(self, server_address: typing.Optional[str]):
        """
        class initialization
        :param server_address: full binding address of the server (ex.: tcp://127.0.0.1:6666)
        """
        self._server_address = server_address
        _lg.debug("client initialized with server address %s", server_address)
        self._ctx = zmqaio.Context()
        self._client = None
        self._id = str(uuid1((id(self))))
        self._active = False

    async def __aenter__(self):
        if self._server_address is not None:                # pragma: no branch
            _lg.debug("connecting client")
            self._client = self._ctx.socket(zmq.REQ)      # type: zmq.Socket
            self._client.connect(self._server_address)
            self._active = True
            _lg.debug("registering client: %s", self._id)
            await self.call("register", data={"client_id": self._id})

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._server_address is not None:                # pragma: no branch
            _lg.debug("unregistering client %s", self._id)
            await self.call("unregister", data={"client_id": self._id})
            _lg.debug("disconnecting client")
            self._client.disconnect(self._server_address)
            self._client = None
            self._active = False

    async def call(self, message_type: str, data: dict):
        if not self._active:                                # pragma: no branch
            _lg.error("no client registered")
            return None
        # noinspection PyBroadException
        try:
            if not data.get("client_id", None):
                data["client_id"] = self._id
            message_ = Message(message_type=message_type, payload=data)
            await self._client.send_multipart([message_.encode()])
            _lg.debug("waiting for answer")
            ans_ = await asyncio.wait_for(self._client.recv_multipart(), 10)
            _lg.debug("received answer %s", ans_)
            if ans_:
                msg_ = Message.decode(b''.join(ans_))
                return msg_.payload
            else:
                return None
        except Exception as e_:
            _lg.error("no answer received: [%s] %s", e_.__class__.__name__, e_)
            return None

    @property
    def server_address(self) -> typing.Optional[str]:
        return self._server_address

    @property
    def client_id(self) -> str:
        return self._id
