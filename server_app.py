import asyncio
import logging
import io
from http import HTTPStatus

from . import ota_cache

logger = logging.getLogger(__name__)

# only expose app
__all__ = "app"


class _App:
    def __init__(self, cache_enabled=False):
        self._ota_cache = ota_cache.OTACache(cache_enable=cache_enabled, init=True)

    async def _respond_with_error(self, status: HTTPStatus, msg: str, send):
        await send(
            {
                "type": "http.response.start",
                "status": status,
                "headers": [
                    [b"content-type", b"text/html;charset=UTF-8"],
                ],
            }
        )
        await send({"type": "http.response.body", "body": msg.encode("utf8")})

    async def _send_chunk(self, data: bytes, more: bool, send):
        await send({"type": "http.response.body", "body": data, "more_body": more})

    async def _init_response(self, status: HTTPStatus, headers: dict, send):
        await send(
            {
                "type": "http.response.start",
                "status": status,
                "headers": headers,
            }
        )

    async def _pull_data_and_send(self, url: str, send):
        f: ota_cache.OTAFile = await self._ota_cache.retrieve_file(url)

        # for any reason the response is not OK,
        # report to the client as 500
        if f is None:
            msg = f"proxy server failed to handle request {url=}"
            await self._respond_with_error(HTTPStatus.INTERNAL_SERVER_ERROR, msg, send)

            # terminate the request processing
            logger.error(f"failed to handle request {url=}")
            return

        # parse response
        # NOTE: currently only record content_type and content_encoding
        content_type = f.content_type
        content_encoding = f.content_encoding
        headers = []
        if content_type:
            headers.append([b"Content-Type", content_type.encode()])
        if content_encoding:
            headers.append([b"Content-Encoding", content_encoding.encode()])

        # prepare the response to the client
        await self._init_response(HTTPStatus.OK, headers, send)

        # stream the response to the client
        async for chunk in f:
            await self._send_chunk(chunk, True, send)

        # finish the streaming
        await self._send_chunk(b"", False, send)

    async def __call__(self, scope, receive, send):
        """
        the entrance of the app
        """
        assert scope["type"] == "http"
        # check method, currently only support GET method
        if scope["method"] != "GET":
            msg = "ONLY SUPPORT GET METHOD."
            await self._respond_with_error(HTTPStatus.BAD_REQUEST, msg, send)
            return

        # get the url from the request
        try:
            url, hash = scope["path"], scope["headers"]
        except Exception:
            self._respond_with_error("lack of critical headers")

        await self._pull_data_and_send(url, send)


app = _App()
