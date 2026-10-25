import asyncio
import re

from hummingbot.connector.derivative.reya_perpetual import reya_perpetual_constants as CONSTANTS
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


def public_rest_url(path_url, domain=CONSTANTS.DEFAULT_DOMAIN):
    if domain != CONSTANTS.DEFAULT_DOMAIN:
        raise ValueError(f"Unsupported Reya domain: {domain}")
    return CONSTANTS.REST_URL + path_url.lstrip("/")


def private_rest_url(path_url, domain=CONSTANTS.DEFAULT_DOMAIN):
    return public_rest_url(path_url, domain)


def is_wallet_address(address):
    return isinstance(address, str) and re.fullmatch(r"0x[0-9a-f]{40}", address, re.IGNORECASE) is not None


def api_wallet_status_url(address, domain=CONSTANTS.DEFAULT_DOMAIN):
    if domain != CONSTANTS.DEFAULT_DOMAIN:
        raise ValueError(f"Unsupported Reya domain: {domain}")
    if not is_wallet_address(address) or int(address, 16) == 0:
        raise ValueError("A non-zero owner wallet address is required")
    return CONSTANTS.LEGACY_REST_URL + f"wallet/{address.lower()}/apiWalletStatus"


def build_api_factory(throttler=None, auth=None):
    return WebAssistantsFactory(throttler=throttler or AsyncThrottler(CONSTANTS.RATE_LIMITS), auth=auth)


def is_exchange_information_valid(market):
    return market.get("status") == "TRADING" and market["symbol"].endswith("RUSDPERP")


async def websocket_messages(ws):
    waiting_for_pong = False
    while True:
        try:
            response = await asyncio.wait_for(ws.receive(), CONSTANTS.HEARTBEAT)
        except asyncio.TimeoutError:
            if waiting_for_pong:
                raise ConnectionError("Reya WebSocket heartbeat timed out")
            await ws.send(WSJSONRequest(payload={"type": "ping"}))
            waiting_for_pong = True
            continue
        if response is None or response.data is None:
            raise ConnectionError("Reya WebSocket disconnected")
        waiting_for_pong = False
        message = response.data
        if message.get("type") == "error":
            raise IOError(f"Reya subscription rejected: {message.get('message')}")
        yield message
