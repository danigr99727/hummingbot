from hummingbot.connector.derivative.reya_perpetual import (
    reya_perpetual_constants as CONSTANTS,
    reya_perpetual_web_utils as web_utils,
)
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest


class ReyaPerpetualAPIUserStreamDataSource(UserStreamTrackerDataSource):
    def __init__(self, wallet_address, api_factory):
        super().__init__()
        self._wallet = wallet_address
        self._api_factory = api_factory

    async def _connected_websocket_assistant(self):
        ws = await self._api_factory.get_ws_assistant()
        await ws.connect(ws_url=CONSTANTS.WS_URL, ping_timeout=30)
        return ws

    async def _subscribe_channels(self, websocket_assistant):
        for resource in ("orderChanges", "perpExecutions", "executionBusts", "accountBalances", "positions"):
            await websocket_assistant.send(WSJSONRequest(payload={
                "type": "subscribe", "channel": f"/v2/wallet/{self._wallet}/{resource}",
            }))

    async def _send_ping(self, websocket_assistant):
        await websocket_assistant.send(WSJSONRequest(payload={"type": "ping"}))

    async def _process_websocket_messages(self, websocket_assistant, queue):
        try:
            async for message in web_utils.websocket_messages(websocket_assistant):
                if message.get("type") in ("subscribed", "channel_data"):
                    queue.put_nowait(message)
        finally:
            # The shared parser raises on channel errors and disconnects. Invalidate
            # cached risk immediately, before the transport's reconnect delay.
            queue.put_nowait({"type": "reya_stream_interrupted"})
