import asyncio
import time
from decimal import Decimal

from hummingbot.connector.derivative.reya_perpetual import (
    reya_perpetual_constants as CONSTANTS,
    reya_perpetual_web_utils as web_utils,
)
from hummingbot.core.data_type.common import TradeType
from hummingbot.core.data_type.funding_info import FundingInfo, FundingInfoUpdate
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.perpetual_api_order_book_data_source import PerpetualAPIOrderBookDataSource
from hummingbot.core.web_assistant.connections.data_types import WSJSONRequest


class ReyaPerpetualAPIOrderBookDataSource(PerpetualAPIOrderBookDataSource):
    def __init__(self, trading_pairs, connector, api_factory):
        super().__init__(trading_pairs)
        self._connector = connector
        self._api_factory = api_factory
        self._books = {}
        self._update_id = 0

    def _next_update_id(self):
        self._update_id = max(self._update_id + 1, time.time_ns() // 1000)
        return self._update_id

    async def get_last_traded_prices(self, trading_pairs, domain=None):
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def get_funding_info(self, trading_pair):
        symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair)
        data = await self._connector._api_get(path_url=f"perpMarket/{symbol}/summary")
        return FundingInfo(trading_pair, Decimal(data["oraclePrice"]), Decimal(data["markPrice"]),
                           0, Decimal(data["fundingRate"]) / 100)

    async def _order_book_snapshot(self, trading_pair):
        symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair)
        # Capture before awaiting HTTP so a newer WS snapshot wins the race.
        update_id = self._next_update_id()
        data = await self._connector._api_get(path_url=f"market/{symbol}/depth", params={"limit": 100})
        return OrderBookMessage(OrderBookMessageType.SNAPSHOT, {
            "trading_pair": trading_pair, "update_id": update_id,
            "bids": [[row["px"], row["qty"]] for row in data["bids"]],
            "asks": [[row["px"], row["qty"]] for row in data["asks"]],
        }, data["updatedAt"] / 1000)

    async def _connected_websocket_assistant(self):
        ws = await self._api_factory.get_ws_assistant()
        await ws.connect(ws_url=CONSTANTS.WS_URL, ping_timeout=30)
        return ws

    async def _send_subscriptions(self, ws, trading_pair, operation):
        symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair)
        channels = [f"/v2/market/{symbol}/depth", f"/v2/market/{symbol}/perpExecutions",
                    f"/v2/perpMarket/{symbol}/summary"]
        for channel in channels:
            await ws.send(WSJSONRequest(payload={"type": operation, "channel": channel}))

    async def _subscribe_channels(self, ws):
        self._books.clear()
        for pair in self._trading_pairs:
            await self._send_subscriptions(ws, pair, "subscribe")

    async def subscribe_to_trading_pair(self, trading_pair):
        if self._ws_assistant is None:
            return False
        await self._send_subscriptions(self._ws_assistant, trading_pair, "subscribe")
        self.add_trading_pair(trading_pair)
        return True

    async def unsubscribe_from_trading_pair(self, trading_pair):
        if self._ws_assistant is None:
            return False
        await self._send_subscriptions(self._ws_assistant, trading_pair, "unsubscribe")
        self._books.pop(trading_pair, None)
        self.remove_trading_pair(trading_pair)
        return True

    def _channel_originating_message(self, event_message):
        if event_message.get("type") not in ("subscribed", "channel_data"):
            return ""
        channel = event_message.get("channel", "")
        if channel.endswith("/depth"):
            # Keep snapshots and diffs on one queue, preserving wire ordering.
            return self._snapshot_messages_queue_key
        if channel.endswith("/perpExecutions"):
            return self._trade_messages_queue_key
        if channel.endswith("/summary"):
            return self._funding_info_messages_queue_key
        return ""

    async def _process_websocket_messages(self, websocket_assistant):
        async for message in web_utils.websocket_messages(websocket_assistant):
            key = self._channel_originating_message(message)
            if key:
                self._message_queue[key].put_nowait(message)

    @staticmethod
    def _contents(message):
        return message.get("contents") if message.get("type") == "subscribed" else message.get("data")

    async def listen_for_order_book_snapshots(self, ev_loop, output):
        # REST and WS have no common sequence; never splice periodic REST reads into a live diff stream.
        queue = self._message_queue[self._snapshot_messages_queue_key]
        while True:
            try:
                message = await queue.get()
                await self._parse_order_book_snapshot_message(message, output)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().exception("Invalid Reya depth message; reconnecting for a fresh snapshot")
                self._books.clear()
                while not queue.empty():
                    queue.get_nowait()
                if self._ws_assistant is not None:
                    try:
                        await self._ws_assistant.disconnect()
                    except Exception:
                        self.logger().warning("Error disconnecting Reya depth stream", exc_info=True)

    async def _parse_order_book_snapshot_message(self, raw_message, message_queue):
        data = self._contents(raw_message)
        if data is None:
            return
        pair = await self._connector.trading_pair_associated_to_exchange_symbol(data["symbol"])
        if data["type"] == "SNAPSHOT":
            self._books[pair] = {"bids": {}, "asks": {}}
        elif pair not in self._books:
            raise IOError("Received depth update before its snapshot")
        book = self._books[pair]
        for side in ("bids", "asks"):
            for level in data.get(side, []):
                price, quantity = Decimal(level["px"]), Decimal(level["qty"])
                if quantity == 0:
                    book[side].pop(price, None)
                else:
                    book[side][price] = quantity
        message_queue.put_nowait(OrderBookMessage(OrderBookMessageType.SNAPSHOT, {
            "trading_pair": pair, "update_id": self._next_update_id(),
            "bids": sorted(book["bids"].items(), reverse=True), "asks": sorted(book["asks"].items()),
        }, data["updatedAt"] / 1000))

    async def _parse_order_book_diff_message(self, raw_message, message_queue):
        await self._parse_order_book_snapshot_message(raw_message, message_queue)

    async def _parse_trade_message(self, raw_message, message_queue):
        data = self._contents(raw_message)
        rows = data if isinstance(data, list) else (data or {}).get("data", [])
        for trade in rows:
            pair = await self._connector.trading_pair_associated_to_exchange_symbol(trade["symbol"])
            message_queue.put_nowait(OrderBookMessage(OrderBookMessageType.TRADE, {
                "trading_pair": pair, "trade_id": str(trade["sequenceNumber"]),
                "trade_type": float(TradeType.BUY.value if trade["side"] == "B" else TradeType.SELL.value),
                "price": trade["price"], "amount": trade["qty"],
            }, trade["timestamp"] / 1000))

    async def _parse_funding_info_message(self, raw_message, message_queue):
        data = self._contents(raw_message)
        if data is None:
            return
        pair = await self._connector.trading_pair_associated_to_exchange_symbol(data["symbol"])
        message_queue.put_nowait(FundingInfoUpdate(
            trading_pair=pair, index_price=Decimal(data["oraclePrice"]) if "oraclePrice" in data else None,
            mark_price=Decimal(data["markPrice"]), rate=Decimal(data["fundingRate"]) / 100,
            next_funding_utc_timestamp=0,  # Reya funding accrues continuously, with no scheduled settlement.
        ))
