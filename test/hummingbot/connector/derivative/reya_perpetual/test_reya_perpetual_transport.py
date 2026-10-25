import asyncio
import json
import unittest
from decimal import Decimal
from test.hummingbot.connector.derivative.reya_perpetual import test_reya_perpetual as fixtures
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from hummingbot.connector.derivative.reya_perpetual import reya_perpetual_web_utils as web
from hummingbot.connector.derivative.reya_perpetual.reya_perpetual_auth import ReyaPerpetualAuth, scale_e18
from hummingbot.connector.derivative.reya_perpetual.reya_perpetual_derivative import ReyaOrderOutcomeUnknown
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, TradeType
from hummingbot.core.data_type.in_flight_order import OrderState
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest

VECTORS = fixtures.VECTORS


class TransportTests(unittest.IsolatedAsyncioTestCase):
    async def test_heartbeat_disconnect_and_server_errors(self):
        ws = SimpleNamespace(receive=AsyncMock(side_effect=[asyncio.TimeoutError, asyncio.TimeoutError]), send=AsyncMock())
        with self.assertRaises(ConnectionError):
            await anext(web.websocket_messages(ws))
        self.assertEqual(ws.send.call_args.args[0].payload, {"type": "ping"})
        ws.receive = AsyncMock(return_value=None)
        with self.assertRaises(ConnectionError):
            await anext(web.websocket_messages(ws))
        ws.receive = AsyncMock(return_value=SimpleNamespace(data={"type": "error", "message": "invalid channel"}))
        with self.assertRaises(IOError):
            await anext(web.websocket_messages(ws))
        ws.receive = AsyncMock(return_value=SimpleNamespace(data={"type": "pong"}))
        self.assertEqual(await anext(web.websocket_messages(ws)), {"type": "pong"})

    async def test_rest_auth_and_exact_scaling(self):
        auth = ReyaPerpetualAuth(VECTORS["PRIVATE_KEY"], {})
        request = RESTRequest(RESTMethod.POST, url="https://api.reya.xyz/v2/cancelAll", data=json.dumps({"accountId": 42}))
        signed = await auth.rest_authenticate(request)
        self.assertEqual(len(json.loads(signed.data)["signature"]), 132)
        read = RESTRequest(RESTMethod.GET, url="https://api.reya.xyz/v2/feeTiers")
        self.assertIs(await auth.rest_authenticate(read), read)
        self.assertIs(await auth.ws_authenticate(read), read)
        self.assertEqual(scale_e18(Decimal("123456789012.123456789012345678")), 123456789012123456789012345678)
        for bad in ["NaN", "-1", "0.0000000000000000001"]:
            with self.assertRaises(ValueError):
                scale_e18(Decimal(bad))
        with self.assertRaises(ValueError):
            auth.sign_payload("unsupported", {"accountId": 42})
        self.assertEqual(web.private_rest_url("feeTiers"), "https://api.reya.xyz/v2/feeTiers")
        with self.assertRaises(ValueError):
            web.public_rest_url("feeTiers", "unknown")


class LifecycleTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = fixtures.ConnectorTests.asyncSetUp
    track = fixtures.ConnectorTests.track

    async def test_write_outcomes_do_not_retry(self):
        target = "hummingbot.connector.exchange_py_base.ExchangePyBase._api_request"
        with patch(target, new_callable=AsyncMock) as request:
            request.side_effect = asyncio.TimeoutError()
            with self.assertRaises(ReyaOrderOutcomeUnknown):
                await self.connector._api_request("createOrder", RESTMethod.POST)
            self.assertEqual(request.await_count, 1)
            request.side_effect = IOError("HTTP status is 400. INPUT_VALIDATION_ERROR")
            with self.assertRaises(IOError) as rejected:
                await self.connector._api_request("createOrder", RESTMethod.POST)
            self.assertNotIsInstance(rejected.exception, ReyaOrderOutcomeUnknown)
            request.side_effect = None
            request.return_value = []
            self.assertEqual(await self.connector._api_request("feeTiers"), [])

    async def test_cancel_unknown_id_and_order_history_recovery(self):
        order = self.track()
        order.exchange_order_id = None
        await self.connector._place_cancel(order.client_order_id, order)
        self.assertIn("clientOrderId", self.connector._api_post.call_args.kwargs["data"])
        self.assertNotIn("orderId", self.connector._api_post.call_args.kwargs["data"])
        row = {"accountId": 42, "symbol": "BTCRUSDPERP", "orderId": "123", "clientOrderId": self.connector._api_post.call_args.kwargs["data"]["clientOrderId"]}
        self.connector._api_get.side_effect = [[], {"data": [row], "meta": {}}]
        self.assertEqual(await self.connector._find_order(order), row)
        self.connector._api_get.side_effect = [[], {"data": [], "meta": {}}]
        with self.assertRaises(IOError):
            await self.connector._find_order(order)

    async def test_settled_fill_then_terminal_and_clear_positions(self):
        order = self.track()
        self.connector._find_order = AsyncMock(return_value={"orderId": "123", "status": "FILLED", "cumQty": "1", "lastUpdateAt": 1000})
        execution = {"sequenceNumber": 3, "takerAccountId": 42, "takerOrderId": "123", "qty": "1", "price": "100", "takerFee": "0.03", "timestamp": 1000}
        self.assertTrue(self.connector._trade_update(execution, order).is_taker)
        maker_execution = {"sequenceNumber": 4, "makerAccountId": 42, "makerOrderId": "123", "qty": "1", "price": "100", "makerFee": "-0.01", "timestamp": 1000}
        self.assertFalse(self.connector._trade_update(maker_execution, order).is_taker)

        async def history(resource, *args):
            for row in ([execution] if resource == "perpExecutions" else []):
                yield row
        self.connector._history = history
        self.assertEqual((await self.connector._request_order_status(order)).new_state, OrderState.FILLED)
        self.assertEqual(order.executed_amount_base, Decimal("1"))
        self.connector._api_get.side_effect = [[fixtures.position_row(side="A")], []]
        await self.connector._update_positions()
        self.assertEqual(next(iter(self.connector.account_positions.values())).amount, Decimal("-2"))
        await self.connector._update_positions()
        self.assertEqual(self.connector.account_positions, {})

    async def test_depth_parser_recovers_after_malformed_message(self):
        source = self.connector._create_order_book_data_source()
        queue = source._message_queue[source._snapshot_messages_queue_key]
        output = asyncio.Queue()
        snapshot = {"type": "subscribed", "contents": {"type": "SNAPSHOT", "symbol": "BTCRUSDPERP", "updatedAt": 1000, "bids": [{"px": "100", "qty": "1"}], "asks": []}}

        async def reconnect():
            queue.put_nowait(snapshot)

        source._ws_assistant = SimpleNamespace(disconnect=AsyncMock(side_effect=reconnect))
        queue.put_nowait({"type": "channel_data", "data": {"symbol": "BTCRUSDPERP", "type": "UPDATE"}})
        listener = asyncio.create_task(source.listen_for_order_book_snapshots(None, output))
        try:
            message = await asyncio.wait_for(output.get(), 1)
            self.assertEqual(message.bids[0].price, 100)
            source._ws_assistant.disconnect.assert_awaited_once()
            self.assertFalse(listener.done())
        finally:
            listener.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await listener

    async def test_market_close_fee_tier_and_leverage(self):
        with patch.object(type(self.connector), "get_price", return_value=Decimal("100")):
            await self.connector._place_order("HBOT-close", "BTC-RUSD", Decimal("1"), TradeType.SELL, OrderType.MARKET, Decimal("100"), PositionAction.CLOSE)
        payload = self.connector._api_post.call_args.kwargs["data"]
        self.assertTrue(payload["reduceOnly"])
        self.assertEqual(payload["timeInForce"], "IOC")
        self.assertEqual(Decimal(payload["limitPx"]), Decimal("99.5"))
        self.connector._api_get.side_effect = [{"feeTierId": 2}, [{"tierId": 2, "makerFee": "-0.0001", "takerFee": "0.0004"}]]
        await self.connector._update_trading_fees()
        fee = self.connector._get_fee("BTC", "RUSD", OrderType.LIMIT, TradeType.BUY, Decimal("1"))
        self.assertEqual(fee.percent, Decimal("0.0004"))
        self.assertTrue((await self.connector._set_trading_pair_leverage("BTC-RUSD", 50))[0])
        self.assertFalse((await self.connector._set_trading_pair_leverage("BTC-RUSD", 51))[0])
        self.assertFalse((await self.connector._trading_pair_position_mode_set(PositionMode.HEDGE, "BTC-RUSD"))[0])
        self.assertEqual((await self.connector._fetch_last_fee_payment("BTC-RUSD"))[0], 0)

    async def test_public_and_wallet_stream_subscription_and_parsing(self):
        source = self.connector._create_order_book_data_source()
        ws = SimpleNamespace(connect=AsyncMock(), send=AsyncMock())
        source._api_factory.get_ws_assistant = AsyncMock(return_value=ws)
        self.assertIs(await source._connected_websocket_assistant(), ws)
        await source._subscribe_channels(ws)
        self.assertEqual(ws.send.await_count, 3)
        self.connector._api_get.side_effect = [{"bids": [{"px": "100", "qty": "1"}], "asks": [], "updatedAt": 1000}, {"oraclePrice": "99", "markPrice": "100", "fundingRate": "0.0001"}]
        self.assertEqual((await source._order_book_snapshot("BTC-RUSD")).bids[0].price, 100)
        self.assertEqual((await source.get_funding_info("BTC-RUSD")).rate, Decimal("0.000001"))
        queue = asyncio.Queue()
        trade = {"symbol": "BTCRUSDPERP", "sequenceNumber": 5, "side": "B", "price": "100", "qty": "1", "timestamp": 1000}
        await source._parse_trade_message({"type": "subscribed", "contents": {"data": [trade]}}, queue)
        self.assertEqual((await queue.get()).trade_id, "5")
        await source._parse_funding_info_message({"type": "channel_data", "data": {"symbol": "BTCRUSDPERP", "markPrice": "100", "fundingRate": "0.0001"}}, queue)
        self.assertEqual((await queue.get()).next_funding_utc_timestamp, 0)
        wallet = self.connector._create_user_stream_data_source()
        await wallet._connected_websocket_assistant()
        await wallet._subscribe_channels(ws)
        self.assertEqual(ws.send.await_count, 8)
        self.assertEqual(source._channel_originating_message({"type": "channel_data", "channel": "/v2/market/BTCRUSDPERP/depth"}), source._snapshot_messages_queue_key)
        self.assertEqual(source._channel_originating_message({"type": "pong"}), "")
