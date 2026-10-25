import asyncio
import json
import time
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock

from hummingbot.connector.derivative.reya_perpetual.reya_perpetual_auth import ReyaPerpetualAuth, wire_client_id
from hummingbot.connector.derivative.reya_perpetual.reya_perpetual_derivative import (
    ReyaOrderOutcomeUnknown,
    ReyaPerpetualDerivative,
)
from hummingbot.core.data_type.common import OrderType, PositionAction, TradeType
from hummingbot.core.data_type.in_flight_order import OrderState

VECTORS = json.loads(Path(__file__).with_name("signatures.json").read_text())
MARKET = {"symbol": "BTCRUSDPERP", "marketId": 2, "status": "TRADING", "qtyStepSize": "0.0001",
          "tickSize": "0.01", "minOrderQty": "0.0001", "initialMarginParameter": "0.02"}


class AuthTests(unittest.TestCase):
    def setUp(self):
        self.auth = ReyaPerpetualAuth(VECTORS["PRIVATE_KEY"], {"ETHRUSDPERP": 1},
                                      VECTORS["CHAIN_ID"], VECTORS["ORDERS_GATEWAY"])

    def test_sdk_golden_signatures(self):
        payload = {"accountId": 12345, "symbol": "ETHRUSDPERP", "exchangeId": 2, "orderType": "LIMIT",
                   "isBuy": True, "qty": "0.5", "limitPx": "3000", "timeInForce": "IOC",
                   "clientOrderId": "42", "nonce": "1700000000000000", "deadline": 1745000000}
        for key, changes in [("order", {}), ("order_sell", {"isBuy": False}), ("order_post_only", {"postOnly": True})]:
            self.assertEqual(self.auth.sign_payload("createOrder", {**payload, **changes})["signature"],
                             VECTORS["EXPECTED_SIGNATURES"][key])
        self.assertEqual(self.auth.sign_payload("cancelOrder", {"accountId": 12345, "symbol": "ETHRUSDPERP",
                                                                "orderId": "63552420354981888", "nonce": "1700000000000001", "deadline": 1745000060})["signature"],
                         VECTORS["EXPECTED_SIGNATURES"]["order_cancel"])
        self.assertEqual(self.auth.sign_payload("cancelAll", {"accountId": 12345,
                                                              "nonce": "1700000000000002", "deadline": 1745000120})["signature"], VECTORS["EXPECTED_SIGNATURES"]["mass_cancel"])

    def test_nonce_and_client_id(self):
        self.assertLess(self.auth.nonce(), self.auth.nonce())
        self.assertEqual(wire_client_id("HBOT-one"), wire_client_id("HBOT-one"))
        self.assertNotEqual(wire_client_id("HBOT-one"), wire_client_id("HBOT-two"))
        self.assertTrue(0 < int(wire_client_id("HBOT-one")) < 2**64)


class ConnectorTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.connector = ReyaPerpetualDerivative(reya_perpetual_private_key=VECTORS["PRIVATE_KEY"],
                                                 reya_perpetual_wallet_address="0x" + "1" * 40, reya_perpetual_account_id="42", trading_pairs=["BTC-RUSD"])
        self.connector._set_current_timestamp(1700000000)
        rules = await self.connector._format_trading_rules([MARKET])
        self.connector._trading_rules = {r.trading_pair: r for r in rules}
        self.connector._api_get = AsyncMock()
        self.connector._api_post = AsyncMock(return_value={"orderId": "123"})

    def track(self):
        self.connector.start_tracking_order("HBOT-one", "123", "BTC-RUSD", TradeType.BUY, Decimal("100"),
                                            Decimal("1"), OrderType.LIMIT, position_action=PositionAction.OPEN)
        return self.connector.in_flight_orders["HBOT-one"]

    async def test_limit_and_reduce_only_guards(self):
        await self.connector._place_order("HBOT-one", "BTC-RUSD", Decimal("1"), TradeType.BUY,
                                          OrderType.LIMIT_MAKER, Decimal("100"), PositionAction.OPEN)
        payload = self.connector._api_post.call_args.kwargs["data"]
        self.assertTrue(payload["postOnly"])
        self.assertEqual(payload["clientOrderId"], wire_client_id("HBOT-one"))
        with self.assertRaises(ValueError):
            await self.connector._place_order("HBOT-two", "BTC-RUSD", Decimal("1"), TradeType.SELL,
                                              OrderType.LIMIT, Decimal("100"), PositionAction.CLOSE)
        self.assertEqual(self.connector._api_post.await_count, 1)

    async def test_unknown_ack_keeps_order_tracked(self):
        order = self.track()
        self.connector._on_order_failure(order.client_order_id, order.trading_pair, order.amount, order.trade_type,
                                         order.order_type, order.price, ReyaOrderOutcomeUnknown())
        self.assertIn(order.client_order_id, self.connector.in_flight_orders)
        self.assertFalse(order.is_failure)

    async def test_margin_budget_and_stale_data(self):
        self.connector._api_get.side_effect = [[{"accountId": 42, "marginBalance": "1000", "freeCollateral": "900",
                                                "pricesUpdatedAt": int(time.time() * 1000)}], []]
        await self.connector._update_balances()
        self.assertEqual(self.connector.get_available_balance("RUSD"), Decimal("900"))
        self.track()
        self.assertEqual(self.connector.get_available_balance("RUSD"), Decimal("800"))
        self.connector._balance_read_at = 0
        self.assertEqual(self.connector.get_available_balance("RUSD"), Decimal("0"))

    async def test_unknown_account_and_external_orders_fail_budget_closed(self):
        self.connector._api_get.return_value = []
        with self.assertRaises(IOError):
            await self.connector._update_balances()
        self.connector._api_get.side_effect = [[{"accountId": 42, "marginBalance": "1000", "freeCollateral": "900",
                                                 "pricesUpdatedAt": None}], [{"accountId": 42, "clientOrderId": "99"}]]
        await self.connector._update_balances()
        self.assertEqual(self.connector.get_available_balance("RUSD"), Decimal("0"))

    async def test_matched_order_waits_for_settlement_and_bust_never_emits_fill(self):
        order = self.track()
        self.connector._find_order = AsyncMock(return_value={"orderId": "123", "status": "FILLED", "cumQty": "1", "lastUpdateAt": 1000})
        self.connector._all_trade_updates_for_order = AsyncMock(return_value=[])

        async def empty(*args):
            for row in []:
                yield row
        self.connector._history = empty
        update = await self.connector._request_order_status(order)
        self.assertEqual(update.new_state, OrderState.OPEN)

        async def busted(*args):
            yield {"sequenceNumber": 1, "takerAccountId": 42, "takerOrderId": "123", "qty": "1"}
        self.connector._history = busted
        update = await self.connector._request_order_status(order)
        self.assertEqual(update.new_state, OrderState.CANCELED)
        self.assertEqual(order.executed_amount_base, 0)

    async def test_settled_execution_has_exact_fee_and_id(self):
        order = self.track()
        update = self.connector._trade_update({"takerAccountId": 42, "takerOrderId": "123", "sequenceNumber": 7,
                                               "qty": "0.5", "price": "100", "timestamp": 1000, "takerFee": "0.01"}, order)
        self.assertEqual(update.fill_quote_amount, Decimal("50"))
        self.assertEqual(update.fee.flat_fees[0].amount, Decimal("0.01"))
        self.assertEqual(update.trade_id, "7")

    async def test_history_paginates_and_never_calls_absence_terminal(self):
        self.connector._api_get.side_effect = [{"data": [{"x": 1}], "meta": {"nextCursor": "next"}},
                                               {"data": [{"x": 2}], "meta": {}}]
        self.assertEqual([r async for r in self.connector._history("orderHistory", 0)], [{"x": 1}, {"x": 2}])
        self.assertEqual(self.connector._api_get.call_args.kwargs["params"]["cursor"], "next")
        self.assertFalse(self.connector._is_order_not_found_during_status_update_error(IOError("not found")))

    async def test_depth_deletes_levels_and_accepts_equal_timestamps(self):
        source = self.connector._create_order_book_data_source()
        queue = asyncio.Queue()

        def frame(kind, bids):
            return {"type": "channel_data", "data": {"type": kind, "symbol": "BTCRUSDPERP", "updatedAt": 1000,
                                                     "bids": bids, "asks": [{"px": "101", "qty": "1"}]}}
        await source._parse_order_book_snapshot_message(frame("SNAPSHOT", [{"px": "100", "qty": "1"}]), queue)
        first = await queue.get()
        await source._parse_order_book_snapshot_message(frame("UPDATE", [{"px": "100", "qty": "0"}, {"px": "99", "qty": "2"}]), queue)
        second = await queue.get()
        self.assertGreater(second.update_id, first.update_id)
        self.assertEqual([(r.price, r.amount) for r in second.bids], [(99.0, 2.0)])
        await source._parse_order_book_snapshot_message(frame("SNAPSHOT", []), queue)
        self.assertEqual((await queue.get()).bids, [])
