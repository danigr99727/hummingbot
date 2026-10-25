import asyncio
import time
import unittest
from test.hummingbot.connector.derivative.reya_perpetual import test_reya_perpetual as fixtures
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

MODULE = "hummingbot.connector.derivative.reya_perpetual.reya_perpetual_derivative"


class RiskStreamTests(unittest.IsolatedAsyncioTestCase):
    asyncSetUp = fixtures.ConnectorTests.asyncSetUp

    def frame(self, rows, **changes):
        return {"type": "channel_data", "channel": "/v2/" + self.connector._wallet_path("positions"),
                "data": rows, **changes}

    async def emit(self, *frames):
        async def events():
            for frame in frames:
                yield frame
        self.connector._iter_user_event_queue = events
        await self.connector._user_stream_event_listener()

    async def ready(self):
        self.connector._api_get.return_value = [fixtures.position_row()]
        await self.connector._update_positions()
        self.connector._api_get.side_effect = [[{"accountId": 42, "marginBalance": "1000", "freeCollateral": "900",
                                                 "pricesUpdatedAt": None}], []]
        await self.connector._update_balances()
        self.connector._api_get.side_effect = None
        self.connector._poll_notifier.clear()

    async def test_valuation_burst_preserves_budget_until_periodic_rest_refresh(self):
        await self.ready()
        original_positions = dict(self.connector.account_positions)
        original_time = self.connector._positions_read_at
        await self.emit(*(self.frame([fixtures.position_row(unrealizedPnl=str(pnl))]) for pnl in range(100)))
        self.assertEqual(self.connector.get_available_balance("RUSD"), 900)
        self.assertEqual(self.connector._positions_read_at, original_time)
        self.assertEqual(self.connector.account_positions, original_positions)
        self.assertEqual(self.connector._account_state_version, 0)
        self.assertTrue(self.connector._poll_notifier.is_set())
        self.connector._user_stream_tracker = SimpleNamespace(last_recv_time=time.time())
        self.assertEqual(self.connector._get_poll_interval(time.time()), 5)
        with patch(MODULE + ".time.time", return_value=original_time + 31):
            self.assertEqual(self.connector.get_available_balance("RUSD"), 0)

    async def test_base_change_invalidates_once_and_live_upserts_preserve_other_positions(self):
        await self.ready()
        other = fixtures.position_row(symbol="ETHRUSDPERP")
        self.connector._position_bases[other["symbol"]] = self.connector._position_base(other)
        await self.emit(self.frame([fixtures.position_row(qty="3")]))
        self.assertEqual(self.connector.get_available_balance("RUSD"), 0)
        self.assertEqual(self.connector._account_state_version, 1)
        await self.emit(self.frame([fixtures.position_row(qty="3", unrealizedPnl="9")]))
        self.assertEqual(self.connector._account_state_version, 1)
        self.assertIn("ETHRUSDPERP", self.connector._position_bases)
        await self.emit(self.frame([fixtures.position_row(qty="0", unrealizedPnl=None)]))
        self.assertNotIn("BTCRUSDPERP", self.connector._position_bases)
        self.assertIn("ETHRUSDPERP", self.connector._position_bases)
        self.assertEqual(self.connector._account_state_version, 2)

    async def test_snapshot_replaces_observed_base_set_and_invalidates(self):
        await self.ready()
        await self.emit(self.frame(None, type="subscribed", contents={"data": [], "unavailableAccountIds": []}))
        self.assertEqual(self.connector._position_bases, {})
        self.assertEqual(self.connector.get_available_balance("RUSD"), 0)
        self.assertTrue(self.connector._poll_notifier.is_set())

    async def test_closed_base_only_rest_position_removes_previous_position_without_risk(self):
        await self.ready()
        closed = fixtures.position_row(qty="0")
        del closed["unrealizedPnl"]
        del closed["pricesUpdatedAt"]
        self.connector._api_get.return_value = [closed]
        await self.connector._update_positions()
        self.assertEqual(self.connector.account_positions, {})
        self.assertEqual(self.connector._position_bases, {})
        self.assertEqual(self.connector.get_available_balance("RUSD"), 900)
        self.connector._api_get.return_value = [fixtures.position_row(qty="NaN")]
        with self.assertRaisesRegex(IOError, "quantity is invalid"):
            await self.connector._update_positions()
        self.assertEqual(self.connector.get_available_balance("RUSD"), 0)

    async def test_foreign_account_updates_and_unavailability_do_not_invalidate(self):
        await self.ready()
        await self.emit(self.frame([{"accountId": 99, "symbol": "UNKNOWN"}], unavailableAccountIds=[99]))
        self.assertEqual(self.connector.get_available_balance("RUSD"), 900)
        self.assertFalse(self.connector._poll_notifier.is_set())

    async def test_missing_stale_unavailable_and_malformed_risk_fail_closed(self):
        frames = [self.frame([fixtures.position_row(unrealizedPnl=None)]),
                  self.frame([fixtures.position_row(unrealizedPnl="NaN")]),
                  self.frame([fixtures.position_row(pricesUpdatedAt=int((time.time() - 31) * 1000))]),
                  self.frame([], unavailableAccountIds=[42]),
                  self.frame([], unavailableAccountIds="42"), self.frame({}),
                  self.frame([{"accountId": 42}]), {"type": "error"}, {"type": "reya_stream_interrupted"}]
        for frame in frames:
            with self.subTest(frame=frame):
                await self.ready()
                await self.emit(frame)
                self.assertEqual(self.connector.get_available_balance("RUSD"), 0)
                self.assertEqual(self.connector._positions_read_at, 0)
                self.assertEqual(self.connector._balance_read_at, 0)
                self.assertTrue(self.connector._poll_notifier.is_set())

    async def test_order_execution_and_cash_notifications_invalidate_immediately(self):
        for resource in ["orderChanges", "perpExecutions", "executionBusts", "accountBalances"]:
            with self.subTest(resource=resource):
                await self.ready()
                await self.emit({"type": "channel_data", "channel": "/v2/" + self.connector._wallet_path(resource),
                                 "data": [{"accountId": 42}]})
                self.assertEqual(self.connector.get_available_balance("RUSD"), 0)

    async def test_invalidation_during_rest_reads_prevents_old_responses_restoring_freshness(self):
        await self.ready()
        positions_started, balances_started, release = asyncio.Event(), asyncio.Event(), asyncio.Event()

        async def response(path_url, **kwargs):
            if path_url.endswith("positions"):
                positions_started.set()
                await release.wait()
                return [fixtures.position_row(unrealizedPnl="99")]
            if path_url.endswith("accountSummaries"):
                balances_started.set()
                await release.wait()
                return [{"accountId": 42, "marginBalance": "2000", "freeCollateral": "1900"}]
            return []

        self.connector._api_get.side_effect = response
        polls = [asyncio.create_task(self.connector._update_positions()),
                 asyncio.create_task(self.connector._update_balances())]
        await positions_started.wait()
        await balances_started.wait()
        await self.emit(self.frame([], unavailableAccountIds=[42]))
        release.set()
        results = await asyncio.gather(*polls, return_exceptions=True)
        self.assertTrue(all(isinstance(result, IOError) for result in results))
        self.assertEqual(self.connector.get_balance("RUSD"), 1000)
        self.assertEqual(next(iter(self.connector.account_positions.values())).unrealized_pnl, 2)
        self.assertEqual(self.connector._positions_read_at, 0)
        self.assertEqual(self.connector._balance_read_at, 0)

    async def test_first_poll_is_immediate_and_subsequent_cycles_are_bounded(self):
        self.connector._update_positions = AsyncMock()
        self.connector._update_balances = AsyncMock()
        self.connector._update_order_status = AsyncMock()
        clock = [100.0]

        async def advance(delay):
            clock[0] += delay

        self.connector._sleep = AsyncMock(side_effect=advance)
        with patch(MODULE + ".time.monotonic", side_effect=lambda: clock[0]):
            await self.connector._status_polling_loop_fetch_updates()
            self.connector._sleep.assert_not_awaited()
            await self.connector._status_polling_loop_fetch_updates()
            self.connector._sleep.assert_awaited_once_with(5.0)
        self.assertEqual(self.connector._update_positions.await_count, 2)

    async def test_failed_cycle_awaits_siblings_before_retry_and_midpoll_changes_stay_pending(self):
        started, release = asyncio.Event(), asyncio.Event()

        async def slow_balance():
            started.set()
            await release.wait()

        self.connector._update_positions = AsyncMock(side_effect=IOError("missing risk"))
        self.connector._update_balances = AsyncMock(side_effect=slow_balance)
        self.connector._update_order_status = AsyncMock()
        poll = asyncio.create_task(self.connector._status_polling_loop_fetch_updates())
        await started.wait()
        self.assertFalse(poll.done())
        release.set()
        with self.assertRaisesRegex(IOError, "missing risk"):
            await poll
        self.connector._update_positions.side_effect = self.connector._invalidate_account_state
        self.connector._sleep = AsyncMock()
        with self.assertRaisesRegex(IOError, "changed during reconciliation"):
            await self.connector._status_polling_loop_fetch_updates()
        self.assertTrue(self.connector._poll_notifier.is_set())

    async def test_stream_error_and_disconnect_reach_budget_guard_before_reconnect(self):
        source = self.connector._create_user_stream_data_source()
        for failure, expected in [(None, ConnectionError),
                                  (SimpleNamespace(data={"type": "error", "message": "valuation unavailable"}), IOError)]:
            with self.subTest(failure=failure):
                await self.ready()
                frame = self.frame([fixtures.position_row()])
                ws = SimpleNamespace(receive=AsyncMock(side_effect=[SimpleNamespace(data=frame), failure]))
                queue = asyncio.Queue()
                with self.assertRaises(expected):
                    await source._process_websocket_messages(ws, queue)
                self.assertEqual(queue.qsize(), 2)
                await self.emit(queue.get_nowait(), queue.get_nowait())
                self.assertEqual(self.connector.get_available_balance("RUSD"), 0)
