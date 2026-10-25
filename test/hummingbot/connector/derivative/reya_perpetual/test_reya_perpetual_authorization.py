import asyncio
import time
import unittest
from test.hummingbot.connector.derivative.reya_perpetual import test_reya_perpetual as fixtures
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from hummingbot.connector.derivative.reya_perpetual import (
    reya_perpetual_constants as CONSTANTS,
    reya_perpetual_web_utils as web,
)
from hummingbot.connector.derivative.reya_perpetual.reya_perpetual_auth import ReyaPerpetualAuth
from hummingbot.core.web_assistant.connections.data_types import RESTMethod


class SignerAuthorizationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        await fixtures.ConnectorTests.asyncSetUp(self)
        self.assistant = SimpleNamespace(execute_request=AsyncMock())
        self.connector._web_assistants_factory.get_rest_assistant = AsyncMock(return_value=self.assistant)
        self.signer = self.connector.authenticator.signer_address
        self.summary = [{"accountId": 42, "marginBalance": "1000", "freeCollateral": "900", "pricesUpdatedAt": None}]

    async def refresh_balances(self):
        self.connector._api_get.side_effect = [self.summary, []]
        await self.connector._update_balances()

    async def empty_positions(self):
        self.connector._api_get.side_effect = None
        self.connector._api_get.return_value = []
        await self.connector._update_positions()

    def delegate(self):
        self.connector._wallet = "0x" + "A" * 40
        self.assistant.execute_request.return_value = {"enabledApiWallets": ["0x" + self.signer[2:].upper()]}

    async def test_owner_needs_no_delegate_grant_but_account_must_belong_to_owner(self):
        self.connector._wallet = self.signer.upper()
        await self.empty_positions()
        await self.refresh_balances()
        self.assertEqual(self.connector.get_available_balance("RUSD"), 900)
        self.assistant.execute_request.assert_not_awaited()
        self.connector._api_get.side_effect = [[]]
        with self.assertRaisesRegex(IOError, "does not belong"):
            await self.connector._update_balances()
        self.assertEqual(self.connector.get_available_balance("RUSD"), 0)

    async def test_delegate_read_uses_fixed_legacy_url_shared_throttler_and_no_signature(self):
        self.delegate()
        await self.empty_positions()
        await self.refresh_balances()
        self.assertEqual(self.connector.get_available_balance("RUSD"), 900)
        self.assistant.execute_request.assert_awaited_once_with(
            url=CONSTANTS.LEGACY_REST_URL + "wallet/0x" + "a" * 40 + "/apiWalletStatus",
            method=RESTMethod.GET, is_auth_required=False,
            throttler_limit_id=CONSTANTS.RATE_LIMIT_ID, timeout=CONSTANTS.SIGNER_VALIDATION_TIMEOUT,
        )
        self.connector._api_post.assert_not_awaited()

    async def test_revoked_delegate_cannot_use_old_balance_or_fresh_positions(self):
        self.delegate()
        await self.empty_positions()
        await self.refresh_balances()
        self.assistant.execute_request.return_value = {"enabledApiWallets": []}
        with self.assertRaisesRegex(IOError, "not authorized"):
            await self.refresh_balances()
        await self.empty_positions()
        self.assertEqual(self.connector.get_balance("RUSD"), 1000)
        self.assertEqual(self.connector.get_available_balance("RUSD"), 0)
        self.assertEqual(self.connector._signer_authorized_at, 0)
        self.assertEqual(self.assistant.execute_request.await_count, 2)

    async def test_unauthorized_delegate_fails_before_balance_read(self):
        self.delegate()
        self.assistant.execute_request.return_value = {"enabledApiWallets": ["0x" + "2" * 40]}
        with self.assertRaisesRegex(IOError, "not authorized"):
            await self.connector._update_balances()
        self.connector._api_get.assert_not_awaited()
        self.assertEqual(self.connector.get_available_balance("RUSD"), 0)

    async def test_earlier_balance_poll_cannot_overwrite_a_later_authorization_failure(self):
        self.delegate()
        await self.empty_positions()
        balance_started, finish_balance = asyncio.Event(), asyncio.Event()

        async def response(path_url, **kwargs):
            if path_url.endswith("accountSummaries"):
                balance_started.set()
                await finish_balance.wait()
                return self.summary
            return []

        self.connector._api_get.side_effect = response
        earlier_poll = asyncio.create_task(self.connector._update_balances())
        await balance_started.wait()
        self.assistant.execute_request.return_value = {"enabledApiWallets": []}
        try:
            with self.assertRaisesRegex(IOError, "not authorized"):
                await self.connector._update_balances()
        finally:
            finish_balance.set()
            await earlier_poll
        self.assertGreater(self.connector._balance_read_at, 0)
        self.assertEqual(self.connector._signer_authorized_at, 0)
        self.assertEqual(self.connector.get_available_balance("RUSD"), 0)

    async def test_malformed_permission_responses_fail_closed(self):
        self.delegate()
        responses = [None, [], {}, {"enabledApiWallets": self.signer}, {"enabledApiWallets": None},
                     {"enabledApiWallets": [self.signer, 1]}, {"enabledApiWallets": [self.signer, "invalid"]}]
        for response in responses:
            with self.subTest(response=response):
                self.assistant.execute_request.return_value = response
                self.connector._balance_read_at = self.connector._signer_authorized_at = time.time()
                with self.assertRaisesRegex(IOError, "Invalid Reya signer authorization response"):
                    await self.connector._update_balances()
                self.assertEqual(self.connector._balance_read_at, 0)
                self.assertEqual(self.connector._signer_authorized_at, 0)
                self.assertEqual(self.connector.get_available_balance("RUSD"), 0)
        self.connector._api_get.assert_not_awaited()

    async def test_unavailable_permission_reads_fail_closed(self):
        self.delegate()
        for error in [IOError("HTTP status is 503"), ValueError("invalid JSON"), TimeoutError()]:
            with self.subTest(error=type(error).__name__):
                self.assistant.execute_request.side_effect = error
                with self.assertRaisesRegex(IOError, "Unable to verify"):
                    await self.connector._update_balances()
                self.assertEqual(self.connector._signer_authorized_at, 0)
                self.assertEqual(self.connector.get_available_balance("RUSD"), 0)
        self.connector._api_get.assert_not_awaited()

    async def test_permission_validation_total_timeout_cancels_the_request(self):
        self.delegate()
        cancelled = asyncio.Event()

        async def never_finishes(**kwargs):
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

        self.assistant.execute_request.side_effect = never_finishes
        with patch.object(CONSTANTS, "SIGNER_VALIDATION_TIMEOUT", 0.01):
            with self.assertRaisesRegex(IOError, "Unable to verify"):
                await self.connector._update_balances()
        self.assertTrue(cancelled.is_set())
        self.assertEqual(self.connector.get_available_balance("RUSD"), 0)

    async def test_missing_key_or_invalid_owner_fails_without_network(self):
        auth = self.connector._reya_auth
        self.connector._reya_auth = None
        with self.assertRaisesRegex(IOError, "private key is required"):
            await self.connector._update_balances()
        self.connector._reya_auth = auth
        for address in ["", "https://example.com", "0x" + "0" * 40, self.signer + "/../../other"]:
            with self.subTest(address=address):
                self.connector._wallet = address
                with self.assertRaisesRegex(ValueError, "owner wallet address"):
                    await self.connector._update_balances()
        self.connector._api_get.assert_not_awaited()
        self.assistant.execute_request.assert_not_awaited()
        self.assertEqual(self.connector.get_available_balance("RUSD"), 0)

    def test_signer_address_is_read_only_and_invalid_keys_are_rejected(self):
        auth = self.connector.authenticator
        self.assertTrue(web.is_wallet_address(auth.signer_address))
        with self.assertRaises(AttributeError):
            auth.signer_address = "0x" + "1" * 40
        with self.assertRaises(ValueError):
            ReyaPerpetualAuth("invalid", {})
        with self.assertRaisesRegex(ValueError, "Unsupported Reya domain"):
            web.api_wallet_status_url(self.signer, "other")
