import asyncio
import math
import time
from decimal import Decimal

from bidict import bidict

from hummingbot.connector.derivative.position import Position
from hummingbot.connector.derivative.reya_perpetual import (
    reya_perpetual_constants as CONSTANTS,
    reya_perpetual_web_utils as web_utils,
)
from hummingbot.connector.derivative.reya_perpetual.reya_perpetual_api_order_book_data_source import (
    ReyaPerpetualAPIOrderBookDataSource,
)
from hummingbot.connector.derivative.reya_perpetual.reya_perpetual_api_user_stream_data_source import (
    ReyaPerpetualAPIUserStreamDataSource,
)
from hummingbot.connector.derivative.reya_perpetual.reya_perpetual_auth import ReyaPerpetualAuth, wire_client_id
from hummingbot.connector.perpetual_derivative_py_base import PerpetualDerivativePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.core.data_type.common import OrderType, PositionAction, PositionMode, PositionSide, TradeType
from hummingbot.core.data_type.in_flight_order import OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.trade_fee import TokenAmount, TradeFeeBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod


class ReyaOrderOutcomeUnknown(IOError):
    pass


class ReyaPerpetualDerivative(PerpetualDerivativePyBase):
    web_utils = web_utils
    LONG_POLL_INTERVAL = 5.0

    def __init__(self, balance_asset_limit=None, rate_limits_share_pct=Decimal("100"),
                 reya_perpetual_private_key=None, reya_perpetual_wallet_address=None,
                 reya_perpetual_account_id=None, trading_pairs=None, trading_required=True,
                 domain=CONSTANTS.DEFAULT_DOMAIN):
        self._wallet = reya_perpetual_wallet_address or ""
        self._account_id = int(reya_perpetual_account_id or 0)
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs or []
        self._domain = domain
        self._market_ids = {}
        self._market_definitions = {}
        self._signing_lock = asyncio.Lock()
        self._reconciliation_lock = asyncio.Lock()
        self._last_reconciliation_at = None
        self._account_state_version = 0
        self._position_bases = {}
        self._reya_auth = ReyaPerpetualAuth(reya_perpetual_private_key, self._market_ids) if reya_perpetual_private_key else None
        self._balance_read_at = 0
        self._positions_read_at = 0
        self._signer_authorized_at = 0
        self._external_orders = False
        self._maker_fee = Decimal("0")
        self._taker_fee = Decimal("0.0003")
        super().__init__(balance_asset_limit, rate_limits_share_pct)

    @property
    def name(self):
        return self._domain

    @property
    def domain(self):
        return self._domain

    @property
    def authenticator(self):
        return self._reya_auth

    @property
    def rate_limits_rules(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def client_order_id_max_length(self):
        return 32

    @property
    def client_order_id_prefix(self):
        return "HBOT"

    @property
    def trading_rules_request_path(self):
        return CONSTANTS.MARKETS

    @property
    def trading_pairs_request_path(self):
        return CONSTANTS.MARKETS

    @property
    def check_network_request_path(self):
        return CONSTANTS.MARKETS

    @property
    def trading_pairs(self):
        return self._trading_pairs

    @property
    def is_trading_required(self):
        return self._trading_required

    @property
    def is_cancel_request_in_exchange_synchronous(self):
        # Cancellation can still have matched fills awaiting settlement.
        return False

    @property
    def funding_fee_poll_interval(self):
        return 120

    def supported_order_types(self):
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER, OrderType.MARKET]

    def supported_position_modes(self):
        return [PositionMode.ONEWAY]

    def get_buy_collateral_token(self, trading_pair):
        return "RUSD"

    def get_sell_collateral_token(self, trading_pair):
        return "RUSD"

    def _is_request_exception_related_to_time_synchronizer(self, request_exception):
        return False

    def _is_order_not_found_during_status_update_error(self, status_update_exception):
        # History is bounded and can lag acknowledgement; absence is not a terminal state.
        return False

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception):
        return False

    def _create_web_assistants_factory(self):
        return web_utils.build_api_factory(self._throttler, self._auth)

    def _create_order_book_data_source(self):
        return ReyaPerpetualAPIOrderBookDataSource(self._trading_pairs, self, self._web_assistants_factory)

    def _create_user_stream_data_source(self):
        return ReyaPerpetualAPIUserStreamDataSource(self._wallet, self._web_assistants_factory)

    async def _api_request(self, path_url, method=RESTMethod.GET, **kwargs):
        kwargs.setdefault("limit_id", CONSTANTS.RATE_LIMIT_ID)
        if method != RESTMethod.POST:
            return await super()._api_request(path_url=path_url, method=method, **kwargs)
        # Serialise signing and dispatch: the server requires increasing signer nonces.
        async with self._signing_lock:
            try:
                return await super()._api_request(path_url=path_url, method=method, **kwargs)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                # HTTP 400 is a definitive venue rejection, except the explicit unknown-outcome code.
                message = str(error)
                if "ORDER_OUTCOME_UNKNOWN_ERROR" in message or "HTTP status is 400" not in message:
                    raise ReyaOrderOutcomeUnknown("Reconcile this order before resubmitting") from error
                raise

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info):
        mapping = bidict()
        for market in exchange_info:
            if web_utils.is_exchange_information_valid(market):
                symbol = market["symbol"]
                pair = symbol.removesuffix("RUSDPERP") + "-RUSD"
                mapping[symbol] = pair
                self._market_ids[symbol] = int(market["marketId"])
                self._market_definitions[pair] = market
        self._set_trading_pair_symbol_map(mapping)

    async def _format_trading_rules(self, exchange_info_dict):
        self._initialize_trading_pair_symbols_from_exchange_info(exchange_info_dict)
        return [TradingRule(trading_pair=pair, min_order_size=Decimal(row["minOrderQty"]),
                            min_price_increment=Decimal(row["tickSize"]),
                            min_base_amount_increment=Decimal(row["qtyStepSize"]),
                            buy_order_collateral_token="RUSD", sell_order_collateral_token="RUSD")
                for pair, row in self._market_definitions.items()]

    async def _get_last_traded_price(self, trading_pair):
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair)
        row = await self._api_get(path_url=f"perpMarket/{symbol}/summary")
        return float(row["markPrice"])

    def _get_fee(self, base_currency, quote_currency, order_type, order_side, amount,
                 price=Decimal("NaN"), position_action=PositionAction.OPEN, is_maker=None):
        maker = order_type == OrderType.LIMIT_MAKER if is_maker is None else is_maker
        return TradeFeeBase.new_perpetual_fee(self.trade_fee_schema(), position_action,
                                              percent=self._maker_fee if maker else self._taker_fee,
                                              percent_token="RUSD")

    async def _update_trading_fees(self):
        configuration = await self._api_get(path_url=self._wallet_path("configuration"))
        tiers = await self._api_get(path_url="feeTiers")
        tier = next(row for row in tiers if int(row["tierId"]) == int(configuration["feeTierId"]))
        # Ignore discounts in pre-trade budgets; executions carry the actual charged fee.
        self._maker_fee = max(Decimal("0"), Decimal(tier["makerFee"]))
        self._taker_fee = max(Decimal("0"), Decimal(tier["takerFee"]))

    async def _place_order(self, order_id, trading_pair, amount, trade_type, order_type,
                           price, position_action=PositionAction.NIL, **kwargs):
        if position_action == PositionAction.CLOSE and order_type != OrderType.MARKET:
            raise ValueError("Reya reduce-only is supported only for IOC orders; use MARKET for CLOSE")
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair)
        is_buy = trade_type == TradeType.BUY
        if order_type == OrderType.MARKET:
            best = self.get_price(trading_pair, is_buy)
            price = self.quantize_order_price(trading_pair, best * (Decimal("1.005") if is_buy else Decimal("0.995")))
        if not price.is_finite() or price <= 0 or amount <= 0:
            raise ValueError("A positive amount and a valid order book price are required")
        payload = {"accountId": self._account_id, "exchangeId": CONSTANTS.EXCHANGE_ID,
                   "symbol": symbol, "isBuy": is_buy, "orderType": "LIMIT", "qty": str(amount),
                   "limitPx": str(price), "clientOrderId": wire_client_id(order_id),
                   "timeInForce": "IOC" if order_type == OrderType.MARKET else "GTC"}
        if order_type == OrderType.MARKET:
            payload["reduceOnly"] = position_action == PositionAction.CLOSE
        else:
            payload["postOnly"] = order_type == OrderType.LIMIT_MAKER
        result = await self._api_post(path_url="createOrder", data=payload, is_auth_required=True)
        if not result.get("orderId"):
            raise ReyaOrderOutcomeUnknown("Acknowledgement has no order id; reconcile by client id")
        return str(result["orderId"]), self.current_timestamp

    def _on_order_failure(self, order_id, trading_pair, amount, trade_type, order_type, price, exception, **kwargs):
        if isinstance(exception, ReyaOrderOutcomeUnknown):
            self.logger().warning(f"Order {order_id} has an unknown outcome; keeping it tracked for reconciliation.")
            return
        super()._on_order_failure(order_id, trading_pair, amount, trade_type, order_type, price, exception, **kwargs)

    async def _place_cancel(self, order_id, tracked_order):
        symbol = await self.exchange_symbol_associated_to_pair(tracked_order.trading_pair)
        payload = {"accountId": self._account_id, "symbol": symbol,
                   "clientOrderId": wire_client_id(tracked_order.client_order_id)}
        if tracked_order.exchange_order_id:
            payload["orderId"] = str(tracked_order.exchange_order_id)
        await self._api_post(path_url="cancelOrder", data=payload, is_auth_required=True)
        return True

    def _wallet_path(self, resource):
        return f"wallet/{self._wallet.lower()}/{resource}"

    async def _history(self, resource, creation_timestamp):
        cursor = None
        seen = set()
        for _ in range(20):
            params = {"limit": 100, "startTime": int(creation_timestamp * 1000)}
            if cursor:
                params["cursor"] = cursor
            response = await self._api_get(path_url=self._wallet_path(resource), params=params)
            rows = response["data"]
            for row in rows:
                yield row
            cursor = response.get("meta", {}).get("nextCursor")
            if not cursor:
                return
            if cursor in seen:
                raise IOError("Reya history cursor did not advance")
            seen.add(cursor)
        raise IOError("Reya history scan limit reached; order remains unresolved")

    async def _find_order(self, order):
        symbol = await self.exchange_symbol_associated_to_pair(order.trading_pair)

        def matches(row):
            return (int(row["accountId"]) == self._account_id and row["symbol"] == symbol
                    and (str(row.get("orderId")) == order.exchange_order_id
                         or str(row.get("clientOrderId")) == wire_client_id(order.client_order_id)))
        for row in await self._api_get(path_url=self._wallet_path("openOrders")):
            if matches(row):
                return row
        async for row in self._history("orderHistory", order.creation_timestamp):
            if matches(row):
                return row
        raise IOError("Order not visible in retained Reya history; keep tracking and reconcile")

    def _execution_role(self, row, order):
        for role in ("taker", "maker"):
            if (int(row.get(role + "AccountId", -1)) == self._account_id
                    and str(row.get(role + "OrderId")) == order.exchange_order_id):
                return role
        return None

    def _trade_update(self, row, order):
        role = self._execution_role(row, order)
        if role is None:
            return None
        fee = TradeFeeBase.new_perpetual_fee(self.trade_fee_schema(), order.position,
                                             flat_fees=[TokenAmount("RUSD", Decimal(row.get(role + "Fee", "0")))])
        qty, price = Decimal(row["qty"]), Decimal(row["price"])
        return TradeUpdate(str(row["sequenceNumber"]), order.client_order_id, order.exchange_order_id,
                           order.trading_pair, row["timestamp"] / 1000, price, qty, qty * price, fee,
                           is_taker=role == "taker")

    async def _all_trade_updates_for_order(self, order):
        if not order.exchange_order_id:
            row = await self._find_order(order)
            order.update_exchange_order_id(str(row["orderId"]))
        updates = []
        async for row in self._history("perpExecutions", order.creation_timestamp):
            update = self._trade_update(row, order)
            if update:
                updates.append(update)
        return updates

    async def _request_order_status(self, tracked_order):
        row = await self._find_order(tracked_order)
        tracked_order.update_exchange_order_id(str(row["orderId"]))
        state = OrderState.OPEN
        cum_qty = Decimal(row.get("cumQty", "0"))
        if row["status"] in ("FILLED", "CANCELLED"):
            # Engine matching is provisional. Only settled executions emit fills.
            settled = Decimal("0")
            for update in await self._all_trade_updates_for_order(tracked_order):
                settled += update.fill_base_amount
                self._order_tracker.process_trade_update(update)
            busted = Decimal("0")
            seen = set()
            async for bust in self._history("executionBusts", tracked_order.creation_timestamp):
                key = str(bust["sequenceNumber"])
                if self._execution_role(bust, tracked_order) and key not in seen:
                    seen.add(key)
                    busted += Decimal(bust["qty"])
            if settled + busted >= cum_qty:
                state = OrderState.FILLED if row["status"] == "FILLED" and busted == 0 else OrderState.CANCELED
                self._balance_read_at = 0
        elif tracked_order.executed_amount_base > 0:
            state = OrderState.PARTIALLY_FILLED
        return OrderUpdate(tracked_order.trading_pair, row["lastUpdateAt"] / 1000, state,
                           tracked_order.client_order_id, str(row["orderId"]))

    async def _validate_signer_authorization(self):
        self._signer_authorized_at = 0
        if self._reya_auth is None:
            raise IOError("A Reya signer private key is required")
        url = web_utils.api_wallet_status_url(self._wallet, self.domain)
        signer = self._reya_auth.signer_address.lower()
        # OrdersGateway always permits the owner; no delegate grant is needed.
        # _update_balances separately verifies that this wallet owns the account.
        if signer == self._wallet.lower():
            self._signer_authorized_at = time.time()
            return
        try:
            # Bound throttling, transport and response decoding together. The
            # request uses the normal REST assistant and shared request budget.
            async with asyncio.timeout(CONSTANTS.SIGNER_VALIDATION_TIMEOUT):
                assistant = await self._web_assistants_factory.get_rest_assistant()
                response = await assistant.execute_request(
                    url=url, method=RESTMethod.GET, is_auth_required=False,
                    throttler_limit_id=CONSTANTS.RATE_LIMIT_ID,
                    timeout=CONSTANTS.SIGNER_VALIDATION_TIMEOUT,
                )
        except (TimeoutError, IOError, ValueError) as error:
            raise IOError("Unable to verify Reya signer authorization") from error
        delegates = response.get("enabledApiWallets") if isinstance(response, dict) else None
        if not isinstance(delegates, list) or not all(web_utils.is_wallet_address(address) for address in delegates):
            raise IOError("Invalid Reya signer authorization response")
        if signer not in {address.lower() for address in delegates}:
            raise IOError("Configured Reya signer is not authorized for this owner wallet")
        self._signer_authorized_at = time.time()

    async def _update_balances(self):
        self._balance_read_at = 0
        version = self._account_state_version
        await self._validate_signer_authorization()
        rows = await self._api_get(path_url=self._wallet_path("accountSummaries"), params={"accountId": self._account_id})
        if len(rows) != 1 or int(rows[0]["accountId"]) != self._account_id:
            raise IOError("Configured perpetual account does not belong to this wallet")
        row = rows[0]
        timestamp = row.get("pricesUpdatedAt")
        if timestamp is not None and time.time() * 1000 - timestamp > 30000:
            raise IOError("Reya account valuation prices are stale")
        balance = Decimal(row["marginBalance"])
        available = max(Decimal("0"), Decimal(row["freeCollateral"]))
        orders = await self._api_get(path_url=self._wallet_path("openOrders"))
        local_ids = {wire_client_id(order.client_order_id) for order in self.in_flight_orders.values()}
        external_orders = any(int(order["accountId"]) == self._account_id
                              and str(order.get("clientOrderId")) not in local_ids for order in orders)
        if version != self._account_state_version:
            raise IOError("Reya account changed during balance reconciliation")
        self._account_balances["RUSD"] = balance
        self._account_available_balances["RUSD"] = available
        self._external_orders = external_orders
        self._balance_read_at = time.time()

    def get_available_balance(self, currency):
        if (currency != "RUSD" or self._external_orders or time.time() - self._balance_read_at > 30
                or time.time() - self._positions_read_at > 30 or time.time() - self._signer_authorized_at > 30):
            return Decimal("0")
        available = super().get_available_balance(currency)
        # Local policy: reserve tracked orders until terminal reconciliation; Reya has no resting-order holds.
        for order in self.in_flight_orders.values():
            price = order.price
            if not price.is_finite() or price <= 0:
                return Decimal("0")
            available -= order.amount * price / Decimal(self.get_leverage(order.trading_pair))
        return max(Decimal("0"), available)

    async def _update_positions(self):
        # Keep this guard independent of balance polling: a successful balance read
        # must not make missing or stale position valuations usable again.
        self._positions_read_at = 0
        version = self._account_state_version
        rows = await self._api_get(path_url=self._wallet_path("positions"))
        fresh_at = time.time()
        positions = {}
        bases = {}
        for row in rows:
            if int(row["accountId"]) != self._account_id:
                continue
            quantity = Decimal(row["qty"])
            if not quantity.is_finite():
                raise IOError("Reya position quantity is invalid")
            # A closed position can retain its base row without any valuation.
            if quantity == 0:
                continue
            unrealized_pnl, timestamp = self._position_risk(row)
            bases[row["symbol"]] = self._position_base(row)
            fresh_at = min(fresh_at, timestamp / 1000)
            pair = await self.trading_pair_associated_to_exchange_symbol(row["symbol"])
            side = PositionSide.LONG if row["side"] == "B" else PositionSide.SHORT
            key = self._perpetual_trading.position_key(pair, side)
            positions[key] = Position(pair, side, unrealized_pnl, Decimal(row["avgEntryPrice"]),
                                      quantity * (1 if side == PositionSide.LONG else -1),
                                      Decimal(self.get_leverage(pair)))
        # Validate the complete account snapshot before changing any tracked positions.
        if version != self._account_state_version:
            raise IOError("Reya account changed during position reconciliation")
        for key, position in positions.items():
            self._perpetual_trading.set_position(key, position)
        for key in list(self.account_positions):
            if key not in positions:
                self._perpetual_trading.remove_position(key)
        self._positions_read_at = fresh_at
        self._position_bases = bases

    @staticmethod
    def _position_risk(row):
        timestamp = row.get("pricesUpdatedAt")
        if (not isinstance(timestamp, (int, float)) or not math.isfinite(timestamp)
                or timestamp <= 0 or time.time() * 1000 - timestamp > 30000):
            raise IOError("Reya position valuation prices are missing or stale")
        if row.get("unrealizedPnl") is None:
            raise IOError("Reya position unrealized PnL is unavailable")
        unrealized_pnl = Decimal(row["unrealizedPnl"])
        if not unrealized_pnl.is_finite():
            raise IOError("Reya position unrealized PnL is invalid")
        return unrealized_pnl, timestamp

    @staticmethod
    def _position_base(row):
        return tuple(row[field] for field in ("exchangeId", "symbol", "accountId", "qty", "side", "avgEntryPrice",
                                              "avgEntryFundingValue", "lastTradeSequenceNumber"))

    def _invalidate_account_state(self):
        self._account_state_version += 1
        self._balance_read_at = self._positions_read_at = 0
        self._poll_notifier.set()

    async def _status_polling_loop_fetch_updates(self):
        # Price-driven wallet frames can arrive much faster than REST's request budget.
        # Serialize complete cycles, including failures, and let the first one run immediately.
        async with self._reconciliation_lock:
            if self._last_reconciliation_at is not None:
                delay = self.LONG_POLL_INTERVAL - (time.monotonic() - self._last_reconciliation_at)
                if delay > 0:
                    await self._sleep(delay)
            self._last_reconciliation_at = time.monotonic()
            version = self._account_state_version
            results = await asyncio.gather(self._update_positions(), self._update_balances(),
                                           self._update_order_status(), return_exceptions=True)
            for result in results:
                if isinstance(result, BaseException):
                    raise result
            if version != self._account_state_version:
                # The base loop clears its notifier after success. Keep this change pending.
                self._invalidate_account_state()
                raise IOError("Reya account changed during reconciliation")

    def _position_event_invalidates_budget(self, event):
        snapshot = event["type"] == "subscribed"
        payload = event["contents"] if snapshot else event
        rows = payload["data"]
        unavailable = payload.get("unavailableAccountIds", [])
        if not isinstance(rows, list) or not isinstance(unavailable, list):
            raise ValueError("Invalid Reya position notification")
        invalidates = snapshot or self._account_id in [int(account_id) for account_id in unavailable]
        bases = {} if snapshot else self._position_bases.copy()
        for row in rows:
            if int(row["accountId"]) != self._account_id:
                continue
            self._poll_notifier.set()
            symbol, base = row["symbol"], self._position_base(row)
            invalidates |= bases.get(symbol) != base
            if Decimal(row["qty"]) == 0:
                bases.pop(symbol, None)
                invalidates = True
            else:
                bases[symbol] = base
            try:
                self._position_risk(row)
            except (IOError, ValueError, TypeError, ArithmeticError):
                invalidates = True
        # Live frames are upserts; absent rows retain their base state. Remember base
        # changes now so subsequent valuation frames do not invalidate an in-flight poll.
        self._position_bases = bases
        return invalidates

    async def _trading_pair_position_mode_set(self, mode, trading_pair):
        return mode == PositionMode.ONEWAY, "Reya supports one-way cross margin only"

    async def _set_trading_pair_leverage(self, trading_pair, leverage):
        market = self._market_definitions[trading_pair]
        maximum = Decimal("1") / Decimal(market["initialMarginParameter"])
        return 1 <= leverage <= maximum, "Leverage is a local budgeting preference, bounded by market initial margin"

    async def _fetch_last_fee_payment(self, trading_pair):
        # Reya funding accrues continuously in realized balances, without discrete payment events.
        return 0, Decimal("0"), Decimal("0")

    async def _user_stream_event_listener(self):
        async for event in self._iter_user_event_queue():
            try:
                if event.get("type") in ("error", "reya_stream_interrupted"):
                    self._invalidate_account_state()
                elif event.get("type") in ("subscribed", "channel_data"):
                    channel = event.get("channel", "").lower()
                    if channel == ("/v2/" + self._wallet_path("positions")).lower():
                        if self._position_event_invalidates_budget(event):
                            self._invalidate_account_state()
                    else:
                        # Execution, order and cash events require fresh settled state.
                        # Neither orderChanges.cumQty nor executionBusts creates a fill.
                        self._invalidate_account_state()
            except (KeyError, ValueError, TypeError, AttributeError, ArithmeticError):
                self._invalidate_account_state()
