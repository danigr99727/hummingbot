# Reya perpetual connector

Draft native perpetual connector for the Reya v2 API. Requires the new `/wallet/{address}/accountSummaries` resource and valuation fields on the existing `/wallet/{address}/positions` resource proposed in [reya-api-specs #119](https://github.com/Reya-Labs/reya-api-specs/pull/119) and [reya-off-chain-monorepo #3376](https://github.com/Reya-Labs/reya-off-chain-monorepo/pull/3376). These API changes must be deployed before this connector can obtain a usable trading budget. Live signed order lifecycle validation is still pending.

Configure `reya_perpetual` with the owner wallet address, perpetual account ID, and a dedicated authorized signer key. Use an account exclusively for this bot and restore its saved tracking state after a restart. A signer must not be shared with another process because nonces must increase. As a conservative connector policy, untracked open orders withhold new order budget until they are reconciled. The connector never auto-resubmits an ambiguous order entry.

The `connect` balance check verifies account ownership and the signer derived from the configured private key. The owner can sign directly; a delegated signer must appear in the existing read-only `/api/trading/wallet/{address}/apiWalletStatus` response, compared case-insensitively. Delegated authorization is refreshed on every balance poll using the normal REST assistant and throttler, with a 10-second timeout. Missing keys, unauthorized or revoked delegates, unavailable reads and malformed permission responses fail the check and withhold budget. This performs no live-order probe or direct RPC call. Devnet QA with valid owner/delegate credentials and invalid credentials remains pending.

Supported: one-way cross-margin perpetuals quoted in RUSD; GTC limit and maker-only orders; market orders represented by IOC limits with a 0.5% price bound; cancellation; public depth/trade/funding streams; wallet change notifications; polling reconciliation of settled fills, execution busts, orders, margin and positions.

Reya permits reduce-only perpetual orders only with IOC. Use `MARKET` for `PositionAction.CLOSE`; limit closes are rejected locally. Stop orders, hedge mode, isolated margin, transfers and withdrawals are not implemented. Leverage is a local budgeting preference bounded by the market's initial margin requirement, not an exchange setting.

Balances are account-level margin value in RUSD after collateral haircuts. The connector clamps signed `freeCollateral` at zero and applies conservative local reservations for tracked orders until terminal settlement reconciliation. Untracked open orders withhold new order budget until reconciled.

Positions are filtered locally to the configured account. Missing unrealized PnL, missing or stale position price timestamps, failed position reads, or failed margin reads withhold budget; successful balance polling cannot override a failed position check. All scoped positions are validated before the snapshot is applied. Valuation prices and successful reads must remain within 30 seconds. The budget is not a withdrawal amount or a guaranteed order-size quote.

Price-only position stream updates request reconciliation without clearing a still-fresh budget. Position base changes, unavailable valuation, order/execution/cash events and stream interruption invalidate it immediately; REST responses started before an invalidating event cannot restore freshness. Position updates are upserts and zero quantity removes a position; subscription snapshots replace the observed set. Reconciliation runs at most once every five seconds, with periodic polling even on a healthy stream, and waits for every outstanding read before retrying a failed cycle. Stream valuation never produces a settled fill.

The account valuation reflects settled state. `orderChanges.cumQty` does not emit a trade fill; only `perpExecutions` does. A terminal matched order remains tracked until settled executions and execution busts account for its cumulative matched quantity. A busted fill contributes no trade or fee. The endpoint's bounded order history means an absent order remains unresolved, rather than being declared failed and resubmitted.

Funding accrues continuously in realized balances. No synthetic discrete funding-payment events are emitted. Fee budgets use the account's fee tier without discounts; settled executions supply actual fees.

Offline tests include official SDK EIP-712 vectors, margin/account isolation, order lifecycle outcomes and a replay of order-book snapshots/diffs with equal timestamps. They require the repository's compiled Cython extensions. Example:

```sh
python -m pytest -q test/hummingbot/connector/derivative/reya_perpetual
```

Before enabling this connector in a release: deploy account summaries and the enriched existing positions resource, validate owner/delegate and invalid-credential connection checks plus create/cancel/partial-fill/bust/reconnect/restart flows on devnet with a dedicated account, and complete Hummingbot's connector admission/maintenance process. No live trading has been performed by these tests.
