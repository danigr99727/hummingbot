# Reya perpetual connector

Draft native perpetual connector for the Reya v2 API. Requires the account-risk API proposed in [reya-api-specs #119](https://github.com/Reya-Labs/reya-api-specs/pull/119) and [reya-off-chain-monorepo #3376](https://github.com/Reya-Labs/reya-off-chain-monorepo/pull/3376). Those endpoints must be deployed before this connector can obtain a usable margin balance. Live signed order lifecycle validation is still pending.

Configure `reya_perpetual` with the owner wallet address, perpetual account ID, and a dedicated authorized signer key. Use an account exclusively for this bot and restore its saved tracking state after a restart. A signer must not be shared with another process because nonces must increase. The connector detects untracked open orders and withholds new order budget until they are reconciled. It never auto-resubmits an ambiguous order entry.

Release blocker: the current `connect` balance check uses public wallet reads. A successful read does not prove that the configured delegated signer is authorized. Reya validates signer permissions when processing writes, but the connector still needs a read-only authorization check to satisfy Hummingbot's invalid-credential QA requirement. Do not use a live order as a credential-validation probe.

Supported: one-way cross-margin perpetuals quoted in RUSD; GTC limit and maker-only orders; market orders represented by IOC limits with a 0.5% price bound; cancellation; public depth/trade/funding streams; wallet change notifications; polling reconciliation of settled fills, execution busts, orders, margin and positions.

Reya permits reduce-only perpetual orders only with IOC. Use `MARKET` for `PositionAction.CLOSE`; limit closes are rejected locally. Stop orders, hedge mode, isolated margin, transfers and withdrawals are not implemented. Leverage is a local budgeting preference bounded by the market's initial margin requirement, not an exchange setting.

Balances are account-level margin value in RUSD after collateral haircuts. Available budget uses the new endpoint's signed free collateral, clamped at zero, with additional reservations for tracked orders. These reservations deliberately retain the full order until terminal settlement reconciliation. Prices older than 30 seconds or failed margin reads withhold budget. This is not a withdrawal amount or a guaranteed order-size quote.

Matching is provisional: `orderChanges.cumQty` does not emit a trade fill. Only `perpExecutions` does. A terminal matched order remains tracked until settled executions and execution busts account for its cumulative matched quantity. A busted fill contributes no trade or fee. The endpoint's bounded order history means an absent order remains unresolved, rather than being declared failed and resubmitted.

Funding accrues continuously in realized balances. No synthetic discrete funding-payment events are emitted. Fee budgets use the account's fee tier without discounts; settled executions supply actual fees.

Offline tests include official SDK EIP-712 vectors, margin/account isolation, order lifecycle outcomes and a replay of order-book snapshots/diffs with equal timestamps. They require the repository's compiled Cython extensions. Example:

```sh
python -m pytest -q test/hummingbot/connector/derivative/reya_perpetual
```

Before enabling this connector in a release: deploy the two API routes, add read-only signer authorization validation, validate create/cancel/partial-fill/bust/reconnect/restart flows on devnet with a dedicated account, and complete Hummingbot's connector admission/maintenance process. No live trading has been performed by these tests.
