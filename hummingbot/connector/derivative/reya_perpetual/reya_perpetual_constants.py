from hummingbot.core.api_throttler.data_types import RateLimit

DEFAULT_DOMAIN = "reya_perpetual"
REST_URL = "https://api.reya.xyz/v2/"
LEGACY_REST_URL = "https://api.reya.xyz/api/trading/"
SIGNER_VALIDATION_TIMEOUT = 10
WS_URL = "wss://ws.reya.xyz"
ORDERS_GATEWAY = "0xfc8c96be87da63cecddbf54abfa7b13ee8044739"
CHAIN_ID = 1729
EXCHANGE_ID = 2
RATE_LIMIT_ID = "REYA_REST"
# Conservative shared budget; server-side per-account limits still apply.
RATE_LIMITS = [RateLimit(limit_id=RATE_LIMIT_ID, limit=5, time_interval=1)]
MARKETS = "perpMarketDefinitions"
HEARTBEAT = 15
