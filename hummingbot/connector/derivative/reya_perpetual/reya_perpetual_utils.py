from decimal import Decimal

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = False
EXAMPLE_PAIR = "BTC-RUSD"
DEFAULT_FEES = TradeFeeSchema(maker_percent_fee_decimal=Decimal("0"), taker_percent_fee_decimal=Decimal("0.0003"))


class ReyaPerpetualConfigMap(BaseConnectorConfigMap):
    connector: str = "reya_perpetual"
    reya_perpetual_private_key: SecretStr = Field(
        default=...,
        json_schema_extra={"prompt": "Enter your dedicated Reya signer private key", "is_secure": True,
                           "is_connect_key": True, "prompt_on_new": True},
    )
    reya_perpetual_wallet_address: SecretStr = Field(
        default=...,
        json_schema_extra={"prompt": "Enter the owner wallet address (not the delegated signer)", "is_secure": False,
                           "is_connect_key": True, "prompt_on_new": True},
    )
    reya_perpetual_account_id: SecretStr = Field(
        default=...,
        json_schema_extra={"prompt": "Enter the Reya perpetual account ID", "is_secure": False,
                           "is_connect_key": True, "prompt_on_new": True},
    )
    model_config = ConfigDict(title="reya_perpetual")


KEYS = ReyaPerpetualConfigMap.model_construct()
