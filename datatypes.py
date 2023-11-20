from pydantic import BaseModel, Field
from typing import Optional
import re


class Txn(BaseModel):
    tx_hash: str = Field(serialization_alias="hash", description="Transaction hash")
    from_address: str = Field(serialization_alias="fromAddress", description="From address")
    to_address: str = Field(serialization_alias="toAddress", description="To address")
    block_number: int = Field(serialization_alias="blockNumber", description="Block number")
    tx_ts: str = Field(serialization_alias="executedAt", description="Appx. transaction timestamp")
    gas_used: float = Field(serialization_alias="gasUsed", description="Gas units used")
    gas_cost_usd: float = Field(serialization_alias="gasCostInDollars", description="Gas cost in USD")


class TxnStats(BaseModel):
    count: int = Field(serialization_alias="totalTransactionsInDB")
    total_gas_used: float = Field(serialization_alias="totalGasUsed")
    total_gas_cost_usd: float = Field(serialization_alias="totalGasCostInDollars")
