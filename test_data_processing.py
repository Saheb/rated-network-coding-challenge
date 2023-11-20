import pytest
from datetime import datetime
from data_processing import calculate_gas_cost_gwei, calculate_tx_ts, datetime_str, PriceFetchingError, extract

def test_datetime_str():
    dt = datetime(2021, 9, 1, 12, 0, 0, 0)
    assert datetime_str(dt) == '2021-09-01T12:00:00.000000 UTC'

def test_calculate_tx_ts():
    block_ts_str = '2021-09-01 12:00:00.000000 UTC'
    position = '1'
    block_length = 12
    assert calculate_tx_ts(block_ts_str, position, block_length) == datetime(2021, 9, 1, 12, 0, 0, 0)

@pytest.mark.parametrize(
    "gas_used, gas_price, max_priority_fee_per_gas, expected",
    [
        ('21000', '1000000000', '0', 0.000021),
        ('21000', '1000000000', '1000000000', 0.000042),
        ('21000', '1000000000', '', 0.000021),
    ]
)
def test_calculate_gas_cost_gwei(gas_used, gas_price, max_priority_fee_per_gas, expected):
    assert calculate_gas_cost_gwei(gas_used, gas_price, max_priority_fee_per_gas) == expected


def test_extract():
    obj = {
        'hash': '0x1',
        'from_address': '0x2',
        'to_address': '0x3',
        'block_number': '4',
        'block_timestamp': '2021-09-01 12:00:00.000000 UTC',
        'transaction_index': '10',
        'receipts_gas_used': '21000',
        'receipts_effective_gas_price': '1000000000',
        'max_priority_fee_per_gas': '1000000000'
    }
    assert extract(obj) == {
        'hash': '0x1',
        'from_address': '0x2',
        'to_address': '0x3',
        'block_number': '4',
        'tx_ts': '2021-09-01T12:00:00.000000 UTC',
        'gas_cost_gwei': 0.000042,
        'gas_cost_usd': 0.14450361810724993,
        'gas_used': '21000'
    }


def test_extract_with_price_fetching_error():
    obj = {
        'hash': '0x1',
        'from_address': '0x2',
        'to_address': '0x3',
        'block_number': '4',
        'block_timestamp': '2001-09-01 12:00:00.000000 UTC',
        'transaction_index': '10',
        'receipts_gas_used': '21000',
        'receipts_effective_gas_price': '1000000000',
        'max_priority_fee_per_gas': '1000000000'
    }
    with pytest.raises(PriceFetchingError):
        extract(obj)

