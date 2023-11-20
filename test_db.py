import pytest
from unittest.mock import MagicMock
from db import DB
from datatypes import Txn, TxnStats

@pytest.fixture
def db():
    return DB()

def test_fetch_txn_existing(db):
    # Mock the cursor and execute method
    db.cursor = MagicMock()
    db.cursor.execute.return_value.fetchone.return_value = (
        '0x1',
        '0x2',
        '0x3',
        4,
        '2021-09-01T12:00:00.000000 UTC',
        21000,
        0.14450361810724993
    )
    
    txn = db.fetch_txn('0x1')
    
    assert txn == Txn(
        tx_hash='0x1',
        from_address='0x2',
        to_address='0x3',
        block_number=4,
        tx_ts='2021-09-01T12:00:00.000000 UTC',
        gas_used=21000,
        gas_cost_usd=0.14
    )
    db.cursor.execute.assert_called_once_with(
        'SELECT hash, from_address, to_address, block_number, txn_ts, gas_used, gas_cost_usd FROM txns WHERE hash = ?',
        ('0x1',)
    )

def test_fetch_txn_non_existing(db):
    # Mock the cursor and execute method
    db.cursor = MagicMock()
    db.cursor.execute.return_value.fetchone.return_value = None
    
    txn = db.fetch_txn('0x1')
    
    assert txn is None
    db.cursor.execute.assert_called_once_with(
        'SELECT hash, from_address, to_address, block_number, txn_ts, gas_used, gas_cost_usd FROM txns WHERE hash = ?',
        ('0x1',)
    )

def test_stats(db):
    # Mock the cursor and execute method
    db.cursor = MagicMock()
    db.cursor.execute.return_value.fetchone.return_value = (10, 100000, 144.50)
    
    stats = db.stats()
    
    assert stats == TxnStats(
        count=10,
        total_gas_used=100000,
        total_gas_cost_usd=144.50
    )
    db.cursor.execute.assert_called_once_with(
        'SELECT COUNT(hash), SUM(gas_used), SUM(gas_cost_usd) FROM txns'
    )

def test_insert_txn(db):
    # Mock the fetch_txn method
    db.fetch_txn = MagicMock(return_value=None)
    # Mock the cursor and execute method
    db.cursor = MagicMock()
    
    txn = {
        'hash': '0x1',
        'from_address': '0x2',
        'to_address': '0x3',
        'block_number': 4,
        'tx_ts': '2021-09-01T12:00:00.000000 UTC',
        'gas_cost_gwei': 0.000042,
        'gas_cost_usd': 0.14450361810724993,
        'gas_used': 21000
    }
    
    result = db.insert_txn(txn)
    
    assert result == 1
    db.fetch_txn.assert_called_once_with('0x1')
    db.cursor.execute.assert_called_once_with(
        'INSERT INTO txns VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
        (
            '0x1',
            '0x2',
            '0x3',
            4,
            '2021-09-01T12:00:00.000000 UTC',
            0.000042,
            0.14450361810724993,
            21000
        )
    )

def test_insert_txn_existing(db):
    # Mock the fetch_txn method
    db.fetch_txn = MagicMock(return_value=Txn(
        tx_hash='0x1',
        from_address='0x2',
        to_address='0x3',
        block_number=4,
        tx_ts='2021-09-01T12:00:00.000000 UTC',
        gas_used=21000,
        gas_cost_usd=0.14
    ))
    # Mock the logger
    db.logger = MagicMock()
    
    txn = {
        'hash': '0x1',
        'from_address': '0x2',
        'to_address': '0x3',
        'block_number': 4,
        'tx_ts': '2021-09-01T12:00:00.000000 UTC',
        'gas_cost_gwei': 0.000042,
        'gas_cost_usd': 0.14450361810724993,
        'gas_used': 21000
    }
    
    result = db.insert_txn(txn)
    
    assert result == 0
    db.fetch_txn.assert_called_once_with('0x1')