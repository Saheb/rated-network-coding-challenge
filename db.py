import sqlite3

from datatypes import Txn, TxnStats
import logging

logger = logging.getLogger(__name__)

class DB:

    def __init__(self):
        self.db = sqlite3.connect('ethereum_txs.db', check_same_thread=False)
        self.cursor = self.db.cursor()
        self.db.execute('''
            CREATE TABLE IF NOT EXISTS txns (
                hash text PRIMARY KEY,
                from_address text, 
                to_address text, 
                block_number integer, 
                txn_ts text, 
                gas_cost_gwei real, 
                gas_cost_usd real,
                gas_used real
            )
        ''')
        self.db.commit()

    def fetch_txn(self, tx_hash: str):
        res = self.cursor.execute('SELECT hash, from_address, to_address, block_number, txn_ts, gas_used, gas_cost_usd FROM txns WHERE hash = ?', (tx_hash,))
        txn = res.fetchone()
        if txn:
            return Txn(
                tx_hash=txn[0],
                from_address=txn[1],
                to_address=txn[2],
                block_number=txn[3],
                tx_ts=txn[4],
                gas_used=txn[5],
                gas_cost_usd=txn[6].__round__(2) # round to 2 decimal places
            )
        else:
            return None
        
    def stats(self):
        res = self.cursor.execute('SELECT COUNT(hash), SUM(gas_used), SUM(gas_cost_usd) FROM txns')
        stats = res.fetchone()
        return TxnStats(
            count=stats[0],
            total_gas_used=stats[1],
            total_gas_cost_usd=stats[2].__round__(2) # round to 2 decimal places
        )

    def insert_txn(self, txn):
        if self.fetch_txn(txn['hash']):
            logger.debug(f"Transaction {txn['hash']} already exists in database")
            return 0
        logger.debug(f"Writing {txn['hash']} to database")
        self.cursor.execute('INSERT INTO txns VALUES (?, ?, ?, ?, ?, ?, ?, ?)', (
            txn['hash'],
            txn['from_address'],
            txn['to_address'],
            txn['block_number'],
            txn['tx_ts'],
            txn['gas_cost_gwei'],
            txn['gas_cost_usd'],
            txn['gas_used']
        ))
        self.db.commit()
        return 1
