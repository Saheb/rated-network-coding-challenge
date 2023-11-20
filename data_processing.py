from datetime import datetime, timedelta
from functools import lru_cache
import sqlite3
import sys
from bytewax.dataflow import Dataflow
from bytewax.connectors.files import CSVInput
from bytewax.connectors.stdio import StdOutput
import requests
from db import DB
import logging


logger = logging.getLogger(__name__)

class PriceFetchingError(Exception):
    pass

def extract(obj):
    try:
        txn_ts = calculate_tx_ts(obj['block_timestamp'], obj['transaction_index'])
        txn_ts_str = datetime_str(txn_ts)
        txn_date_str = txn_ts.strftime('%d-%m-%Y')
        
        eth_price = fetch_price(txn_date_str)
        
        gas_cost_gwei = calculate_gas_cost_gwei(obj['receipts_gas_used'], obj['receipts_effective_gas_price'], obj['max_priority_fee_per_gas'])
        gas_cost_usd = gas_cost_gwei * eth_price
    except PriceFetchingError as e:
        logger.error(f"An error occurred while fetching eth price for date {txn_date_str}: {e}")
        raise
    except Exception as e:
        logger.error(f"An error occurred while processing {obj['hash']}: {e}")
        raise
    return {
        'hash': obj['hash'],
        'from_address': obj['from_address'],
        'to_address': obj['to_address'],
        'block_number': obj['block_number'],
        'tx_ts': txn_ts_str,
        'gas_cost_gwei': gas_cost_gwei,
        'gas_cost_usd': gas_cost_usd,
        'gas_used': obj['receipts_gas_used']
    }

def calculate_gas_cost_gwei(gas_used: str, gas_price: str, max_priority_fee_per_gas: str):
    base_fee = int(gas_price) / 1e9
    try:
        priority_fee = int(max_priority_fee_per_gas) / 1e9
    except ValueError:
        priority_fee = 0 # if max_priority_fee_per_gas is empty
    return (int(gas_used) * (base_fee + priority_fee)) / 1e9

# using block_timestamp as approximate transaction timestamp (ignoring block length)
def calculate_tx_ts(block_ts_str, position: str, block_length = 12):
    # Extract the timestamp part without the ' UTC' suffix
    block_ts = block_ts_str.split(' UTC')[0]

    # Convert block timestamp string to datetime object
    block_timestamp = datetime.strptime(block_ts, '%Y-%m-%d %H:%M:%S.%f')

    # Calculate execution timestamp
    execution_timestamp = block_timestamp
    return execution_timestamp

def datetime_str(dt: datetime):
    return dt.isoformat(timespec='microseconds') + ' UTC'

# call coingecko API to get the price of ETH on a given date
@lru_cache(maxsize=None)
def fetch_price(date, symbol: str = 'ethereum'):
    logger.debug(f'Fetching price for {symbol} on {date}')
    response = requests.get(f'https://api.coingecko.com/api/v3/coins/{symbol}/history?date={date}&localization=false')
    if response.status_code == 200 and 'market_data' in response.json():
        return response.json()['market_data']['current_price']['usd']
    else:
        if response.status_code == 200 and 'market_data' not in response.json():
            raise PriceFetchingError(f"An error occurred while fetching price for {symbol} on {date}: {response.json()}")
        else:
            raise PriceFetchingError(f"An error occurred while fetching price for {symbol} on {date}: {response.status_code}")


try:
    db = DB()
except Exception as e:
    logger.error(f"An error occurred while initializing the database: {e}")
    sys.exit(1)

flow = Dataflow()
flow.input("inp", CSVInput("ethereum_txs.csv")) # type: ignore
flow.map(extract)
flow.map(db.insert_txn) # returns 0 if txn already exists in db, 1 otherwise
flow.output("out", StdOutput()) # type: ignore


