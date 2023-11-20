## Context

[Rated](rated.network) offers a solution to the poor contextualization of validator quality. The solution is centered around reputation scores for machines and their operators, starting with the Ethereum Beacon Chain. 

Rated seeks to embed a large swathe of available information from all layers of a given network, and compress it in an easily legible and generalizable reputation score that can act as an input to human workflows but most importantly, machines (e.g. an API that acts as an input to insurance or derivatives Smart Contracts).

## The Exercise

The purpose of this exercise is to manipulate an Ethereum transaction [dataset](https://github.com/rated-network/coding-challenge/blob/main/ethereum_txs.csv) using Python.

The following resources provide the required background:

- [ETH transaction](https://ethereum.org/en/developers/docs/transactions/)
- [Blockchain Explorer](https://etherscan.io/) 
- [Gas fees](https://ethereum.org/en/developers/docs/gas/)
- [ETH Gas Tracker](https://etherscan.io/gastracker)

## Guidelines

This solution should consist of 5 parts:

1. Assuming block length is 12 seconds, compute the approximate execution timestamp of each transaction.
2. Compute the gas cost of each transaction in Gwei (1e-9 ETH).
3. Using [Coingecko's](https://www.coingecko.com/en/api/documentation) API, retrieve the approximate price of ETH at transaction execution time and compute the dollar cost of gas used.
4. Populate a local database with the processed transactions.
5. Using the database in part 4, implement an API endpoint in a framework of your choosing that serves the following endpoints:

### Transactions API

API endpoint that returns a compact transaction view.
```
GET /transactions/:hash

{
  "hash": "0xaaaaa",
  "fromAddress": "0x000000",
  "toAddress": "0x000001",
  "blockNumber": 1234,
  "executedAt": "Jul-04-2022 12:02:24 AM +UTC",
  "gasUsed": 12345678,
  "gasCostInDollars": 4.23,
}
```

### Stats API

API endpoint that returns aggregated global transaction stats.
```
GET /stats

{
  "totalTransactionsInDB": 1234,
  "totalGasUsed": 1234567890,
  "totalGasCostInDollars": 12345
}

```

### What are we looking for?
We place a strong emphasis on delivering exceptional and reliable software. However, it's crucial to acknowledge that our applications will continuously evolve as we expand and refine our product offerings. 

As a result, we prioritize flexibility and adaptability over purely architectural aesthetics. While we value elegant design, our focus remains on building resilient systems that can gracefully accommodate future changes and improvements. 

Therefore, we recommend you to focus on code simplicity, readability and maintainability.

That being said,
* The solution should adhere to production-like coding standards.
* Your code must be delivered in a Github repository.
* Your code should include tests.
* Nice to have: `pydantic`, `FastAPI`, `pytest`.
* You will stand out by converting the CSV into an event stream and processing that stream.

Good luck!

## Solution

As there are not many files, they are all present on the root level itself.

data_processing.py - processes the ethereum csv file and writes them to a sqlite database
server.py - fastapi server and endpoints
db.py - code for db operations
datatypes.py - data models

### Install libs
```
python3 -m venv .venv
source .venv/bin/activate
pip3 install -r requirements.txt
```

### Run

1. Data Processing

```
python -m bytewax.run data_processing:flow
```
This will create a local filebased sqlite database and populate it with transaction data

2. Run the server
```
python server.py
```
This should start the server at http://127.0.0.1:8000

3. Verify the endpoints

http://127.0.0.1:8000/transactions/0x4843f0d69e489360b57e4bb2a261493ea57b65ad6abfd43e5ebe6074026d6c66

```json
{
  "hash": "0x4843f0d69e489360b57e4bb2a261493ea57b65ad6abfd43e5ebe6074026d6c66",
  "fromAddress": "0xd1da03feeae645f76b729a9ec6012d80c0805dcc",
  "toAddress": "0xdac17f958d2ee523a2206206994597c13d831ec7",
  "blockNumber": 17818510,
  "executedAt": "2023-08-01T06:58:35.000000 UTC",
  "gasUsed": 46121,
  "gasCostInDollars": 1.64
}
```


http://127.0.0.1:8000/stats

```json
{
  "totalTransactionsInDB": 5000,
  "totalGasUsed": 494112901,
  "totalGasCostInDollars": 21813.48
}
```

### Tests

```
pytest
```

### Things I'd do if I had more time

- Define an bytewax Output Sqlite Sink
- Make DB a interface like class with abstract methods (so db specific code can be decoupled and easily replaced)
- improve repo structure (src, test folders, etc.)
- Use stateful operation and aggregate/total stats once during data-processing
- cache eth price in our db (or some other persistent storage)
- refactored fetch_price for testability (so it can be mocked)
- api unit tests
- property testing 