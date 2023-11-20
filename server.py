from fastapi import FastAPI
import uvicorn
import sqlite3
from datatypes import Txn
from db import DB
import typing as t
from fastapi import HTTPException
import logging

logger = logging.getLogger(__name__)

try:
    app = FastAPI()
    db = DB()
except Exception as e:
    logger.error(f"An error occurred while initializing the app: {e}")
    exit(1)

@app.get("/transactions/{hash}")
def transaction(hash: str) -> t.Optional[Txn]:
    txn = db.fetch_txn(hash)
    if txn is None:
        raise HTTPException(status_code=404, detail="Transaction not found")
    return txn

@app.get("/stats")
def stats():
    return db.stats()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)