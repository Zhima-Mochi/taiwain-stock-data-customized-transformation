import asyncio
from contextlib import asynccontextmanager
from sqlalchemy import select
from data_transformation.models import *
from databases import Database
import pytest
import pandas as pd

DATABASE_URL = f"mysql+aiomysql://root:root@127.0.0.1/stock_data_storage"

database_test = Database(DATABASE_URL)

engine_test = sqlalchemy.create_engine(DATABASE_URL)

stock_data_table = create_sa_stock_data_table("stock_data_201503")


@asynccontextmanager
async def get_test_database() -> Database:
    db = database_test
    await db.connect()
    try:
        yield db
    finally:
        await db.disconnect()


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()


@pytest.mark.asyncio
async def test_connection():
    async with get_test_database() as database_test:
        result = await database_test.fetch_all(query="show tables;")
        print(result)


@pytest.mark.asyncio
async def test_query():
    async with get_test_database() as database_test:
        print(await database_test.fetch_all(query="desc stock_data_201503;"))
        result = await database_test.fetch_one(query=select(stock_data_table))
        print(result)


@pytest.mark.asyncio
async def test_pandas():
    async with get_test_database() as database_test:
        result = await database_test.fetch_all(query=select(stock_data_table))
        print(pd.DataFrame([dict(row) for row in result]))
