import asyncio
from data_transformation.models import *
from databases import Database
from sqlalchemy import Text
import pytest

DATABASE_URL = f"mysql+aiomysql://root:root@127.0.0.1/stock_data_storage"

database_test = Database(DATABASE_URL)

engine_test = sqlalchemy.create_engine(DATABASE_URL)


def get_test_database() -> Database:
    return database_test


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()


@pytest.mark.asyncio
async def test_connection():
    await get_test_database().connect()
    await get_test_database().execute(query="show tables;")
    await get_test_database().disconnect()

