import sqlalchemy
from databases import Database
from contextlib import asynccontextmanager
from data_transformation.settings import Settings

settings = Settings()

DATABASE_URL = f"{settings.DATABASE_USER}:{settings.DATABASE_PASSWORD}@{settings.DATABASE_HOST}:{settings.DATABASE_PORT}/{settings.DATABASE_NAME}"


engine = sqlalchemy.create_engine(f"mysql://{DATABASE_URL}")


@asynccontextmanager
async def get_database() -> Database:
    database = Database(f"mysql+aiomysql://{DATABASE_URL}")
    try:
        await database.connect()
        yield database
    finally:
        await database.disconnect()
