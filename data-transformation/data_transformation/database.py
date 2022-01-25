import sqlalchemy
from databases import Database

from data_transformation.settings import Settings

settings = Settings()

DATABASE_URL = f"mysql://{settings.DATABASE_USER}:{settings.DATABASE_PASSWORD}@{settings.DATABASE_HOST}:{settings.DATABASE_PORT}/{settings.DATABASE_NAME}"

database = Database(DATABASE_URL)

engine = sqlalchemy.create_engine(DATABASE_URL)


def get_database() -> Database:
    return database
