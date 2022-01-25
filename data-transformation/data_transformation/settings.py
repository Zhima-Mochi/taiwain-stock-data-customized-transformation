from pydantic import BaseSettings


class Settings(BaseSettings):
    DEBUG: bool = False
    DATABASE_HOST: str
    DATABASE_PORT: str = '3306'
    DATABASE_NAME: str
    DATABASE_USER: str
    DATABASE_PASSWORD:str

    class Config:
        env_file = ".env"
