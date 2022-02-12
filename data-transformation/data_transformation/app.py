import asyncio
from asyncio.log import logger
import datetime
from data_transformation.crud import get_task_table_names
from data_transformation.database import get_database, engine
from data_transformation.models import metadata
from data_transformation.settings import Settings
from data_transformation.utils import task

settings = Settings()


async def main():
    if settings.DEBUG:
        metadata.drop_all(engine)
    metadata.create_all(engine)
    async with get_database() as database:
        task_table_names = list(await get_task_table_names(database))
        for task_table_name in task_table_names:
            logger.info(f"{datetime.datetime.now().ctime()}: Finish: {await task(task_table_name)}")


if __name__ == '__main__':
    asyncio.run(main())
