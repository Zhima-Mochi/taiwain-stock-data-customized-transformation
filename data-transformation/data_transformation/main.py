import asyncio
from functools import partial
from data_transformation.crud import get_task_table_names
from data_transformation.database import get_database, engine
from data_transformation.models import metadata
from data_transformation.utils import task
from multiprocessing import Process, Manager, Pool


async def main():
    metadata.create_all(engine)
    async with get_database() as database:
        with Manager()as manager:
            task_table_names = manager.list(await get_task_table_names(database))
            with Pool() as pool:
                iter = pool.imap_unordered(
                    task, (task_table_name for task_table_name in task_table_names))
                for val in iter:
                    print(val)


if __name__ == '__main__':
    asyncio.run(main())
