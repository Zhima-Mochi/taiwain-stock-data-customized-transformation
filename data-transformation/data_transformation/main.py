

import asyncio
from data_transformation.database import get_database, engine
from data_transformation.models import metadata


async def main():
    await get_database().connect()
    metadata.create_all(engine)


if __name__ == '__main__':
    asyncio.run(main())
