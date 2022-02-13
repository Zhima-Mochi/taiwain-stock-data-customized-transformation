import asyncio
import logging
from data_transformation import app
if __name__ == '__main__':
    # logging.basicConfig(level=logging.INFO)
    asyncio.run(app.main())
