from aiomysql import IntegrityError
from data_transformation.settings import Settings
from databases import Database
import pandas as pd
from sqlalchemy import select
from data_transformation.models import *

settings = Settings()


async def get_task_table_names(database: Database):
    stmt = '''
        SELECT a.table_name FROM information_schema.tables a
        LEFT JOIN records b ON
        a.table_name = b. table_name
        WHERE a.table_schema = 'stock_data_storage' AND
        a.table_name like '%stock_data_%' AND
        ( b.status is null OR 
        b.status != '1');
        ''' if settings.DEBUG==False else '''
        SELECT a.table_name FROM information_schema.tables a
        LEFT JOIN records b ON
        a.table_name = b. table_name
        WHERE a.table_schema = 'stock_data_storage' AND
        a.table_name like '%stock_data_%';
        '''
    result = await database.fetch_all(query=stmt)
    return [res[0] for res in result]


async def get_table_content_dataframe(database: Database, task_sa_table: Table):
    result = await database.fetch_all(query=select(task_sa_table))
    return pd.DataFrame((dict(row) for row in result))


async def get_dates_in_month(database: Database, task_sa_table: Table):
    result = await database.fetch_all(query=select(task_sa_table.c.data_date).group_by(task_sa_table.c.data_date).order_by(task_sa_table.c.data_date))
    return [res[0] for res in result]


async def get_each_date_content_dataframe(database: Database, task_sa_table: Table, data_date):
    result = await database.fetch_all(query=select(task_sa_table).where(task_sa_table.c.data_date == data_date).order_by(task_sa_table.c.stock_id, task_sa_table.c.matching_time))
    return pd.DataFrame((dict(row) for row in result))


async def set_task_table_record_status(database: Database, task_table_name: str, status: str):
    try:
        await database.execute(query=records.insert().values(table_name=task_table_name, status=status))
    except IntegrityError as e:
        await database.execute(query=records.update().where(records.c.table_name == task_table_name).values(status=status))
