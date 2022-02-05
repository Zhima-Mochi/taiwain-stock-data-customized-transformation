"""
date 
stock_id
time
the_best_ask_tick_price
the_best_ask_tick_volume
the_best_bid_tick_price
the_best_bid_tick_volume
middle_price
transaction_price
QBA
indicator_q
tick_rule_flag
espread_1
espread_2
rspread_1_5min
advsele_1_5min
rspread_2_5min
advsele_2_5min
rspread_1_30min
advsele_1_30min
rspread_2_30min
advsele_2_30min
buyer_side_init_match_count
buyer_side_init_match_accum_volume
seller_side_init_match_count
seller_side_init_match_accum_volume
total_match_count
total_match_accum_volume
avg_match_volume
avg_buyer_side_init_match_volume
avg_seller_side_init_match_volume
return_rate
amihud_p
amihud_m
"""

import asyncio
import logging
from data_transformation.database import get_database
from data_transformation.crud import get_dates_in_month, get_each_date_content_dataframe, get_table_content_dataframe, set_task_table_record_status
from data_transformation.models import create_sa_stock_data_table


def task(task_table_name):
    try:
        # create sqlalchemy table
        task_sa_table = create_sa_stock_data_table(task_table_name)
    except Exception as e:
        logging.warning(e)

    async def async_task():
        async with get_database() as database:
            await set_task_table_record_status(database, task_table_name, "0")
            try:
                data_dates = await get_dates_in_month(database, task_sa_table)
                for data_date in data_dates:
                    data = await get_each_date_content_dataframe(
                        database, task_sa_table, data_date)
                    print(data)
                await set_task_table_record_status(database, task_table_name, "1")
            except Exception as e:
                await set_task_table_record_status(database, task_table_name, "0")
                print(e)

    return asyncio.run(async_task())
