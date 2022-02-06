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

from bisect import bisect_left, bisect_right
import asyncio
import logging
from data_transformation.database import get_database
from data_transformation.crud import get_dates_in_month, get_each_date_content_dataframe, get_table_content_dataframe, set_task_table_record_status
from data_transformation.models import create_sa_stock_data_table
import pandas as pd
import datetime

stock_start_time = datetime.time(8, 30)
stock_end_time = datetime.time(13, 30)


start_seconds = stock_start_time.hour*60*60 + \
    stock_start_time.minute*60+stock_start_time.second
end_seconds = stock_end_time.hour*60*60 + \
    stock_end_time.minute*60+stock_end_time.second


def task(task_table_name):
    try:
        # create sqlalchemy table
        task_sa_table = create_sa_stock_data_table(task_table_name)
    except Exception as e:
        logging.warning(e)

    periods = pd.DataFrame(
        {'matching_time': (datetime.time(sec//60//60, sec % 3600//60, sec % 60) for sec in range(start_seconds, end_seconds+1, 1))})

    async def async_task():
        async with get_database() as database:
            # set current table status pending
            await set_task_table_record_status(database, task_table_name, "0")
            try:
                # data conversion date by date in month
                data_dates = await get_dates_in_month(database, task_sa_table)
                for data_date in data_dates:
                    data = await get_each_date_content_dataframe(
                        database, task_sa_table, data_date)
                    stock_id_list = data["stock_id"].unique()
                    for stock_id in stock_id_list:
                        stock_data = pd.DataFrame(data[bisect_left(data["stock_id"], stock_id):bisect_right(
                            data["stock_id"], stock_id)])
                        stock_data = periods.join(stock_data.set_index(
                            'matching_time'), on='matching_time')
                        print(stock_data)
                # set current table status finished
                await set_task_table_record_status(database, task_table_name, "1")
            except Exception as e:
                # set current table status pending
                await set_task_table_record_status(database, task_table_name, "0")
                print(e)
    return asyncio.run(async_task())
