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
"""
 `data_date` date NOT NULL,
    `stock_id` char(6) NOT NULL,
    `matching_time` time(6) NOT NULL,
    `is_matching` tinyint DEFAULT NULL,
    `best_ask_tick_number` tinyint DEFAULT NULL,
    `best_bid_tick_number` tinyint DEFAULT NULL,
    `matching_price_limit_mark` tinyint DEFAULT NULL,
    `best_ask_tick_price_limit_mark` tinyint DEFAULT NULL,
    `best_bid_tick_price_limit_mark` tinyint DEFAULT NULL,
    `momentary_price_movement` tinyint DEFAULT NULL,
    `matching_price` float DEFAULT NULL,
    `matching_volume` int DEFAULT NULL,
    `the_best_ask_tick_price` float DEFAULT NULL,
    `the_best_ask_tick_volume` int DEFAULT NULL,
    `the_best_bid_tick_price` float DEFAULT NULL,
    `the_best_bid_tick_volume` int DEFAULT NULL
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


start_mins = stock_start_time.hour*60 + \
    stock_start_time.minute
end_mins = stock_end_time.hour*60 + \
    stock_end_time.minute


def task(task_table_name):
    try:
        # create sqlalchemy table
        task_sa_table = create_sa_stock_data_table(task_table_name)
    except Exception as e:
        logging.warning(e)

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
                    # data conversion stock_id by stock_id in a date
                    for stock_id in stock_id_list:
                        stock_data = pd.DataFrame(data[bisect_left(data["stock_id"], stock_id):bisect_right(
                            data["stock_id"], stock_id)])
                        # get the nearest data per minute
                        stock_data["matching_time"] = pd.to_datetime([datetime.datetime.combine(
                            data_date, mt) for mt in stock_data["matching_time"]])
                        stock_data["time"] = stock_data["matching_time"].dt.ceil(
                            'T')
                        # stock_data["buyer_side_init_match_count"]

                        stock_data.loc[stock_data["the_best_bid_tick_price"].isnull(
                        ), "the_best_bid_tick_price"] = stock_data["the_best_ask_tick_price"][stock_data["the_best_bid_tick_price"].isnull()]

                        stock_data.loc[stock_data["the_best_bid_tick_price"].isnull(
                        ), "the_best_bid_tick_price"] = stock_data["matching_price"][stock_data["the_best_bid_tick_price"].isnull()]

                        stock_data.loc[stock_data["the_best_bid_tick_price"].isnull(
                        ), "the_best_bid_tick_price"] = 0

                        stock_data.loc[stock_data["the_best_ask_tick_price"].isnull(
                        ), "the_best_ask_tick_price"] = stock_data["the_best_bid_tick_price"][stock_data["the_best_ask_tick_price"].isnull()]

                        stock_data.loc[stock_data["the_best_ask_tick_price"].isnull(
                        ), "the_best_ask_tick_price"] = stock_data["matching_price"][stock_data["the_best_ask_tick_price"].isnull()]

                        stock_data["middle_price"] = (
                            stock_data["the_best_bid_tick_price"]+stock_data["the_best_ask_tick_price"])/2

                        stock_data["transaction_price"] = stock_data["matching_price"]
                        stock_data.loc[stock_data["transaction_price"].isnull(
                        ), "transaction_price"] = stock_data["middle_price"]

                        stock_data = stock_data[stock_data["transaction_price"] > 0]

                        stock_data["QBA"] = (
                            stock_data["the_best_ask_tick_price"]-stock_data["the_best_bid_tick_price"])/stock_data["transaction_price"]

                        stock_data["previous_transaction_price"] = stock_data["transaction_price"].shift(
                            1)
                            
                        stock_data["indicator_q"] = [-1 if stock_data["transaction_price"][i] < stock_data["middle_price"][i]
                                                     else 1 if stock_data["transaction_price"][i] > stock_data["middle_price"][i] else 1 if stock_data["transaction_price"][i] > stock_data["previous_transaction_price"][i] else -1 if stock_data["transaction_price"][i] < stock_data["previous_transaction_price"][i] else 0 for i in stock_data.index]
                        print(stock_data)
                        # stock_data = stock_data.groupby("time").last()

                        # stock_data = stock_data.reindex(pd.date_range(datetime.datetime.combine(
                        #     data_date, stock_start_time), datetime.datetime.combine(data_date, stock_end_time), freq="T"))
                        # stock_data = stock_data.ffill()

                # set current table status finished
                await set_task_table_record_status(database, task_table_name, "1")
            except Exception as e:
                # set current table status pending
                await set_task_table_record_status(database, task_table_name, "0")
                print(e)
    return asyncio.run(async_task())
