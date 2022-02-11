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


from asyncio.log import logger
from bisect import bisect_left, bisect_right
import asyncio
import logging
from data_transformation.database import get_database
from data_transformation.crud import get_dates_in_month, get_each_date_content_dataframe, set_task_table_record_status
from data_transformation.models import create_sa_stock_data_table
from data_transformation.settings import Settings
import pandas as pd
import numpy as np
import datetime
import os
from pathlib import Path
np.seterr(divide='ignore')

stock_start_time = datetime.time(8, 30)
stock_end_time = datetime.time(13, 30)


start_mins = stock_start_time.hour*60 + \
    stock_start_time.minute
end_mins = stock_end_time.hour*60 + \
    stock_end_time.minute

required_columns = [
    "data_date",
    "stock_id",
    "time",
    "the_best_ask_tick_price",
    "the_best_ask_tick_volume",
    "the_best_bid_tick_price",
    "the_best_bid_tick_volume",
    "middle_price",
    "transaction_price",
    "QBA",
    "indicator_q",
    "tick_rule_flag",
    "espread_1",
    "espread_2",
    "rspread_1_5min",
    "advsele_1_5min",
    "rspread_2_5min",
    "advsele_2_5min",
    "rspread_1_30min",
    "advsele_1_30min",
    "rspread_2_30min",
    "advsele_2_30min",
    "buyer_side_init_match_count",
    "buyer_side_init_match_accum_volume",
    "seller_side_init_match_count",
    "seller_side_init_match_accum_volume",
    "total_match_count",
    "total_match_accum_volume",
    "avg_match_volume",
    "avg_buyer_side_init_match_volume",
    "avg_seller_side_init_match_volume",
    "return_rate",
    "amihud_p",
    "amihud_m"
]
settings = Settings()

output_path = Path(settings.OUTPUT_PATH)

if not output_path.exists() or not output_path.is_dir():
    raise Exception(f'OUTPUT_PATH: {output_path} is not valid path.')


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
                if len(data_dates) == 0:
                    logger.warning(f"There is no row in {task_table_name}")
                    return "No data: "+task_table_name
                for data_date in data_dates:
                    data = await get_each_date_content_dataframe(
                        database, task_sa_table, data_date)

                    output_file_path = output_path / \
                        (task_table_name+'_'+data_date.strftime("%Y%m%d")+".csv")

                    if output_file_path.exists():
                        output_file_path.unlink()

                    stock_id_list = data["stock_id"].unique()
                    # data conversion stock_id by stock_id in a date
                    for stock_id in stock_id_list:
                        stock_data = pd.DataFrame(data[bisect_left(data["stock_id"], stock_id):bisect_right(
                            data["stock_id"], stock_id)])

                        # cleaning
                        stock_data = stock_data.loc[~((stock_data["the_best_bid_tick_price"]
                                                       == 0) & (stock_data["the_best_ask_tick_price"] == 0)), :]

                        # combine data_date and matching_time to datetime type
                        stock_data["matching_time"] = pd.to_datetime([datetime.datetime.combine(
                            data_date, mt) for mt in stock_data["matching_time"]])
                        stock_data["time"] = stock_data["matching_time"].dt.ceil(
                            'T')

                        stock_data.loc[:, [
                            "the_best_bid_tick_price", "the_best_ask_tick_price", "matching_price"]] = stock_data.loc[:, [
                                "the_best_bid_tick_price", "the_best_ask_tick_price", "matching_price"]].ffill()

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

                        # generate previous_transaction_price by shifting 1 period of transaction_price
                        stock_data["previous_transaction_price"] = stock_data["transaction_price"].shift(
                            1)

                        stock_data["tick_rule_flag"] = [1 if stock_data["transaction_price"][i] == stock_data["middle_price"][i]
                                                        else 0 for i in stock_data.index]

                        stock_data["indicator_q"] = [-1 if stock_data["transaction_price"][i] < stock_data["middle_price"][i]
                                                     else 1 if stock_data["transaction_price"][i] > stock_data["middle_price"][i] else 1 if stock_data["transaction_price"][i] > stock_data["previous_transaction_price"][i] else -1 if stock_data["transaction_price"][i] < stock_data["previous_transaction_price"][i] else 0 for i in stock_data.index]

                        stock_data_minute = pd.DataFrame()
                        stock_data_minute["total_match_count"] = stock_data.groupby("time")[
                            "is_matching"].sum()
                        stock_data_minute["total_match_accum_volume"] = stock_data.groupby("time")[
                            "matching_volume"].sum()

                        is_matching = data["is_matching"] == 1
                        stock_data_minute["buyer_side_init_match_count"] = stock_data.groupby(
                            "time").apply(lambda data:  ((data["indicator_q"] > 0) & is_matching).sum())
                        stock_data_minute["buyer_side_init_match_accum_volume"] = stock_data.groupby(
                            "time").apply(lambda data: data["matching_volume"][(data["indicator_q"] > 0) & is_matching].sum())

                        stock_data_minute["seller_side_init_match_count"] = stock_data.groupby(
                            "time").apply(lambda data:  ((data["indicator_q"] < 0) & is_matching).sum())
                        stock_data_minute["seller_side_init_match_accum_volume"] = stock_data.groupby(
                            "time").apply(lambda data: data["matching_volume"][(data["indicator_q"] < 0) & is_matching].sum())
                        stock_data_minute["accumulated_matching_price_multiply_volume"] = stock_data.groupby(
                            "time").apply(lambda data: (data["transaction_price"]*data["matching_volume"]*is_matching).sum())
                        stock_data_minute["accumulated_middle_price_multiply_volume"] = stock_data.groupby(
                            "time").apply(lambda data: (data["middle_price"]*data["matching_volume"]*is_matching).sum())

                        stock_data.drop(
                            columns=["matching_time", "is_matching", "best_ask_tick_number", "best_bid_tick_number", "matching_price_limit_mark", "best_ask_tick_price_limit_mark", "best_bid_tick_price_limit_mark", "momentary_price_movement", "matching_price", "matching_volume"], inplace=True)

                        stock_data = stock_data.groupby("time").last()
                        stock_data = stock_data.reindex(pd.date_range(datetime.datetime.combine(
                            data_date, stock_start_time), datetime.datetime.combine(data_date, stock_end_time), freq="T"))

                        stock_data.loc[:, ["data_date", "stock_id",
                                           "the_best_bid_tick_price", "the_best_ask_tick_price",
                                           "QBA", "transaction_price", "middle_price", "indicator_q", "previous_transaction_price", "tick_rule_flag"]] = stock_data.loc[:, ["data_date", "stock_id",
                                                                                                                                                                            "the_best_bid_tick_price", "the_best_ask_tick_price",
                                                                                                                                                                            "QBA", "transaction_price", "middle_price", "indicator_q", "previous_transaction_price", "tick_rule_flag"]].ffill()
                        # delete the rows with time before first transaction
                        stock_data = stock_data[stock_data["data_date"].notnull(
                        )]

                        stock_data = pd.concat(
                            [stock_data, stock_data_minute], axis=1)

                        stock_data.loc[:, ["the_best_ask_tick_volume", "the_best_bid_tick_volume", "total_match_count", "total_match_accum_volume", "buyer_side_init_match_count", "buyer_side_init_match_accum_volume", "seller_side_init_match_count",
                                           "seller_side_init_match_accum_volume", "accumulated_matching_price_multiply_volume", "accumulated_middle_price_multiply_volume"]] = stock_data.loc[:, ["the_best_ask_tick_volume", "the_best_bid_tick_volume", "total_match_count", "total_match_accum_volume", "buyer_side_init_match_count", "buyer_side_init_match_accum_volume", "seller_side_init_match_count",
                                                                                                                                                                                                  "seller_side_init_match_accum_volume", "accumulated_matching_price_multiply_volume", "accumulated_middle_price_multiply_volume"]].fillna(0)

                        stock_data["avg_match_volumn"] = stock_data["total_match_accum_volume"] / \
                            stock_data["total_match_count"]
                        stock_data["avg_buyer_side_init_match_volume"] = stock_data["buyer_side_init_match_accum_volume"] / \
                            stock_data["buyer_side_init_match_count"]

                        stock_data["avg_seller_side_init_match_volume"] = stock_data["seller_side_init_match_accum_volume"] / \
                            stock_data["seller_side_init_match_count"]

                        stock_data["middle_return_rate"] = np.log(
                            stock_data["middle_price"])-np.log(stock_data["middle_price"].shift(1))
                        stock_data["return_rate"] = np.log(
                            stock_data["transaction_price"])-np.log(stock_data["transaction_price"].shift(1))

                        stock_data["amihud_p"] = np.abs(
                            stock_data["return_rate"])/stock_data["accumulated_matching_price_multiply_volume"]
                        stock_data["amihud_m"] = np.abs(
                            stock_data["middle_return_rate"])/stock_data["accumulated_middle_price_multiply_volume"]

                        stock_data["espread_1"] = 2*stock_data["indicator_q"] * \
                            (stock_data["transaction_price"] -
                             stock_data["middle_price"])
                        stock_data["espread_2"] = stock_data["indicator_q"]*(
                            stock_data["transaction_price"]-stock_data["middle_price"])/stock_data["middle_price"]

                        stock_data["rspread_1_5min"] = 2*stock_data["indicator_q"]*(
                            stock_data["transaction_price"]-stock_data["middle_price"].shift(-5))

                        stock_data["advsele_1_5min"] = 2*stock_data["indicator_q"]*(
                            stock_data["middle_price"].shift(-5)-stock_data["middle_price"])

                        stock_data["rspread_2_5min"] = stock_data["indicator_q"]*(
                            stock_data["transaction_price"]-stock_data["middle_price"].shift(-5))/stock_data["middle_price"]

                        stock_data["advsele_2_5min"] = stock_data["indicator_q"]*(
                            stock_data["middle_price"].shift(-5)-stock_data["middle_price"])/stock_data["middle_price"]

                        stock_data["rspread_1_30min"] = 2*stock_data["indicator_q"]*(
                            stock_data["transaction_price"]-stock_data["middle_price"].shift(30))

                        stock_data["advsele_1_30min"] = 2*stock_data["indicator_q"]*(
                            stock_data["middle_price"].shift(30)-stock_data["middle_price"])

                        stock_data["rspread_2_30min"] = stock_data["indicator_q"]*(
                            stock_data["transaction_price"]-stock_data["middle_price"].shift(30))/stock_data["middle_price"]

                        stock_data["advsele_2_30min"] = stock_data["indicator_q"]*(
                            stock_data["middle_price"].shift(30)-stock_data["middle_price"])/stock_data["middle_price"]

                        stock_data.drop(columns=stock_data.columns.difference(
                            required_columns), inplace=True)
                        stock_data.index.name = 'time'
                        # stock_data.replace(np.inf,np.nan,inplace=True)
                        stock_data.to_csv(
                            output_file_path, mode='a', header=not os.path.exists(output_file_path))
                # set current table status finished
                await set_task_table_record_status(database, task_table_name, "1")
            except Exception as e:
                # set current table status pending
                await set_task_table_record_status(database, task_table_name, "0")
                return f"Error: {task_table_name} {e}"
            return "Finish: "+task_table_name
    return asyncio.run(async_task())
