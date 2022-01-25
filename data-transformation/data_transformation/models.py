from datetime import datetime
import sqlalchemy
from sqlalchemy import Column, Integer, String, Table, Time, Float, Date, DateTime
from sqlalchemy.dialects.mysql import TINYINT

metadata = sqlalchemy.MetaData()


def create_sa_stock_data_table(table_name: str) -> Table:
    tb = sqlalchemy.Table(
        table_name,
        metadata,
        Column("data_date", Date, primary_key=True),
        Column("stock_id", String(6), primary_key=True),
        Column("matching_time", Time(6), primary_key=True),
        Column("is_matching", TINYINT),
        Column("best_ask_tick_number", TINYINT),
        Column("best_bid_tick_number", TINYINT),
        Column("matching_price_limit_mark", TINYINT),
        Column("best_ask_tick_price_limit_mark", TINYINT),
        Column("best_bid_tick_price_limit_mark", TINYINT),
        Column("momentary_price_movement", TINYINT),
        Column("matching_price", Float),
        Column("matching_volume", Integer),
        Column("the_best_ask_tick_price", Float),
        Column("the_best_ask_tick_volume", Integer),
        Column("the_best_bid_tick_price", Float),
        Column("the_best_bid_tick_volume", Integer)
    )
    return tb


'''
CREATE TABLE `stock_data_xxxxxx` (
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
);
'''

records = Table(
    "records",
    metadata,
    Column('table_name', String(50), primary_key=True),
    Column('status', String(1)),
    Column('status_date', DateTime,
           default=datetime.utcnow, onupdate=datetime.utcnow)
)
