CREATE TABLE `stock_data_201503` (
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

CREATE TABLE `stock_data_201504` (
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

CREATE TABLE `stock_data_201505` (
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