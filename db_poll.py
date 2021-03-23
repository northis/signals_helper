import logging
import os
import sqlite3
import threading
from enum import Enum
from threading import Thread

from dotenv import load_dotenv

import classes
import config
import helper
import parsers

load_dotenv()
DT_INPUT_FORMAT = r"%Y.%m.%dT%H:%M:%S.%f"
DT_INPUT_TIMEZONE = "EET"
DB_SYMBOLS_PATH = os.getenv("db_symbols_path")
DB_STATS_PATH = os.getenv("db_stats_path")

POLL_WORK_FLAG = True
poll_event = threading.Event()
POLL_INTERVAL_SEC = 60 * 60
POLL_THROTTLE_SEC = 2 * 60


class AccessType(Enum):
    alphavantage = 0
    bitfinex = 1
    investing = 2


db_time_ranges = {
    classes.Symbol.AUDUSD: (None, None),
    classes.Symbol.BTCUSD: (None, None),
    classes.Symbol.EURUSD: (None, None),
    classes.Symbol.GBPUSD: (None, None),
    classes.Symbol.NZDUSD: (None, None),
    classes.Symbol.USDCAD: (None, None),
    classes.Symbol.USDCHF: (None, None),
    classes.Symbol.USDJPY: (None, None),
    classes.Symbol.USDRUB: (None, None),
    classes.Symbol.XAGUSD: (None, None),
    classes.Symbol.XAUUSD: (None, None)
}

# lock = threading.Lock()

timer: threading.Timer = None


def process_price_data(symbol, access_type):
    symbol_last_datetime = db_time_ranges[symbol][1]

    if access_type == AccessType.bitfinex:
        sorted_items = parsers.parse_bitfinex(symbol, symbol_last_datetime)

    elif access_type == AccessType.alphavantage:
        sorted_items = parsers.parse_alphavantage(symbol, symbol_last_datetime)

    elif access_type == AccessType.investing:
        sorted_items = parsers.parse_investing(
            symbol, symbol_last_datetime)
    else:
        return

    sql_connection = sqlite3.connect(DB_SYMBOLS_PATH)
    cur = sql_connection.cursor()

    try:
        for price_item in sorted_items:
            utc_date = price_item[0]
            if symbol_last_datetime.replace(tzinfo=None) > utc_date.replace(tzinfo=None):
                continue

            utc_date_str = utc_date.strftime(config.DB_DATE_FORMAT)

            exec_string = f"INSERT INTO {symbol} VALUES ('{utc_date_str}',{price_item[1]},{price_item[2]},{price_item[3]},{price_item[4]}) ON CONFLICT(DateTime) DO UPDATE SET Close=excluded.Close"
            cur.execute(exec_string)
    except Exception as ex:
        logging.info('process_price_data: %s', ex)
    finally:
        sql_connection.commit()
        sql_connection.close()


def update_db_time_ranges():
    for symbol in db_time_ranges:
        update_db_time_range(symbol)


def update_db_time_range(symbol):
    sql_connection = sqlite3.connect(DB_SYMBOLS_PATH)
    cur = sql_connection.cursor()
    try:
        # Fill the DB first, this code thinks it will return something anyway
        exec_string = f"SELECT [DateTime] From {symbol} ORDER BY [DateTime]"

        dates = db_time_ranges[symbol]
        date_start = None

        if dates[0] is None:
            result = cur.execute(exec_string).fetchall()[0][0]
            date_start = helper.str_to_utc_datetime(
                result, "UTC", config.DB_DATE_FORMAT)
        else:
            date_start = dates[0]

        exec_string = f"{exec_string} DESC"
        result = cur.execute(exec_string).fetchall()[0][0]
        date_end = helper.str_to_utc_datetime(
            result, "UTC", config.DB_DATE_FORMAT)

        db_time_ranges[symbol] = (date_start, date_end)

        dates = f"symbol:{symbol}, date_start: {date_start}, date_end: {date_end}"
        print(dates)
        logging.info(dates)
    except Exception as ex:
        logging.info('update_db_time_range: %s', ex)
    finally:
        sql_connection.close()


def poll_symbols():
    while POLL_WORK_FLAG:

        for symbol in parsers.investing_symbol_api_mapping:
            process_price_data(symbol, AccessType.investing)

        for symbol in parsers.crypto_symbol_api_mapping:
            process_price_data(symbol, AccessType.bitfinex)

        for symbol in parsers.symbol_api_mapping:
            try_request = True
            while try_request:
                try:
                    # Naughty API, may behave badly sometimes
                    process_price_data(symbol, AccessType.alphavantage)
                    try_request = False
                except Exception as ex:
                    logging.info(
                        'poll_symbols, alphavantage, symbol %s, error: %s', symbol, ex)
                    poll_event.clear()
                    poll_event.wait(POLL_THROTTLE_SEC)

            poll_event.clear()
            poll_event.wait(POLL_THROTTLE_SEC)

        update_db_time_ranges()
        poll_event.clear()
        poll_event.wait(POLL_INTERVAL_SEC)


def start_poll():
    poll_thread = Thread(target=poll_symbols)
    poll_thread.start()


def main_exec():

    logging.basicConfig(filename='db_poll.log', encoding='utf-8',
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
    update_db_time_ranges()
    start_poll()


if __name__ == "__main__":
    helper.run_as_daemon_until_press_any_key(main_exec)
    POLL_WORK_FLAG = False
    poll_event.set()