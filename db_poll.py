import logging
import threading
from enum import Enum
from threading import Thread

import classes
import config
import helper
import parsers

DT_INPUT_FORMAT = r"%Y.%m.%dT%H:%M:%S.%f"

poll_event = threading.Event()
POLL_INTERVAL_SEC = 4 * 60 * 60
POLL_THROTTLE_SEC = 2 * 60
lock = threading.Lock()


class AccessType(Enum):
    alphavantage = 0
    bitfinex = 1
    investing = 2


db_time_ranges = {
    classes.Symbol.AUDUSD: (None, None, None),
    classes.Symbol.BTCUSD: (None, None, None),
    classes.Symbol.EURUSD: (None, None, None),
    classes.Symbol.GBPUSD: (None, None, None),
    classes.Symbol.NZDUSD: (None, None, None),
    classes.Symbol.USDCAD: (None, None, None),
    classes.Symbol.USDCHF: (None, None, None),
    classes.Symbol.USDJPY: (None, None, None),
    classes.Symbol.USDRUB: (None, None, None),
    classes.Symbol.XAGUSD: (None, None, None),
    classes.Symbol.XAUUSD: (None, None, None)
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

    with classes.SQLite(config.DB_SYMBOLS_PATH, 'process_price_data:', lock) as cur:
        for price_item in sorted_items:
            utc_date = price_item[0]
            if symbol_last_datetime.replace(tzinfo=None) > utc_date.replace(tzinfo=None):
                continue

            utc_date_str = utc_date.strftime(config.DB_DATE_FORMAT)

            exec_string = f"INSERT INTO {symbol} VALUES ('{utc_date_str}',{price_item[1]},{price_item[2]},{price_item[3]},{price_item[4]}) ON CONFLICT(DateTime) DO UPDATE SET Close=excluded.Close"
            cur.execute(exec_string)


def update_db_time_ranges():
    for symbol in db_time_ranges:
        update_db_time_range(symbol)


def update_db_time_range(symbol):
    with classes.SQLite(config.DB_SYMBOLS_PATH, 'update_db_time_range:', None) as cur:
        # Fill the DB first, this code thinks it will return something anyway
        exec_string = f"SELECT [DateTime], Close From {symbol} ORDER BY [DateTime]"

        dates = db_time_ranges[symbol]
        date_start = None

        if dates[0] is None:
            first_row = cur.execute(exec_string).fetchone()
            result = first_row[0]
            last_price = first_row[1]
            date_start = helper.str_to_utc_datetime(
                result, "UTC", config.DB_DATE_FORMAT)
        else:
            date_start = dates[0]

        exec_string = f"{exec_string} DESC"
        result = cur.execute(exec_string).fetchone()
        date_end = helper.str_to_utc_datetime(
            result[0], "UTC", config.DB_DATE_FORMAT)
        last_price = result[1]

        db_time_ranges[symbol] = (date_start, date_end, last_price)

        dates = f"symbol:{symbol}, date_start: {date_start}, date_end: {date_end}"
        logging.info(dates)


def poll_symbols(signal_event: threading.Event):
    while not signal_event.is_set():

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


def main_exec(wait_event: threading.Event):
    update_db_time_ranges()
    poll_thread = Thread(target=poll_symbols, args=[wait_event])
    poll_thread.start()
    wait_event.wait()
    poll_event.set()
