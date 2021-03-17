import threading
import time
import sqlite3
import fileinput
import helper
import pytz
import datetime
import decimal
import os
import classes
from dotenv import load_dotenv
from helper import get_array_item_contains_key
import requests
from time import sleep
from threading import Thread
import json
import logging

load_dotenv()
DT_INPUT_FORMAT = r"%Y.%m.%dT%H:%M:%S.%f"
DT_INPUT_TIMEZONE = "EET"
POLL_INPUT_FORMAT = r"%Y-%m-%d %H:%M:%S"
DB_DATE_FORMAT = r"%Y-%m-%d %H:%M:%S+00:00"
DB_PATH = os.getenv("db_path")
API_KEY = os.getenv("api_key")
BASE_URL = f"https://www.alphavantage.co/query?&interval=1min&apikey={API_KEY}"
FX_URL = f"{BASE_URL}&function=FX_INTRADAY"
CRYPTO_URL = f"https://api-pub.bitfinex.com/v2/candles/trade:1m:"

poll_work_flag = True
poll_thread: Thread = None
poll_event = threading.Event()
poll_interval_sec = 60 * 60
poll_throttle_sec = 2 * 60
api_extended_poll_threshold_min = 90

commit_throttle = 1000000

symbol_api_mapping = {
    classes.Symbol.AUDUSD: f"{FX_URL}&from_symbol=AUD&to_symbol=USD",
    classes.Symbol.EURUSD: f"{FX_URL}&from_symbol=EUR&to_symbol=USD",
    classes.Symbol.GBPUSD: f"{FX_URL}&from_symbol=GBP&to_symbol=USD",
    classes.Symbol.NZDUSD: f"{FX_URL}&from_symbol=NZD&to_symbol=USD",
    classes.Symbol.USDCAD: f"{FX_URL}&from_symbol=USD&to_symbol=CAD",
    classes.Symbol.USDCHF: f"{FX_URL}&from_symbol=USD&to_symbol=CHF",
    classes.Symbol.USDJPY: f"{FX_URL}&from_symbol=USD&to_symbol=JPY",
    classes.Symbol.USDRUB: f"{FX_URL}&from_symbol=USD&to_symbol=RUB",
    # classes.Symbol.XAGUSD: f"{BASE_URL}&function=TIME_SERIES_INTRADAY&symbol=SILVER",
    classes.Symbol.XAUUSD: f"{BASE_URL}&function=TIME_SERIES_INTRADAY&symbol=GOLD"
}

crypto_symbol_api_mapping = {
    classes.Symbol.BTCUSD: f"{CRYPTO_URL}tBTCUSD/hist"}

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
    # classes.Symbol.XAGUSD: (None, None),
    classes.Symbol.XAUUSD: (None, None)
}

# lock = threading.Lock()

timer: threading.Timer = None


def import_csv(symbol, input_file):
    sql_connection = sqlite3.connect(DB_PATH)
    cur = sql_connection.cursor()

    count = 0
    high_prev: decimal.Decimal = None
    low_prev: decimal.Decimal = None
    open_prev: decimal.Decimal = None
    rounded_min_datetime_prev: datetime.datetime = None

    symbol_last_datetime = db_time_ranges[symbol][1]

    # parse ordered datetimes only. You can export them from MetaTrader
    for line in fileinput.input([input_file]):

        array = str(line).split("\t")

        if len(array) < 4:
            continue

        bid = helper.str_to_decimal(array[2])
        if bid == None:
            continue

        try:
            date = helper.str_to_utc_datetime(
                f'{array[0]}T{array[1]}', DT_INPUT_TIMEZONE, DT_INPUT_FORMAT)

            if date == None or symbol_last_datetime > date:
                continue

            rounded_min_datetime = date - \
                datetime.timedelta(seconds=date.second) - \
                datetime.timedelta(microseconds=date.microsecond)
            iso_date = rounded_min_datetime.isoformat(" ")

            high_ = bid
            low_ = bid
            open_ = bid

            exec_string = None

            if rounded_min_datetime_prev == rounded_min_datetime:
                if bid < high_prev:
                    high_ = high_prev

                if bid > low_prev:
                    low_ = low_prev
                open_ = open_prev

                exec_string = f"UPDATE {symbol} SET High={high_}, Low={low_}, Close={bid} WHERE [DateTime]='{iso_date}'"
            else:
                exec_string = f"INSERT INTO {symbol} VALUES ('{iso_date}',{bid},{bid},{bid},{bid}) ON CONFLICT(DateTime) DO UPDATE SET Close=excluded.Close"

            cur.execute(exec_string)
            count += 1
            if count % commit_throttle == 0:
                sql_connection.commit()
                print("Count %s, symbol %s" % (count, symbol))

            rounded_min_datetime_prev = rounded_min_datetime
            high_prev = high_
            low_prev = low_
            open_prev = open_

        except Exception as ex:
            print("Error %s" % ex)
            continue
    sql_connection.commit()
    sql_connection.close()


def import_all_example():
    print("Importing...")
    import_csv(classes.Symbol.AUDUSD,
               r"E:\latest\AUDUSD_202103121800_202103172342.csv")

    import_csv(classes.Symbol.EURUSD,
               r"E:\latest\EURUSD_202103121800_202103172343.csv")

    import_csv(classes.Symbol.GBPUSD,
               r"E:\latest\GBPUSD_202103121800_202103172343.csv")

    import_csv(classes.Symbol.NZDUSD,
               r"E:\latest\NZDUSD_202103121800_202103172343.csv")

    import_csv(classes.Symbol.USDCAD,
               r"E:\latest\USDCAD_202103121800_202103172343.csv")

    import_csv(classes.Symbol.USDCHF,
               r"E:\latest\USDCHF_202103121800_202103172343.csv")

    import_csv(classes.Symbol.USDJPY,
               r"E:\latest\USDJPY_202103121800_202103172344.csv")

    import_csv(classes.Symbol.USDRUB,
               r"E:\latest\USDRUB_202103150900_202103171729.csv")

    import_csv(classes.Symbol.XAGUSD,
               r"E:\latest\XAGUSD_202103121800_202103172254.csv")

    import_csv(classes.Symbol.XAUUSD,
               r"E:\latest\XAUUSD_202103121800_202103172254.csv")
    print("Done")


def process_price_data(symbol, price_data, use_ctypto):
    if use_ctypto:
        sorted_items = price_data
    else:
        meta = get_array_item_contains_key(price_data, "meta")
        timezone = get_array_item_contains_key(meta, "time zone")
        price_data = get_array_item_contains_key(price_data, "series")
        sorted_items = sorted(price_data.keys())

    sql_connection = sqlite3.connect(DB_PATH)
    cur = sql_connection.cursor()
    symbol_last_datetime = db_time_ranges[symbol][1]

    try:
        for price_item in sorted_items:
            if use_ctypto:
                utc_date = datetime.datetime.utcfromtimestamp(
                    price_item[0] / 1000)
            else:
                utc_date = helper.str_to_utc_datetime(
                    price_item, timezone, POLL_INPUT_FORMAT)

            if symbol_last_datetime.replace(tzinfo=None) > utc_date.replace(tzinfo=None):
                continue

            utc_date_str = utc_date.strftime(DB_DATE_FORMAT)

            if use_ctypto:
                open_ = price_item[1]
                close = price_item[2]
                high = price_item[3]
                low = price_item[4]
            else:
                values = price_data[price_item]
                open_ = get_array_item_contains_key(values, "open")
                high = get_array_item_contains_key(values, "high")
                low = get_array_item_contains_key(values, "low")
                close = get_array_item_contains_key(values, "close")

            exec_string = f"INSERT INTO {symbol} VALUES ('{utc_date_str}',{open_},{high},{low},{close}) ON CONFLICT(DateTime) DO UPDATE SET Close=excluded.Close"
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
    sql_connection = sqlite3.connect(DB_PATH)
    cur = sql_connection.cursor()
    try:
        # Fill the DB first, this code thinks it will return something anyway
        exec_string = f"SELECT [DateTime] From {symbol} ORDER BY [DateTime]"

        dates = db_time_ranges[symbol]
        date_start = None

        if dates[0] == None:
            result = cur.execute(exec_string).fetchall()[0][0]
            date_start = helper.str_to_utc_datetime(
                result, "UTC", DB_DATE_FORMAT)
        else:
            date_start = dates[0]

        exec_string = f"{exec_string} DESC"
        result = cur.execute(exec_string).fetchall()[0][0]
        date_end = helper.str_to_utc_datetime(
            result, "UTC", DB_DATE_FORMAT)

        db_time_ranges[symbol] = (date_start, date_end)

        dates = f"symbol:{symbol}, date_start: {date_start}, date_end: {date_end}"
        print(dates)
        logging.info(dates)
    except Exception as ex:
        logging.info('update_db_time_range: %s', ex)
    finally:
        sql_connection.close()


def get_lag_mins(symbol):
    symbol_last_datetime = db_time_ranges[symbol][1].replace(
        tzinfo=None)
    lag = (datetime.datetime.utcnow() - symbol_last_datetime).total_seconds()

    return int(lag / 60)


def poll_symbols():
    while poll_work_flag:

        for symbol in crypto_symbol_api_mapping:

            lag = get_lag_mins(symbol) + 1
            url = f"{crypto_symbol_api_mapping[symbol]}?limit={lag}"
            r = requests.get(crypto_symbol_api_mapping[symbol])
            content = r.text
            price_data = json.loads(content)
            process_price_data(symbol, price_data, True)

        for symbol in symbol_api_mapping:

            lag = get_lag_mins(symbol)
            url = symbol_api_mapping[symbol]
            if lag > api_extended_poll_threshold_min:
                url = f"{url}&outputsize=full"

            try_request = True
            while try_request:
                try:
                    r = requests.get(url)
                    content = r.text
                    price_data = json.loads(content)
                    process_price_data(symbol, price_data, False)
                    try_request = False
                except Exception as ex:
                    logging.info(
                        'poll_symbols, symbol_api_mapping, symbol %s, error: %s', symbol, ex)
                    poll_event.clear()
                    poll_event.wait(poll_throttle_sec)

            poll_event.clear()
            poll_event.wait(poll_throttle_sec)

        update_db_time_ranges()
        poll_event.clear()
        poll_event.wait(poll_interval_sec)


def start_poll():
    poll_thread = Thread(target=poll_symbols)
    poll_thread.start()


if __name__ == "__main__":

    logging.basicConfig(filename='db_poll.log', encoding='utf-8',
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
    update_db_time_ranges()
    start_poll()
    print("Press any key to exit")
    input()
    poll_stop_flag = False
    poll_event.set()
