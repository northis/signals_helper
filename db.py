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
DB_PATH = os.getenv("db_path")
API_KEY = os.getenv("api_key")
BASE_URL = f"https://www.alphavantage.co/query?&interval=1min&apikey={API_KEY}"
FX_URL = f"{BASE_URL}&function=FX_INTRADAY"
CRYPTO_URL = f"https://api-pub.bitfinex.com/v2/candles/trade:1m:"

poll_work_flag = True
poll_tread: Thread = None
poll_event = threading.Event()
poll_interval_sec = 60 * 60

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
    classes.Symbol.XAGUSD: f"{BASE_URL}&function=TIME_SERIES_INTRADAY&symbol=SILVER",
    classes.Symbol.XAUUSD: f"{BASE_URL}&function=TIME_SERIES_INTRADAY&symbol=GOLD"
}

crypto_symbol_api_mapping = {
    classes.Symbol.BTCUSD: f"{CRYPTO_URL}tBTCUSD/hist"}

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

            if date == None:
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
               r"E:\AUDUSD_202001021000_202103122358.csv")

    import_csv(classes.Symbol.BTCUSD,
               r"E:\BTCUSD_202001021000_202103122057.csv")

    import_csv(classes.Symbol.EURUSD,
               r"E:\EURUSD_202001021000_202103122100.csv")

    import_csv(classes.Symbol.GBPUSD,
               r"E:\GBPUSD_202001021000_202103122110.csv")

    import_csv(classes.Symbol.NZDUSD,
               r"E:\NZDUSD_202001021000_202103112355.csv")

    import_csv(classes.Symbol.USDCAD,
               r"E:\USDCAD_202001021000_202103122329.csv")

    import_csv(classes.Symbol.USDCHF,
               r"E:\USDCHF_202001021000_202103122358.csv")

    import_csv(classes.Symbol.USDJPY,
               r"E:\USDJPY_202001021000_202103122103.csv")

    import_csv(classes.Symbol.USDRUB,
               r"E:\USDRUB_202001021000_202103121729.csv")

    import_csv(classes.Symbol.XAGUSD,
               r"E:\XAGUSD_202001021000_202103122048.csv")

    import_csv(classes.Symbol.XAUUSD,
               r"E:\XAUUSD_202001021000_202103122043.csv")
    print("Done")


def process_price_data(symbol, price_data):
    meta = get_array_item_contains_key(price_data, "meta")
    timezone = get_array_item_contains_key(meta, "time zone")
    prices = get_array_item_contains_key(price_data, "series")

    sql_connection = sqlite3.connect(DB_PATH)
    cur = sql_connection.cursor()

    try:
        for price_item in prices:
            utc_date = helper.str_to_utc_iso_datetime(
                price_item, timezone, POLL_INPUT_FORMAT)
            values = prices[price_item]
            open_ = get_array_item_contains_key(values, "open")
            high = get_array_item_contains_key(values, "high")
            low = get_array_item_contains_key(values, "low")
            close = get_array_item_contains_key(values, "close")
            exec_string = f"INSERT INTO {symbol} VALUES ('{utc_date}',{open_},{high},{low},{close}) ON CONFLICT(DateTime) DO UPDATE SET Close=excluded.Close"
            cur.execute(exec_string)
    except Exception as ex:
        logging.info('process_price_data: %s', ex)
    finally:
        sql_connection.commit()
        sql_connection.close()


def process_price_data_crypto(symbol, price_data):
    sql_connection = sqlite3.connect(DB_PATH)
    cur = sql_connection.cursor()

    try:
        for price_item in price_data:
            utc_date = datetime.datetime.utcfromtimestamp(
                price_item[0]/1000).strftime(DT_INPUT_FORMAT)

            open_ = price_item[1]
            close = price_item[2]
            high = price_item[3]
            low = price_item[4]
            exec_string = f"INSERT INTO {symbol} VALUES ('{utc_date}',{open_},{high},{low},{close}) ON CONFLICT(DateTime) DO UPDATE SET Close=excluded.Close"
            cur.execute(exec_string)
    except Exception as ex:
        logging.info('process_price_data_crypto: %s', ex)
    finally:
        sql_connection.commit()
        sql_connection.close()


def poll_symbols():
    while poll_work_flag:
        for symbol in symbol_api_mapping:

            r = requests.get(symbol_api_mapping[symbol])
            content = r.text
            price_data = json.loads(content)
            process_price_data(symbol, price_data)

        for symbol in crypto_symbol_api_mapping:
            r = requests.get(crypto_symbol_api_mapping[symbol])
            content = r.text
            price_data = json.loads(content)
            process_price_data_crypto(symbol, price_data)

        poll_event.wait(poll_interval_sec)
        poll_event.clear()


def start_poll():
    poll_tread = Thread(target=poll_symbols, daemon=True)
    poll_tread.start()


if __name__ == "__main__":
    start_poll()
    print("Press any key to exit")
    input()
    poll_stop_flag = False
    poll_event.set()
    poll_tread.join()
