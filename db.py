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
import requests

load_dotenv()
DT_INPUT_FORMAT = r"%Y.%m.%dT%H:%M:%S.%f"
DT_INPUT_TIMEZONE = "EET"
DB_PATH = os.getenv("db_path")
API_KEY = os.getenv("api_key")

FX_URL = f"https://cloud.iexapis.com/v1/query?function=FX_INTRADAY&interval=1min&apikey={API_KEY}&datatype=csv"

symbol_api_mapping = {
    classes.Symbol.AUDUSD: f"{FX_URL}&from_symbol=AUD&to_symbol=USD",
    classes.Symbol.BTCUSD: f"{FX_URL}&from_symbol=BTC&to_symbol=USD"}

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
            if count % 1000000 == 0:
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


def poll_symbols():
    url = symbol_api_mapping[classes.Symbol.AUDUSD]
    r = requests.get(url)
    content = r.content


def start_poll():
    timer = threading.Timer(10, poll_symbols)
    timer.start()


def stop_poll():
    timer.cancel()
    return timer


if __name__ == "__main__":
    url = "https://apidojo-yahoo-finance-v1.p.rapidapi.com/stock/v3/get-historical-data"

    querystring = {"symbol": "GC=F", "region": "US"}

    headers = {
        'x-rapidapi-key': "49e23e5256mshd03ebc7d3cf773dp1c16f4jsnb3ee71622514",
        'x-rapidapi-host': "apidojo-yahoo-finance-v1.p.rapidapi.com"
    }

    response = requests.request(
        "GET", url, headers=headers, params=querystring)

    print(response.text)

    start_poll()
    print("Press any key to exit")
    input()
    stop_poll()
