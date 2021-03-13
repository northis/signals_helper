import sqlite3
import fileinput
import helper
import pytz
import datetime
import decimal
import classes
import concurrent.futures

DT_INPUT_FORMAT = r"%Y.%m.%dT%H:%M:%S.%f"
DT_INPUT_TIMEZONE = "EET"
DB_PATH = "E:\symbols.db"


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
                datetime.timedelta(minutes=date.minute) - \
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
                exec_string = f"INSERT INTO {symbol} VALUES ('{iso_date}',{bid},{bid},{bid},{bid})"

            cur.execute(exec_string)
            count += 1

            rounded_min_datetime_prev = rounded_min_datetime
            high_prev = high_
            low_prev = low_
            open_prev = open_

            if count % 100000 == 0:
                sql_connection.commit()
        except Exception as ex:
            print("Error %s" % ex)
            continue
    sql_connection.commit()
    sql_connection.close()


if __name__ == "__main__":

    print("Importing...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=11) as executor:
        futures = []
        futures.append(executor.submit(import_csv, classes.Symbol.AUDUSD,
                                       r"E:\AUDUSD_202001021000_202103122358.csv"))
        futures.append(executor.submit(import_csv, classes.Symbol.BTCUSD,
                                       r"E:\BTCUSD_202001021000_202103122057.csv"))

        futures.append(executor.submit(import_csv, classes.Symbol.EURUSD,
                                       r"E:\EURUSD_202001021000_202103122100.csv"))

        futures.append(executor.submit(import_csv, classes.Symbol.GBPUSD,
                                       r"E:\GBPUSD_202001021000_202103122110.csv"))

        futures.append(executor.submit(import_csv, classes.Symbol.NZDUSD,
                                       r"E:\NZDUSD_202001021000_202103112355.csv"))

        futures.append(executor.submit(import_csv, classes.Symbol.USDCAD,
                                       r"E:\USDCAD_202001021000_202103122329.csv"))

        futures.append(executor.submit(import_csv, classes.Symbol.USDCHF,
                                       r"E:\USDCHF_202001021000_202103122358.csv"))

        futures.append(executor.submit(import_csv, classes.Symbol.USDJPY,
                                       r"E:\USDJPY_202001021000_202103122103.csv"))

        futures.append(executor.submit(import_csv, classes.Symbol.USDRUB,
                                       r"E:\USDRUB_202001021000_202103121729.csv"))

        futures.append(executor.submit(import_csv, classes.Symbol.XAGUSD,
                                       r"E:\XAGUSD_202001021000_202103122048.csv"))

        futures.append(executor.submit(import_csv, classes.Symbol.XAUUSD,
                                       r"E:\XAUUSD_202001021000_202103122043.csv"))

        for future in concurrent.futures.as_completed(futures):
            print(future.result())

    print("Done")
