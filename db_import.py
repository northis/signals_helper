import datetime
import decimal
import fileinput
import os
import sqlite3

from dotenv import load_dotenv

import config
import helper

load_dotenv()
DT_INPUT_FORMAT = r"%Y.%m.%dT%H:%M:%S.%f"
DT_INPUT_TIMEZONE = "EET"
DB_SYMBOLS_PATH = os.getenv("db_symbols_path")
DB_STATS_PATH = os.getenv("db_stats_path")
COMMIT_BATCH_ROW_COUNT = 1000000


def import_csv(symbol, input_file, symbol_last_datetime):
    # symbol_last_datetime = db_time_ranges[symbol][1] for ex.
    sql_connection = sqlite3.connect(DB_SYMBOLS_PATH)
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
        if bid is None:
            continue

        try:
            date = helper.str_to_utc_datetime(
                f'{array[0]}T{array[1]}', DT_INPUT_TIMEZONE, DT_INPUT_FORMAT)

            if date is None or symbol_last_datetime > date:
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
            if count % COMMIT_BATCH_ROW_COUNT == 0:
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


def import_json(input_file):
    sql_connection = sqlite3.connect(DB_STATS_PATH)
    cur = sql_connection.cursor()

    json_array = config.get_json(input_file)

    for link_item in json_array:
        id_ = link_item["id"]
        add_date_utc = link_item.get("add_date_utc")

        if add_date_utc is None:
            add_date_utc = link_item.get("add_date_utc:")
            if add_date_utc is None:
                add_date_utc = "NULL"

        change_date_utc = link_item.get("change_date_utc")

        if change_date_utc is None:
            change_date_utc = link_item.get("change_date_utc:")
            if change_date_utc is None:
                change_date_utc = "NULL"

        access_url = link_item["access_url"]
        name = link_item["name"].replace("'", "''")
        exec_string = f"INSERT INTO Channel VALUES ({id_},'{name}','{access_url}','{add_date_utc}','{change_date_utc}')"
        cur.execute(exec_string)

    sql_connection.commit()
    sql_connection.close()


def import_all_example():
    print("Importing...")
    # import_csv(classes.Symbol.AUDUSD, r"E:\latest\AUDUSD_202103121800_202103172342.csv")
    print("Done")
