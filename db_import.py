import datetime
import decimal
import fileinput
import sqlite3
import os
import classes

from dotenv import load_dotenv

import config
import helper

load_dotenv()
DT_INPUT_FORMAT = r"%Y.%m.%dT%H:%M:%S.%f"
DT_INPUT_TIMEZONE = "EET"
COMMIT_BATCH_ROW_COUNT = 1000000


def convert_history_json(file_in, timezone_local):
    json = config.get_json(file_in)
    json_array = json.get("messages")
    channel_id = json.get("id")
    if json_array is None or channel_id is None:
        print("Cannot parse Telegram history json")
        return

    messages_list = list()
    for message in json_array:
        msg_props = dict()
        msg_props["id"] = message["id"]
        msg_props["date"] = helper.str_to_utc_datetime(
            message["date"], timezone_local, config.ISO_LOCAL_DATE_FORMAT).isoformat()

        message_item = message.get("text")
        if message_item is not None:
            msg_props["text"] = str(message_item)

        reply_to_message_id = message.get("reply_to_message_id")
        if reply_to_message_id is not None:
            msg_props["reply_to_msg_id"] = reply_to_message_id

        messages_list.append(msg_props)

    out_path = os.path.join(
        config.CHANNELS_HISTORY_DIR, f"{channel_id}.json")
    config.set_json(out_path, messages_list)
    print(f"Saved as {out_path}")


def import_csv(symbol, input_file, symbol_last_datetime):
    # symbol_last_datetime = db_time_ranges[symbol][1] for ex.
    sql_connection = sqlite3.connect(config.DB_SYMBOLS_PATH)
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
    sql_connection = sqlite3.connect(config.DB_STATS_PATH)
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
    # import_csv(classes.Symbol.XAUUSD,
    #            r"E:\XAUUSD_202101040102_202104272037.csv", helper.str_to_utc_datetime("2021-01-02T00:00:00+00:00"))
    print("Done")


if __name__ == "__main__":
    # import_all_example()
    print("Telegram history json import and conversion")
    print("Enter input file path")
    input_file = input()
    print(f"Enter timezone of the file, for ex. {config.DT_INPUT_TIMEZONE}")
    timezone_local = input()
    convert_history_json(input_file, timezone_local)
    print("Done, press any key to exit")
    input()
