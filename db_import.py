import datetime
import decimal
import fileinput
import sqlite3
import os
import re
import requests
import forwarder
import classes
from pyquery import PyQuery

from dotenv import load_dotenv

import config
import helper
import time
import sys
import time
import sys
import concurrent.futures

load_dotenv()
DT_INPUT_FORMAT = r"%Y.%m.%dT%H:%M:%S.%f"
POST_ID_REGEX = r"posts\/(\d+)"
DT_INPUT_TIMEZONE = "EET"
COMMIT_BATCH_ROW_COUNT = 1000000
TELEMETR_BASE_URL = "https://telemetr.io/post-list-ajax"


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


def get_telemetr_url(channel_id, offset, type_query="all"):
    # type_query="deleted|all"
    telemetr_url = f"{TELEMETR_BASE_URL}/{channel_id}?sort=-date&postType={type_query}&before={offset}&period=365"
    return telemetr_url


def telemetr_parse_history(channel_id, start=900, end=6600, step=100):
    messages = list()
    got_text = False
    for offset in range(start, end, step):
        url = get_telemetr_url(channel_id, offset)

        request = None
        total_sec_wait = 5
        while request is None or not request.ok:
            try:
                request = requests.get(url)
                if request.ok:
                    break
            except Exception as ex:
                print(f"Request has failed: {ex}")
            time.sleep(total_sec_wait)
            total_sec_wait = total_sec_wait*2                

        # print(f"{100*(offset-start)/(end-start):3.0f}%", end='\r')
        html = request.text
        if html is None or html == "":
            if got_text:
                break
            else:
                continue

        sys.stdout.write(f"\r{100*(offset-start)/(end-start):3.0f}% %i" % i)
        sys.stdout.flush()

        tags = PyQuery(html)('div[id^="post-"]')
        for tag in tags:
            try:
                msg_datetime = PyQuery(tag)("a.date-link time[datetime]")
                if len(msg_datetime) != 1:
                    continue

                msg_datetime = msg_datetime[0].attrib["datetime"]
                views = int(PyQuery(tag)("div.views").text().replace(" ",""))

                if views < 20:  # We treat this like a mistake and don't take in account such messages
                    continue

                onclick_value = PyQuery(tag)("div.post-link-icon-btn")[0].attrib["onclick"]
                id_search = re.search(POST_ID_REGEX, onclick_value)
                if id_search is None:
                    continue            

                id_ =  int(id_search.groups(1)[0].replace(" ",""))
                is_deleted = len(PyQuery(tag)("div.post-deleted-block")) > 0
                if is_deleted:
                    continue
                text = PyQuery(tag)("div.post-block__content_message").text()
                message_links = re.findall(
                    forwarder.LINKS_REGEX, text, re.IGNORECASE)

                if len(message_links) > 0:
                    continue

            except Exception as ex:
                print(f"Cannot parse {tag}, error {ex}")
                continue
            tag_dict = {"date": msg_datetime,
                        "id": id_, "text": text, "views": views}
            messages.append(tag_dict)
    return messages

def save_id(id):
    history_data = telemetr_parse_history(id,1000,10000)
    res_dict = dict()
    for item in  history_data:
        res_dict[item["id"]] = item

    config.set_json(f"D:/parsed_new/{id}.json",  sorted(res_dict.values(), key=lambda x: x["id"], reverse=False))
    
def bulk_query(array):
    len_arr = len(array)
    for idO in array:
        print(f"channel id: {idO}, len {len_arr}")
        save_id(idO)

if __name__ == "__main__":

    array = [
1428566201
]

    total = len(array)
    count = 0
 
    STEP = 50
    with concurrent.futures.ThreadPoolExecutor(STEP) as executor:
        futures = list()
        for current_num in range(0, total, STEP):
            local_part = array[current_num:current_num+STEP]
            futures.append(executor.submit(bulk_query, local_part))

        _, _ = concurrent.futures.wait(futures)

    # for idO in array:
    #     print(f"channel id: {idO}, {count} from {total}")
    #     save_id(idO)
    #     count = count+1

    #import_all_example()
    
    # exec_string = f"SELECT Id FROM Channel Where Id<> {config.PINNED_EXCEPT_CHANNEL_ID}"
    # channels_ids = None
    # with classes.SQLite(config.DB_STATS_PATH, 'download_history, db:', None) as cur:
    #     channels_ids = cur.execute(exec_string).fetchall()

    # for channel_id in channels_ids:
    #     channel_id = channel_id[0]
    #     history_data = telemetr_parse_history(channel_id, 30000, 0, 1000)
    #     sorted_data = sorted(history_data, key=lambda x: x["id"], reverse=False)   
    #     config.set_json(f"E:/history/{channel_id}.json", sorted_data)

    # E:\history_new
    # main_folder = "E:/history"
    # main_folder_new = "E:/history_new"
    # files = os.listdir(main_folder)
    # for file_item in files:
    #     file_item_path = os.path.join(main_folder, file_item)
    #     file_item_new_path = os.path.join(main_folder_new, file_item)
    #     json_file = config.get_json(file_item_path)
    #     distinct_dict = dict()
    #     distinct_list = list()
    #     for json_item in json_file:
    #         id_ = json_item["id"]
    #         if id_ in distinct_dict:
    #             continue

    #         date_ = json_item["date"].replace("+00:00","").replace("Z","")
    #         date_string = helper.str_to_utc_iso_datetime(date_,"UTC", config.ISO_DATE_IMPORT_FORMAT)
    #         json_item["date"] = date_string
    #         distinct_dict[id_] = json_item
    #         distinct_list.append(json_item)

    #     config.set_json(file_item_new_path, distinct_list)

    print("Telegram history json import and conversion")
    print("Enter input file path")
    input_file = input()
    print(f"Enter timezone of the file, for ex. {config.DT_INPUT_TIMEZONE}")
    timezone_local = input()
    convert_history_json(input_file, timezone_local)
    print("Done, press any key to exit")
    input()
