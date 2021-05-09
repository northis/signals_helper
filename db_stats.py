import asyncio
import sqlite3
import threading
import logging
import json
import traceback
import os
import datetime
from telethon import TelegramClient, errors, functions
import classes
import config

import helper
import forwarder
import db_poll
import signal_parser
from multiprocessing import Pool

STATS_COLLECT_SEC = 2*60*60  # 2 hours
STATS_COLLECT_LOOP_GAP_SEC = 1*60  # 1 minute
STATS_ANALYZE_LOOP_GAP_SEC = 10
lock = threading.Lock()
lock_increment = threading.Lock()
MAX_WORKERS = 1
BUSY_THREADS = 0
pool: Pool
WAIT_EVENT_INNER = threading.Event()
WAIT_EVENT_OUTER: threading.Event = None


def atomic_increment():
    lock_increment.acquire()
    global BUSY_THREADS
    BUSY_THREADS += 1
    lock_increment.release()


def atomic_decrement():
    lock_increment.acquire()
    global BUSY_THREADS
    BUSY_THREADS -= 1
    lock_increment.release()


def is_theads_busy():
    lock_increment.acquire()
    res = BUSY_THREADS >= MAX_WORKERS
    lock_increment.release()
    return res


async def process_history():
    global pool
    pool = Pool(MAX_WORKERS)
    while not WAIT_EVENT_OUTER.is_set():
        try:
            await asyncio.sleep(5)  # wait for data
            analyze_history()
        except Exception as ex:
            logging.error('analyze_history: %s, error: %s',
                          ex, traceback.format_exc())
        if WAIT_EVENT_OUTER.is_set():
            break
        try:
            await download_history()
        except Exception as ex:
            logging.error('download_history: %s, error: %s',
                          ex, traceback.format_exc())
        WAIT_EVENT_OUTER.clear()
        WAIT_EVENT_OUTER.wait(STATS_COLLECT_SEC)
    pool.close()
    WAIT_EVENT_INNER.set()


def analyze_channel(channel_id):
    out_path = os.path.join(config.CHANNELS_HISTORY_DIR, f"{channel_id}.json")
    messages = None
    try:
        messages = config.get_json(out_path)
    except Exception as ex:
        logging.error('analyze_channel: %s, error: %s',
                      ex, traceback.format_exc())
        with classes.SQLite(config.DB_STATS_PATH, 'analyze_channel_error:', lock) as cur:
            update_string = f"UPDATE Channel SET HistoryLoaded = 0 WHERE Id={channel_id}"
            cur.execute(update_string)
        return

    if messages is None or len(messages) < 1:
        logging.info('analyze_channel: no data from %s', out_path)

    ordered_messges = sorted(messages, key=lambda x: x["id"], reverse=False)

    min_channel_date = helper.str_to_utc_datetime(
        ordered_messges[0]["date"])
    max_channel_date = helper.str_to_utc_datetime(
        ordered_messges[len(ordered_messges)-1]["date"])

    for symbol in signal_parser.symbols_regex_map:
        min_date = db_poll.db_time_ranges[symbol][0]
        max_date = db_poll.db_time_ranges[symbol][1]

        if (min_channel_date > min_date):
            min_date = min_channel_date

        if (max_channel_date < max_date):
            max_date = max_channel_date

        min_date_rounded_minutes = min_date - datetime.timedelta(seconds=min_date.second,
                                                                 microseconds=min_date.microsecond)

        max_date_rounded_minutes = max_date - datetime.timedelta(seconds=max_date.second,
                                                                 microseconds=max_date.microsecond)

        while not WAIT_EVENT_OUTER.is_set():

            if is_theads_busy():
                WAIT_EVENT_INNER.wait(STATS_ANALYZE_LOOP_GAP_SEC)
            else:
                logging.info('analyze_channel: id: %s, symbol: %s, start: %s, end: %s',
                             channel_id, symbol, min_date_rounded_minutes, max_date_rounded_minutes)
                process_channel_typle = (ordered_messges,
                                         symbol, min_date_rounded_minutes, max_date_rounded_minutes, channel_id)
                pool.apply_async(process_channel_symbol, process_channel_typle)
                break

        if WAIT_EVENT_OUTER.is_set():
            return


def process_channel_symbol(
        ordered_messges,
        symbol,
        min_date_rounded_minutes,
        max_date_rounded_minutes,
        channel_id):
    try:
        atomic_increment()

        orders_list = signal_parser.analyze_channel_symbol(
            ordered_messges, symbol, min_date_rounded_minutes, max_date_rounded_minutes)

        if orders_list is None:
            return

        logging.info('analyze_channel: writing, symbol: %s, channel_id: %s, BUSY_THREADS: %s',
                     symbol, channel_id, BUSY_THREADS)

        with classes.SQLite(config.DB_STATS_PATH, 'process_channel_symbol:', lock) as cur:
            exec_string = f"DELETE FROM 'Order' WHERE IdChannel = {channel_id}"
            cur.execute(exec_string)

            for order in orders_list:
                params = {}
                params["IdOrder"] = order["id"]
                params["IdChannel"] = channel_id
                params["Symbol"] = symbol
                params["IsBuy"] = 1 if order["is_buy"] else 0
                params["Date"] = order["datetime"]
                params["PriceSignal"] = float(order["price_signal"])
                params["PriceActual"] = float(order["price_actual"])
                params["IsOpen"] = 1 if order["is_open"] else 0
                params["StopLoss"] = float(
                    order["stop_loss"]) if "stop_loss" in order else None
                params["TakeProfit"] = float(
                    order["take_profit"]) if "take_profit" in order else None
                params["CloseDate"] = order.get("close_datetime")
                params["ClosePrice"] = float(
                    order["close_price"]) if "close_price" in order else None
                params["ManualExit"] = 1 if "manual_exit" in order else 0
                params["SlExit"] = 1 if "sl_exit" in order else 0
                params["TpExit"] = 1 if "tp_exit" in order else 0
                params["ErrorState"] = ";".join(
                    order["error_state"]) if "error_state" in order else None

                columns = ', '.join(params.keys())
                placeholders = ':'+', :'.join(params.keys())
                exec_string = "INSERT INTO 'Order' (%s) VALUES (%s)" % (
                    columns, placeholders)
                cur.execute(exec_string, params)

            now_str = helper.get_now_utc_iso()
            update_string = f"UPDATE Channel SET HistoryAnalyzed = 1, HistoryAnalysisUpdateDate = '{now_str}' WHERE Id={channel_id}"
            cur.execute(update_string)
        logging.info('analyze_channel: writing done, symbol: %s, channel_id: %s, BUSY_THREADS: %s',
                     symbol, channel_id, BUSY_THREADS)

    finally:
        atomic_decrement()


def analyze_history():
    # gold like one of
    min_date = db_poll.db_time_ranges[classes.Symbol.XAUUSD][0]

    if min_date is None:
        logging.info('analyze_channel: symbol data is not loaded yet')
        return

    exec_string = "SELECT Id FROM Channel WHERE HistoryLoaded = 1 AND (HistoryAnalyzed <> 1 OR HistoryAnalyzed IS NULL)"
    channels_ids = None
    with classes.SQLite(config.DB_STATS_PATH, 'download_history, db:', None) as cur:
        channels_ids = cur.execute(exec_string).fetchall()

    channels_ready = 0
    for channel_id in channels_ids:
        local_channel_id = channel_id[0]
        analyze_channel(local_channel_id)
        channels_ready += 1
        if WAIT_EVENT_OUTER.is_set():
            return


async def bulk_exit(client):
    exec_string = "SELECT Id, AccessLink FROM Channel WHERE HistoryLoaded = 1 ORDER BY HistoryUpdateDate DESC"
    with classes.SQLite(config.DB_STATS_PATH, 'bulk_exit, db:', None) as cur:
        channels = cur.execute(exec_string).fetchall()
        for channel in channels:
            channel_id = channel[0]
            channel_link = channel[1]
            await forwarder.exit_if_needed(channel_link, channel_id, client)


async def download_history():
    exec_string = "SELECT Id, AccessLink FROM Channel WHERE HistoryLoaded IS NULL OR HistoryLoaded <> 1"
    channels = None

    with classes.SQLite(config.DB_STATS_PATH, 'download_history, db:', None) as cur:
        channels = cur.execute(exec_string).fetchall()

    async with TelegramClient(config.SESSION_FILE, config.api_id, config.api_hash) as client:
        for channel_item in channels:
            channel_id = channel_item[0]
            channel = await forwarder.get_in_channel(channel_id, client)
            channel_link = None
            if channel is None:
                channel_link = channel_item[1]
                channel = await forwarder.join_link(channel_link, client)
            if channel is None:
                logging.info(
                    'Channel id: %s, cannot join or already in', channel_id)
                continue

            messages_list = list()

            messages = await client.get_messages(channel, None)
            for message in messages:
                msg_dict = message.to_dict()
                msg_props = dict()
                msg_props["id"] = msg_dict["id"]
                msg_props["date"] = helper.datetime_to_utc_datetime(
                    msg_dict["date"]).isoformat()

                message_item = msg_dict.get("message")
                if message_item is not None:
                    msg_props["text"] = message_item

                reply_to_message = msg_dict.get("reply_to")
                if reply_to_message is not None:
                    msg_props["reply_to_msg_id"] = reply_to_message["reply_to_msg_id"]

                messages_list.append(msg_props)

            out_path = os.path.join(
                config.CHANNELS_HISTORY_DIR, f"{channel_id}.json")
            config.set_json(out_path, messages_list)
            now_str = helper.get_now_utc_iso()

            update_string = f"UPDATE Channel SET HistoryLoaded = 1, HistoryUpdateDate = '{now_str}' WHERE Id={channel_id}"

            with classes.SQLite(config.DB_STATS_PATH, 'download_history, db update:', lock) as cur:
                channels = cur.execute(update_string)

            if channel_link is not None:
                await forwarder.exit_if_needed(channel_link, channel_id, client)

        if WAIT_EVENT_OUTER.is_set():
            return

        WAIT_EVENT_OUTER.clear()
        WAIT_EVENT_OUTER.wait(STATS_COLLECT_LOOP_GAP_SEC)


def main_exec():
    asyncio.run(process_history())


def get_primary_message_id(id_message, id_channel):
    sql_connection = sqlite3.connect(config.DB_STATS_PATH)
    cur = sql_connection.cursor()
    try:
        exec_string = f"SELECT IdPrimary From ChannelMessageLink WHERE IdMessage = {id_message} AND IdChannel={id_channel}"
        result = cur.execute(exec_string).fetchone()

        if result is None:
            return None

        return result[0]

    except Exception as ex:
        logging.info('get_primary_message_id: %s', ex)
        return None
    finally:
        sql_connection.close()


def set_primary_message_id(id_primary, id_message, id_channel):
    sql_connection = sqlite3.connect(config.DB_STATS_PATH)
    cur = sql_connection.cursor()
    try:

        lock.acquire()
        exec_string = f"INSERT INTO ChannelMessageLink VALUES ({id_primary},{id_message},{id_channel}) ON CONFLICT(IdPrimary) DO UPDATE SET IdMessage=excluded.IdMessage, IdChannel=excluded.IdChannel"
        cur.execute(exec_string)
        sql_connection.commit()
        return (id_primary, id_message, id_channel)

    except Exception as ex:
        logging.info('set_primary_message_id: %s', ex)
        return None
    finally:
        sql_connection.close()
        lock.release()


def get_db_safe_title(title):
    return str(title).replace("'", "''")


def is_history_loaded(channel_id, url, title):
    upsert_res = upsert_channel(channel_id, url, title)
    if upsert_channel is None:
        return False

    got_history = upsert_res[5] is not None
    return got_history


def get_channel(access_url, title):
    if access_url is None and title is None:
        return None

    try:
        sql_connection = sqlite3.connect(config.DB_STATS_PATH)
        cur = sql_connection.cursor()
        select_str = "SELECT Id From Channel WHERE"
        if title is None:
            exec_string = f"{select_str} AccessLink = '{access_url}'"
        elif access_url is None:
            exec_string = f"{select_str} Name = '{get_db_safe_title(title)}'"
        else:
            exec_string = f"{select_str} AccessLink = '{access_url}' AND Name = '{get_db_safe_title(title)}'"

        result = cur.execute(exec_string)

        if result is None:
            return None

        return result.fetchone()
    except Exception as ex:
        logging.info('get_channel: %s', ex)
        return None
    finally:
        sql_connection.close()


def upsert_channel(id_, access_url, title):
    with classes.SQLite(config.DB_STATS_PATH, 'upsert_channel, db:', lock) as cur:
        exec_string = f"SELECT Name, AccessLink, CreateDate, UpdateDate, HistoryLoaded, HistoryUpdateDate, HistoryAnalyzed, HistoryAnalysisUpdateDate FROM Channel WHERE Id = {id_}"

        result = cur.execute(exec_string)
        title_safe = get_db_safe_title(title)
        now_str = helper.get_now_utc_iso()
        select_channel = result.fetchone()

        if access_url is None and title is None:
            return select_channel

        if select_channel is None:
            insert_string = f"INSERT INTO Channel VALUES ({id_},'{title_safe}','{access_url}','{now_str}', NULL, NULL, NULL, NULL, NULL) ON CONFLICT(Id) DO UPDATE SET UpdateDate=excluded.UpdateDate"
            cur.execute(insert_string)
            return (id_, title_safe, access_url, now_str, None, None, None, None, None)

        (name, link, create_date, update_date,
            history_loaded, history_update_date,
            history_analyzed, history_analysis_update_date) = select_channel

        if title_safe != name or access_url != link:
            update_string = f"UPDATE Channel SET Name='{title_safe}', AccessLink='{access_url}', UpdateDate='{now_str}' WHERE Id = {id_}"
            cur.execute(update_string)

        return (id_, title_safe, access_url, create_date, update_date, history_loaded, history_update_date,
                history_analyzed, history_analysis_update_date)
