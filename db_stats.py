import asyncio
import sqlite3
import threading
import logging
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

STATS_COLLECT_SEC = 2*60*60  # 2 hours
STATS_COLLECT_LOOP_GAP_SEC = 1*60  # 1 minute
lock = threading.Lock()


async def process_history(wait_event: threading.Event):
    while not wait_event.is_set():
        try:
            await asyncio.sleep(5)  # wait for data
            analyze_history(wait_event)
        except Exception as ex:
            logging.error('analyze_history: %s, error: %s',
                          ex, traceback.format_exc())
        try:
            await download_history(wait_event)
        except Exception as ex:
            logging.error('download_history: %s, error: %s',
                          ex, traceback.format_exc())
        wait_event.clear()
        wait_event.wait(STATS_COLLECT_SEC)


def analyze_channel(wait_event: threading.Event, channel_id):
    out_path = os.path.join(config.CHANNELS_HISTORY_DIR, f"{channel_id}.json")
    messages = config.get_json(out_path)
    if messages is None or len(messages) < 1:
        logging.info('analyze_channel: no data from %s', out_path)

    ordered_messges = sorted(messages, key=lambda x: x["id"], reverse=False)

    min_channel_date = helper.str_to_utc_datetime(
        ordered_messges[0]["date"])
    max_channel_date = helper.str_to_utc_datetime(
        ordered_messges[len(ordered_messges)-1]["date"])

    order_book = dict()

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

        logging.info('analyze_channel: id: %s, symbol: %s, start: %s, end: %s',
                     channel_id, symbol, min_date_rounded_minutes, max_date_rounded_minutes)

        res = signal_parser.analyze_channel_symbol(
            ordered_messges, symbol, min_date_rounded_minutes, max_date_rounded_minutes)

        order_book[symbol] = res

        if wait_event.is_set():
            return order_book
    return order_book


def analyze_history(wait_event: threading.Event):
    # gold like one of
    min_date = db_poll.db_time_ranges[classes.Symbol.XAUUSD][0]

    if min_date is None:
        logging.info('analyze_channel: symbol data is not loaded yet')
        return

    # exec_string = "SELECT Id FROM Channel WHERE HistoryLoaded = 1 AND (HistoryAnalyzed <> 1 OR HistoryAnalyzed IS NULL) "
    # channels_ids = None
    # with classes.SQLite(config.DB_STATS_PATH, 'download_history, db:', None) as cur:
    #     channels_ids = cur.execute(exec_string).fetchall()

    # 1295992076
    # 1428566201
    # analyze_channel(wait_event, 1289623401)
    analyze_channel(wait_event, 1125658955)
    # for channel_id in channels_ids:
    #     analyze_channel(wait_event, channel_id[0])


async def bulk_exit(client):
    exec_string = "SELECT Id, AccessLink FROM Channel WHERE HistoryLoaded = 1 ORDER BY HistoryUpdateDate DESC"
    with classes.SQLite(config.DB_STATS_PATH, 'bulk_exit, db:', None) as cur:
        channels = cur.execute(exec_string).fetchall()
        for channel in channels:
            channel_id = channel[0]
            channel_link = channel[1]
            await forwarder.exit_if_needed(channel_link, channel_id, client)


async def download_history(wait_event: threading.Event):
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

        if wait_event.is_set():
            return

        wait_event.clear()
        wait_event.wait(STATS_COLLECT_LOOP_GAP_SEC)


def main_exec(wait_event: threading.Event):
    asyncio.run(process_history(wait_event))


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
