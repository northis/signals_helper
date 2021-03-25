import asyncio
import datetime
import sqlite3
import threading
import logging
import os
from telethon import TelegramClient, errors
import config

import helper

DB_STATS_PATH = os.getenv("db_stats_path")
lock = threading.Lock()
CHANNELS_HISTORY_DIR = os.getenv("channels_history_dir")
api_id = os.getenv('api_id')
api_hash = os.getenv('api_hash')


async def download_history():

    sql_connection = sqlite3.connect(DB_STATS_PATH)
    cur = sql_connection.cursor()
    try:

        exec_string = "SELECT Id FROM Channel WHERE HistoryLoaded IS NULL OR HistoryLoaded <> 1"
        channels = cur.execute(exec_string).fetchall()
        async with TelegramClient(config.SESSION_FILE, api_id, api_hash) as client:
            for channel_item in channels:
                channel_id = channel_item[0]
                channel = None
                async for dialog in client.iter_dialogs():
                    if dialog.entity.id == channel_id:
                        channel = dialog
                        break
                if channel is None:
                    logging.info('Channel id: %s, join requred', channel_id)
                    # TODO add join here if needed
                    continue

                messages_list = list()

                messages = await client.get_messages(channel, None)
                for message in messages:
                    messages_list.append(message.to_dict()['message'])

                out_path = os.path.join(
                    CHANNELS_HISTORY_DIR, f"{channel_id}.json")
                config.set_json(out_path, messages_list)
                # Object of type Message is not JSON serializable
                await asyncio.sleep(10)

    except Exception as ex:
        logging.info('download_history: %s', ex)
    finally:
        sql_connection.close()


def download_history_sync():
    asyncio.run(download_history())


def get_primary_message_id(id_message, id_channel):
    sql_connection = sqlite3.connect(DB_STATS_PATH)
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
    sql_connection = sqlite3.connect(DB_STATS_PATH)
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
        lock.release()
        sql_connection.close()


def get_db_safe_title(title):
    return str(title).replace("'", "''")


def has_channel(access_url, title):
    if access_url is None and title is None:
        return None

    sql_connection = sqlite3.connect(DB_STATS_PATH)
    cur = sql_connection.cursor()
    try:

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
        logging.info('has_channel: %s', ex)
        return None
    finally:
        sql_connection.close()


def upsert_channel(id_, access_url, title):
    sql_connection = sqlite3.connect(DB_STATS_PATH)
    cur = sql_connection.cursor()
    try:
        exec_string = f"SELECT Name, AccessLink, CreateDate, UpdateDate From Channel WHERE Id = {id_}"

        lock.acquire()
        result = cur.execute(exec_string)
        title_safe = get_db_safe_title(title)
        now_str = helper.datetime_to_utc_datetime(
            datetime.datetime.utcnow()).isoformat()
        select_channel = result.fetchone()

        if select_channel is None:
            insert_string = f"INSERT INTO Channel VALUES ({id_},'{title_safe}','{access_url}','{now_str}',NULL) ON CONFLICT(Id) DO UPDATE SET UpdateDate=excluded.UpdateDate"
            cur.execute(insert_string)
            sql_connection.commit()
            return (id_, title_safe, access_url, now_str, None)

        (name, link, create_date, update_date) = select_channel

        if title_safe != name or access_url != link:
            update_string = f"UPDATE Channel SET Name='{title_safe}', AccessLink='{access_url}', UpdateDate='{now_str}'' WHERE Id = {id_}"
            cur.execute(update_string)
            sql_connection.commit()

        return (id_, title_safe, access_url, create_date, update_date)

    except Exception as ex:
        logging.info('upsert_channel: %s', ex)
        return (None, None, None, None, None)
    finally:
        lock.release()
        sql_connection.close()


if __name__ == "__main__":
    download_history_sync()
    # res1 = set_primary_message_id(1, 1, 1)
    # res2 = get_primary_message_id(1, 1)
    # res3 = get_primary_message_id(1, 2)
