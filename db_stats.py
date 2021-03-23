import datetime
import sqlite3
import threading
import logging
import os

import config
import helper

DB_STATS_PATH = os.getenv("db_stats_path")
lock = threading.Lock()


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


def upsert_channel(id_, access_url, title):
    sql_connection = sqlite3.connect(DB_STATS_PATH)
    cur = sql_connection.cursor()
    try:
        exec_string = f"SELECT Name, AccessLink, CreateDate, UpdateDate From Channel WHERE Id = {id_}"

        lock.acquire()
        result = cur.execute(exec_string)
        title_safe = str(title).replace("'", "")
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
    res1 = set_primary_message_id(1, 1, 1)
    res2 = get_primary_message_id(1, 1)
    res3 = get_primary_message_id(1, 2)
