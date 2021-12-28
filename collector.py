import logging
import asyncio
import helper
from dotenv import load_dotenv
from telethon import TelegramClient, sync, events, tl, errors
from telethon.tl.functions.messages import ImportChatInviteRequest, CheckChatInviteRequest, GetDialogsRequest, EditMessageRequest
from telethon.tl.functions.channels import GetMessagesRequest, JoinChannelRequest, LeaveChannelRequest
import config
import classes
import datetime
import os
import pytz
import requests
import json
import youtube_dl

utc=pytz.UTC

COLLECTOR_CONFIG = "collector_config.json"
load_dotenv()
config_collector = config.get_json(COLLECTOR_CONFIG)
ON_ERROR_SLEEP_SEC = 60
STEP = 1
FILE_DB = "collector_db.json"
ISO_DATE_FORMAT = r"%Y-%m-%dT%H:%M:%S.%fZ"

def load_cfg():
    global config_collector
    config_collector = config.get_json(COLLECTOR_CONFIG)
    global last_id
    last_id = config_collector["last_id"]
    global main_url_part
    main_url_part = config_collector["main_url_part"]
    global view_url_part
    view_url_part = config_collector["view_url_part"]
    global delay_sec
    delay_sec = config_collector["delay_sec"]
    global length
    length = config_collector["length"]
    global chrome_headers
    chrome_headers = {
        "User-Agent":config_collector["user_agent"],
        "accept":config_collector["accept"]
    }
    global url_tail
    url_tail = config_collector["url_tail"]

    global send_ids
    send_ids = config_collector["send_ids"]
    
    global result_list 
    if os.path.isfile(FILE_DB):
        result_list = config.get_json(FILE_DB)
    else:
        result_list = list()

def should_wait(date_str):
    last_date = helper.str_to_utc_datetime(date_str, "UTC", ISO_DATE_FORMAT)
    next_collect_date = last_date + datetime.timedelta(seconds=delay_sec)
    if next_collect_date > utc.localize(datetime.datetime.utcnow()):
        wait_sec = delay_sec-delta_sec
        logging.info('too close, wait for %s sec', wait_sec)
        return True
    return False


async def main_exec(stop_flag: classes.StopFlag):
    
    while True:
        try:
            load_cfg()
            if should_wait(config_collector["last_date"]):                
                while next_collect_date > datetime.datetime.utcnow() and not stop_flag.Value:
                    await asyncio.sleep(stop_flag.Sleep)               
                if stop_flag.Value:
                    return
            await collect(stop_flag)

        except Exception as ex:
            logging.info('main_exec %s', ex)
            await asyncio.sleep(5)

async def collect(stop_flag: classes.StopFlag):
     async with TelegramClient(config.SESSION_HISTORY_FILE, config.api_id, config.api_hash) as client:
        total = last_id + length
        for current_number in range(last_id+1, total, STEP):
            if stop_flag.Value:
                return
            try:
                url = f"{main_url_part}{current_number}{url_tail}"
                result = requests.get(url, headers=chrome_headers)
                is_404 = result.status_code == 404

                if is_404:
                    continue

                if result.ok:
                    content = json.loads(result.text)
                    name = content['name']
                    published_at = content['published_at']
                    published_at_date = helper.str_to_utc_datetime(published_at, "UTC", ISO_DATE_FORMAT)
                    view_url = f"{view_url_part}{current_number}{url_tail}"
                    logging.info(f"video \"{name}\" ({published_at}) found: {view_url}")

                    try:
                        ydl_opts = {"outtmpl":f"{current_number}.%(ext)s"}
                        with youtube_dl.YoutubeDL(ydl_opts) as ydl:
                            ydl.download([view_url])
                    except Exception as ex:
                        logging.info(f"on grab error: {ex}")
                                        
                    message_sent = await client.send_message(to_primary_id, "")
                    await client.disconnect()

                    to_save = dict()
                    to_save['id'] = current_number
                    to_save['published_at'] = published_at
                    to_save['name'] = content['name']
                    to_save['url'] = view_url
                    result_list.insert(0, to_save)
                    config.set_json(FILE_DB, result_list)
                    config_collector["last_id"] = current_number
                    config_collector["last_date"] = published_at
                    config.set_json(COLLECTOR_CONFIG, config_collector)
                    load_cfg()


                    if should_wait(published_at):
                        return
                else:
                    logging.info(f"resp_code: {result.status_code}, id: {current_number}, sleep for {ON_ERROR_SLEEP_SEC} sec")
                    time.sleep(ON_ERROR_SLEEP_SEC)

            except Exception as ex:
                logging.info(f"collector error: {ex}")
                time.sleep(ON_ERROR_SLEEP_SEC)       