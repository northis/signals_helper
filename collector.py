
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
import concurrent.futures
import time

utc = pytz.UTC

COLLECTOR_CONFIG = "collector_config.json"
load_dotenv()
config_collector = config.get_json(COLLECTOR_CONFIG)
SESSION = 'secure_session_collector.session'
ON_ERROR_SLEEP_SEC = 60
ON_ERROR_SLEEP_LONG_SEC = 60*3
STEP = 10
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
        "User-Agent": config_collector["user_agent"],
        "accept": config_collector["accept"]
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
    now = datetime.datetime.utcnow()
    next_collect_date = None

    weekday = now.weekday()
    if (weekday == 5 and now.hour>=3) or (weekday == 6):  # weekend
        return True
    else:
        next_collect_date = last_date + datetime.timedelta(seconds=delay_sec)

    if next_collect_date > utc.localize(now):
        return True
    return False


async def main_exec(stop_flag: classes.StopFlag):
    global got_collected
    got_collected = False
    no_collect_in_a_row_count = 0
    while True:
        try:
            if stop_flag.Value:
                return
            load_cfg()
            if got_collected:
                got_collected = False
                no_collect_in_a_row_count = 0
            elif no_collect_in_a_row_count > 0:
                length = length * (2 + no_collect_in_a_row_count)
                no_collect_in_a_row_count = no_collect_in_a_row_count + 1
                logging.info(f'cannot collect, extend the range to {length}')

            logging.info('collector - next iteration')
            while should_wait(config_collector["last_date"]) and not stop_flag.Value:
                await asyncio.sleep(stop_flag.Sleep)
                if stop_flag.Value:
                    return
                await asyncio.sleep(stop_flag.Sleep)

            logging.info('going to collect')

            await collect(stop_flag)
            if stop_flag.Value:
                return
            await asyncio.sleep(5)

        except Exception as ex:
            logging.info('main_exec %s', ex)
            await asyncio.sleep(5)


def check_url(current_number):
    url = f"{main_url_part}{current_number}{url_tail}"
    result = requests.get(url, headers=chrome_headers)
    return result

async def collect(stop_flag: classes.StopFlag):
    total = last_id + 1 + length
    log_every = length / 2
    log_count = 0
    
    error_nums = list()
    for current_number in range(last_id + 1, total, STEP):
        log_count = log_count + STEP
        if log_count > log_every:
            log_count = 0
            logging.info("collecting... {0}%".format(
                100*(total - current_number)/length))

        if stop_flag.Value:
            return
        try:
            
            results = dict()
            fail_result_first = None
            with concurrent.futures.ThreadPoolExecutor(STEP) as executor:
                futures = dict()
                for current_num in range(current_number, current_number+STEP, 1):
                    futures[current_num]=executor.submit(check_url, current_num)

                for error_num in error_nums:
                    futures[current_num]=executor.submit(check_url, error_num)

                error_nums.clear()
                fail_result_first = None

                _, _ = concurrent.futures.wait(futures.values())
                for future_key in futures.keys():
                    future_item = futures[future_key]
                    result_inner = future_item.result()
                    is_404 = result_inner.status_code == 404
                    if is_404:
                        continue

                    if result_inner.ok:
                        results[future_key]=result_inner
                    else:
                        error_nums.append(future_key)
                        if fail_result_first == None:
                            fail_result_first = result_inner
            
            
            for result_key in results.keys():
                result = results[result_key]
                content = json.loads(result.text)
                name = content['name']
                duration = time.strftime('%H:%M:%S', time.gmtime(content['duration']/1000))
                published_at = content['published_at']
                published_at_date = helper.str_to_utc_datetime(
                    published_at, "UTC", ISO_DATE_FORMAT)
                view_url = f"{view_url_part}{result_key}{url_tail}"
                logging.info(
                    f"video \"{name}\" ({published_at}) found: {view_url}")

                has_video = True
                out_file = f"{current_number}.mp4"
                try:
                    ydl_opts = {"outtmpl": out_file}
                    with youtube_dl.YoutubeDL(ydl_opts) as ydl:
                        ydl.download([view_url])
                except Exception as ex:
                    logging.info(f"on grab error: {ex}")
                    has_video = False

                text_msg = f"\"{name}\" \n{published_at} ({duration})"

                async with TelegramClient(SESSION, config.api_id, config.api_hash) as client:
                    for send_id in send_ids:
                        if has_video:
                            await client.send_file(send_id, out_file, silent=True)
                            await client.send_message(send_id, f"{text_msg}\n{view_url}")
                        else:
                            await client.send_message(send_id, f"{text_msg}\n{view_url}")

                if has_video:
                    os.remove(out_file)

                to_save = dict()
                to_save['id'] = result_key
                to_save['published_at'] = published_at
                to_save['name'] = content['name']
                to_save['url'] = view_url
                result_list.insert(0, to_save)
                config.set_json(FILE_DB, result_list)
                config_collector["last_id"] = result_key
                config_collector["last_date"] = published_at
                config.set_json(COLLECTOR_CONFIG, config_collector)
                got_collected = True
                load_cfg()

                if should_wait(published_at):
                    return
            
            err_len = len(error_nums)
            if err_len == 0:                
                # print(f"id passed: {current_number}")
                continue

            sec_to_sleep = ON_ERROR_SLEEP_SEC
            if(err_len>STEP):
                sec_to_sleep = ON_ERROR_SLEEP_LONG_SEC
            
            logging.info(
                f"resp_code: {fail_result_first.status_code}, url 1st: {fail_result_first.url},sleep for {sec_to_sleep} sec, failed urls: {err_len}")
            await asyncio.sleep(sec_to_sleep)

        except Exception as ex:
            logging.info(f"collector error: {ex}")
            await asyncio.sleep(ON_ERROR_SLEEP_SEC)