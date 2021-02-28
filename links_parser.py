from telethon import TelegramClient, sync, events, tl, errors
from telethon.tl.functions.messages import ImportChatInviteRequest, CheckChatInviteRequest, GetDialogsRequest
from telethon.tl.functions.channels import GetMessagesRequest, JoinChannelRequest
from telethon.errors import FloodWaitError
import config
import asyncio
import re
import datetime
import os
from datetime import timezone
from dotenv import load_dotenv
import json
LINKS_REGEX = r"https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()!@:%_\+.~#?&\/\/=]*)"
INVITE_REGEX = r"joinchat/(.+)"
URL_REGEX = r"t.me/(.+)"


load_dotenv()
api_id = os.getenv('api_id')
api_hash = os.getenv('api_hash')


def get_links_source():
    with open('links_source.json', 'r', encoding="utf-8") as f:
        config = json.load(f)
        return config


def get_links():
    with open('links.json', 'r', encoding="utf-8") as f:
        config = json.load(f)
        return config


def set_links(config):
    with open('links.json', 'w', encoding="utf-8") as f:
        json.dump(obj=config, fp=f, indent=2,
                  sort_keys=True, ensure_ascii=False)


links_source = get_links_source()['messages']


async def main_exec():
    async with TelegramClient('secure_session.session', api_id, api_hash) as client:
        links_target = get_links()

        unique_urls = {}

        for link in links_source:
            str_text = str(link['text'])
            search_link = re.search(LINKS_REGEX, str_text)
            if search_link == None:
                print("Skip message (no link) %s" % str_text)
                continue
            str_text = search_link.group(0)
            unique_urls[str_text] = str_text

        for str_text in unique_urls:
            search_link = re.search(URL_REGEX, str_text)
            if search_link == None:
                print("Skip message %s" % str_text)
                continue
            invite_str = re.search(INVITE_REGEX, str_text)
            check_str = None

            is_private = False
            if invite_str == None:
                check_str = search_link.group(1)
            else:
                is_private = True
                check_str = invite_str.group(1)

            try:

                already_added = False
                for existed_link in links_target:
                    if (existed_link['access_url'] == str_text):
                        already_added = True
                        break

                if already_added:
                    continue

                if is_private:
                    try:
                        await asyncio.sleep(10)
                        result = await client(CheckChatInviteRequest(
                            hash=check_str
                        ))
                    except FloodWaitError as e:
                        print('Flood waited for', e.seconds)
                        await asyncio.sleep(e.seconds+1)
                        result = await client(CheckChatInviteRequest(
                            hash=check_str
                        ))
                else:
                    result = await client(JoinChannelRequest(check_str))

                need_to_add = True

                chat = None
                if hasattr(result, 'chat'):
                    async for dialog in client.iter_dialogs():
                        if dialog.entity.id == result.chat.id:
                            need_to_add = False
                            chat = result.chat
                            break
                elif hasattr(result, 'chats'):
                    need_to_add = False
                    chat = result.chats[0]

                if need_to_add:
                    chat = await client(ImportChatInviteRequest(check_str))
                    chat = chat.chats[0]

                now = datetime.datetime.now(datetime.timezone.utc).isoformat()

                exist = False
                for existed_link in links_target:
                    if existed_link['id'] == chat.id:
                        if (existed_link['access_url'] != str_text) or (existed_link['name'] != chat.title):
                            existed_link['access_url'] = str_text
                            existed_link['name'] = chat.title
                            existed_link['change_date_utc'] = now
                        exist = True
                        break

                if exist:
                    continue

                links_target.append(
                    {"id": chat.id, "access_url": str_text, "name": chat.title, "add_date_utc:": now, "change_date_utc:": now})

                set_links(links_target)
                links_target = get_links()
            except Exception as ex:
                print("Message %s, error %s" % (str_text, str(ex)))


if __name__ == "__main__":
    print("Press Ctrl+C to exit...")
    asyncio.run(main_exec())
