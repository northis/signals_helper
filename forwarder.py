import os
import asyncio
import logging
import re
from dotenv import load_dotenv
from telethon import TelegramClient, sync, events, tl, errors
from telethon.tl.functions.messages import ImportChatInviteRequest, CheckChatInviteRequest, GetDialogsRequest
from telethon.tl.functions.channels import GetMessagesRequest, JoinChannelRequest
import config
import classes
import db_stats

SIGNAL_REGEX = r"(buy)|(sell)"
LINKS_REGEX = r"https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()!@:%_\+.~#?&\/\/=]*)"

INVITE_REGEX = r"joinchat/(.+)"
URL_REGEX = r"t.me/(.+)"

load_dotenv()
main_config = config.get_config()
api_id = os.getenv('api_id')
api_hash = os.getenv('api_hash')
forwards = main_config['forwards']
join_channels = main_config['join_channels']
links_global = config.get_links()


def message_to_str(message):
    return str(message.to_dict()['message'])


async def main_exec(stop_flag: classes.StopFlag):
    async with TelegramClient(config.SESSION_FILE, api_id, api_hash) as client:
        for forward in forwards:
            await init_forward(forward, client)

        while True:
            await asyncio.sleep(10)
            if stop_flag.Value == True:
                break
        await client.disconnect()


async def get_chat_id_by_name(client, name):
    async for dialog in client.iter_dialogs():
        if name == dialog.title:
            logging.info('chat %s => %s', name, dialog.id)
            return dialog.id
    return 0


async def init_forward(forward, client):
    from_chat_id = forward['from_chat_id']
    from_chat_title = forward['from_chat_title']
    to_primary = forward['to_primary']

    is_ready = False

    if from_chat_id == 0:
        from_chat_id = await get_chat_id_by_name(client, from_chat_title)
        if from_chat_id != 0:
            forward['from_chat_id'] = from_chat_id
            is_ready = True
    else:
        is_ready = True

    if not is_ready:
        logging.info('Cannot add forward source %s', from_chat_title)
        return

    if to_primary['id'] == 0:
        is_ready = False
        id_ = await get_chat_id_by_name(client, to_primary['title'])
        if id_ != 0:
            to_primary['id'] = id_
            is_ready = True

    if not is_ready:
        logging.info('Cannot add primary destination %s', from_chat_title)
        return

    to_secondary_id = 0
    to_primary_id = to_primary['id']

    if 'to_secondary' in forward:
        to_secondary = forward['to_secondary']
        to_secondary_id = to_secondary['id']
        if to_secondary_id == 0:
            to_secondary_id = await get_chat_id_by_name(client, to_secondary['title'])

    config.set_config(main_config)
    forward_exec(from_chat_id, to_primary_id,
                 to_secondary_id, client)


async def define_urls(message):
    join_urls = list()
    is_primary = message.button_count == 0
    reply = None
    if message.is_reply:
        reply = await message.get_reply_message()
        is_primary = is_primary and (reply.button_count == 0)
        if reply.entities is not None:
            for entity in reply.entities:
                if isinstance(entity, tl.types.MessageEntityTextUrl):
                    join_urls.append(entity.url)
                    is_primary = False
                    break

    if message.entities is not None:
        for entity in message.entities:
            if isinstance(entity, tl.types.MessageEntityTextUrl):
                join_urls.append(entity.url)
                is_primary = False
                break

    if message.buttons is not None:
        for button in message.buttons:
            if isinstance(button, tl.types.KeyboardButtonUrl):
                join_urls.append(button.url)
                break
            if not isinstance(button, list):
                continue
            for button_inner in button:
                if isinstance(button_inner.button, tl.types.KeyboardButtonUrl):
                    join_urls.append(button_inner.button.url)
                    break

    if message.entities is not None:
        for entity in message.entities:
            if isinstance(entity, tl.types.MessageEntityUrl):
                is_primary = False
                break

    if (reply is not None) and (reply.entities is not None):
        for entity in reply.entities:
            if isinstance(entity, tl.types.MessageEntityUrl):
                is_primary = False
                break

    return (is_primary, join_urls, reply)


async def join_link(url, client):
    invite_code_typle = get_invite_string_from_url(url)
    if invite_code_typle is None:
        return None

    invite_code = invite_code_typle[0]
    is_public = invite_code_typle[1]
    logging.info('Joining with invite code %s', invite_code)

    try:
        if is_public:
            result = await client(JoinChannelRequest(invite_code))
        else:
            result = await client(CheckChatInviteRequest(hash=invite_code))
        need_to_add = True
        chat = None
        if hasattr(result, 'chat'):
            chat = result.chat
        elif hasattr(result, 'chats'):
            need_to_add = False
            chat = result.chats[0]

        if chat is None:
            logging.info('Cannot find right channel for %s', invite_code)
            return None

        upsert_res = db_stats.upsert_channel(chat.id, url, chat.title)
        exist = upsert_res[4] is not None
        if exist:
            logging.info('Known channel %s', chat.title)
            return

        logging.info('Checking channel %s', chat.title)
        async for dialog in client.iter_dialogs():
            if dialog.entity.id == chat.id:
                need_to_add = False
                break

        if not need_to_add:
            logging.info('Already in channel %s', chat.title)
            return chat

        chat = await client(ImportChatInviteRequest(invite_code))
        chat = chat.chats[0]
        logging.info('Just added to channel %s', chat.title)
        return chat

    except Exception as err_chat:
        logging.info(
            'Cannot add by code %s: error:%s', invite_code, str(err_chat))
        return None


async def forward_primary(to_primary_id, message, reply, client: TelegramClient):
    primary_reply_outer = None
    if message.is_reply:
        saved_reply_id = db_stats.get_primary_message_id(
            reply.id, reply.chat.id)

        if saved_reply_id is None:
            primary_reply_outer = await client.forward_messages(to_primary_id, reply)
        else:
            primary_reply_outer = await client.get_messages(
                to_primary_id, saved_reply_id)

        logging.info(
            'Forwarded to reply to primary (reply id = %s, message id = %s)', reply.id, message.id)

    else:
        message_forwarded = await client.forward_messages(to_primary_id, message)
        db_stats.set_primary_message_id(
            message_forwarded.id, message.id, message.chat.id)
        logging.info('Forwarding to primary (id = %s)', message.id)
        return

    await client.send_message(to_primary_id, message, reply_to=primary_reply_outer)
    logging.info('Replying to primary (id = %s)', message.id)


async def main_forward_message(to_primary_id, to_secondary_id, client, event):
    message = event.message
    orig_message_text = str(message.to_dict()['message'])
    message_text = orig_message_text.lower()

    (is_primary, join_urls, reply) = await define_urls(message)

    reply_text = None
    contains_no_links = None
    is_signal = None

    message_link = re.search(LINKS_REGEX, orig_message_text, re.IGNORECASE)
    contains_no_links = message_link is None
    if message_link is not None:
        join_urls.append(message_link.group(0))

    if reply is not None:
        reply_text = str(reply.to_dict()['message'])
        message_link = re.search(LINKS_REGEX, reply_text, re.IGNORECASE)
        contains_no_links = contains_no_links and (message_link is None)
        if message_link is not None:
            join_urls.append(message_link.group(0))

    is_signal = re.search(SIGNAL_REGEX, message_text) is not None
    is_primary = is_primary and contains_no_links and (
        message.is_reply or is_signal)

    logging.info(
        'Message %s (contains_no_links=%s, is_signal=%s)', message.id, contains_no_links, is_signal)

    try:
        if is_primary:
            await forward_primary(to_primary_id, message, reply, client)
        elif to_secondary_id != 0:
            if join_channels:
                for url in join_urls:
                    url_exists = db_stats.has_channel(url, None) is not None
                    if url_exists:
                        logging.info('Already exists url %s', url)
                        continue
                    await join_link(url, client)

            if len(join_urls) > 0:
                await client.forward_messages(to_secondary_id, message)
                logging.info('Forwarding to secondary (id = %s)', message.id)
            else:
                logging.info('No link, no signal (id = %s, text = %s)',
                             message.id, orig_message_text)
        else:
            logging.info(
                'Message has not been forwarded (id = %s)', message.id)

    except errors.rpcerrorlist.MessageIdInvalidError:
        try:
            if (not is_primary) and (to_secondary_id != 0):
                await client.send_message(to_secondary_id, message_text)
                logging.info(
                    'Sending deleted to secondary (id = %s)', message.id)
        except Exception as err_del:
            logging.exception(err_del)
    except Exception as err:
        logging.exception(err)


def forward_exec(from_chat_id, to_primary_id, to_secondary_id, client):
    @client.on(events.NewMessage(chats=(from_chat_id)))
    async def handler(event):
        await main_forward_message(to_primary_id, to_secondary_id, client, event)


def get_invite_string_from_url(url):
    search_link = re.search(INVITE_REGEX, url)
    if search_link is not None:
        return (search_link.group(1), False)

    search_link = re.search(URL_REGEX, url)
    if search_link is None:
        return None
    return (search_link.group(1), True)
