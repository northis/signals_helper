import os
import logging
import asyncio
import re
from dotenv import load_dotenv
from telethon import TelegramClient, sync, events, tl, errors
from telethon.tl.functions.messages import ImportChatInviteRequest, CheckChatInviteRequest, GetDialogsRequest
from telethon.tl.functions.channels import GetMessagesRequest, JoinChannelRequest, LeaveChannelRequest
import config
import classes
import db_stats
import traceback

SIGNAL_REGEX = r"(buy)|(sell)"
LINKS_REGEX = r"https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()!@:%_\+.~#?&\/\/=]*)"

INVITE_REGEX = r"joinchat\/(.+)"
URL_REGEX = r"t.me\/(.+)"
TRADINGVIEW_REGEX = r"tradingview.com\/(.+)"

load_dotenv()
main_config = config.get_config()
forwards = main_config['forwards']
join_channels = main_config['join_channels']
main_channels = list()


def message_to_str(message):
    return str(message.to_dict()['message'])


async def main_exec(stop_flag: classes.StopFlag):
    while True:
        try:
            async with TelegramClient(config.SESSION_HISTORY_FILE, config.api_id, config.api_hash) as client:
                for forward in forwards:
                    await init_forward(forward, client)

                await stop_flag.wait()
                await client.disconnect()
                break

        except Exception as ex:
            logging.info('main_exec %s', ex)
            await asyncio.sleep(5)


async def get_chat_id_by_name(client, name):
    chat = await get_chat_by_name(client, name)
    if chat is None:
        return 0
    return chat.id


async def get_chat_by_name(client, name):
    async for dialog in client.iter_dialogs():
        if name == dialog.title:
            logging.info('chat %s => %s', name, dialog.id)
            return dialog.entity
    return None


async def get_chat_by_id(client, id_):
    async for dialog in client.iter_dialogs():
        if id_ == dialog.entity.id or id_ == dialog.id:
            return dialog
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

    main_channels.append(from_chat_id)

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


async def get_in_channel(id_channel, client):
    async for dialog in client.iter_dialogs():
        if dialog.entity.id == id_channel:
            return dialog
    return None


async def join_link(url, client):
    channel_id = db_stats.get_channel(url, None)
    invite_code_typle = get_invite_string_from_url(url)
    if invite_code_typle is None:
        return None

    invite_code = invite_code_typle[0]
    is_public = invite_code_typle[1]
    logging.info('Joining with invite code %s', invite_code)

    try:
        if channel_id is not None:
            get_in = await get_in_channel(channel_id, client)
            if get_in is not None:
                db_stats.upsert_channel(get_in.id, url, get_in.title)
                return None

        if is_public:
            result = await client(JoinChannelRequest(invite_code))
        else:
            result = await client(CheckChatInviteRequest(hash=invite_code))

        chat = None
        if hasattr(result, 'chat'):
            chat = result.chat
        elif hasattr(result, 'chats'):
            chat = result.chats[0]

        if channel_id is not None:
            if not hasattr(result, 'title'):
                result = chat

            if result is not None:
                logging.info('Already in channel %s', result.title)
                got_history = db_stats.is_history_loaded(
                    channel_id[0], url, result.title)
                if got_history:
                    logging.info(
                        'History has already been loaded for channel %s', result.title)
                    return None

        if chat is None:
            try:
                chat = await client(ImportChatInviteRequest(invite_code))
            except errors.FloodWaitError as ex_flood_wait:
                await asyncio.sleep(ex_flood_wait.seconds + 10)
                chat = await client(ImportChatInviteRequest(invite_code))
            chat = chat.chats[0]

            db_stats.upsert_channel(chat.id, url, chat.title)
            logging.info('Just added to channel %s', chat.title)
            return chat

        db_stats.upsert_channel(chat.id, url, chat.title)
        return chat

    except Exception:
        logging.info(
            'Cannot add via %s: error:%s', url, traceback.format_exc())
        return None


async def forward_primary(to_primary_id, message, reply, client: TelegramClient):
    primary_reply_outer = None
    if message.is_reply:
        saved_reply_id = db_stats.get_primary_message_id(
            reply.id, reply.chat.id)

        if saved_reply_id is None:
            primary_reply_outer = await client.forward_messages(to_primary_id, reply)
        else:
            chat = await get_chat_by_id(client, to_primary_id)
            primary_reply_outer = await client.get_messages(chat, ids=saved_reply_id)

        logging.info(
            'Forwarded to reply to primary (reply id = %s, message id = %s)', reply.id, message.id)

    else:
        message_forwarded = await client.forward_messages(to_primary_id, message)
        db_stats.set_primary_message_id(
            message_forwarded.id, message.id, message.chat.id)
        logging.info('Forwarding to primary (id = %s)', message.id)
        return

    message_sent = await client.send_message(
        to_primary_id, message, reply_to=primary_reply_outer)
    db_stats.set_primary_message_id(
        message_sent.id, message.id, message.chat_id)
    logging.info('Replying to primary (id = %s)', message.id)


async def exit_if_needed(url, channel_id, client):
    logging.info('Already exists url %s', url)
    is_in = await get_in_channel(channel_id, client)
    if is_in is None:
        return

    if db_stats.is_history_loaded(channel_id, None, None) and channel_id not in main_channels:
        logging.info('Exit from channel %s', channel_id)
        exit_res = await client(LeaveChannelRequest(channel_id))
        logging.info('Exited %s', exit_res)


async def main_forward_message(to_primary_id, to_secondary_id, client, event):
    message = event.message
    orig_message_text = str(message.to_dict()['message'])
    message_text = orig_message_text.lower()

    (is_primary, join_urls, reply) = await define_urls(message)

    reply_text = None
    contains_no_links = None
    is_signal = None
    is_tradingview = False

    message_link = re.finditer(LINKS_REGEX, orig_message_text, re.IGNORECASE)
    contains_no_links = True
    for message_link_match in message_link:
        res_group = message_link_match.group(0)
        if re.search(TRADINGVIEW_REGEX, res_group) is not None:
            is_tradingview = True
            continue
        join_urls.append(res_group)
        contains_no_links = False

    if reply is not None:
        reply_text = str(reply.to_dict()['message'])
        message_link = re.finditer(LINKS_REGEX, reply_text, re.IGNORECASE)
        for message_link_match in message_link:
            contains_no_links = False
            break

    is_signal = re.search(SIGNAL_REGEX, message_text) is not None
    has_photo = message.photo is not None
    is_primary = (is_primary or is_tradingview) and contains_no_links and (
        message.is_reply or is_signal or has_photo)

    logging.info(
        'Message %s (contains_no_links=%s, is_signal=%s, has_photo=%s)',
        message.id, contains_no_links, is_signal, has_photo)

    try:
        if is_primary:
            await forward_primary(to_primary_id, message, reply, client)
        elif to_secondary_id != 0:
            joined = False
            if join_channels:
                for url in set(join_urls):
                    channel_ids = db_stats.get_channel(url, None)
                    url_exists = channel_ids is not None
                    if url_exists:
                        channel_id = channel_ids[0]
                        await exit_if_needed(url, channel_id, client)
                        continue
                    joined_res = await join_link(url, client)
                    joined = joined_res is not None

            if joined:
                await client.forward_messages(to_secondary_id, message)
                logging.info('Forwarding to secondary (id = %s)', message.id)
            else:
                logging.info('No link, no signal (id = %s) or already exists url',
                             message.id)
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
