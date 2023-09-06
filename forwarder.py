import logging
import asyncio
import re
import traceback
import helper
from dotenv import load_dotenv
from telethon import TelegramClient, sync, events, tl, errors
from telethon.tl.functions.messages import ImportChatInviteRequest, CheckChatInviteRequest, GetDialogsRequest, EditMessageRequest
from telethon.tl.functions.channels import GetMessagesRequest, JoinChannelRequest, LeaveChannelRequest
import config
import classes
import db_stats
import signal_parser
import db_poll

LINKS_REGEX = r"https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()!@:%_\+.~#?&\/\/=]*)"

INVITE_REGEX = r"joinchat\/(.+)"
URL_REGEX = r"t.me\/(.+)"
TRADINGVIEW_REGEX = r"tradingview.com\/(.+)"
SESSION = 'secure_session_history_forwarder.session'
load_dotenv()
main_config = config.get_config()
forwards = main_config['forwards']
join_channels = main_config['join_channels']
main_channels = list()
tg_client = None


def message_to_str(message):
    return str(message.to_dict()['message'])


def is_price_actual(symbol: classes.Symbol, price: float):
    if price is None:
        return False
    price_actual = float(db_poll.db_time_ranges[symbol][2])
    res = abs(float(price)-price_actual)/price_actual < 0.1
    return res


async def main_exec(stop_flag: classes.StopFlag):
    async with TelegramClient(SESSION, config.api_id, config.api_hash) as client:
        try:
            primary = None
            dialogs = list(await client.get_dialogs())

            for forward in forwards:
                await init_forward(forward, client, dialogs)
                if primary is None:
                    primary = forward.get('to_primary')

            primary_chat = await get_chat_by_id(client, int(primary["id"]))
            await db_stats.set_pinned(client, forwards, primary_chat)
            logging.info('Pinned is set')
            # await bulk_exit(client)
            # logging.info('Bulk exit is done')
            global tg_client
            tg_client = client
            await client.run_until_disconnected()
            logging.info('Disconnecting is done')

        except Exception as ex:
            logging.info('forwarder main_exec %s', ex)
            await stop_flag.wait()


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
    return None


def get_chat_by_id_list(id_, dialogs):
    for dialog in dialogs:
        if id_ == dialog.entity.id or id_ == dialog.id:
            return dialog
    return None


async def init_forward(forward, client, dialogs: list):
    
    from_chat_id = forward['from_chat_id']
    from_chat_title = forward['from_chat_title']
    to_primary = forward['to_primary']
    logging.info(f'init_forward: {from_chat_id}')

    is_ready = False

    if from_chat_id == 0:
        from_chat_id = await get_chat_id_by_name(client, from_chat_title)
        if from_chat_id != 0:
            forward['from_chat_id'] = from_chat_id
            is_ready = True
    else:
        chat = get_chat_by_id_list(from_chat_id, dialogs)
        if chat is not None:
            chat_title = chat.title
            forward['from_chat_title'] = chat_title
            db_stats.upsert_channel(from_chat_id, None, chat_title)
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
            upserted_channel = db_stats.upsert_channel(channel_id[0], url, None)
            logging.info('Updated channel (id: %s) with code %s', channel_id[0], invite_code)

            if upserted_channel is not None and upserted_channel[7] == 1:
                logging.info('Skip joining, already analyzed channel, id: %s', channel_id[0])
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
            logging.info('Just added to channel %s', chat.title)

        upsert_res = db_stats.upsert_channel(chat.id, url, chat.title)
        is_new = upsert_res is None or upsert_res[4] is None
        if is_new:
            return chat

        return None

    except Exception:
        logging.info('Cannot add via %s:', url)
        return None


async def forward_primary(to_primary_id, message, reply, client: TelegramClient):
    primary_reply_outer = None
    if message.is_reply:
        saved_reply_id = db_stats.get_primary_message_id(
            reply.id, reply.chat.id)

        if saved_reply_id is None:
            primary_reply_outer = await client.forward_messages(to_primary_id, reply, silent=True)
        else:
            chat = await get_chat_by_id(client, to_primary_id)
            primary_reply_outer = await client.get_messages(chat, ids=saved_reply_id)

        logging.info(
            'Forwarded to reply to primary (reply id = %s, message id = %s)', reply.id, message.id)

    else:
        message_forwarded = await client.forward_messages(to_primary_id, message)

        chat_id = None
        if message.fwd_from is not None and message.fwd_from.from_id is not None:
            chat_id = message.fwd_from.from_id.channel_id
        elif message.chat is None:
            chat_id = message.chat_id
        else:
            chat_id = message.chat.id

        db_stats.set_primary_message_id(
            message_forwarded.id, message.id, chat_id)
        logging.info('Forwarding to primary (id = %s)', message.id)
        return (message_forwarded.id, chat_id)

    if primary_reply_outer is None:
        logging.error(
            'Cannot reply to primary (id = %s), primary_reply_outer is None', message.id)
        return None

    message_sent = await client.send_message(
        to_primary_id, message, reply_to=primary_reply_outer)
    db_stats.set_primary_message_id(
        message_sent.id, message.id, message.chat_id)
    logging.info('Replying to primary (id = %s)', message.id)
    return (message_sent.id, message.chat_id)


async def exit_if_needed(url, channel_id, client):
    logging.info('Already exists url %s', url)
    is_in = await get_in_channel(channel_id, client)
    if is_in is None:
        return

    if db_stats.is_history_loaded(channel_id, None, None) and channel_id not in main_channels:
        logging.info('Exit from channel %s', channel_id)
        exit_res = await client(LeaveChannelRequest(channel_id))
        logging.info('Exited %s', exit_res)


async def bulk_exit(client):
    async for dialog in client.iter_dialogs():
        channel_id = dialog.entity.id
        if db_stats.is_history_loaded(channel_id, None, None) and channel_id not in main_channels:
            logging.info('Exit from channel %s', channel_id)
            exit_res = await client(LeaveChannelRequest(channel_id))
            logging.info('Exited %s', exit_res)


async def main_edit_message(to_primary_id, client, event):
    message = event.message
    id_message = message.id
    id_channel = message.chat.id

    saved_message_id = db_stats.get_primary_message_id(
        id_message, id_channel)

    if saved_message_id is None or saved_message_id == 0:
        logging.info('Cannot find message with id %s to edit (channel id is %s)',
                     id_message, id_channel)
        return

    msg = await client.get_messages(to_primary_id, ids=saved_message_id)
    if msg is None:
        logging.info('Cannot find message with id %s to edit, msg is None (channel id is %s)',
                     id_message, id_channel)
        return

    message_sent = await client.send_message(
        to_primary_id, message, reply_to=msg, silent=True)

    db_stats.delete_primary_message_id(id_message, id_channel)
    db_stats.set_primary_message_id(
        message_sent.id, id_message, id_channel)
    logging.info('Edit message with id %s (channel id is %s) is ok', id_message, id_channel)


async def main_forward_message(to_primary_id, to_secondary_id, client, event):
    message = event.message
    orig_message_text = str(message.to_dict()['message'])
    message_text = orig_message_text.lower()
    id_channel = message.chat.id
    # logging.info('Got new message: %s, channel id: %s', orig_message_text.encode('utf8'), id_channel)

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

    is_reply_signal = False
    if reply is not None:
        reply_text = message_to_str(reply).lower()
        is_reply_signal = re.search(
            signal_parser.SIGNAL_REGEX, reply_text) is not None
        message_link = re.finditer(LINKS_REGEX, reply_text, re.IGNORECASE)
        for message_link_match in message_link:
            contains_no_links = False
            break

    symbol_search = None
    symbol_search_reply = None
    price = None
    is_buy = None
    sl = None
    tps = None

    symbol = None

    for symbol_item in signal_parser.symbols_regex_map:
        symbol_search, price, is_buy, sl, tps = signal_parser.message_to_signal(
            message_text, signal_parser.symbols_regex_map[symbol_item])

        if is_reply_signal:
            reply_res = signal_parser.message_to_signal(
                reply_text, signal_parser.symbols_regex_map[symbol_item])
            if symbol_search_reply is None:
                symbol_search_reply = reply_res[0]

        if symbol_search is None:
            continue
        symbol = symbol_item
        break

    is_symbol = symbol_search is not None
    is_symbol_reply = symbol_search_reply is not None
    is_signal = price is not None
    is_primary = (is_primary or is_tradingview) and contains_no_links and (
        (message.is_reply and is_reply_signal and is_symbol_reply) or (is_signal and is_symbol))

    logging.info(
        'Message %s (contains_no_links=%s, is_signal=%s)',
        message.id, contains_no_links, is_signal)

    has_sl = sl is not None
    has_tp = tps is not None and len(tps) > 0
    try:
        if is_primary:
            forward_res = await forward_primary(to_primary_id, message, reply, client)

            # should_send_stat_msg = is_price_actual(
            #     symbol, price) and forward_res is not None and is_signal and is_symbol and (has_sl or has_tp)
            # if not should_send_stat_msg:
            #     return

            tp_item = None
            for tps_inner in tps:
                tp_item = tps_inner
                break

            now_str = helper.get_now_utc_iso()
            db_stats.save_signal(
                symbol, forward_res[0], forward_res[1], is_buy, now_str, price, tp_item, sl)

            last_signals = db_stats.gel_last_signals(symbol)
            await client.send_message(to_primary_id, last_signals, silent=True)
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
    logging.info(f'forward_exec: chat: {from_chat_id}')
    @client.on(events.NewMessage())
    async def handler(event):
        logging.info(f'NewMessage, event: {event}')
        if event.chat_id == from_chat_id:
            await main_forward_message(to_primary_id, to_secondary_id, client, event)

    @client.on(events.MessageEdited())
    async def handler_edit(event):
        logging.info(f'MessageEdited, event: {event}')
        if event.chat_id== from_chat_id:
            await main_edit_message(to_primary_id, client, event)

        
def get_invite_string_from_url(url):
    search_link = re.search(INVITE_REGEX, url)
    if search_link is not None:
        return (search_link.group(1), False)

    search_link = re.search(URL_REGEX, url)
    if search_link is None:
        return None
    return (search_link.group(1), True)
