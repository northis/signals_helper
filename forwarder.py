import os
import asyncio
import logging
import re
import datetime
from dotenv import load_dotenv
from telethon import TelegramClient, sync, events, tl, errors
from telethon.tl.functions.messages import ImportChatInviteRequest, CheckChatInviteRequest, GetDialogsRequest
from telethon.tl.functions.channels import GetMessagesRequest, JoinChannelRequest
import config

from datetime import timezone

SIGNAL_REGEX = r"(buy)|(sell)"
LINKS_REGEX = r"https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()!@:%_\+.~#?&\/\/=]*)"
INVITE_REGEX = r"joinchat/(.+)"
URL_REGEX = r"t.me/(.+)"

logging.basicConfig(filename='forwarder.log', encoding='utf-8', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)


load_dotenv()
main_config = config.get_config()
api_id = os.getenv('api_id')
api_hash = os.getenv('api_hash')
forwards = main_config['forwards']
join_channels = main_config['join_channels']
links_global = config.get_links()


async def main_exec():
    async with TelegramClient('secure_session.session', api_id, api_hash) as client:
        for forward in forwards:
            await init_forward(forward, client)
        await client.run_until_disconnected()


async def get_chat_id_by_name(client, name):
    async for dialog in client.iter_dialogs():
        if name == dialog.title:
            logging.info('chat %s => %s' % (name, dialog.id))
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
        logging.info('Cannot add forward source %s' % from_chat_title)
        return

    if to_primary['id'] == 0:
        is_ready = False
        id_ = await get_chat_id_by_name(client, to_primary['title'])
        if id_ != 0:
            to_primary['id'] = id_
            is_ready = True

    if not is_ready:
        logging.info('Cannot add primary destination %s' %
                     from_chat_title)
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
    if (message_link is not None):
        join_urls.append(message_link.group(0))

    if reply is not None:
        reply_text = str(reply.to_dict()['message'])
        message_link = re.search(LINKS_REGEX, reply_text, re.IGNORECASE)
        contains_no_links = contains_no_links and (message_link is None)
        if (message_link is not None):
            join_urls.append(message_link.group(0))

    is_signal = re.search(SIGNAL_REGEX, message_text) is not None
    is_primary = is_primary and contains_no_links and (
        message.is_reply or is_signal)

    logging.info(
        'Message %s (contains_no_links=%s, is_signal=%s)', message.id, contains_no_links, is_signal)

    try:
        if is_primary:
            primary_reply_outer = None
            if message.is_reply:
                async for primary_reply in client.iter_messages(
                        to_primary_id, from_user=reply.chat.id, search=reply_text):
                    primary_reply_outer = primary_reply
                    break

                logging.info(
                    'Forwarded to reply to primary (reply id = %s, message id = %s)', reply.id, message.id)

            if primary_reply_outer is None:
                if reply_text is None:
                    await client.forward_messages(to_primary_id, message)
                    logging.info(
                        'Forwarding to primary (id = %s)', message.id)
                else:
                    ready_message = "`" + reply_text + "`\n" + \
                        message_text + "\n\n" + message.chat.title
                    await client.send_message(to_primary_id, ready_message)
                    logging.info(
                        'Replying missed message to primary (id = %s)', message.id)

            else:
                await client.send_message(to_primary_id, message, reply_to=primary_reply_outer)
                logging.info('Replying to primary (id = %s)', message.id)

        elif to_secondary_id != 0:
            if join_channels:
                links_local = config.get_links()
                for url in join_urls:
                    url_exists = False
                    for link_url in links_local:
                        if link_url == url:
                            url_exists = True
                            break

                    if url_exists:
                        logging.info('Already exists url %s', url)
                        continue

                    invite_code_typle = getInviteStringFromUrl(url)
                    if invite_code_typle is not None:
                        invite_code = invite_code_typle[0]
                        is_public = invite_code_typle[1]
                        logging.info(
                            'Joining with invite code %s', invite_code)
                        try:
                            if is_public:
                                result = await client(JoinChannelRequest(invite_code))
                            else:
                                result = await client(CheckChatInviteRequest(
                                    hash=invite_code
                                ))
                            need_to_add = True
                            chat = None
                            if hasattr(result, 'chat'):
                                chat = result.chat
                            elif hasattr(result, 'chats'):
                                need_to_add = False
                                chat = result.chats[0]

                            if chat is None:
                                logging.info(
                                    'Cannot find right channel for %s', invite_code)
                            else:
                                exist = False
                                now = datetime.datetime.now(
                                    datetime.timezone.utc).isoformat()
                                for existed_link in links_local:
                                    if existed_link['id'] == chat.id:
                                        if (existed_link['access_url'] != url) or (existed_link['name'] != chat.title):
                                            existed_link['access_url'] = url
                                            existed_link['name'] = chat.title
                                            existed_link['change_date_utc'] = now
                                        config.set_links(links_local)
                                        exist = True
                                        break
                                if exist:
                                    logging.info(
                                        'Known channel %s', chat.title)
                                    continue

                                logging.info(
                                    'Checking channel %s', chat.title)
                                async for dialog in client.iter_dialogs():
                                    if dialog.entity.id == chat.id:
                                        need_to_add = False
                                        break
                                if need_to_add:
                                    chat = await client(ImportChatInviteRequest(invite_code))
                                    chat = chat.chats[0]

                                    links_local.append(
                                        {"id": chat.id, "access_url": url, "name": chat.title, "add_date_utc:": now, "change_date_utc:": now})

                                    print("Added %s " % chat.title)
                                    config.set_links(links_local)
                                    links_global = config.get_links()
                                    logging.info(
                                        'Just added to channel %s', chat.title)
                                else:
                                    logging.info(
                                        'Already in channel %s', chat.title)

                        except Exception as err_chat:
                            logging.info(
                                'Cannot add by code %s: error:%s', invite_code, str(err_chat))
                        break

            await client.forward_messages(to_secondary_id, message)
            logging.info(
                'Forwarding to secondary (id = %s)', message.id)
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


def getInviteStringFromUrl(url):
    search_link = re.search(INVITE_REGEX, url)
    if search_link is not None:
        return (search_link.group(1), False)

    search_link = re.search(URL_REGEX, url)
    if search_link is None:
        return None
    return (search_link.group(1), True)


if __name__ == "__main__":
    print("Press Ctrl+C to exit...")
    asyncio.run(main_exec())
