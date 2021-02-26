from telethon import TelegramClient, sync, events, tl, errors
from telethon.tl.functions.messages import ImportChatInviteRequest, CheckChatInviteRequest, GetDialogsRequest
from telethon.tl.functions.channels import GetMessagesRequest
import config
import os
from dotenv import load_dotenv
import asyncio
import logging
import re

SIGNAL_REGEX = r"(buy)|(sell)"
LINKS_REGEX = r"https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()!@:%_\+.~#?&\/\/=]*)"
INVITE_REGEX = r"joinchat/(.+)"

logging.basicConfig(filename='forwarder.log', encoding='utf-8', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)


load_dotenv()
main_config = config.get_config()
api_id = os.getenv('api_id')
api_hash = os.getenv('api_hash')
forwards = main_config['forwards']
join_channels = main_config['join_channels']


async def main_exec():
    async with TelegramClient('secure_session.session', api_id, api_hash) as client:
        for forward in forwards:
            from_chat_id = forward['from_chat_id']
            from_chat_title = forward['from_chat_title']
            to_primary = forward['to_primary']

            is_ready = False

            if from_chat_id == 0:
                async for dialog in client.iter_dialogs():
                    if from_chat_title == dialog.title:
                        from_chat_id = dialog.id
                        forward['from_chat_id'] = from_chat_id
                        logging.info('Adding from_chat_id %s' %
                                     from_chat_id)
                        is_ready = True
                        break
            else:
                is_ready = True

            if not is_ready:
                logging.info('Cannot add forward source %s' % from_chat_title)
                continue

            if to_primary['id'] == 0:
                is_ready = False
                async for dialog in client.iter_dialogs():
                    if to_primary['title'] == dialog.title:
                        to_primary['id'] = dialog.id
                        logging.info('Adding to_primary id %s' %
                                     dialog.id)
                        is_ready = True
                        break

            if not is_ready:
                logging.info('Cannot add primary destination %s' %
                             from_chat_title)
                continue

            to_secondary_id = 0
            to_primary_id = to_primary['id']

            if 'to_secondary' in forward:
                to_secondary = forward['to_secondary']
                to_secondary_id = to_secondary['id']
                if to_secondary_id == 0:
                    async for dialog in client.iter_dialogs():
                        if to_secondary['title'] == dialog.title:
                            to_secondary['id'] = dialog.id
                            logging.info('Adding to_secondary id %s' %
                                         dialog.id)
                            break

            config.set_config(main_config)
            forward_exec(from_chat_id, to_primary_id, to_secondary_id, client)
        await client.run_until_disconnected()


def forward_exec(from_chat_id, to_primary_id, to_secondary_id, client):
    @client.on(events.NewMessage(chats=(from_chat_id)))
    async def handler(event):
        message = event.message
        orig_message_text = str(message.to_dict()['message'])
        message_text = orig_message_text.lower()

        is_primary = message.button_count == 0
        join_urls = list()

        reply = None
        reply_text = None
        if message.is_reply:
            reply = await message.get_reply_message()
            is_primary = is_primary and (reply.button_count == 0)
            if reply.entities != None:
                for entity in reply.entities:
                    if isinstance(entity, tl.types.MessageEntityTextUrl):
                        join_urls.append(entity.url)
                        is_primary = False
                        break

        if message.entities != None:
            for entity in message.entities:
                if isinstance(entity, tl.types.MessageEntityTextUrl):
                    join_urls.append(entity.url)
                    is_primary = False
                    break

        if message.buttons != None:
            for button in message.buttons:
                if isinstance(button, tl.types.KeyboardButtonUrl):
                    join_urls.append(button.url)
                    break
                if not isinstance(button, list):
                    continue
                for buttonInner in button:
                    if isinstance(buttonInner.button, tl.types.KeyboardButtonUrl):
                        join_urls.append(buttonInner.button.url)
                        break

# join_channels
        if message.entities != None:
            for entity in message.entities:
                if isinstance(entity, tl.types.MessageEntityUrl):
                    is_primary = False
                    break

        if (reply != None) and (reply.entities != None):
            for entity in reply.entities:
                if isinstance(entity, tl.types.MessageEntityUrl):
                    is_primary = False
                    break

        contains_no_links = None
        is_signal = None

        message_link = re.search(LINKS_REGEX, orig_message_text, re.IGNORECASE)
        contains_no_links = message_link == None
        if (message_link != None):
            join_urls.append(message_link.group(0))

        if reply != None:
            reply_text = str(reply.to_dict()['message'])
            message_link = re.search(LINKS_REGEX, reply_text, re.IGNORECASE)
            contains_no_links = contains_no_links and (message_link == None)
            if (message_link != None):
                join_urls.append(message_link.group(0))

        is_signal = re.search(SIGNAL_REGEX, message_text) != None
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

                if primary_reply_outer == None:
                    if reply_text == None:
                        await client.forward_messages(to_primary_id, message)
                        logging.info(
                            'Forwarding to primary (id = %s)', message.id)
                    else:
                        ready_message = "`" + reply_text + "`\n" + \
                            message_text + "\n*" + message.chat.title + "*"
                        await client.send_message(to_primary_id, ready_message)
                        logging.info(
                            'Replying missed message to primary (id = %s)', message.id)

                else:
                    answer_message = orig_message_text + "\n*" + message.chat.title + "*"
                    await client.send_message(to_primary_id, answer_message, reply_to=primary_reply_outer)
                    logging.info('Replying to primary (id = %s)', message.id)

            elif to_secondary_id != 0:
                if join_channels:
                    for url in join_urls:
                        invite_code = getInviteStringFromUrl(url)
                        if invite_code != None:
                            logging.info(
                                'Joining with invite code %s', invite_code)
                            try:
                                result = await client(CheckChatInviteRequest(
                                    hash=invite_code
                                ))
                                logging.info('Checking channel %s',
                                             result.chat.title)
                                need_to_add = True
                                async for dialog in client.iter_dialogs():
                                    if dialog.entity.id == result.chat.id:
                                        need_to_add = False
                                        break
                                if need_to_add:
                                    await client(ImportChatInviteRequest(invite_code))
                                    logging.info(
                                        'Just added to channel %s', result.chat.title)
                                else:
                                    logging.info(
                                        'Already in channel %s', result.chat.title)

                            except Exception as errChat:
                                errChat.with_traceback
                                logging.info(
                                    'Cannot add by code %s: error:%s', invite_code, str(errChat))
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
            except Exception as errDel:
                logging.exception(errDel)
        except Exception as err:
            logging.exception(err)


def getInviteStringFromUrl(url):
    search_link = re.search(INVITE_REGEX, url)
    if search_link != None:
        return search_link.group(1)
    return None


if __name__ == "__main__":
    print("Press Ctrl+C to exit...")
    asyncio.run(main_exec())
