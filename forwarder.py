from telethon import TelegramClient, sync, events, tl
import config
import os
from dotenv import load_dotenv
import asyncio
import logging
import re

logging.basicConfig(filename='forwarder.log', encoding='utf-8', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)


load_dotenv()
main_config = config.get_config()
api_id = os.getenv('api_id')
api_hash = os.getenv('api_hash')
forwards = main_config['forwards']


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
        message_text = str(message.to_dict()['message']).lower()

        is_primary = True

        if message.entities != None:
            for entity in message.entities:
                if isinstance(entity, tl.types.MessageEntityUrl):
                    is_primary = False
                    break

        if is_primary:
            is_primary = re.match(
                r"https?: \/\/(www\.)?[-a-zA-Z0-9@: % ._\+ ~  # =]{1,256}\.[a-zA-Z0-9()]{1,6}\b([-a-zA-Z0-9()!@:%_\+.~#?&\/\/=]*)", message_text) == None

        try:
            if is_primary:
                await client.forward_messages(to_primary_id, message)
                logging.info('Forwarding to primary (id = %s)', message.id)
            elif to_secondary_id != 0:
                await client.forward_messages(to_secondary_id, message)
                logging.info('Forwarding to secondary (id = %s)', message.id)
            else:
                logging.info(
                    'Message has not been forwarded (id = %s)', message.id)

        except Exception as err:
            logging.exception(err)


if __name__ == "__main__":
    print("Press Ctrl+C to exit...")
    asyncio.run(main_exec())
