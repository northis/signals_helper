import json
import os
from dotenv import load_dotenv
load_dotenv()

DB_DATE_FORMAT = r"%Y-%m-%d %H:%M:%S+00:00"
ISO_DATE_FORMAT = r"%Y-%m-%dT%H:%M:%S+00:00"
ISO_LOCAL_DATE_FORMAT = r"%Y-%m-%dT%H:%M:%S"
ISO_DATE_IMPORT_FORMAT = r"%Y-%m-%dT%H:%M:%S.%f"
SESSION_FILE = 'secure_session.session'
SESSION_HISTORY_FILE = 'secure_session_history.session'
DT_INPUT_FORMAT = r"%Y-%m-%dT%H:%M:%S"
DT_INPUT_TIMEZONE = "Europe/Moscow"
DB_STATS_PATH = os.getenv("db_stats_path")
DB_SYMBOLS_PATH = os.getenv("db_symbols_path")
CHANNELS_HISTORY_DIR = os.getenv("channels_history_dir")
CHANNELS_ANALYSIS_DIR = os.getenv("channels_analysis_dir")
api_id = os.getenv('api_id')
api_hash = os.getenv('api_hash')
PINNED_EXCEPT_CHANNEL_ID = os.getenv('pinned_except_channel_id')
API_KEY = os.getenv("api_key")
API_KEY_INVESTING = os.getenv("api_key_investing")
PINNED_INFO_MESSAGE_ID = os.getenv("pinned_info_message_id")
TEMPLATE_PINNED_PATH = "template.pinned.txt"


def get_json(file):
    with open(file, 'r', encoding="utf-8") as f:
        config = json.load(f)
        return config


def set_json(file, json_object):
    with open(file, 'w', encoding="utf-8") as f:
        json.dump(obj=json_object, fp=f, indent=2,
                  sort_keys=True, ensure_ascii=False)


def get_file_text(file):
    with open(file, 'r', encoding="utf-8") as f:
        return f.read()


def set_file_text(file, string):
    with open(file, 'w', encoding="utf-8") as f:
        f.write(string)


def get_config():
    return get_json('config.json')


def set_config(config):
    set_json('config.json', config)


def get_links():
    return get_json('links.json')


def set_links(links):
    set_json('links.json', links)
