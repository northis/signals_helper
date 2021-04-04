import json

DB_DATE_FORMAT = r"%Y-%m-%d %H:%M:%S+00:00"
ISO_DATE_FORMAT = r"%Y-%m-%dT%H:%M:%S+00:00"
SESSION_FILE = 'secure_session.session'
SESSION_HISTORY_FILE = 'secure_session_history.session'
DT_INPUT_FORMAT = r"%Y-%m-%dT%H:%M:%S"
DT_INPUT_TIMEZONE = "Europe/Moscow"


def get_json(file):
    with open(file, 'r', encoding="utf-8") as f:
        config = json.load(f)
        return config


def set_json(file, json_object):
    with open(file, 'w', encoding="utf-8") as f:
        json.dump(obj=json_object, fp=f, indent=2,
                  sort_keys=True, ensure_ascii=False)


def get_config():
    return get_json('config.json')


def set_config(config):
    set_json('config.json', config)


def get_links():
    return get_json('links.json')


def set_links(links):
    set_json('links.json', links)
