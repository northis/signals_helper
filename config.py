import json


def get_config():
    with open('config.json', 'r', encoding="utf-8") as f:
        config = json.load(f)
        return config


def set_config(config):
    with open('config.json', 'w', encoding="utf-8") as f:
        json.dump(obj=config, fp=f, indent=2,
                  sort_keys=True, ensure_ascii=False)
