import json
from enum import Enum
import config
import re


class Symbol(Enum):
    XAUUSD = 1
    BTCUSD = 2
    EURUSD = 3


symbols_regex_map = {}
symbols_regex_map[Symbol.XAUUSD] = "(gold)|(xau)|(xauusd)"
symbols_regex_map[Symbol.BTCUSD] = "(btc)|(btcusd)|(btcusdt)|(bitcoin)"
symbols_regex_map[Symbol.EURUSD] = "(eurusd)"

SIGNAL_REGEX = r"(buy)|(sell)[\D]*([0-9\s]{2,8}\.?[0-9]{0,5})"
TP_REGEX = r"tp[\d]?[\D]*([0-9\s]{2,8}\.?[0-9]{0,5})"
SL_REGEX = r"sl[\D]*([0-9\s]{2,8}\.?[0-9]{0,5})"
PRICE_REGEX = r"([0-9]{4}\.?[0-9]{0,2})"
BREAKEVEN_REGEX = r"(book)|(entry point)|(breakeven)"
SHIFT_SL_REGEX = r"move[\D]*sl[\D]*([0-9\s]{2,8}\.?[0-9]{0,5})"
CLOSE_REGEX = r"(exit)|(close)"


def channel_history_to_verifier_csv(input_hisory_json_file_path, output_verifier_file_path, symbol: Symbol):
    history = config.get_json(input_hisory_json_file_path)
    messages = history['messages']

    symbol_regex = symbols_regex_map[symbol]

    with open(output_verifier_file_path, 'a', encoding="utf-8") as out_file:
        for message in messages:
            process_message(message, out_file, symbol_regex)


def process_message(message, out_file, symbol_regex):
    text = str(message["text"]).lower()

    symbol_search = re.search(symbol_regex, text, re.IGNORECASE)
    signal_search = re.search(SIGNAL_REGEX, text, re.IGNORECASE)

    if (symbol_search == None or signal_search == None):
        return
