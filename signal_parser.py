import json
import config
import re
from classes import *
import typing

import datetime
from datetime import timezone
import pytz

symbols_regex_map = {}
symbols_regex_map[Symbol.XAUUSD] = "(gold)|(xau)|(xauusd)"
symbols_regex_map[Symbol.BTCUSD] = "(btc)|(btcusd)|(btcusdt)|(bitcoin)"
symbols_regex_map[Symbol.EURUSD] = "(eurusd)"

SIGNAL_REGEX = r"((buy)|(sel[l]?))[\D]*([0-9]{1,8}[.,]?[0-9]{0,5})"
TP_REGEX = r"tp[\D]*\d?[^.,\d]*([0-9]{1,8}[.,]?[0-9]{0,5})"
SL_REGEX = r"sl[\D]*([0-9\s]{1,8}\.?[0-9]{0,5})"
# PRICE_REGEX = r"([0-9]{4}\.?[0-9]{0,2})"
BREAKEVEN_REGEX = r"(book)|(entry point)|(breakeven)"
CLOSE_REGEX = r"(exit)|(close)"
BUY_REGEX = r"buy"
DT_INPUT_FORMAT = r"%Y-%m-%dT%H:%M:%S"
DT_INPUT_TIMEZONE = "Europe/Moscow"

signals: typing.Dict[int, SignalProps] = dict()


def channel_history_to_verifier_csv(input_hisory_json_file_path, output_verifier_file_path, symbol: Symbol):
    history = config.get_json(input_hisory_json_file_path)
    messages = history['messages']

    symbol_regex = symbols_regex_map[symbol]

    with open(output_verifier_file_path, 'a', encoding="utf-8") as out_file:
        for message in messages:
            process_message(message, out_file, symbol_regex)


last_signal: SignalProps = None


def str_to_utc_iso_datetime(dt):
    tz1 = pytz.timezone(DT_INPUT_TIMEZONE)
    tz2 = pytz.timezone("UTC")

    dt_typed = datetime.datetime.strptime(dt, DT_INPUT_FORMAT)
    dt_typed = tz1.localize(dt_typed)
    dt_typed = dt_typed.astimezone(tz2)
    return dt_typed.isoformat()


def string_to_signal(text: str, symbol_regex: str, date: str, reply_to: SignalProps):
    signal = SignalProps()
    signal.is_buy = re.match(BUY_REGEX, text) != None
    signal.datetime_utc = str_to_utc_iso_datetime(date)

    symbol_search = re.search(symbol_regex, text)
    signal_search = re.search(SIGNAL_REGEX, text)
    tp_search = re.search(TP_REGEX, text)
    sl_search = re.search(SL_REGEX, text)
    breakeven_search = re.search(BREAKEVEN_REGEX, text)
    close_search = re.search(CLOSE_REGEX, text)

    is_reply = reply_to != None
    is_signal = signal_search != None
    is_tp_sl = tp_search != None and sl_search != None
    is_close = close_search != None
    is_breakeven = breakeven_search != None
    is_symbol = symbol_search != None or (is_reply and is_signal)

    if is_signal and is_symbol:
        price_str = signal_search.group(4)
        if price_str == None or (not price_str.replace(",", ".").isdecimal()):
            print("Cannot get price from signal, ignore it. Message text: %s" % text)
            return None
        price_group_dec = Decimal(price_str)
        signal.price = price_group_dec

    if ((not is_symbol) and (not is_signal)):
        # ignore this message
        return None

    # price_group = signal_search.groups(4)
    # if price_group == None:
    #     return None

    # signal.price = Decimal(price_group)

    # if tp_search == None or sl_search == None:
    #     signal.is_sl_tp_delayed = True
    #     signal.datetime_utc
    #     return signal

    # if tp_search != None:
    #     for tp_group in tp_search.groups():
    #         print(tp_group)

    return signal


def message_to_text(message):
    if message == None:
        return None

    raw_text = message.get("text")
    if raw_text == None:
        return None

    text = str(raw_text).lower()
    return text


def process_message(message, out_file, symbol_regex):
    id = int(message["id"])
    date = message["date"]
    reply_to_message_id = message.get("reply_to_message_id")
    text = message_to_text(message)

    is_reply = reply_to_message_id != None
    # if is_reply:
    #     local_signal

    reply_to: SignalProps = None
    if is_reply:
        reply_to = signals.get(int(reply_to_message_id))
        if reply_to == None:
            print("Cannot parse the source %s for reply %s" %
                  (reply_to_message_id, text))
            return

    local_signal: SignalProps = string_to_signal(
        text, symbol_regex, date, reply_to)

    # if source.is_sl_tp_delayed and local_signal != None:
    #     source.is_sl_tp_delayed = False
    #     source.stop_loss = local_signal.stop_loss
    #     source.take_profits = local_signal.take_profits
    #

    if local_signal != None:
        local_signal.id = id
        signals[id] = local_signal


if __name__ == "__main__":
    channel_history_to_verifier_csv(r"C:\Users\north\Desktop\SignalsChannels\ForexWizards\result.json",
                                    r"C:\Users\north\Desktop\SignalsChannels\ForexWizards\result.out.xau.json", Symbol.XAUUSD)
