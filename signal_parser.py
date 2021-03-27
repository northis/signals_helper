import config
import re
from classes import *
import typing

import helper

symbols_regex_map = {}
symbols_regex_map[Symbol.XAUUSD] = "(gold)|(xau)|(xauusd)"
symbols_regex_map[Symbol.BTCUSD] = "(btc)|(btcusd)|(btcusdt)|(bitcoin)"
symbols_regex_map[Symbol.EURUSD] = "(eurusd)"

SIGNAL_REGEX = r"((buy)|(sel[l]?))[\D]*([0-9]{1,8}[.,]?[0-9]{0,5})"
TP_REGEX = r"tp\s?\d?[\D]+([0-9]{1,8}[.,]?[0-9]{0,5})"
SL_REGEX = r"sl[\D]*([0-9]{1,8}\.?[0-9]{0,5})"
# PRICE_REGEX = r"([0-9]{4}\.?[0-9]{0,2})"
BREAKEVEN_REGEX = r"(book)|(entry point)|(breakeven)"
SL_HIT_REGEX = r"(sl)|(stoplos[s]?).*hit"
TP_HIT_REGEX = r"tp[\D]*\d?[^.,\d].*hit"
CLOSE_REGEX = r"(exit)|(close)"
BUY_REGEX = r"buy"

signals: typing.Dict[int, SignalProps] = dict()
signal_robot = list()


def channel_history_to_verifier_csv(input_hisory_json_file_path, output_verifier_file_path, symbol: Symbol):
    history = config.get_json(input_hisory_json_file_path)
    messages = history['messages']

    symbol_regex = symbols_regex_map[symbol]

    with open(output_verifier_file_path, 'a', encoding="utf-8") as out_file:
        for message in messages:
            process_message(message, out_file, symbol_regex)


last_signal: SignalProps = None


def str_to_utc_iso_datetime(dt):
    return helper.str_to_utc_iso_datetime(dt, config.DT_INPUT_TIMEZONE, config.DT_INPUT_FORMAT)


def string_to_signal(id: int, text: str, symbol_regex: str, date: str, reply_to: SignalProps):
    signal = SignalProps()
    signal.is_buy = re.match(BUY_REGEX, text) != None
    signal.date = str_to_utc_iso_datetime(date)

    symbol_search = re.search(symbol_regex, text)
    signal_search = re.search(SIGNAL_REGEX, text)
    tp_search = re.findall(TP_REGEX, text)
    sl_search = re.search(SL_REGEX, text)
    breakeven_search = re.search(BREAKEVEN_REGEX, text)
    close_search = re.search(CLOSE_REGEX, text)
    tp_hit_search = re.search(TP_HIT_REGEX, text)

    is_reply = reply_to != None
    is_signal = signal_search != None
    is_sl = sl_search != None
    is_tp_hit = tp_hit_search != None
    is_tp = tp_search != None
    is_close = close_search != None
    is_breakeven = breakeven_search != None
    is_symbol = symbol_search != None or (is_reply and is_signal)

    if is_signal and is_symbol:
        # We want to parse signals with price only.
        price_dec = helper.str_to_decimal(signal_search.group(4))
        if price_dec == None:
            print("Cannot get price from signal, ignore it. Message text: %s" % text)
            return None
        signal.price = price_dec

    sl_dec: Decimal = None
    if is_sl:
        sl_dec = helper.str_to_decimal(sl_search.group(1))
        if sl_dec == None:
            print("Cannot get stoploss from signal, ignore it. Message text: %s" % text)
            return None
        signal.stop_loss = sl_dec
    elif not is_reply:
        signal.is_sl_tp_delayed = True

    if is_tp:
        take_profits = list()
        for tp in tp_search:
            tp_dec: Decimal = helper.str_to_decimal(tp)
            if tp_dec == None:
                continue
            take_profits.append(tp_dec)

        signal.take_profits = take_profits

    if is_reply:
        reply_message = MessageProps()
        reply_message.id = id
        reply_message.reply_to_message_id = reply_to.id
        reply_message.text = text
        reply_message.date = signal.date

        if is_breakeven:
            reply_to.move_sl_to_entry = reply_message

        if is_close:
            reply_to.exit = reply_message

        if is_tp_hit:
            if reply_to.tp_hit == None:
                reply_to.tp_hit = list()

            reply_to.tp_hit.append(reply_message)

        if is_sl and reply_to.stop_loss != None:
            if reply_to.move_sl_to_profit == None:
                reply_to.move_sl_to_profit = list()

            reply_message.price = sl_dec
            reply_to.move_sl_to_profit.append(reply_message)

    return signal


def message_to_text(message):
    if message is None:
        return None

    raw_text = message.get("text")
    if raw_text is None:
        return None

    text = str(raw_text).lower()
    return text


def process_message(message, out_file, symbol_regex):
    id = int(message["id"])
    date = message["date"]
    reply_to_message_id = message.get("reply_to_message_id")
    text = message_to_text(message)

    is_reply = reply_to_message_id != None
    reply_to: SignalProps = None
    if is_reply:
        reply_to = signals.get(int(reply_to_message_id))
        if reply_to == None:
            print("Cannot parse the source %s for reply %s" %
                  (reply_to_message_id, text))
            return

    local_signal: SignalProps = string_to_signal(
        id, text, symbol_regex, date, reply_to)

    if local_signal == None:
        return

    local_signal.id = id
    signals[id] = local_signal
    command = ""
    price = ""
    signal_string = f'id:{id}\tdate:{local_signal.date}\tcmd:{command}\tvalue:{price}'
    signal_robot.append(signal_string)


if __name__ == "__main__":
    channel_history_to_verifier_csv(r"C:\Users\north\Desktop\SignalsChannels\ForexWizards\result.json",
                                    r"C:\Users\north\Desktop\SignalsChannels\ForexWizards\result.out.xau.json", Symbol.XAUUSD)
