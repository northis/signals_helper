import re
import typing
import logging
import classes
import config

import helper

symbols_regex_map = {}
symbols_regex_map[classes.Symbol.XAUUSD] = "(gold)|(xau)|(xauusd)"
symbols_regex_map[classes.Symbol.BTCUSD] = "(btc)|(btcusd)|(btcusdt)|(bitcoin)"
symbols_regex_map[classes.Symbol.EURUSD] = "(eurusd)"

SIGNAL_REGEX = r"((buy)|(sel[l]?))[\D]*([0-9]{1,8}[.,]?[0-9]{0,5})"
TP_REGEX = r"tp\s?\d?[\D]+([0-9]{1,8}[.,]?[0-9]{0,5})"
SL_REGEX = r"sl[\D]*([0-9]{1,8}\.?[0-9]{0,5})"
# PRICE_REGEX = r"([0-9]{4}\.?[0-9]{0,2})"
BREAKEVEN_REGEX = r"(book)|(entry point)|(breakeven)"
SL_HIT_REGEX = r"(sl)|(stoplos[s]?).*hit"
TP_HIT_REGEX = r"tp[\D]*\d?[^.,\d].*hit"
CLOSE_REGEX = r"(exit)|(close)"
BUY_REGEX = r"buy"

signals: typing.Dict[int, classes.SignalProps] = dict()


def analyze_channel_symbol(ordered_messges, symbol, min_date, max_date):
    symbol_regex = symbols_regex_map[symbol]
    order_book_symbol = list()

    min_date_str = min_date.strftime(config.DB_DATE_FORMAT)
    max_date_str = max_date.strftime(config.DB_DATE_FORMAT)

    symbol_data = None
    exec_string = f"SELECT [DateTime], High, Low, Close FROM {symbol} WHERE [DateTime] BETWEEN '{min_date_str}' AND '{max_date_str}' ORDER BY [DateTime]"
    with classes.SQLite(config.DB_SYMBOLS_PATH, 'analyze_channel_symbol, db:', None) as cur:
        symbol_data = cur.execute(exec_string).fetchall()

    if symbol_data is None or len(symbol_data) == 0:
        logging.info('analyze_channel_symbol: no data for symbol %s', symbol)

    min_date_str_iso = min_date.strftime(config.ISO_DATE_FORMAT)
    max_date_str_iso = max_date.strftime(config.ISO_DATE_FORMAT)

    filtered_messages = list(filter(lambda x:
                                    x["date"] >= min_date_str_iso and
                                    x["date"] <= max_date_str_iso, ordered_messges))
    root_messages = dict()
    last_signal: classes.SignalProps = None

    symbol_data_len = len(symbol_data)
    i = 0
    for symbol_value in symbol_data:
        if i < symbol_data_len-1:
            next_value = symbol_data[i + 1]
        else:
            break

        i += 1

        current_date_str = symbol_value[0]
        next_date_str = next_value[0]

        orders_open = list(filter(lambda x:
                                  x["is_open"] is True and
                                  x["symbol"] == symbol, order_book_symbol))

        found_messages = list(filter(lambda x:
                                     x["date"] >= current_date_str and
                                     x["date"] < next_date_str, filtered_messages))

        has_messages_in_min = len(found_messages) > 0
        has_orders_open = len(orders_open) > 0

        if not has_messages_in_min and not has_orders_open:
            continue

        # if has_orders_open:
        #     for order in orders_open:
        #         if order["is_buy"] is True:
        #             print("buy")

        for msg in found_messages:
            id_ = msg["id"]
            reply_to_message_id = msg.get("reply_to_message_id")
            is_reply = reply_to_message_id is not None
            root_message = None

            if is_reply:
                root_message = root_messages[id_]
            else:
                root_message = last_signal

            signal = string_to_signal(msg, symbol_regex, root_message)
            if signal is None:
                continue

            root_messages[id_] = signal
            last_signal = signal

    return order_book_symbol


def str_to_utc_iso_datetime(dt):
    return helper.str_to_utc_iso_datetime(dt, config.DT_INPUT_TIMEZONE, config.DT_INPUT_FORMAT)


def string_to_signal(msg: str, symbol_regex: str, reply_to: classes.SignalProps):
    id: int = msg["id"]
    text: str = message_to_text(msg)
    date: str = msg["date"]

    if reply_to is None and text is None:
        print("Cannot get price from signal, ignore it. Message text ia null and we don't have a reply")
        return None

    signal = classes.SignalProps()
    signal.is_buy = re.match(BUY_REGEX, text) != None
    signal.date = str_to_utc_iso_datetime(date)

    symbol_search = re.search(symbol_regex, text)
    signal_search = re.search(SIGNAL_REGEX, text)
    tp_search = re.findall(TP_REGEX, text)
    sl_search = re.search(SL_REGEX, text)
    breakeven_search = re.search(BREAKEVEN_REGEX, text)
    close_search = re.search(CLOSE_REGEX, text)
    tp_hit_search = re.search(TP_HIT_REGEX, text)

    is_reply = reply_to is not None
    is_signal = signal_search is not None
    is_sl = sl_search is not None
    is_tp_hit = tp_hit_search is not None
    is_tp = tp_search is not None
    is_close = close_search is not None
    is_breakeven = breakeven_search is not None
    is_symbol = symbol_search is not None or (is_reply and is_signal)

    if is_signal and is_symbol:
        # We want to parse signals with price only.
        price_dec = helper.str_to_decimal(signal_search.group(4))
        if price_dec is None:
            print("Cannot get price from signal, ignore it. Message text: %s" % text)
            return None
        signal.price = price_dec

    sl_dec: classes.Decimal = None
    if is_sl:
        sl_dec = helper.str_to_decimal(sl_search.group(1))
        if sl_dec is None:
            print("Cannot get stoploss from signal, ignore it. Message text: %s" % text)
            return None
        signal.stop_loss = sl_dec
    elif not is_reply:
        signal.is_sl_tp_delayed = True

    if is_tp:
        take_profits = list()
        for tp in tp_search:
            tp_dec: classes.Decimal = helper.str_to_decimal(tp)
            if tp_dec is None:
                continue
            take_profits.append(tp_dec)

        signal.take_profits = take_profits

    if is_reply:
        reply_message = classes.MessageProps()
        reply_message.id = id
        reply_message.reply_to_message_id = reply_to.id
        reply_message.text = text
        reply_message.date = signal.date

        if is_breakeven:
            reply_to.move_sl_to_entry = reply_message

        if is_close:
            reply_to.exit = reply_message

        if is_tp_hit:
            if reply_to.tp_hit is None:
                reply_to.tp_hit = list()

            reply_to.tp_hit.append(reply_message)

        if is_sl and reply_to.stop_loss is not None:
            if reply_to.move_sl_to_profit is None:
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
