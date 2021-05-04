import re
import logging
import classes
import config
import copy

import helper

symbols_regex_map = {}
symbols_regex_map[classes.Symbol.XAUUSD] = "(gold)|(xau)|(xauusd)"
# symbols_regex_map[classes.Symbol.BTCUSD] = "(btc)|(btcusd)|(btcusdt)|(bitcoin)"
# symbols_regex_map[classes.Symbol.EURUSD] = "(eurusd)"
# TODO some issues in parsing eurusd 1.xxx prices, going to resolve it in the future, now I wanna focus on gold only


SIGNAL_REGEX = r"((buy)|(sel[l]?))[\D]*([0-9]{1,8}[.,]?[0-9]{0,5})"
TP_REGEX = r"tp\s?\d?[\D]+([0-9]{1,8}[.,]?[0-9]{0,5})"
SL_REGEX = r"sl[\D]*([0-9]{1,8}\.?[0-9]{0,5})"
# PRICE_REGEX = r"([0-9]{4}\.?[0-9]{0,2})"
BREAKEVEN_REGEX = r"(book)|(entry point)|(breakeven)"
SL_HIT_REGEX = r"(sl)|(stoplos[s]?).*hit"
TP_HIT_REGEX = r"tp[\D]*\d?[^.,\d].*hit"
CLOSE_REGEX = r"(exit)|(close)"
BUY_REGEX = r"buy"


def signal_to_orders(signal: classes.SignalProps, next_date: str, next_value: classes.Decimal, order_book: list):
    if not signal.is_signal:
        logging.debug("Cannot convert to order non-signal entity")
        return

    order = None
    is_exit = signal.exit_ is not None
    is_move_sl_to_entry = signal.move_sl_to_entry is not None
    is_move_sl_to_profit = signal.move_sl_to_profit is not None
    is_tp_hit = signal.tp_hit is not None
    is_sl_hit = signal.sl_hit is not None
    is_buy = signal.is_buy

    is_initial_signal = not(
        is_exit or is_move_sl_to_entry or is_move_sl_to_profit)

    if is_initial_signal:
        order = {}
        order["id"] = signal.id_
        order["is_buy"] = is_buy
        order["datetime"] = next_date
        order["price_signal"] = signal.price
        order["price_actual"] = next_value
        order["is_open"] = True

        if signal.stop_loss is not None:
            order["stop_loss"] = signal.stop_loss

        if signal.take_profits is not None:
            take_profits_len = len(signal.take_profits)
            if take_profits_len == 1:
                order["take_profit"] = signal.take_profits[0]
            elif take_profits_len > 1:
                for t_p in signal.take_profits:
                    tp_order = copy.deepcopy(order)
                    tp_order["take_profit"] = t_p
                    validate_order(tp_order, signal, next_value)
                    order_book.append(tp_order)

        validate_order(order, signal, next_value)
        order_book.append(order)
        return

    related_orders = list(
        filter(lambda x: x["id"] == signal.id_ and x["is_open"] is True, order_book))

    if len(related_orders) == 0:
        logging.debug("No related orders found")
        return

    for related_order in related_orders:
        if is_move_sl_to_entry:
            related_order["stop_loss"] = related_order["price_actual"]
            related_order["close_datetime"] = next_date

        if is_move_sl_to_profit:
            if is_buy:
                order["stop_loss"] = max(
                    signal.move_sl_to_profit, key=lambda x: float(x.price))
            else:
                order["stop_loss"] = min(
                    signal.move_sl_to_profit, key=lambda x: float(x.price))
            related_order["last_sl_move"] = next_date

        if is_exit or is_sl_hit:
            order["close_price"] = next_value
            order["close_datetime"] = next_date
            order["is_open"] = False

        if is_exit:
            order["manual_exit"] = True

        if is_sl_hit:
            order["sl_exit"] = True

        if is_tp_hit:
            if is_buy and order["take_profit"] > next_value or (not is_buy) and order["take_profit"] < next_value:
                order["close_price"] = next_value
                order["close_datetime"] = next_date
                order["is_open"] = False
                order["tp_exit"] = True

        validate_order(related_order, signal, next_value)


def validate_order(order: dict, signal: classes.SignalProps, next_value: classes.Decimal):
    errors = list()
    take_profit = order.get("take_profit")
    stop_loss = order.get("stop_loss")

    if signal.is_buy:
        if stop_loss is not None and stop_loss > next_value:
            logging.debug("Wrong stoploss (buy), close the order now")
            errors.append("wrong_sl_buy")

        if take_profit is not None and take_profit < order["price_actual"]:
            errors.append("wrong_tp_buy")

    else:
        if stop_loss is not None and stop_loss < next_value:
            logging.debug("Wrong stoploss (sell), close the order now")
            errors.append("wrong_sl_sell")

        if take_profit is not None and take_profit > order["price_actual"]:
            errors.append("wrong_tp_sell")

    if len(errors) > 0:
        order["error_state"] = errors
        order["is_open"] = False


def update_orders(next_date: str, next_value: classes.Decimal, open_orders: list):
    for order in open_orders:
        stop_loss = order.get("stop_loss")
        take_profit = order.get("take_profit")

        buy_close_cond = order["is_buy"] and (
            stop_loss is not None and next_value <= stop_loss or take_profit is not None and next_value >= take_profit)
        sell_close_cond = (not order["is_buy"]) and (
            stop_loss is not None and next_value >= stop_loss or take_profit is not None and next_value <= take_profit)

        if buy_close_cond or sell_close_cond:
            order["auto_hit"] = True
            order["close_price"] = next_value
            order["close_datetime"] = next_date
            order["is_open"] = False


def analyze_channel_symbol(ordered_messges, symbol, min_date, max_date):
    symbol_regex = symbols_regex_map[symbol]

    order_book = list()
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
    current_date_str = None
    next_date_str = None

    for symbol_value in symbol_data:
        if i < symbol_data_len-1:
            next_value = symbol_data[i + 1]
        else:
            break

        i += 1

        current_date_str = helper.str_to_utc_datetime(
            symbol_value[0], input_format=config.DB_DATE_FORMAT).isoformat()
        next_date_str = helper.str_to_utc_datetime(
            next_value[0], input_format=config.DB_DATE_FORMAT).isoformat()

        orders_open = list(filter(lambda x:
                                  x["is_open"] is True, order_book))

        found_messages = list(filter(lambda x:
                                     x["date"] >= current_date_str and
                                     x["date"] < next_date_str, filtered_messages))

        has_messages_in_min = len(found_messages) > 0
        has_orders_open = len(orders_open) > 0

        if not has_messages_in_min and not has_orders_open:
            continue

        # We decide that this and appropriate time to catch up a signal -
        # close of current minute and time of text minute starts (next_date_str)
        value_close = symbol_value[3]

        if has_orders_open:
            update_orders(next_date_str, value_close, orders_open)

        for msg in found_messages:
            id_ = msg["id"]
            reply_to_message_id = msg.get("reply_to_msg_id")
            is_reply = reply_to_message_id is not None
            root_message = None

            if is_reply:
                root_message = root_messages.get(reply_to_message_id)
            else:
                root_message = last_signal

            signal = string_to_signal(msg, symbol_regex, root_message)
            if signal is None:
                continue

            if root_message is None and signal.is_signal:
                root_message = signal

            if root_message is not None:
                root_messages[id_] = root_message

            signal_to_orders(signal, next_date_str, value_close, order_book)

            if signal.is_sl_tp_delayed:
                last_signal = signal
            elif last_signal is not None and not last_signal.is_sl_tp_delayed:
                last_signal = None

    out_list = list(
        filter(lambda x: x.is_signal is True, root_messages.values()))
    return (out_list, order_book)


def str_to_utc_iso_datetime(dt):
    return helper.str_to_utc_iso_datetime(dt, config.DT_INPUT_TIMEZONE, config.DT_INPUT_FORMAT)


def string_to_signal(
        msg: str, symbol_regex: str, reply_to: classes.SignalProps):

    id_: int = msg["id"]
    text: str = message_to_text(msg)
    date: str = msg["date"]

    if reply_to is None and text is None:
        logging.debug(
            "Cannot get price from signal, ignore it. Message text is null and we don't have a reply")
        return None

    if text is None:
        text = ""

    signal = classes.SignalProps()
    signal.is_buy = re.search(BUY_REGEX, text) != None
    signal.date = date
    signal.id_ = id_
    signal.text = text

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
    is_symbol = symbol_search is not None or (is_reply and reply_to.is_signal)

    signal.is_signal = is_signal

    if is_reply:
        reply_to.update_date = date

    if not is_signal and not is_reply:
        logging.debug(
            "Cannot get price from signal, ignore it. Message text isn't a signal and we don't have a reply")
        return None

    if not is_symbol:
        logging.debug("Not a symbol we need")
        return None

    if is_signal:
        if not is_sl or not is_tp:
            signal.is_sl_tp_delayed = True

        # We want to parse signals with price only.
        price_dec = helper.str_to_decimal(signal_search.group(4))
        if price_dec is None:
            logging.debug(
                "Cannot get price from signal, ignore it. Message text: %s", text)
            return None
        signal.price = price_dec

    sl_dec: classes.Decimal = None

    need_reset_delayed_flag = False
    if is_sl:
        sl_dec = helper.str_to_decimal(sl_search.group(1))
        if sl_dec is None:
            logging.debug(
                "Cannot get stoploss from signal, ignore it. Message text: %s", text)
            return None
        signal.stop_loss = sl_dec
        if is_reply and reply_to.is_sl_tp_delayed:
            need_reset_delayed_flag = True

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
        if is_reply and reply_to.is_sl_tp_delayed:
            need_reset_delayed_flag = True

    if need_reset_delayed_flag:
        reply_to.is_sl_tp_delayed = False

    if is_reply:
        reply_message = classes.MessageProps()
        reply_message.id_ = id_
        reply_message.reply_to_message_id = reply_to.id_
        reply_message.text = text
        reply_message.date = signal.date

        if is_breakeven:
            reply_to.move_sl_to_entry = reply_message

        if is_close:
            reply_to.exit_ = reply_message

        if is_tp_hit:
            if reply_to.tp_hit is None:
                reply_to.tp_hit = list()
                reply_to.move_sl_to_entry = reply_message
                # If we hit TP (1), we move sl to breakeven too.

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
