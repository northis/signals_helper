import re
import logging
import copy
import classes
import config
from collections import namedtuple

import helper

symbols_regex_map = {}
symbols_regex_map[classes.Symbol.XAUUSD] = "(gold)|(xau[\s\\\/-]*usd)"
symbols_regex_map[classes.Symbol.BTCUSD] = "(btc[\s\\\/-]*usd[t]?)"
# symbols_regex_map[classes.Symbol.EURUSD] = "(eurusd)"


SIGNAL_REGEX = r"(b[a|u]y|sel[l]?)[\D]*([0-9]{1,8}[.,;:\s]?[0-9]{0,5})"
TP_REGEX = r"t(ake\s)?p(rofit)?[\D]*([1-5]?)([\D])*([0-9]{1,8}[.,:\s]?[0-9]{0,5})"
SL_REGEX = r"s(top\s)?[l|t](oss)?[\D]*([0-9]{1,8}[.,;:\s]?[0-9]{0,5}?)"
BREAKEVEN_REGEX = r"(book)|(entry point)|(breakeven)"
SL_HIT_REGEX = r"(sl|stop[\s]?los[s]?)(.)*hit"
TP_HIT_REGEX = r"tp[\D]*\d?[^.,\d].*hit"
CLOSE_REGEX = r"(exit)|(close)"
BUY_REGEX = r"b[a|u]y"
PRICE_VALID_PERCENT = 10
USE_FAST_BOOK = True
SignalTyple = namedtuple(
    'Signal', 'symbol_search price is_buy sl tps')


def validate_order(order: dict, next_value: classes.Decimal):
    errors = list()
    take_profit = order.get("take_profit")
    stop_loss = order.get("stop_loss")
    has_sl = stop_loss is not None
    has_tp = take_profit is not None
    next_value_fl = float(next_value)

    if has_sl and (100*abs(float(stop_loss) - next_value_fl)/next_value_fl > PRICE_VALID_PERCENT):
        errors.append("wrong_sl")

    if has_tp and (100*abs(float(take_profit) - next_value_fl)/next_value_fl > PRICE_VALID_PERCENT):
        errors.append("wrong_tp")

    if order["is_buy"]:
        if has_sl and stop_loss > next_value:
            logging.debug("Wrong stoploss (buy)")
            errors.append("wrong_sl_buy")

        if has_tp and take_profit < next_value:
            errors.append("wrong_tp_buy")

    else:
        if has_sl and stop_loss < next_value:
            logging.debug("Wrong stoploss (sell)")
            errors.append("wrong_sl_sell")

        if has_tp and take_profit > next_value:
            errors.append("wrong_tp_sell")

    if len(errors) > 0:
        order["error_state"] = errors
        order["is_open"] = False
        return False
    return True


def update_orders(next_date: str, high: classes.Decimal, low: classes.Decimal, close: classes.Decimal, open_orders: list):

    is_closed = False
    for order in open_orders:
        stop_loss = order.get("stop_loss")
        take_profit = order.get("take_profit")

        buy_close_cond = order["is_buy"] and (
            stop_loss is not None and low <= stop_loss or take_profit is not None and high >= take_profit)
        sell_close_cond = (not order["is_buy"]) and (
            stop_loss is not None and high >= stop_loss or take_profit is not None and low <= take_profit)

        if buy_close_cond or sell_close_cond:
            order["auto_hit"] = True
            order["close_price"] = close
            order["close_datetime"] = next_date
            order["is_open"] = False
            is_closed = True

    if not is_closed:
        return

    filtered = list(filter(lambda x: x["is_open"] and x.get(
        "has_breakeven") == None, open_orders))
    for order in filtered:
        order["stop_loss"] = order["price_actual"]


def analyze_channel_symbol(ordered_messges, symbol, min_date, max_date, channel_id):
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

    symbol_data_len = len(symbol_data)
    i = 0
    current_date_str = None
    next_date_str = None

    logging.info('analyze_channel_symbol: setting orders... %s', symbol)
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

        value_high = symbol_value[1]
        value_low = symbol_value[2]
        value_close = symbol_value[3]

        if has_orders_open:
            update_orders(next_date_str, value_high,
                          value_low, value_close, orders_open)

        for msg in found_messages:
            id_ = msg["id"]
            reply_to_message_id = msg.get("reply_to_msg_id")
            is_reply = reply_to_message_id is not None
            target_orders = None

            if is_reply:
                reply_sources = list(
                    filter(lambda x: x["id"] == reply_to_message_id, orders_open))
                target_orders = reply_sources
            else:
                target_orders = orders_open

            string_to_orders(
                msg, symbol_regex, target_orders, next_date_str, value_close, order_book)

    return (order_book, symbol, channel_id)


def str_to_utc_iso_datetime(dt):
    return helper.str_to_utc_iso_datetime(dt, config.DT_INPUT_TIMEZONE, config.DT_INPUT_FORMAT)


def message_to_signal(text, symbol_regex):
    symbol_search = re.search(symbol_regex, text)
    signal_search = re.search(SIGNAL_REGEX, text, flags=re.MULTILINE)
    tp_search = re.findall(TP_REGEX, text, flags=re.MULTILINE)
    sl_search = re.search(SL_REGEX, text, flags=re.MULTILINE)
    is_buy = re.search(BUY_REGEX, text) != None

    price = get_price(signal_search)
    sl = get_sl(sl_search)
    price = get_price(signal_search)
    tps = get_tps(tp_search, is_buy)

    tp_len = len(tps)
    # They can mess up buy and sell words, so we try to fix this by guessing on the price, tp and sl
    if price is None:
        if sl is not None and tp_len > 0:
            is_buy = tps[0] > sl
    else:
        if sl is not None:
            is_buy = price > sl
        elif tp_len > 0:
            is_buy = price < tps[0]

    res = SignalTyple(symbol_search, price, is_buy, sl, tps)
    return res


def get_tps(tp_search, is_buy):
    tp_list = list()
    if tp_search is None:
        return get_tps
    for tp_entry in tp_search:

        tp_str = tp_entry[4]
        tp_group_3 = tp_entry[3]
        if tp_group_3 == '' or tp_group_3 is None:
            tp_str = f"{tp_entry[2]}{tp_str}"

        tp_dec: classes.Decimal = helper.str_to_decimal(tp_str)
        if tp_dec is None:
            continue
        tp_list.append(tp_dec)

    tp_list = sorted(tp_list)
    if not is_buy:
        tp_list = list(reversed(tp_list))
    return tp_list


def get_sl(sl_search):
    if sl_search is None:
        return None
    sl_dec = helper.str_to_decimal(sl_search.group(3))
    if sl_dec is None:
        return None
    return sl_dec


def get_price(signal_search):
    if signal_search is None:
        return None
    price = helper.str_to_decimal(signal_search.group(2))
    if price is None:
        return None
    return price


def string_to_orders(
        msg: str,
        symbol_regex: str,
        target_orders: list,
        next_date: str,
        next_value: classes.Decimal,
        order_book: list):

    id_: int = msg["id"]
    text: str = message_to_text(msg)
    date: str = msg["date"]

    if target_orders is None and text is None:
        logging.debug(
            "Cannot get price from signal, ignore it. Message text is null and we don't have a reply")
        return None

    if text is None:
        text = ""

    symbol_search, price, is_buy, sl, tps = message_to_signal(
        text, symbol_regex)

    breakeven_search = re.search(BREAKEVEN_REGEX, text)
    close_search = re.search(CLOSE_REGEX, text)
    tp_hit_search = re.search(TP_HIT_REGEX, text)
    sl_hit_search = re.search(SL_HIT_REGEX, text)

    has_target_orders = target_orders is not None and len(target_orders) > 0
    is_signal = price is not None
    is_sl = sl is not None
    is_tp_hit = tp_hit_search is not None
    is_sl_hit = sl_hit_search is not None
    is_tp = tps is not None and len(tps) > 0
    # If we hit TP (1), we move sl to breakeven too.
    is_breakeven = breakeven_search is not None or is_tp_hit
    is_close = close_search is not None and breakeven_search is None
    is_symbol = symbol_search is not None or (
        has_target_orders)

    order = None

    if not is_signal and not has_target_orders:
        logging.debug(
            "Cannot parse signal or replies")
        return None

    if not is_symbol:
        logging.debug("Not a symbol we need")
        return None

    if is_signal:
        order = {}
        order["id"] = id_
        order["is_buy"] = is_buy
        order["datetime"] = next_date
        order["price_actual"] = next_value
        order["price_signal"] = price
        order["is_open"] = True

        if is_sl:
            order["stop_loss"] = sl

        if is_tp:
            take_profit_index = 0
            for tp_dec in tps:
                take_profit_index += 1
                if take_profit_index == 1:
                    order["take_profit"] = tp_dec
                    if USE_FAST_BOOK:
                        break
                    else:
                        continue

                tp_order = copy.deepcopy(order)
                tp_order["take_profit"] = tp_dec
                if validate_order(tp_order, next_value):
                    order_book.append(tp_order)

        if not validate_order(order, next_value):
            return

        if order["is_open"] and not USE_FAST_BOOK:
            breakeven_order = copy.deepcopy(order)
            breakeven_order["for_breakeven"] = True
            order_book.append(breakeven_order)

        order_book.append(order)

        if len(target_orders) > 0 and USE_FAST_BOOK:
            is_close = True  # We want to close if another signal comes

    for target_order in target_orders:
        target_order["update_date"] = date
        if is_sl:
            target_order["stop_loss"] = sl

        if is_tp and not is_tp_hit:
            tp_single = tps[0]
            target_order["take_profit"] = tp_single

        is_close_local = is_close
        if is_breakeven and target_order.get("has_breakeven") is None:
            target_order["has_breakeven"] = True
            if target_order.get("for_breakeven") is None and not USE_FAST_BOOK:
                target_order["stop_loss"] = target_order["price_actual"]
            else:
                is_close_local = True

        if is_close_local:
            target_order["close_price"] = next_value
            target_order["close_datetime"] = next_date
            target_order["is_open"] = False
            target_order["manual_exit"] = True

        # Just for information, this is not a real sl\tp hit
        if is_sl_hit:
            target_order["sl_exit"] = True
        if is_tp_hit:
            target_order["tp_exit"] = True

        validate_order(target_order, next_value)


def message_to_text(message):
    if message is None:
        return None

    raw_text = message.get("text")
    if raw_text is None:
        return None

    text = str(raw_text).lower()
    return text
