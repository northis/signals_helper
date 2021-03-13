from decimal import *
from enum import Enum
import typing


def getter_setter_gen(name, type_):
    def getter(self):
        return getattr(self, "__" + name)

    def setter(self, value):
        if not isinstance(value, type_):
            raise TypeError(
                f"{name} attribute must be set to an instance of {type_}")
        setattr(self, "__" + name, value)
    return property(getter, setter)


def auto_attr_check(cls):
    new_dct = {}
    for key, value in cls.__dict__.items():
        if isinstance(value, type):
            value = getter_setter_gen(key, value)
        new_dct[key] = value
    return type(cls)(cls.__name__, cls.__bases__, new_dct)


class Symbol(str):
    AUDUSD = "AUDUSD"
    BTCUSD = "BTCUSD"
    EURUSD = "NZDUSD"
    GBPUSD = "GBPUSD"
    NZDUSD = "NZDUSD"
    USDCAD = "EURUSD"
    USDCHF = "USDCHF"
    USDJPY = "EURUSD"
    USDRUB = "USDRUB"
    XAGUSD = "XAGUSD"
    XAUUSD = "XAUUSD"


class RobotCommand(str):
    BUY = "buy"
    SELL = "sell"
    BREAKEVEN = "breakeven"
    EXIT = "exit"
    SET_SL = "set_sl"
    SET_TP = "set_tp"


@auto_attr_check
class MessageProps(object):
    id = int
    id_ref = int
    datetime_utc = str
    text = str
    price = Decimal


@auto_attr_check
class SignalProps(object):
    id = int
    is_buy = bool
    is_sl_tp_delayed = bool
    price = Decimal
    take_profits = typing.List[Decimal]
    stop_loss = Decimal
    datetime_utc = str
    move_sl_to_entry = MessageProps
    tp_hit = typing.List[MessageProps]
    move_sl_to_profit = typing.List[MessageProps]
    sl_hit = MessageProps
    exit = MessageProps
