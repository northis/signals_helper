from decimal import *
import typing
import asyncio
import sqlite3
import logging
import json
from typing import Optional


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
    EURUSD = "EURUSD"
    GBPUSD = "GBPUSD"
    NZDUSD = "NZDUSD"
    USDCAD = "USDCAD"
    USDCHF = "USDCHF"
    USDJPY = "USDJPY"
    USDRUB = "USDRUB"
    XAGUSD = "XAGUSD"
    XAUUSD = "XAUUSD"


class StopFlag:
    Value = False
    Sleep = 10

    async def wait(self):
        while True:
            await asyncio.sleep(self.Sleep)
            if self.Value is True:
                break


@auto_attr_check
class MessageProps(object):
    def __init__(self):
        self.id_ = 0
        self.reply_to_message_id = 0
        self.date = ""
        self.text = ""
        self.price = None
    id_ = int
    reply_to_message_id = int
    date = str
    text = str
    price = Optional[Decimal]


@auto_attr_check
class SignalProps(object):
    def __init__(self):
        self.id_ = 0
        self.is_buy = None
        self.is_sl_tp_delayed = False
        self.price = None
        self.take_profits = None
        self.stop_loss = None
        self.date = ""
        self.update_date = ""
        self.move_sl_to_entry = None
        self.tp_hit = None
        self.move_sl_to_profit = None
        self.sl_hit = None
        self.exit_ = None
    id_ = int
    is_buy = Optional[bool]
    is_sl_tp_delayed = bool
    price = Optional[Decimal]
    take_profits = Optional[typing.List[Decimal]]
    stop_loss = Optional[Decimal]
    date = str
    update_date = str
    move_sl_to_entry = Optional[MessageProps]
    tp_hit = Optional[typing.List[MessageProps]]
    move_sl_to_profit = Optional[typing.List[MessageProps]]
    sl_hit = Optional[MessageProps]
    exit_ = Optional[MessageProps]


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if obj is None:
            return 0
        return json.JSONEncoder.default(self, obj)

class MessagePropsEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, MessageProps):
            decimal_encoder = DecimalEncoder()
            mp: MessageProps() = obj

            return {"id" : mp.id_,
                    "date": mp.date,
                    "price": decimal_encoder.default(mp.price),
                    "reply_to_message_id": mp.reply_to_message_id,
                    "text": mp.text}
        return json.JSONEncoder.default(self, obj)


class SignalPropsEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, SignalProps):

            message_props_encoder = MessagePropsEncoder()
            decimal_encoder = DecimalEncoder()
            sp: SignalProps = obj

            out_res = {
                "id" : sp.id_,
                "is_buy" : sp.is_buy,
                "is_sl_tp_delayed" : sp.is_sl_tp_delayed,
                "price" : decimal_encoder.default(sp.price),
                "stop_loss" : decimal_encoder.default(sp.stop_loss),
                "update_date" : sp.update_date }

            if sp.exit_ is not None:
                exit_ = message_props_encoder.default(sp.exit_)
                out_res["exit"] = exit_

            if sp.move_sl_to_entry is not None:
                move_sl_to_entry = message_props_encoder.default(sp.move_sl_to_entry)
                out_res["move_sl_to_entry"] = move_sl_to_entry
            

            if sp.move_sl_to_profit is not None:
                move_sl_to_profit = list()
                for move_sl in sp.move_sl_to_profit:   
                    move_sl_to_profit.append(message_props_encoder.default(move_sl))
                out_res["move_sl_to_profit"] = move_sl_to_profit
            
            if sp.sl_hit is not None:                
                sl_hit = message_props_encoder.default(sp.sl_hit)
                out_res["sl_hit"] = sl_hit

            if sp.take_profits is not None:
                take_profits = list()
                for t_p in sp.take_profits:
                    take_profits.append(decimal_encoder.default(t_p))
                out_res["take_profits"] = take_profits

            if sp.tp_hit is not None:
                tp_hit = list()
                for tp_hit_item in sp.tp_hit:
                    tp_hit.append(message_props_encoder.default(tp_hit_item))
                out_res["tp_hit"] = tp_hit

            return out_res
        return json.JSONEncoder.default(self, obj)


class SQLite():
    def __init__(self, file, method_name, lock_object):
        self.conn = sqlite3.connect(file)
        self.method_name = method_name
        self.lock_object = lock_object

    def __enter__(self):
        if self.lock_object is not None:
            self.lock_object.acquire()

        return self.conn.cursor()

    def __exit__(self, ex_typ, e_val, trcbak):
        try:
            if self.lock_object is not None:
                self.conn.commit()
                self.conn.close()
                self.lock_object.release()
        except Exception as ex:
            logging.info('%s: %s', self.method_name, ex)

        if e_val is not None:
            logging.info('%s: %s', self.method_name, e_val)
        return True
