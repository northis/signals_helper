from decimal import *
import typing
import asyncio
import sqlite3
import logging
import json


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
        self.price = Decimal(0)
    id_ = int
    reply_to_message_id = int
    date = str
    text = str
    price = Decimal


@auto_attr_check
class SignalProps(object):
    def __init__(self):
        self.id_ = 0
        self.is_buy = False
        self.is_sl_tp_delayed = False
        self.price = Decimal(0)
        self.take_profits = list()
        self.stop_loss = Decimal(0)
        self.date = ""
        self.update_date = ""
        self.move_sl_to_entry = MessageProps()
        self.tp_hit = list()
        self.move_sl_to_profit = list()
        self.sl_hit = MessageProps()
        self.exit_ = MessageProps()
    id_ = int
    is_buy = bool
    is_sl_tp_delayed = bool
    price = Decimal
    take_profits = typing.List[Decimal]
    stop_loss = Decimal
    date = str
    update_date = str
    move_sl_to_entry = MessageProps
    tp_hit = typing.List[MessageProps]
    move_sl_to_profit = typing.List[MessageProps]
    sl_hit = MessageProps
    exit_ = MessageProps


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        return json.JSONEncoder.default(self, obj)

class MessagePropsEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, MessageProps):
            mp: MessageProps() = obj

            return {"id" : mp.id_,
                    "date": mp.date,
                    "price": json.dumps(mp.price, cls=DecimalEncoder),
                    "reply_to_message_id": mp.reply_to_message_id,
                    "text": mp.text}
        return json.JSONEncoder.default(self, obj)


class SignalPropsEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, SignalProps):
            sp: SignalProps = obj

            exit_ = ""
            move_sl_to_entry = ""
            sl_hit = ""
            if sp.exit_ is not None:
                exit_ = json.dumps(sp.exit_, cls=MessagePropsEncoder)

            if sp.move_sl_to_entry is not None:
                move_sl_to_entry = json.dumps(sp.move_sl_to_entry, cls=MessagePropsEncoder)
            
            move_sl_to_profit = list()

            if sp.move_sl_to_profit is not None:
                for move_sl in sp.move_sl_to_profit:
                    move_sl_to_profit.append(json.dumps(move_sl, cls=MessagePropsEncoder))

            
            if sp.sl_hit is not None:
                sl_hit = json.dumps(sp.sl_hit, cls=MessagePropsEncoder)

            take_profits = list()
            if sp.take_profits is not None:
                for t_p in sp.take_profits:
                    take_profits.append(json.dumps(t_p, cls=DecimalEncoder))

            tp_hit = list()
            if sp.tp_hit is not None:
                for tp_hit_item in sp.tp_hit:
                    tp_hit.append(json.dumps(tp_hit_item, cls=MessagePropsEncoder))


            return {
                "id" : sp.id_,
                "is_buy" : sp.is_buy,
                "exit" : exit_,
                "is_sl_tp_delayed" : sp.is_sl_tp_delayed,
                "move_sl_to_entry" : move_sl_to_entry,
                "move_sl_to_profit" : json.dumps(move_sl_to_profit),
                "price" : json.dumps(sp.price, cls=DecimalEncoder),
                "sl_hit" : sl_hit,
                "stop_loss" : json.dumps(sp.stop_loss, cls=DecimalEncoder),
                "take_profits" : json.dumps(take_profits),
                "tp_hit" : json.dumps(tp_hit),
                "update_date" : sp.update_date }
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
