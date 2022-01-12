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
    Sleep = 0.01

    async def wait(self):
        while True:
            await asyncio.sleep(self.Sleep)
            if self.Value is True:
                break


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        if obj is None:
            return 0
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
