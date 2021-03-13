import datetime
from datetime import timezone
import pytz
import decimal


def str_to_utc_datetime(dt, timezone, input_format):
    tz1 = pytz.timezone(timezone)
    tz2 = pytz.timezone("UTC")

    dt_typed = datetime.datetime.strptime(dt, input_format)
    dt_typed = tz1.localize(dt_typed)
    dt_typed = dt_typed.astimezone(tz2)
    return dt_typed


def str_to_utc_iso_datetime(dt, timezone, input_format):
    tz1 = pytz.timezone(timezone)
    tz2 = pytz.timezone("UTC")

    dt_typed = datetime.datetime.strptime(dt, input_format)
    dt_typed = tz1.localize(dt_typed)
    dt_typed = dt_typed.astimezone(tz2)
    return dt_typed.isoformat()


def str_to_decimal(string):
    if string == None:
        return None
    try:
        dec = decimal.Decimal(string.replace(",", "."))
        return dec
    except:
        return None
