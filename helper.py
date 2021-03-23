import threading
import datetime
import decimal
import pytz


def datetime_to_utc_datetime(dt_typed):
    tz_utc = pytz.timezone("UTC")
    dt_typed = dt_typed.astimezone(tz_utc)
    return dt_typed


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
    if string is None:
        return None
    try:
        dec = decimal.Decimal(string.replace(",", "."))
        return dec
    except:
        return None


def get_array_item_contains_key(array, key):
    for item in array:
        if key.lower() in item.lower():
            return array[item]
    return []


def run_as_daemon_until_press_any_key(func_to_execute):
    exec_thread = threading.Thread(target=func_to_execute, daemon=True)
    exec_thread.start()
    print("Press any key to exit")
    input()
