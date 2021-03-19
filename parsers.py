import os
import datetime
import json
import requests
from dotenv import load_dotenv
import helper
import classes
from helper import get_array_item_contains_key


load_dotenv()
API_KEY = os.getenv("api_key")
API_KEY_INVESTING = os.getenv("api_key_investing")

POLL_INPUT_FORMAT = r"%Y-%m-%d %H:%M:%S"
# BASE_URL = f"https://www.alphavantage.co/query?&interval=1min&apikey={API_KEY}"
BASE_URL_INVESTING = f"https://tvc4.forexpros.com/{API_KEY_INVESTING}/0/0/0/0/history?resolution=1"
# FX_URL = f"{BASE_URL}&function=FX_INTRADAY"
# CRYPTO_URL = f"https://api-pub.bitfinex.com/v2/candles/trade:1m:"

investing_chrome_headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36",
}

symbol_api_mapping = {
    # classes.Symbol.EURUSD: f"{FX_URL}&from_symbol=EUR&to_symbol=USD",
}

crypto_symbol_api_mapping = {
    # classes.Symbol.BTCUSD: f"{CRYPTO_URL}tBTCUSD/hist"
}
api_extended_poll_threshold_min = 90


investing_symbol_api_mapping = {
    classes.Symbol.AUDUSD: f"{BASE_URL_INVESTING}&symbol=5",
    classes.Symbol.BTCUSD: f"{BASE_URL_INVESTING}&symbol=945629",
    classes.Symbol.EURUSD: f"{BASE_URL_INVESTING}&symbol=1",
    classes.Symbol.GBPUSD: f"{BASE_URL_INVESTING}&symbol=2",
    classes.Symbol.NZDUSD: f"{BASE_URL_INVESTING}&symbol=8",
    classes.Symbol.USDCAD: f"{BASE_URL_INVESTING}&symbol=7",
    classes.Symbol.USDCHF: f"{BASE_URL_INVESTING}&symbol=4",
    classes.Symbol.USDJPY: f"{BASE_URL_INVESTING}&symbol=3",
    classes.Symbol.USDRUB: f"{BASE_URL_INVESTING}&symbol=2186",
    classes.Symbol.XAGUSD: f"{BASE_URL_INVESTING}&symbol=8836",
    classes.Symbol.XAUUSD: f"{BASE_URL_INVESTING}&symbol=8830"}


def get_lag_mins(symbol_last_datetime):
    symbol_last_datetime = symbol_last_datetime.replace(tzinfo=None)
    lag = (datetime.datetime.utcnow() - symbol_last_datetime).total_seconds()

    return int(lag / 60)


def parse_alphavantage(symbol, symbol_last_datetime):

    lag = get_lag_mins(symbol_last_datetime)
    url = symbol_api_mapping[symbol]
    if lag > api_extended_poll_threshold_min:
        url = f"{url}&outputsize=full"

    req = requests.get(url)
    content = req.text
    price_data = json.loads(content)

    meta = get_array_item_contains_key(price_data, "meta")
    timezone = get_array_item_contains_key(meta, "time zone")
    price_data = get_array_item_contains_key(price_data, "series")
    sorted_items = sorted(price_data.keys())

    for price_item in sorted_items:
        utc_date = helper.str_to_utc_datetime(
            price_item, timezone, POLL_INPUT_FORMAT)

        open_ = price_item[1]
        close = price_item[2]
        high = price_item[3]
        low = price_item[4]

        yield (utc_date, open_, high, low, close)


def parse_bitfinex(symbol, symbol_last_datetime):
    lag = get_lag_mins(symbol_last_datetime) + 1
    url = f"{crypto_symbol_api_mapping[symbol]}?limit={lag}"
    req = requests.get(url)
    content = req.text
    price_data = json.loads(content)

    for price_item in price_data:
        utc_date = datetime.datetime.utcfromtimestamp(
            price_item[0] / 1000)

        values = price_data[price_item]
        open_ = get_array_item_contains_key(values, "open")
        high = get_array_item_contains_key(values, "high")
        low = get_array_item_contains_key(values, "low")
        close = get_array_item_contains_key(values, "close")

        yield (utc_date, open_, high, low, close)


def parse_investing(symbol, symbol_last_datetime):

    start_unix_dt = int(symbol_last_datetime.timestamp())
    end_unix_dt = int(datetime.datetime.now().timestamp())
    url = f"{investing_symbol_api_mapping[symbol]}&from={start_unix_dt}&to={end_unix_dt}"
    req = requests.get(url, headers=investing_chrome_headers)
    content = req.text
    price_data = json.loads(content)

    times = price_data["t"]
    o_array = price_data["o"]
    h_array = price_data["h"]
    l_array = price_data["l"]
    c_array = price_data["c"]

    i = 0
    for time in times:
        utc_date = datetime.datetime.utcfromtimestamp(time)

        open_ = o_array[i]
        high = h_array[i]
        low = l_array[i]
        close = c_array[i]

        i += 1
        yield (utc_date, open_, high, low, close)
