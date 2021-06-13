import datetime
import json
import requests
import helper
import classes
import config
from helper import get_array_item_contains_key
from pyquery import PyQuery
import forwarder
import re


POLL_INPUT_FORMAT = r"%Y-%m-%d %H:%M:%S"
# BASE_URL = f"https://www.alphavantage.co/query?&interval=1min&apikey={config.API_KEY}"
BASE_URL_INVESTING = f"https://tvc4.forexpros.com/{config.API_KEY_INVESTING}/0/0/0/0/history?resolution=1"
# FX_URL = f"{BASE_URL}&function=FX_INTRADAY"
# CRYPTO_URL = f"https://api-pub.bitfinex.com/v2/candles/trade:1m:"
TELEMETR_BASE_URL = "https://telemetr.io/post-list-ajax"

investing_chrome_headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36",
}

symbol_api_mapping = {
    # classes.Symbol.EURUSD: f"{FX_URL}&from_symbol=EUR&to_symbol=USD",
}

crypto_symbol_api_mapping = {
    # classes.Symbol.BTCUSD: f"{CRYPTO_URL}tBTCUSD/hist"
}
API_EXTENDED_POLL_THRESHOLD_MIN = 90


def get_telemetr_url(channel_id, offset, type_query="deleted"):
    # type_query="deleted|all"
    telemetr_url = f"{TELEMETR_BASE_URL}/{channel_id}?sort=-date&postType={type_query}&before={offset}&period=365"
    return telemetr_url


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
    classes.Symbol.XAGUSD: f"{BASE_URL_INVESTING}&symbol=69",
    classes.Symbol.XAUUSD: f"{BASE_URL_INVESTING}&symbol=68"}


def get_lag_mins(symbol_last_datetime):
    symbol_last_datetime = symbol_last_datetime.replace(tzinfo=None)
    lag = (datetime.datetime.utcnow() - symbol_last_datetime).total_seconds()

    return int(lag / 60)


def parse_alphavantage(symbol, symbol_last_datetime):

    lag = get_lag_mins(symbol_last_datetime)
    url = symbol_api_mapping[symbol]
    if lag > API_EXTENDED_POLL_THRESHOLD_MIN:
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


def telemetr_parse_history(channel_id, max_offset, start=0, step=100):
    messages = list()
    for offset in range(start, max_offset, step):
        url = get_telemetr_url(channel_id, offset)
        html = requests.get(url).text
        tags = PyQuery(html)('div[id^="post-"]')
        for tag in tags:
            try:
                msg_datetime = PyQuery(tag)("a.date-link time[datetime]")
                if len(msg_datetime) != 1:
                    continue

                msg_datetime = msg_datetime[0].attrib["datetime"]
                views = int(PyQuery(tag)("div.views").text())

                if views < 10:  # We treat this like a mistake and don't take in account such messages
                    continue

                id_ = int(tag.attrib["id"].replace("post-", ""))
                # is_deleted = len(PyQuery(tag)("div.post-deleted-block")) > 0
                text = PyQuery(tag)("div.post-block__content_message").text()
                message_links = re.findall(
                    forwarder.LINKS_REGEX, text, re.IGNORECASE)

                if len(message_links) > 0:
                    continue

            except Exception as ex:
                print(f"Cannot parse {tag}, error {ex}")
                continue
            tag_dict = {"date": msg_datetime,
                        "id": id_, "text": text, "views": views}
            messages.append(tag_dict)
    return messages


if __name__ == "__main__":

    channel_id_local = 1434334125
    history_data = telemetr_parse_history(channel_id_local, 6132, 5032)
    config.set_json(f"E:/{channel_id_local}.json", history_data)
