import os
import time
import json
import requests
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta

CACHE_DIR = "cache"
os.makedirs(CACHE_DIR, exist_ok=True)

BTC_CACHE_FILE = os.path.join(CACHE_DIR, "btc_history_365.csv")

BTC_CACHE_EXPIRE = 60 * 60          # 1 hour
STOCK_CACHE_EXPIRE = 60 * 60        # 1 hour
META_CACHE_EXPIRE = 60 * 60         # 1 hour
BTC_LIVE_CACHE_EXPIRE = 60 * 5      # 5 minutes


def is_cache_valid(file_path, expire_seconds):
    if not os.path.exists(file_path):
        return False
    modified_time = os.path.getmtime(file_path)
    age = time.time() - modified_time
    return age < expire_seconds


def get_btc_history(days=180):
    full_df = _get_btc_history_365()
    full_df = full_df.sort_values("date").reset_index(drop=True)

    if days >= len(full_df):
        return full_df.copy()

    return full_df.tail(days).reset_index(drop=True)


def _get_btc_history_365():
    url = "https://api.coingecko.com/api/v3/coins/bitcoin/market_chart"
    params = {
        "vs_currency": "usd",
        "days": 365,
        "interval": "daily"
    }

    if is_cache_valid(BTC_CACHE_FILE, BTC_CACHE_EXPIRE):
        btc_df = pd.read_csv(BTC_CACHE_FILE)
        btc_df["date"] = pd.to_datetime(btc_df["date"]).dt.date
        return btc_df

    try:
        response = requests.get(url, params=params, timeout=20)
        response.raise_for_status()

        data = response.json()
        prices = data["prices"]

        btc_df = pd.DataFrame(prices, columns=["timestamp", "btc_price_usd"])
        btc_df["date"] = pd.to_datetime(btc_df["timestamp"], unit="ms").dt.date
        btc_df = btc_df[["date", "btc_price_usd"]].drop_duplicates(subset=["date"])

        btc_df.to_csv(BTC_CACHE_FILE, index=False)
        return btc_df

    except requests.exceptions.RequestException:
        if os.path.exists(BTC_CACHE_FILE):
            btc_df = pd.read_csv(BTC_CACHE_FILE)
            btc_df["date"] = pd.to_datetime(btc_df["date"]).dt.date
            return btc_df
        else:
            raise ValueError("CoinGecko API failed and no BTC cache exists.")


def get_btc_live_price():
    live_cache_file = os.path.join(CACHE_DIR, "btc_live_price.txt")

    if is_cache_valid(live_cache_file, BTC_LIVE_CACHE_EXPIRE):
        with open(live_cache_file, "r", encoding="utf-8") as f:
            return float(f.read().strip())

    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": "bitcoin",
        "vs_currencies": "usd"
    }

    try:
        response = requests.get(url, params=params, timeout=20)
        response.raise_for_status()
        data = response.json()
        price = data["bitcoin"]["usd"]

        with open(live_cache_file, "w", encoding="utf-8") as f:
            f.write(str(price))

        return price

    except requests.exceptions.RequestException:
        if os.path.exists(live_cache_file):
            with open(live_cache_file, "r", encoding="utf-8") as f:
                return float(f.read().strip())
        return None


def _get_jpy_usd_history(days=365):
    fx_cache_file = os.path.join(CACHE_DIR, "usd_jpy_365.csv")

    if is_cache_valid(fx_cache_file, STOCK_CACHE_EXPIRE):
        fx_df = pd.read_csv(fx_cache_file)
        fx_df["date"] = pd.to_datetime(fx_df["date"]).dt.date
        return fx_df

    fx = yf.Ticker("JPY=X")

    end_date = datetime.today()
    start_date = end_date - timedelta(days=days + 20)

    try:
        fx_hist = fx.history(
            start=start_date.strftime("%Y-%m-%d"),
            end=end_date.strftime("%Y-%m-%d")
        )

        fx_hist = fx_hist.reset_index()
        fx_hist["date"] = pd.to_datetime(fx_hist["Date"]).dt.date
        fx_hist = fx_hist[["date", "Close"]].rename(columns={"Close": "usd_jpy"})
        fx_hist = fx_hist.drop_duplicates(subset=["date"])

        fx_hist.to_csv(fx_cache_file, index=False)
        return fx_hist

    except Exception:
        if os.path.exists(fx_cache_file):
            fx_df = pd.read_csv(fx_cache_file)
            fx_df["date"] = pd.to_datetime(fx_df["date"]).dt.date
            return fx_df
        else:
            raise ValueError("FX data unavailable and no cache exists.")


def get_stock_meta(ticker):
    """
    動態抓股票 metadata（shares outstanding / currency）
    並做快取
    """
    meta_cache_file = os.path.join(CACHE_DIR, f"meta_{ticker}.json")

    if is_cache_valid(meta_cache_file, META_CACHE_EXPIRE):
        with open(meta_cache_file, "r", encoding="utf-8") as f:
            return json.load(f)

    try:
        stock = yf.Ticker(ticker)

        # 優先抓 fast_info，通常比較輕
        fast_info = stock.fast_info if stock.fast_info else {}
        info = stock.info if stock.info else {}

        currency = info.get("currency", "USD")

        shares_outstanding = (
            info.get("sharesOutstanding")
            or info.get("impliedSharesOutstanding")
        )

        if shares_outstanding is None:
            market_cap = fast_info.get("market_cap") or info.get("marketCap")
            last_price = fast_info.get("last_price")

            if market_cap and last_price:
                shares_outstanding = market_cap / last_price

        if shares_outstanding is None:
            raise ValueError(f"Cannot determine shares outstanding for {ticker}")

        meta = {
            "currency": currency,
            "shares_outstanding": shares_outstanding
        }

        with open(meta_cache_file, "w", encoding="utf-8") as f:
            json.dump(meta, f)

        return meta

    except Exception:
        if os.path.exists(meta_cache_file):
            with open(meta_cache_file, "r", encoding="utf-8") as f:
                return json.load(f)
        else:
            raise ValueError(f"Failed to fetch metadata for {ticker} and no cache exists.")


def get_stock_history(ticker="MSTR", days=180):
    """
    動態抓 stock history + 動態抓 metadata
    兩者都做快取
    """
    cache_file = os.path.join(CACHE_DIR, f"stock_{ticker}_365.csv")

    if is_cache_valid(cache_file, STOCK_CACHE_EXPIRE):
        hist = pd.read_csv(cache_file)
        hist["date"] = pd.to_datetime(hist["date"]).dt.date
    else:
        end_date = datetime.today()
        start_date = end_date - timedelta(days=365 + 20)

        try:
            stock = yf.Ticker(ticker)
            hist = stock.history(
                start=start_date.strftime("%Y-%m-%d"),
                end=end_date.strftime("%Y-%m-%d")
            )

            if hist.empty:
                raise ValueError(f"Cannot fetch stock price data for {ticker}")

            hist = hist.reset_index()
            hist["date"] = pd.to_datetime(hist["Date"]).dt.date
            hist = hist[["date", "Close"]].rename(columns={"Close": "stock_close_local"})
            hist = hist.drop_duplicates(subset=["date"])

            hist.to_csv(cache_file, index=False)

        except Exception:
            if os.path.exists(cache_file):
                hist = pd.read_csv(cache_file)
                hist["date"] = pd.to_datetime(hist["date"]).dt.date
            else:
                raise ValueError(f"Failed to fetch stock history for {ticker} and no cache exists.")

    meta = get_stock_meta(ticker)
    currency = meta["currency"]
    shares_outstanding = meta["shares_outstanding"]

    hist["market_cap_local"] = hist["stock_close_local"] * shares_outstanding
    hist["market_cap_usd"] = hist["market_cap_local"]

    if currency.upper() == "JPY":
        fx_df = _get_jpy_usd_history(365)
        hist = pd.merge(hist, fx_df, on="date", how="left")
        hist["usd_jpy"] = hist["usd_jpy"].ffill().bfill()
        hist["market_cap_usd"] = hist["market_cap_local"] / hist["usd_jpy"]

    hist = hist.sort_values("date").reset_index(drop=True)

    if days < len(hist):
        hist = hist.tail(days).reset_index(drop=True)

    stock_df = hist[["date", "stock_close_local", "market_cap_usd"]].copy()
    latest_market_cap_usd = stock_df["market_cap_usd"].iloc[-1]

    return stock_df, latest_market_cap_usd, currency