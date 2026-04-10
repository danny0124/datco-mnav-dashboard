import os
import requests
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta

CACHE_DIR = "cache"
os.makedirs(CACHE_DIR, exist_ok=True)

BTC_CACHE_FILE = os.path.join(CACHE_DIR, "btc_history_365.csv")


def get_btc_history(days=180):
    """
    永遠先以 365 天資料為主。
    API 成功就更新 365 天快取。
    API 失敗就讀 365 天快取。
    最後再依使用者選擇切出最後 N 天。
    """
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
            raise ValueError("CoinGecko API 失敗，而且 365 天 BTC 快取也不存在")


def get_btc_live_price():
    """
    取得 BTC 即時價格（USD）
    若失敗則回傳 None
    """
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": "bitcoin",
        "vs_currencies": "usd"
    }

    try:
        response = requests.get(url, params=params, timeout=20)
        response.raise_for_status()
        data = response.json()
        return data["bitcoin"]["usd"]
    except requests.exceptions.RequestException:
        return None


def _get_jpy_usd_history(days=365):
    fx = yf.Ticker("JPY=X")

    end_date = datetime.today()
    start_date = end_date - timedelta(days=days + 20)

    fx_hist = fx.history(
        start=start_date.strftime("%Y-%m-%d"),
        end=end_date.strftime("%Y-%m-%d")
    )

    fx_hist = fx_hist.reset_index()
    fx_hist["date"] = pd.to_datetime(fx_hist["Date"]).dt.date
    fx_hist = fx_hist[["date", "Close"]].rename(columns={"Close": "usd_jpy"})
    fx_hist = fx_hist.drop_duplicates(subset=["date"])

    return fx_hist


def get_stock_history(ticker="MSTR", days=180):
    stock = yf.Ticker(ticker)

    end_date = datetime.today()
    start_date = end_date - timedelta(days=max(days, 365) + 20)

    hist = stock.history(
        start=start_date.strftime("%Y-%m-%d"),
        end=end_date.strftime("%Y-%m-%d")
    )

    if hist.empty:
        raise ValueError(f"無法取得 {ticker} 的股價資料")

    hist = hist.reset_index()
    hist["date"] = pd.to_datetime(hist["Date"]).dt.date
    hist = hist[["date", "Close"]].rename(columns={"Close": "stock_close_local"})
    hist = hist.drop_duplicates(subset=["date"])

    info = stock.info if stock.info else {}

    currency = info.get("currency", "USD")
    shares_outstanding = (
        info.get("sharesOutstanding")
        or info.get("impliedSharesOutstanding")
    )
    latest_market_cap = info.get("marketCap")

    latest_close = hist["stock_close_local"].iloc[-1]

    if not shares_outstanding and latest_market_cap and latest_close:
        shares_outstanding = latest_market_cap / latest_close

    if not shares_outstanding:
        raise ValueError(f"無法取得 {ticker} 的 sharesOutstanding / marketCap")

    hist["market_cap_local"] = hist["stock_close_local"] * shares_outstanding
    hist["market_cap_usd"] = hist["market_cap_local"]

    if currency.upper() == "JPY":
        fx_df = _get_jpy_usd_history(365)
        hist = pd.merge(hist, fx_df, on="date", how="left")
        hist["usd_jpy"] = hist["usd_jpy"].ffill().bfill()
        hist["market_cap_usd"] = hist["market_cap_local"] / hist["usd_jpy"]

    hist = hist.sort_values("date").reset_index(drop=True)
    stock_df = hist[["date", "stock_close_local", "market_cap_usd"]].copy()

    if days < len(stock_df):
        stock_df = stock_df.tail(days).reset_index(drop=True)

    latest_market_cap_usd = stock_df["market_cap_usd"].iloc[-1]

    return stock_df, latest_market_cap_usd, currency