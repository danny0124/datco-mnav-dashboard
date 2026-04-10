import os
import time
import json
import requests
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()
COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY")

CACHE_DIR = "cache"
os.makedirs(CACHE_DIR, exist_ok=True)

BTC_CACHE_FILE = os.path.join(CACHE_DIR, "btc_history_365.csv")

BTC_CACHE_EXPIRE = 60 * 60          # 1 hour
STOCK_CACHE_EXPIRE = 60 * 60        # 1 hour
BTC_LIVE_CACHE_EXPIRE = 60 * 5      # 5 minutes


def is_cache_valid(file_path, expire_seconds):
    if not os.path.exists(file_path):
        return False
    modified_time = os.path.getmtime(file_path)
    age = time.time() - modified_time
    return age < expire_seconds


def get_coingecko_headers():
    if not COINGECKO_API_KEY:
        raise ValueError("COINGECKO_API_KEY is missing")
    return {
        "x-cg-demo-api-key": COINGECKO_API_KEY
    }


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
        response = requests.get(
            url,
            params=params,
            headers=get_coingecko_headers(),
            timeout=20
        )
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
        response = requests.get(
            url,
            params=params,
            headers=get_coingecko_headers(),
            timeout=20
        )
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


def get_entity_id_for_ticker(ticker):
    entity_cache_file = os.path.join(CACHE_DIR, "coingecko_entities.json")
    entity_cache_expire = 60 * 60 * 24  # 24 hours

    entities = None

    if is_cache_valid(entity_cache_file, entity_cache_expire):
        with open(entity_cache_file, "r", encoding="utf-8") as f:
            entities = json.load(f)
    else:
        url = "https://api.coingecko.com/api/v3/entities/list"
        params = {
            "entity_type": "company",
            "per_page": 250,
            "page": 1
        }

        try:
            response = requests.get(
                url,
                params=params,
                headers=get_coingecko_headers(),
                timeout=20
            )
            response.raise_for_status()
            entities = response.json()

            with open(entity_cache_file, "w", encoding="utf-8") as f:
                json.dump(entities, f)

        except requests.exceptions.RequestException:
            if os.path.exists(entity_cache_file):
                with open(entity_cache_file, "r", encoding="utf-8") as f:
                    entities = json.load(f)
            else:
                raise ValueError("Failed to fetch CoinGecko entities list and no cache exists.")

    if not entities:
        raise ValueError("CoinGecko entities list is empty.")

    normalized_ticker = ticker.upper()
    ticker_aliases = {normalized_ticker}

    if normalized_ticker == "MSTR":
        ticker_aliases.update(["MSTR.US"])
    elif normalized_ticker == "MARA":
        ticker_aliases.update(["MARA.US"])

    for entity in entities:
        symbol = str(entity.get("symbol", "")).upper()
        if symbol in ticker_aliases:
            return entity["id"]

    ticker_name_map = {
        "MSTR": ["strategy", "microstrategy"],
        "MARA": ["mara holdings", "mara"]
    }

    for keyword in ticker_name_map.get(normalized_ticker, []):
        for entity in entities:
            name = str(entity.get("name", "")).lower()
            if keyword in name:
                return entity["id"]

    raise ValueError(f"Cannot find CoinGecko entity_id for ticker: {ticker}")


def get_btc_holdings(ticker):
    entity_id = get_entity_id_for_ticker(ticker)
    holdings_cache_file = os.path.join(CACHE_DIR, f"btc_holdings_{entity_id}.json")
    holdings_cache_expire = 60 * 60  # 1 hour

    data = None
    source = None

    if is_cache_valid(holdings_cache_file, holdings_cache_expire):
        with open(holdings_cache_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        source = "Cache"
    else:
        url = f"https://api.coingecko.com/api/v3/public_treasury/{entity_id}"

        try:
            response = requests.get(
                url,
                headers=get_coingecko_headers(),
                timeout=20
            )
            response.raise_for_status()
            data = response.json()

            with open(holdings_cache_file, "w", encoding="utf-8") as f:
                json.dump(data, f)

            source = "Live API"

        except requests.exceptions.RequestException:
            if os.path.exists(holdings_cache_file):
                with open(holdings_cache_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                source = "Cache"
            else:
                raise ValueError(f"Failed to fetch BTC holdings for entity {entity_id} and no cache exists.")

    if not data or "holdings" not in data:
        raise ValueError(f"Invalid treasury response for ticker: {ticker}")

    for item in data["holdings"]:
        if item.get("coin_id") == "bitcoin":
            return float(item.get("amount", 0)), source

    raise ValueError(f"No bitcoin holding found for ticker: {ticker}")


def get_btc_holdings_history(ticker, days=365):
    entity_id = get_entity_id_for_ticker(ticker)
    cache_file = os.path.join(CACHE_DIR, f"btc_holdings_history_{entity_id}_{days}.csv")
    cache_expire = 60 * 60  # 1 hour

    if is_cache_valid(cache_file, cache_expire):
        df = pd.read_csv(cache_file)
        df["date"] = pd.to_datetime(df["date"]).dt.date
        return df

    url = f"https://api.coingecko.com/api/v3/public_treasury/{entity_id}/bitcoin/holding_chart"
    params = {
        "days": days,
        "include_empty_intervals": "true"
    }

    try:
        response = requests.get(
            url,
            params=params,
            headers=get_coingecko_headers(),
            timeout=20
        )
        response.raise_for_status()
        data = response.json()

        if "holdings" not in data:
            raise ValueError(f"Invalid holding_chart response for {ticker}")

        df = pd.DataFrame(data["holdings"], columns=["timestamp", "btc_holdings"])
        df["date"] = pd.to_datetime(df["timestamp"], unit="ms").dt.date
        df = df[["date", "btc_holdings"]].drop_duplicates(subset=["date"])

        df.to_csv(cache_file, index=False)
        return df

    except requests.exceptions.RequestException:
        if os.path.exists(cache_file):
            df = pd.read_csv(cache_file)
            df["date"] = pd.to_datetime(df["date"]).dt.date
            return df
        else:
            raise ValueError(f"Failed to fetch historical BTC holdings for {ticker} and no cache exists.")


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


def get_local_shares_outstanding(ticker):
    local_file = "shares_outstanding_data.json"

    if os.path.exists(local_file):
        with open(local_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        if ticker in data:
            return float(data[ticker])

    raise ValueError(f"No local shares_outstanding data for {ticker}")


def get_stock_history(ticker="MSTR", days=180, shares_outstanding=None, currency="USD"):
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

    if shares_outstanding is None:
        raise ValueError(f"{ticker} missing shares_outstanding")

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