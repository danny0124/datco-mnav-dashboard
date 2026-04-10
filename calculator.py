import pandas as pd


def calculate_mnav(stock_df, btc_df, holdings_history_df):
    df = pd.merge(stock_df, btc_df, on="date", how="inner")
    df = pd.merge(df, holdings_history_df, on="date", how="left")

    df = df.sort_values("date").reset_index(drop=True)
    df["btc_holdings"] = df["btc_holdings"].ffill().bfill()

    df["btc_nav_usd"] = df["btc_holdings"] * df["btc_price_usd"]
    df["mnav"] = df["market_cap_usd"] / df["btc_nav_usd"]

    return df