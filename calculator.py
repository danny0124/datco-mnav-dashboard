import pandas as pd


def calculate_mnav(stock_df, btc_df, btc_holdings):
    """
    計算 simplified mNAV
    mNAV = market_cap_usd / (btc_holdings * btc_price_usd)
    """
    df = pd.merge(stock_df, btc_df, on="date", how="inner")
    df = df.sort_values("date").reset_index(drop=True)

    df["btc_nav_usd"] = btc_holdings * df["btc_price_usd"]
    df["mnav"] = df["market_cap_usd"] / df["btc_nav_usd"]

    return df