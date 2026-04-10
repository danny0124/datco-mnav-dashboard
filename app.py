from flask import Flask, render_template, request
import plotly.express as px

from data_fetcher import (
    get_btc_history,
    get_stock_history,
    get_btc_live_price,
    get_btc_holdings,
    get_btc_holdings_history,
    get_alpha_vantage_shares_outstanding
)
from calculator import calculate_mnav

app = Flask(__name__)

# 你之後可以手動更新 holdings
COMPANIES = {
    "MSTR": {
        "name": "Strategy",
        "ticker": "MSTR",
        "currency": "USD"
    },
    "MARA": {
        "name": "MARA Holdings",
        "ticker": "MARA",
        "currency": "USD"
    }
}

RANGE_OPTIONS = {
    "30": 30,
    "90": 90,
    "180": 180,
    "365": 365
}


def format_large_number(value):
    if value is None:
        return "N/A"
    return f"{value:,.0f}"


@app.route("/")
def index():
    company_key = request.args.get("company", "MSTR").upper()
    range_key = request.args.get("range", "180")

    if company_key not in COMPANIES:
        company_key = "MSTR"

    if range_key not in RANGE_OPTIONS:
        range_key = "180"

    selected_company = COMPANIES[company_key]
    days = RANGE_OPTIONS[range_key]

    btc_df = get_btc_history(days)
    btc_live_price = get_btc_live_price()

    shares_outstanding = get_alpha_vantage_shares_outstanding(selected_company["ticker"])

    stock_df, latest_market_cap_usd, currency = get_stock_history(
        ticker=selected_company["ticker"],
        days=days,
        shares_outstanding=shares_outstanding,
        currency=selected_company["currency"]
    )

    current_btc_holdings = get_btc_holdings(selected_company["ticker"])
    holdings_history_df = get_btc_holdings_history(selected_company["ticker"], days)

    df = calculate_mnav(
        stock_df=stock_df,
        btc_df=btc_df,
        holdings_history_df=holdings_history_df
    )

    latest = df.iloc[-1]

    # 圖 1：mNAV
    fig_mnav = px.line(
        df,
        x="date",
        y="mnav",
        title=f"{selected_company['name']} ({selected_company['ticker']}) Simplified mNAV"
    )
    fig_mnav.update_layout(
        xaxis_title="Date",
        yaxis_title="mNAV",
        template="plotly_white",
        margin=dict(l=40, r=20, t=60, b=40)
    )
    chart_mnav_html = fig_mnav.to_html(full_html=False)

    # 圖 2：BTC Price
    fig_btc = px.line(
        df,
        x="date",
        y="btc_price_usd",
        title=f"Bitcoin (BTC) Price - Last {days} Days"
    )
    fig_btc.update_layout(
        xaxis_title="Date",
        yaxis_title="BTC Price (USD)",
        template="plotly_white",
        margin=dict(l=40, r=20, t=60, b=40)
    )
    chart_btc_html = fig_btc.to_html(full_html=False)

    return render_template(
        "index.html",
        companies=COMPANIES,
        range_options=RANGE_OPTIONS,
        selected_company_key=company_key,
        selected_range_key=range_key,
        company_name=selected_company["name"],
        company_ticker=selected_company["ticker"],
        latest_mnav=round(latest["mnav"], 3),
        latest_stock=round(latest["stock_close_local"], 2),
        latest_stock_currency=currency,
        latest_btc_daily=round(latest["btc_price_usd"], 2),
        latest_btc_live=round(btc_live_price, 2) if btc_live_price is not None else "N/A",
        latest_market_cap_usd=format_large_number(latest_market_cap_usd),
        btc_holdings=format_large_number(current_btc_holdings),
        chart_mnav_html=chart_mnav_html,
        chart_btc_html=chart_btc_html
    )


if __name__ == "__main__":
    app.run(debug=True)