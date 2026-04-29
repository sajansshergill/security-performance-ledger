from __future__ import annotations

import pandas as pd
import streamlit as st


st.set_page_config(
    page_title="Security Performance Ledger",
    page_icon=":chart_with_upwards_trend:",
    layout="wide",
)


@st.cache_data
def load_sample_positions() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "as_of_date": "2026-04-24",
                "portfolio_id": "CORE",
                "security_name": "Apple Inc.",
                "ticker": "AAPL",
                "asset_category": "Equity",
                "market_sector": "Technology",
                "quantity": 900,
                "market_value_usd": 154_800,
                "cost_basis_usd": 139_000,
                "unrealized_gain_loss_usd": 15_800,
            },
            {
                "as_of_date": "2026-04-24",
                "portfolio_id": "CORE",
                "security_name": "US Treasury 10Y",
                "ticker": "UST10Y",
                "asset_category": "Fixed Income",
                "market_sector": "Government",
                "quantity": 1_100,
                "market_value_usd": 108_900,
                "cost_basis_usd": 110_250,
                "unrealized_gain_loss_usd": -1_350,
            },
            {
                "as_of_date": "2026-04-24",
                "portfolio_id": "GROWTH",
                "security_name": "NVIDIA Corp.",
                "ticker": "NVDA",
                "asset_category": "Equity",
                "market_sector": "Technology",
                "quantity": 250,
                "market_value_usd": 205_000,
                "cost_basis_usd": 160_500,
                "unrealized_gain_loss_usd": 44_500,
            },
            {
                "as_of_date": "2026-04-24",
                "portfolio_id": "INCOME",
                "security_name": "iShares Core US Aggregate Bond ETF",
                "ticker": "AGG",
                "asset_category": "ETF",
                "market_sector": "Fixed Income",
                "quantity": 1_750,
                "market_value_usd": 171_500,
                "cost_basis_usd": 168_800,
                "unrealized_gain_loss_usd": 2_700,
            },
        ]
    )


@st.cache_data
def load_sample_performance() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    daily_returns = {
        "CORE": [0.0018, -0.0009, 0.0024, 0.0012, 0.0031],
        "GROWTH": [0.0032, -0.0014, 0.0047, 0.0021, 0.0055],
        "INCOME": [0.0007, 0.0002, 0.0009, -0.0001, 0.0011],
    }
    dates = pd.date_range("2026-04-20", periods=5, freq="D")
    for portfolio_id, returns in daily_returns.items():
        nav_begin = 1_000_000 if portfolio_id != "INCOME" else 750_000
        for as_of_date, twr_daily in zip(dates, returns, strict=True):
            external_flow = 5_000 if portfolio_id == "GROWTH" and as_of_date.day == 23 else 0
            nav_end = nav_begin * (1 + twr_daily) + external_flow
            rows.append(
                {
                    "as_of_date": as_of_date.date().isoformat(),
                    "portfolio_id": portfolio_id,
                    "nav_begin_usd": nav_begin,
                    "nav_end_usd": nav_end,
                    "external_flow_usd": external_flow,
                    "twr_daily": twr_daily,
                    "mwr_daily": (nav_end - nav_begin) / (nav_begin + external_flow),
                    "benchmark_return": twr_daily - 0.0004,
                    "active_return": 0.0004,
                }
            )
            nav_begin = nav_end
    return pd.DataFrame(rows)


def uploaded_csv(label: str, fallback: pd.DataFrame) -> pd.DataFrame:
    upload = st.sidebar.file_uploader(label, type="csv")
    if upload is None:
        return fallback.copy()
    return pd.read_csv(upload)


def format_currency(value: float) -> str:
    return f"${value:,.0f}"


def coerce_dates(frame: pd.DataFrame) -> pd.DataFrame:
    if "as_of_date" in frame.columns:
        frame = frame.copy()
        frame["as_of_date"] = pd.to_datetime(frame["as_of_date"])
    return frame


positions = coerce_dates(uploaded_csv("Upload fact_positions or enriched positions CSV", load_sample_positions()))
performance = coerce_dates(uploaded_csv("Upload fact_performance CSV", load_sample_performance()))

st.title("Security Performance Ledger")
st.caption(
    "Portfolio holdings, security-master coverage, and daily performance metrics for the modeled mart layer."
)

portfolio_options = sorted(performance["portfolio_id"].dropna().unique())
selected_portfolios = st.sidebar.multiselect(
    "Portfolios",
    options=portfolio_options,
    default=portfolio_options,
)

if selected_portfolios:
    positions = positions[positions["portfolio_id"].isin(selected_portfolios)]
    performance = performance[performance["portfolio_id"].isin(selected_portfolios)]

latest_positions = positions[positions["as_of_date"] == positions["as_of_date"].max()]
total_market_value = latest_positions["market_value_usd"].sum()
total_cost_basis = latest_positions["cost_basis_usd"].sum()
total_unrealized = latest_positions["unrealized_gain_loss_usd"].sum()
latest_active_return = performance.sort_values("as_of_date").groupby("portfolio_id")["active_return"].last().mean()

metric_cols = st.columns(4)
metric_cols[0].metric("Market Value", format_currency(total_market_value))
metric_cols[1].metric("Cost Basis", format_currency(total_cost_basis))
metric_cols[2].metric("Unrealized P/L", format_currency(total_unrealized))
metric_cols[3].metric("Avg Active Return", f"{latest_active_return:.2%}")

st.divider()

chart_cols = st.columns((1.2, 1))

with chart_cols[0]:
    st.subheader("Daily Time-Weighted Return")
    return_chart = performance.pivot_table(
        index="as_of_date",
        columns="portfolio_id",
        values="twr_daily",
        aggfunc="sum",
    ).sort_index()
    st.line_chart(return_chart)

with chart_cols[1]:
    st.subheader("Exposure by Asset Category")
    exposure = (
        latest_positions.groupby("asset_category", as_index=False)["market_value_usd"]
        .sum()
        .sort_values("market_value_usd", ascending=False)
    )
    st.bar_chart(exposure, x="asset_category", y="market_value_usd")

st.subheader("Latest Holdings")
holding_columns = [
    "portfolio_id",
    "security_name",
    "ticker",
    "asset_category",
    "market_sector",
    "quantity",
    "market_value_usd",
    "cost_basis_usd",
    "unrealized_gain_loss_usd",
]
st.dataframe(
    latest_positions[holding_columns].sort_values("market_value_usd", ascending=False),
    width="stretch",
    hide_index=True,
)

st.subheader("Performance Ledger")
st.dataframe(
    performance.sort_values(["as_of_date", "portfolio_id"], ascending=[False, True]),
    width="stretch",
    hide_index=True,
)
