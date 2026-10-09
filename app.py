#Option B: Streamlit Application

import pandas as pd
import requests
import streamlit as st

st.set_page_config(page_title="Treasury Telemetry", layout="wide")

st.title("⚓ Treasury Demand Telemetry Engine")
st.caption(
    "Live structural demand visualizer tracking primary dealer appetite."
)


@st.cache_data(ttl=14400)  # Cache data for 4 hours to preserve API limits
def load_live_data():
    # Use the live, open API endpoint path
    url = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/od/auctions_query"
    
    params = {
        "fields": "security_type,security_term,auction_date,bid_to_cover_ratio,high_yield",
        "filter": "security_type:in:(Notes,Bonds,TIPS)",
        "sort": "-auction_date",
        "page[size]": 60,
    }
    res = requests.get(url, params=params)
    return pd.DataFrame(res.json().get("data", []))



df = load_live_data()

if not df.empty:
    df["bid_to_cover_ratio"] = pd.to_numeric(df["bid_to_cover_ratio"])
    df["high_yield"] = pd.to_numeric(df["high_yield"])
    df["Security Type & Duration"] = (
        df["security_term"] + " " + df["security_type"]
    )

    # Sidebar Filter Control Block
    tenors = st.sidebar.multiselect(
        "Filter Duration/Tenor",
        options=df["Security Type & Duration"].unique(),
        default=df["Security Type & Duration"].unique()[:5],
    )

    filtered_df = df[df["Security Type & Duration"].isin(tenors)]

    # Final Output Matrix
    output_df = filtered_df[
        [
            "Security Type & Duration",
            "auction_date",
            "bid_to_cover_ratio",
            "high_yield",
        ]
    ].rename(
        columns={
            "auction_date": "Auction Date",
            "bid_to_cover_ratio": "Bid-to-Cover Ratio",
            "high_yield": "High Awarded Yield (%)",
        }
    )

    st.dataframe(output_df, use_container_width=True, hide_index=True)
else:
    st.error("Telemetry link broken. Check upstream API status.")
