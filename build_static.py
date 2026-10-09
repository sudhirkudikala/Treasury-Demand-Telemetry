#This script runs headlessly inside the GitHub runner, 
#fetches the raw data from the U.S. Bureau of the Fiscal Service API, 
#and compiles a responsive index.html file using a built-in clean layout.

import os
import pandas as pd
import requests


def generate_static_page():
    # Target the dedicated Fiscal Data API gateway domain
    base_url = "https://api.fiscaldata.treasury.gov/services/api/fiscal_service/"
    endpoint = "v1/accounting/od/auctions_query"
    
    url = f"{base_url}{endpoint}"
    
    params = {
        "fields": "security_type,security_term,auction_date,bid_to_cover_ratio,high_yield",
        "filter": "security_type:in:(Notes,Bonds,TIPS)",
        "sort": "-auction_date",
        "page[size]": 50,
    }
    


    try:
        res = requests.get(url, params=params)
        res.raise_for_status()
        data = res.json().get("data", [])

        df = pd.DataFrame(data)
        if df.empty:
            return

        # Formatting values
        df["auction_date"] = pd.to_datetime(df["auction_date"]).dt.strftime(
            "%b %d, %Y"
        )
        df["bid_to_cover_ratio"] = pd.to_numeric(
            df["bid_to_cover_ratio"]
        ).map("{:,.2x}".format)
        df["high_yield"] = pd.to_numeric(df["high_yield"]).map(
            lambda x: f"{x:.3f}%" if not pd.isna(x) else "N/A"
        )
        df["Security"] = df["security_term"] + " " + df["security_type"]

        # Re-index to four columns
        html_table = df[
            ["Security", "auction_date", "bid_to_cover_ratio", "high_yield"]
        ].to_html(
            index=False,
            classes="table table-striped table-hover",
            border=0,
        )

        # Basic HTML template
        html_content = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Treasury Demand Telemetry</title>
            <link href="https://jsdelivr.net" rel="stylesheet">
            <style>body {{ padding: 2rem; background-color: #f8f9fa; }} .container {{ max-width: 900px; background: white; padding: 2rem; border-radius: 8px; box-shadow: 0 4px 6px rgba(0,0,0,0.05); }}</style>
        </head>
        <body>
            <div class="container">
                <h2 class="mb-4">📊 Treasury Demand Telemetry</h2>
                <p class="text-muted">Automated 4-Month rolling snapshot of core auction parameters.</p>
                <div class="table-responsive">{html_table}</div>
                <footer class="mt-4 text-center text-muted small">Data sourced directly via Bureau of the Fiscal Service REST API</footer>
            </div>
        </body>
        </html>
        """

        with open("index.html", "w") as f:
            f.write(html_content)
        print("Success: index.html generated cleanly.")

    except Exception as e:
        print(f"Pipeline Error: {e}")
        import sys
        sys.exit(1) # Forces the GitHub Action step to explicitly fail if index.html isn't built



if __name__ == "__main__":
    generate_static_page()
