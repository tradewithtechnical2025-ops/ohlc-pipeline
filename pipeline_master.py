# pipeline_master.py

```python id="f8q0za"
import asyncio
import json
import os

import boto3
import httpx

# =========================================================
# CONFIG
# =========================================================

TOKEN = os.getenv("FINEDGE_TOKEN")

STOCK_SYMBOLS_API = (
    f"https://data.finedgeapi.com/api/v1/stock-symbols"
    f"?token={TOKEN}"
)

OUTPUT_FILE = "master.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

# =========================================================
# FILTERS
# =========================================================

EXCLUDE_KEYWORDS = [

    "NIFTY",
    "SENSEX",
    "ETF",
    "BEES",
    "LIQUID",
    "GOLD",
    "SILVER",
    "INDEX",
]


def is_valid_stock(stock):

    symbol = str(
        stock.get("symbol", "")
    ).upper()

    name = str(
        stock.get("name", "")
    ).upper()

    if not symbol:
        return False

    if not name:
        return False

    for keyword in EXCLUDE_KEYWORDS:

        if keyword in symbol:
            return False

        if keyword in name:
            return False

    return True


# =========================================================
# FETCH STOCKS
# =========================================================

async def fetch_stocks(client):

    print("Fetching stock universe...")

    r = await client.get(
        STOCK_SYMBOLS_API
    )

    r.raise_for_status()

    data = r.json()

    print(f"Fetched {len(data)} symbols")

    return data


# =========================================================
# BUILD MASTER
# =========================================================

def build_master(data):

    print()
    print("=== Building Master Universe ===")

    seen = set()

    master = []

    total = len(data)

    skipped = 0

    for idx, stock in enumerate(data, start=1):

        if not is_valid_stock(stock):
            skipped += 1
            continue

        symbol = stock.get("symbol")

        if symbol in seen:
            continue

        seen.add(symbol)

        item = {

            "symbol": symbol,

            "name": stock.get("name"),

            "bse_code": stock.get("bse_code"),

            "nse_code": stock.get("nse_code"),

            "consolidated_ind": stock.get(
                "consolidated_ind",
                False
            )
        }

        master.append(item)

        print(
            f"[{idx}/{total}] ✓ {symbol}"
        )

    print()
    print("=== Summary ===")
    print(f"✓ Final Stocks : {len(master)}")
    print(f"✗ Skipped      : {skipped}")

    return master


# =========================================================
# SAVE
# =========================================================

def save_json(data, filename):

    with open(filename, "w", encoding="utf-8") as f:

        json.dump(
            data,
            f,
            ensure_ascii=False,
            indent=2
        )

    print()
    print(f"✅ Saved -> {filename}")


# =========================================================
# UPLOAD TO R2
# =========================================================

def upload_to_r2(filepath):

    account_id = os.getenv("R2_ACCOUNT_ID")

    access_key = os.getenv("R2_ACCESS_KEY_ID")

    secret_key = os.getenv("R2_SECRET_ACCESS_KEY")

    bucket = os.getenv("R2_BUCKET")

    s3 = boto3.client(
        "s3",
        endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="auto"
    )

    filename = os.path.basename(filepath)

    s3.upload_file(
        filepath,
        bucket,
        filename,
        ExtraArgs={
            "ContentType": "application/json"
        }
    )

    print(f"✅ Uploaded {filename}")


# =========================================================
# MAIN
# =========================================================

async def main():

    async with httpx.AsyncClient(
        timeout=60,
        headers=HEADERS
    ) as client:

        data = await fetch_stocks(client)

        master = build_master(data)

        save_json(
            master,
            OUTPUT_FILE
        )

        upload_to_r2(
            OUTPUT_FILE
        )

        print()
        print("🎉 master.json ready")


if __name__ == "__main__":
    asyncio.run(main())
```
