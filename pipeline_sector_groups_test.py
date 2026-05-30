#!/usr/bin/env python3

import asyncio
import json
import os
from collections import Counter

import httpx

# =========================================================
# CONFIG
# =========================================================

WORKER_URL = os.environ["WORKER_URL"].rstrip("/")
WORKER_TOKEN = os.environ["WORKER_TOKEN"]

INPUT_FILE = "classification.json"

OUTPUT_FILE = "classification_test.json"
SUMMARY_FILE = "sector_group_summary.json"
UNMAPPED_FILE = "unmapped.json"

# =========================================================
# HEADERS
# =========================================================

WORKER_HEADERS = {
    "X-Secret-Token": WORKER_TOKEN,
    "Content-Type": "application/json",
}

# =========================================================
# R2 DOWNLOAD
# =========================================================

async def r2_download(client, filename):

    url = f"{WORKER_URL}/{filename}"

    r = await client.get(
        url,
        headers=WORKER_HEADERS,
        timeout=120,
    )

    if r.status_code != 200:
        raise RuntimeError(
            f"{filename} download failed"
        )

    return r.json()

# =========================================================
# R2 UPLOAD
# =========================================================

async def r2_upload(client, filename, data):

    url = f"{WORKER_URL}?file={filename}"

    r = await client.post(
        url,
        headers=WORKER_HEADERS,
        content=json.dumps(data).encode(),
        timeout=120,
    )

    if r.status_code != 200:
        raise RuntimeError(
            f"{filename} upload failed"
        )

    print(f"✅ Uploaded {filename}")

# =========================================================
# MAPPING
# =========================================================

SECTOR_GROUP_MAP = {

    # Energy
    "Refineries & Marketing": "Energy",
    "Oil Exploration & Production": "Energy",
    "Gas Transmission/Marketing": "Energy",
    "LPG/CNG/PNG/LNG Supplier": "Energy",
    "Coal": "Energy",

    # Metals
    "Iron & Steel": "Metals & Mining",
    "Aluminium": "Metals & Mining",
    "Zinc": "Metals & Mining",
    "Industrial Minerals": "Metals & Mining",
    "Diversified Metals": "Metals & Mining",

    # Banks
    "Private Sector Bank": "Banks",
    "Public Sector Bank": "Banks",
    "Other Bank": "Banks",

    # NBFC
    "Non Banking Financial Company (NBFC)": "NBFC",
    "Housing Finance Company": "NBFC",

    # Insurance
    "Life Insurance": "Insurance",
    "General Insurance": "Insurance",

    # Broking
    "Stockbroking & Allied": "Broking & Wealth",

    # AMC
    "Asset Management Company": "Asset Management",

    # Market Infra
    "Exchange and Data Platform": "Market Infrastructure",

    # Auto
    "Passenger Cars & Utility Vehicles": "Automobiles",
    "Commercial Vehicles": "Automobiles",
    "2/3 Wheelers": "Automobiles",
    "Auto Components & Equipments": "Automobiles",

    # Chemicals
    "Specialty Chemicals": "Chemicals",
    "Commodity Chemicals": "Chemicals",

    # Cement
    "Cement & Cement Products": "Cement",

    # Pharma
    "Pharmaceuticals": "Pharma",

    # Hospitals
    "Hospital": "Hospitals",

    # Defence
    "Aerospace & Defense": "Defence",

    # Wires
    "Cables - Electricals": "Wires & Cables",

    # Water / Waste
    "Water Treatment": "Environmental Services",
    "Waste Management": "Environmental Services",
    # IT
    "Computers - Software & Consulting": "IT Services",
    "IT Enabled Services": "IT Services",
    "Software Products": "Software",
    
    # Power
    "Power Generation": "Power",
    "Power - Transmission": "Power",
    "Power Distribution": "Power",
    "Integrated Power Utilities": "Power",
    "Power Trading": "Power",
    
    # Telecom
    "Telecom - Cellular & Fixed line services": "Telecom",
    "Telecom - Infrastructure": "Telecom",
    "Telecom - Equipment & Accessories": "Telecom",
    "Other Telecom Services": "Telecom",
    
    # FMCG
    "Diversified FMCG": "FMCG",
    "Packaged Foods": "FMCG",
    "Personal Care": "FMCG",
    "Other Beverages": "FMCG",
    "Tea & Coffee": "FMCG",
    "Edible Oil": "FMCG",
    "Dairy Products": "FMCG",
    
    # Infrastructure
    "Civil Construction": "Infrastructure",
    
    # Realty
    "Residential, Commercial Projects": "Real Estate",
    
    # Retail
    "Speciality Retail": "Retail",
    "Diversified Retail": "Retail",
    "E-Retail/ E-Commerce": "Retail",
    "Internet & Catalogue Retail": "Retail",
    "Pharmacy Retail": "Retail",
    
    # Electrical Equipment
    "Heavy Electrical Equipment": "Electrical Equipment",
    "Other Electrical Equipment": "Electrical Equipment",
    
    # Agriculture
    "Fertilizers": "Agriculture",
    "Pesticides & Agrochemicals": "Agriculture",
    "Animal Feed": "Agriculture",
    
    # Logistics
    "Logistics Solution Provider": "Logistics",
    "Shipping": "Logistics",
    "Port & Port services": "Logistics",
    "Airport & Airport services": "Logistics",
    
    # Travel
    "Hotels & Resorts": "Travel & Hospitality",
    "Airline": "Travel & Hospitality",
    "Restaurants": "Travel & Hospitality",
    "Tour, Travel Related Services": "Travel & Hospitality",
    
    # Financials
    "Financial Institution": "NBFC",
    "Investment Company": "NBFC",
    "Holding Company": "NBFC",
    }

# =========================================================
# LOGIC
# =========================================================

def get_sector_group(stock):

    for field in [
        "sub_industry",
        "industry",
        "sector"
    ]:

        value = (
            stock.get(field)
            or ""
        ).strip()

        if value in SECTOR_GROUP_MAP:
            return SECTOR_GROUP_MAP[value]

    return "UNMAPPED"

# =========================================================
# MAIN
# =========================================================

async def main():

    async with httpx.AsyncClient() as client:

        print(
            f"Downloading {INPUT_FILE}..."
        )

        stocks = await r2_download(
            client,
            INPUT_FILE
        )

        print(
            f"Loaded {len(stocks)} stocks"
        )

        summary = Counter()
        unmapped = []

        for stock in stocks:

            sector_group = get_sector_group(
                stock
            )

            stock["sector_group"] = (
                sector_group
            )

            summary[sector_group] += 1

            if sector_group == "UNMAPPED":

                unmapped.append({

                    "symbol":
                        stock.get(
                            "symbol"
                        ),

                    "sector":
                        stock.get(
                            "sector"
                        ),

                    "industry":
                        stock.get(
                            "industry"
                        ),

                    "sub_industry":
                        stock.get(
                            "sub_industry"
                        )
                })

        summary_json = [

            {
                "sector_group": k,
                "count": v
            }

            for k, v in sorted(
                summary.items(),
                key=lambda x: x[1],
                reverse=True
            )
        ]

        print()
        print(
            f"Stocks     : {len(stocks)}"
        )

        print(
            f"Unmapped   : {len(unmapped)}"
        )

        print(
            f"Groups     : {len(summary)}"
        )

        await r2_upload(
            client,
            OUTPUT_FILE,
            stocks
        )

        await r2_upload(
            client,
            SUMMARY_FILE,
            summary_json
        )

        await r2_upload(
            client,
            UNMAPPED_FILE,
            unmapped
        )

        print()
        print("🎉 Done")

if __name__ == "__main__":
    asyncio.run(main())
