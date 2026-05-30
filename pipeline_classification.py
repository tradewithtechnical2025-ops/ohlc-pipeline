#!/usr/bin/env python3

import asyncio
import json
import os

import httpx

# =========================================================
# CONFIG
# =========================================================

FINEDGE_TOKEN = os.environ["FINEDGE_TOKEN"]

WORKER_URL   = os.environ["WORKER_URL"].rstrip("/")
WORKER_TOKEN = os.environ["WORKER_TOKEN"]

FINEDGE_BASE = "https://data.finedgeapi.com/api/v1"

OUTPUT_FILE = "classification.json"

# Slower + safer
CONCURRENCY = 3
BATCH_SIZE  = 25

RATE_DELAY = 0.25
RETRY = 3

MIN_MARKET_CAP_CR = 50
# =========================================================
# SECTOR GROUPS
# =========================================================
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
    # Industrials
    "Industrial Products": "Industrials",
    "Other Industrial Products": "Industrials",
    "Compressors, Pumps & Diesel Engines": "Industrials",
    "Castings & Forgings": "Industrials",
    "Abrasives & Bearings": "Industrials",
    "Plastic Products - Industrial": "Industrials",
    "Packaging": "Industrials",
    "Glass - Industrial": "Industrials",
    "Electrodes & Refractories": "Industrials",
    
    # Healthcare
    "Healthcare Service Provider": "Healthcare",
    "Healthcare Research, Analytics & Technology": "Healthcare",
    "Medical Equipment & Supplies": "Healthcare",
    "Biotechnology": "Healthcare",
    # Paints
    "Paints": "Paints",
    
    # Tobacco
    "Cigarettes & Tobacco Products": "Tobacco",
    
    # Consumer Services
    "Other Consumer Services": "Business Services",
    
    # Publishing / Media
    "Printing & Publication": "Media & Publishing",
    "Electronic Media": "Entertainment",
    
    # Travel
    "Amusement Parks/ Other Recreation": "Travel & Hospitality",
    "Tour Travel Related Services": "Travel & Hospitality",
    "Wellness": "Travel & Hospitality",
    
    # Consumer
    "Granites & Marbles": "Consumer Durables",
    "Leisure Products": "Consumer Durables",
    "Cycles": "Consumer Durables",
    "Diversified consumer products": "Consumer Durables",
    
    # Jewellery
    "Gems Jewellery And Watches": "Jewellery",
    "Gems, Jewellery And Watches": "Jewellery",
    
    # Textile
    "Trading - Textile Products": "Textiles",
    
    # Auto
    "Trading - Auto Components": "Automobiles",
    "Dealers-Commercial Vehicles, Tractors, Construction Vehicles": "Automobiles",
    
    # Retail
    "Distributors": "Retail",
    
    # Paper
    "Forest Products": "Paper & Packaging",
    
    # Energy
    "Trading Coal": "Energy",
    
    # Utilities
    "Other Utilities": "Utilities",
    
    # Metals
    "Precious Metals": "Metals & Mining",
    
    # Entertainment
    "Media & Entertainment": "Entertainment",
    "TV Broadcasting & Software Production": "Entertainment",
    "Film Production, Distribution & Exhibition": "Entertainment",
    "Digital Entertainment": "Entertainment",
    
    # Textiles
    "Garments & Apparels": "Textiles",
    "Other Textile Products": "Textiles",
    
    # Capital Markets
    "Depositories, Clearing Houses and Other Intermediaries": "Capital Markets",
    "Ratings": "Capital Markets",
    "Financial Products Distributor": "Capital Markets",
    
    # Railways
    "Railway Wagons": "Railways",
    
    # Water
    "Water Supply & Management": "Environmental Services",
    
    # Consumer
    "Footwear": "Consumer Durables",
    "Furniture, Home Furnishing": "Consumer Durables",
    "Houseware": "Consumer Durables",
    "Sanitary Ware": "Consumer Durables",
    "Plywood Boards/ Laminates": "Consumer Durables",
    
    # Paper
    "Paper & Paper Products": "Paper & Packaging",
    
    # Metals
    "Iron & Steel Products": "Metals & Mining",
    "Trading - Metals": "Metals & Mining",
    "Copper": "Metals & Mining",
    "Pig Iron": "Metals & Mining",
    "Ferro & Silica Manganese": "Metals & Mining",
    "Aluminium, Copper & Zinc Products": "Metals & Mining",
    # Consumer Durables
    "Gems, Jewellery And Watches": "Jewellery",
    "Household Appliances": "Consumer Durables",
    "Consumer Electronics": "Consumer Durables",
    "Ceramics": "Consumer Durables",
    "Plastic Products - Consumer": "Consumer Durables",
    "Leather And Leather Products": "Consumer Durables",
    "Furniture Home Furnishing": "Consumer Durables",
    "Glass - Consumer": "Consumer Durables",
    
    # Chemicals
    "Industrial Gases": "Chemicals",
    "Petrochemicals": "Chemicals",
    "Dyes And Pigments": "Chemicals",
    "Carbon Black": "Chemicals",
    "Printing Inks": "Chemicals",
    "Trading - Chemicals": "Chemicals",
    "Explosives": "Chemicals",
    
    # Agriculture
    "Sugar": "Agriculture",
    "Other Agricultural Products": "Agriculture",
    "Meat Products including Poultry": "Agriculture",
    "Seafood": "Agriculture",
    
    # FMCG
    "Breweries & Distilleries": "FMCG",
    "Other Food Products": "FMCG",
    "Household Products": "FMCG",
    
    # Financials
    "Financial Technology (Fintech)": "Fintech",
    "Other Financial Services": "NBFC",
    "Microfinance Institutions": "NBFC",
    "Insurance Distributors": "Insurance",
    "Depositories Clearing Houses and Other Intermediaries": "Capital Markets",
    "Other Capital Market related Services": "Capital Markets",
    
    # Telecom
    "Telecom -  Equipment & Accessories": "Telecom",
    "Computers Hardware & Equipments": "IT Hardware",
    
    # Defence
    "Ship Building & Allied Services": "Defence",
    
    # Auto
    "Tyres & Rubber Products": "Automobiles",
    "Construction Vehicles": "Automobiles",
    "Tractors": "Automobiles",
    "Auto -Dealer": "Automobiles",
    
    # Energy
    "Lubricants": "Energy",
    "Oil Equipment & Services": "Energy",
    "Oil Storage & Transportation": "Energy",
    "Trading - Gas": "Energy",
    "Offshore Support Solution Drilling": "Energy",
    
    # Media
    "Print Media": "Entertainment",
    "Advertising & Media Agencies": "Entertainment",
    "Web based media and service": "Entertainment",
    "Film Production Distribution & Exhibition": "Entertainment",
    
    # Education
    "Education": "Education",
    "E-Learning": "Education",
    
    # Services
    "Diversified Commercial Services": "Business Services",
    "Trading & Distributors": "Business Services",
    "Consulting Services": "Business Services",
    "Data Processing Services": "Business Services",
    "Business Process Outsourcing (BPO)/ Knowledge Process Outsourcing (KPO)": "Business Services",
    
    # Transport
    "Road Transport": "Logistics",
    "Transport Related Services": "Logistics",
    "Dredging": "Logistics",
    
    # Construction
    "Other Construction Materials": "Infrastructure",
    "Residential Commercial Projects": "Real Estate",
    "Real Estate related services": "Real Estate",
    "Road Assets–Toll, Annuity, Hybrid-Annuity": "Infrastructure",
    
    # Metals
    "Trading - Minerals": "Metals & Mining",
    "Sponge Iron": "Metals & Mining",
    
    # Misc
    "Diversified": "Diversified",
    "Stationary": "Consumer Products",
    "Jute & Jute Products": "Paper & Packaging",
    "Multi Utilities": "Utilities"
    }

def get_sector_group(profile):

    for field in [
        "sub_industry",
        "industry",
        "sector"
    ]:

        value = (
            profile.get(field)
            or ""
        ).strip()

        if value in SECTOR_GROUP_MAP:
            return SECTOR_GROUP_MAP[value]

    return profile.get(
        "sector",
        "Other"
    )
# =========================================================
# HEADERS
# =========================================================

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

WORKER_HEADERS = {
    "X-Secret-Token": WORKER_TOKEN,
    "Content-Type": "application/json",
}

# =========================================================
# PROFILE API
# =========================================================

PROFILE_URL = (
    "https://data.finedgeapi.com/api/v1/company-profile"
)

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
# PROFILE FETCH
# =========================================================

async def fetch_profile(client, symbol, semaphore):

    async with semaphore:

        for attempt in range(RETRY):

            await asyncio.sleep(RATE_DELAY)

            try:

                r = await client.get(
                    f"{PROFILE_URL}/{symbol}",
                    params={
                        "token": FINEDGE_TOKEN
                    },
                    timeout=60
                )

            except Exception as e:

                print(
                    f"{symbol} Network Error: {e}"
                )

                await asyncio.sleep(
                    2 ** attempt
                )

                continue

            if r.status_code == 429:

                print(f"{symbol} -> 429")

                await asyncio.sleep(15)

                continue

            if r.status_code == 503:

                print(f"{symbol} -> 503")

                await asyncio.sleep(5)

                continue

            if r.status_code != 200:

                print(
                    f"{symbol} -> HTTP {r.status_code}"
                )

                return None

            try:

                return r.json()

            except Exception:

                return None

    return None

# =========================================================
# PROCESS STOCK
# =========================================================

async def process_stock(
    client,
    stock,
    semaphore
):

    symbol = stock["symbol"]

    profile = await fetch_profile(
        client,
        symbol,
        semaphore
    )

    if not profile:

        print(
            f"✗ {symbol} | profile fail"
        )

        return None

    market_cap = (
        profile.get("market_cap")
        or 0
    )

    try:

        market_cap = float(market_cap)

    except:

        market_cap = 0

    # Skip tiny companies

    if market_cap < MIN_MARKET_CAP_CR:

        print(
            f"✗ {symbol} | "
            f"market cap < 50cr"
        )

        return None

    sector_group = get_sector_group(
        profile
    )

    print(
        f"✓ {symbol} | "
        f"{market_cap:.0f}cr"
    )

    return {

        "symbol": symbol,

        "name": stock.get("name"),

        "exchange": stock.get("exchange"),

        "market_cap_cr": market_cap,

        "macro_sector": profile.get(
            "macro_sector"
        ),

        "sector_group": sector_group,

        "sector": profile.get(
            "sector"
        ),

        "industry": profile.get(
            "industry"
        ),

        "sub_industry": profile.get(
            "sub_industry"
        ),

        "bse_code": profile.get(
            "bse_code"
        ),

        "nse_code": profile.get(
            "nse_code"
        ),

        "consolidated_ind": stock.get(
            "consolidated_ind",
            False
        )
    }

# =========================================================
# MAIN
# =========================================================

async def main():

    semaphore = asyncio.Semaphore(
        CONCURRENCY
    )

    async with httpx.AsyncClient(
        headers=HEADERS
    ) as client:

        print()
        print(
            "Downloading master.json..."
        )

        master = await r2_download(
            client,
            "master.json"
        )

        print(
            f"Loaded {len(master)} stocks"
        )

        results = []

        total = len(master)

        for i in range(0, total, BATCH_SIZE):

            batch = master[
                i:i+BATCH_SIZE
            ]

            tasks = [

                process_stock(
                    client,
                    stock,
                    semaphore
                )

                for stock in batch
            ]

            batch_results = await asyncio.gather(
                *tasks
            )

            results.extend(batch_results)

            print()

            print(
                f"Processed "
                f"{min(i+BATCH_SIZE, total)}"
                f"/{total}"
            )

            # Extra cooling delay
            await asyncio.sleep(2)

        classification = [
            x for x in results
            if x
        ]

        classification.sort(
            key=lambda x: x["market_cap_cr"],
            reverse=True
        )

        print()
        print("=== SUMMARY ===")

        print(
            f"✓ Final Stocks : "
            f"{len(classification)}"
        )

        print(
            f"✗ Removed      : "
            f"{len(master) - len(classification)}"
        )

        await r2_upload(
            client,
            OUTPUT_FILE,
            classification
        )

        print()
        print(
            "🎉 classification.json uploaded"
        )


if __name__ == "__main__":
    asyncio.run(main())

