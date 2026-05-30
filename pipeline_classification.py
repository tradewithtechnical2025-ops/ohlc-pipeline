#!/usr/bin/env python3

import asyncio
import json
import os

import httpx

# =========================================================
# CONFIG
# =========================================================

FINEDGE_TOKEN = os.environ["FINEDGE_TOKEN"]
WORKER_URL    = os.environ["WORKER_URL"].rstrip("/")
WORKER_TOKEN  = os.environ["WORKER_TOKEN"]

OUTPUT_FILE          = "classification.json"
FUNDAMENTAL_FILE     = "fundamental.json"

CONCURRENCY = 3
BATCH_SIZE  = 25
RATE_DELAY  = 0.25
RETRY       = 3

MIN_MARKET_CAP_CR = 50

# =========================================================
# SECTOR GROUP MAP
# sub_industry → sector_group
# =========================================================

SECTOR_GROUP_MAP = {

    # ── Energy ──
    "Refineries & Marketing":                   "Energy",
    "Oil Exploration & Production":             "Energy",
    "Gas Transmission/Marketing":               "Energy",
    "LPG/CNG/PNG/LNG Supplier":                 "Energy",
    "Coal":                                     "Energy",
    "Lubricants":                               "Energy",
    "Oil Equipment & Services":                 "Energy",
    "Oil Storage & Transportation":             "Energy",
    "Trading - Gas":                            "Energy",
    "Offshore Support Solution Drilling":       "Energy",
    "Trading Coal":                             "Energy",

    # ── Metals & Mining ──
    "Iron & Steel":                             "Metals & Mining",
    "Iron & Steel Products":                    "Metals & Mining",
    "Aluminium":                                "Metals & Mining",
    "Zinc":                                     "Metals & Mining",
    "Copper":                                   "Metals & Mining",
    "Industrial Minerals":                      "Metals & Mining",
    "Diversified Metals":                       "Metals & Mining",
    "Precious Metals":                          "Metals & Mining",
    "Ferro & Silica Manganese":                 "Metals & Mining",
    "Trading - Metals":                         "Metals & Mining",
    "Sponge Iron":                              "Metals & Mining",
    "Pig Iron":                                 "Metals & Mining",
    "Trading - Minerals":                       "Metals & Mining",
    "Aluminium, Copper & Zinc Products":        "Metals & Mining",
    "Aluminium Copper & Zinc Products":         "Metals & Mining",

    # ── Banks ──
    "Private Sector Bank":                      "Banks",
    "Public Sector Bank":                       "Banks",
    "Other Bank":                               "Banks",

    # ── Financial Services ──
    "Non Banking Financial Company (NBFC)":     "Financial Services",
    "Housing Finance Company":                  "Financial Services",
    "Financial Institution":                    "Financial Services",
    "Investment Company":                       "Financial Services",
    "Holding Company":                          "Financial Services",
    "Other Financial Services":                 "Financial Services",
    "Microfinance Institutions":                "Financial Services",
    "Life Insurance":                           "Financial Services",
    "General Insurance":                        "Financial Services",
    "Insurance Distributors":                   "Financial Services",
    "Stockbroking & Allied":                    "Financial Services",
    "Asset Management Company":                 "Financial Services",
    "Exchange and Data Platform":               "Financial Services",
    "Financial Products Distributor":           "Financial Services",
    "Depositories, Clearing Houses and Other Intermediaries": "Financial Services",
    "Depositories Clearing Houses and Other Intermediaries":  "Financial Services",
    "Other Capital Market related Services":    "Financial Services",
    "Ratings":                                  "Financial Services",
    "Financial Technology (Fintech)":           "Financial Services",

    # ── Automobiles ──
    "Passenger Cars & Utility Vehicles":        "Automobiles",
    "Commercial Vehicles":                      "Automobiles",
    "2/3 Wheelers":                             "Automobiles",
    "Auto Components & Equipments":             "Automobiles",
    "Tyres & Rubber Products":                  "Automobiles",
    "Tractors":                                 "Automobiles",
    "Auto -Dealer":                             "Automobiles",
    "Trading - Auto Components":                "Automobiles",
    "Construction Vehicles":                    "Automobiles",
    "Dealers-Commercial Vehicles, Tractors, Construction Vehicles": "Automobiles",

    # ── Chemicals ──
    "Specialty Chemicals":                      "Chemicals",
    "Commodity Chemicals":                      "Chemicals",
    "Dyes And Pigments":                        "Chemicals",
    "Petrochemicals":                           "Chemicals",
    "Industrial Gases":                         "Chemicals",
    "Carbon Black":                             "Chemicals",
    "Printing Inks":                            "Chemicals",
    "Trading - Chemicals":                      "Chemicals",
    "Explosives":                               "Chemicals",

    # ── Cement ──
    "Cement & Cement Products":                 "Cement",

    # ── Pharma ──
    "Pharmaceuticals":                          "Pharma",

    # ── Healthcare ──
    "Hospital":                                 "Healthcare",
    "Healthcare Service Provider":              "Healthcare",
    "Healthcare Research, Analytics & Technology": "Healthcare",
    "Medical Equipment & Supplies":             "Healthcare",
    "Biotechnology":                            "Healthcare",

    # ── Defence ──
    "Aerospace & Defense":                      "Defence",
    "Ship Building & Allied Services":          "Defence",

    # ── Wires & Cables ──
    "Cables - Electricals":                     "Wires & Cables",

    # ── Environmental Services ──
    "Water Treatment":                          "Environmental Services",
    "Waste Management":                         "Environmental Services",
    "Water Supply & Management":                "Environmental Services",

    # ── IT Services ──
    "Computers - Software & Consulting":        "IT Services",
    "IT Enabled Services":                      "IT Services",

    # ── Software ──
    "Software Products":                        "Software",

    # ── IT Hardware ──
    "Computers Hardware & Equipments":          "IT Hardware",

    # ── Power ──
    "Power Generation":                         "Power",
    "Power - Transmission":                     "Power",
    "Power Distribution":                       "Power",
    "Integrated Power Utilities":               "Power",
    "Power Trading":                            "Power",
    "Multi Utilities":                          "Power",
    "Other Utilities":                          "Power",

    # ── Telecom ──
    "Telecom - Cellular & Fixed line services": "Telecom",
    "Telecom - Infrastructure":                 "Telecom",
    "Telecom - Equipment & Accessories":        "Telecom",
    "Telecom -  Equipment & Accessories":       "Telecom",
    "Other Telecom Services":                   "Telecom",

    # ── FMCG ──
    "Diversified FMCG":                         "FMCG",
    "Packaged Foods":                           "FMCG",
    "Personal Care":                            "FMCG",
    "Other Beverages":                          "FMCG",
    "Tea & Coffee":                             "FMCG",
    "Edible Oil":                               "FMCG",
    "Dairy Products":                           "FMCG",
    "Breweries & Distilleries":                 "FMCG",
    "Other Food Products":                      "FMCG",
    "Household Products":                       "FMCG",
    "Cigarettes & Tobacco Products":            "FMCG",

    # ── Infrastructure ──
    "Civil Construction":                       "Infrastructure",
    "Other Construction Materials":             "Infrastructure",
    "Road Assets–Toll, Annuity, Hybrid-Annuity": "Infrastructure",

    # ── Real Estate ──
    "Residential, Commercial Projects":         "Real Estate",
    "Residential Commercial Projects":          "Real Estate",
    "Real Estate related services":             "Real Estate",

    # ── Retail ──
    "Speciality Retail":                        "Retail",
    "Diversified Retail":                       "Retail",
    "E-Retail/ E-Commerce":                     "Retail",
    "Internet & Catalogue Retail":              "Retail",
    "Pharmacy Retail":                          "Retail",
    "Distributors":                             "Retail",

    # ── Electrical Equipment ──
    "Heavy Electrical Equipment":               "Electrical Equipment",
    "Other Electrical Equipment":               "Electrical Equipment",

    # ── Agriculture ──
    "Fertilizers":                              "Agriculture",
    "Pesticides & Agrochemicals":               "Agriculture",
    "Animal Feed":                              "Agriculture",
    "Sugar":                                    "Agriculture",
    "Other Agricultural Products":              "Agriculture",
    "Meat Products including Poultry":          "Agriculture",
    "Seafood":                                  "Agriculture",

    # ── Logistics ──
    "Logistics Solution Provider":              "Logistics",
    "Shipping":                                 "Logistics",
    "Port & Port services":                     "Logistics",
    "Airport & Airport services":               "Logistics",
    "Road Transport":                           "Logistics",
    "Transport Related Services":               "Logistics",
    "Dredging":                                 "Logistics",

    # ── Travel & Hospitality ──
    "Hotels & Resorts":                         "Travel & Hospitality",
    "Airline":                                  "Travel & Hospitality",
    "Restaurants":                              "Travel & Hospitality",
    "Tour, Travel Related Services":            "Travel & Hospitality",
    "Amusement Parks/ Other Recreation":        "Travel & Hospitality",
    "Tour Travel Related Services":             "Travel & Hospitality",
    "Wellness":                                 "Travel & Hospitality",

    # ── Industrials ──
    "Industrial Products":                      "Industrials",
    "Other Industrial Products":                "Industrials",
    "Compressors, Pumps & Diesel Engines":      "Industrials",
    "Compressors Pumps & Diesel Engines":       "Industrials",
    "Castings & Forgings":                      "Industrials",
    "Abrasives & Bearings":                     "Industrials",
    "Plastic Products - Industrial":            "Industrials",
    "Packaging":                                "Industrials",
    "Glass - Industrial":                       "Industrials",
    "Electrodes & Refractories":                "Industrials",
    "Railway Wagons":                           "Industrials",
    "Rubber":                                   "Industrials",

    # ── Consumer Durables ──
    "Household Appliances":                     "Consumer Durables",
    "Consumer Electronics":                     "Consumer Durables",
    "Granites & Marbles":                       "Consumer Durables",
    "Leisure Products":                         "Consumer Durables",
    "Cycles":                                   "Consumer Durables",
    "Diversified consumer products":            "Consumer Durables",
    "Footwear":                                 "Consumer Durables",
    "Furniture, Home Furnishing":               "Consumer Durables",
    "Furniture Home Furnishing":                "Consumer Durables",
    "Houseware":                                "Consumer Durables",
    "Sanitary Ware":                            "Consumer Durables",
    "Plywood Boards/ Laminates":                "Consumer Durables",
    "Glass - Consumer":                         "Consumer Durables",
    "Leather And Leather Products":             "Consumer Durables",
    "Plastic Products - Consumer":              "Consumer Durables",
    "Ceramics":                                 "Consumer Durables",
    "Gems, Jewellery And Watches":              "Consumer Durables",
    "Gems Jewellery And Watches":               "Consumer Durables",
    "Paints":                                   "Consumer Durables",
    "Stationary":                               "Consumer Durables",

    # ── Entertainment ──
    "Media & Entertainment":                    "Entertainment",
    "TV Broadcasting & Software Production":    "Entertainment",
    "Film Production, Distribution & Exhibition": "Entertainment",
    "Film Production Distribution & Exhibition":  "Entertainment",
    "Digital Entertainment":                    "Entertainment",
    "Advertising & Media Agencies":             "Entertainment",
    "Web based media and service":              "Entertainment",
    "Print Media":                              "Entertainment",
    "Printing & Publication":                   "Entertainment",
    "Electronic Media":                         "Entertainment",

    # ── Business Services ──
    "Diversified Commercial Services":          "Business Services",
    "Trading & Distributors":                   "Business Services",
    "Consulting Services":                      "Business Services",
    "Business Process Outsourcing (BPO)/ Knowledge Process Outsourcing (KPO)": "Business Services",
    "Other Consumer Services":                  "Business Services",
    "Data Processing Services":                 "Business Services",

    # ── Education ──
    "Education":                                "Education",
    "E-Learning":                               "Education",

    # ── Paper & Packaging ──
    "Paper & Paper Products":                   "Paper & Packaging",
    "Forest Products":                          "Paper & Packaging",
    "Jute & Jute Products":                     "Paper & Packaging",

    # ── Textiles ──
    "Other Textile Products":                   "Textiles",
    "Garments & Apparels":                      "Textiles",
    "Trading - Textile Products":               "Textiles",

    # ── Diversified ──
    "Diversified":                              "Diversified",
}

# =========================================================
# INDUSTRY MAP
# sub_industry → display_industry
# =========================================================

INDUSTRY_MAP = {
    "Other Agricultural Products": "Agri Products",
    "Sugar": "Sugar",
    "Pesticides & Agrochemicals": "Agrochemicals",
    "Fertilizers": "Fertilizers",
    "Animal Feed": "Agri Products",
    "Seafood": "Agri Products",
    "Meat Products including Poultry": "Agri Products",
    "Auto Components & Equipments": "Auto Components",
    "Tyres & Rubber Products": "Tyres & Rubber",
    "2/3 Wheelers": "2/3 Wheelers",
    "Passenger Cars & Utility Vehicles": "Passenger Vehicles",
    "Construction Vehicles": "Commercial Vehicles",
    "Commercial Vehicles": "Commercial Vehicles",
    "Tractors": "Commercial Vehicles",
    "Auto -Dealer": "Auto Dealers",
    "Trading - Auto Components": "Auto Components",
    "Dealers-Commercial Vehicles, Tractors, Construction Vehicles": "Commercial Vehicles",
    "Private Sector Bank": "Private Banks",
    "Public Sector Bank": "PSU Banks",
    "Other Bank": "Other Banks",
    "Stockbroking & Allied": "Stockbroking",
    "Asset Management Company": "Asset Management",
    "Exchange and Data Platform": "Exchanges",
    "Financial Products Distributor": "Wealth Mgmt",
    "Depositories, Clearing Houses and Other Intermediaries": "Depositories",
    "Depositories Clearing Houses and Other Intermediaries": "Depositories",
    "Other Capital Market related Services": "Other Cap Markets",
    "Ratings": "Ratings",
    "Financial Technology (Fintech)": "Fintech",
    "Non Banking Financial Company (NBFC)": "NBFC",
    "Other Financial Services": "Other Financials",
    "Investment Company": "Investment Cos",
    "Holding Company": "Holding Cos",
    "Housing Finance Company": "Housing Finance",
    "Financial Institution": "Other Financials",
    "Microfinance Institutions": "Microfinance",
    "Life Insurance": "Life Insurance",
    "General Insurance": "General Insurance",
    "Insurance Distributors": "Insurance Broking",
    "Diversified Commercial Services": "Commercial Services",
    "Trading & Distributors": "Trading & Distribution",
    "Consulting Services": "Consulting",
    "Business Process Outsourcing (BPO)/ Knowledge Process Outsourcing (KPO)": "BPO/KPO",
    "Other Consumer Services": "Other Services",
    "Data Processing Services": "Other Services",
    "Cement & Cement Products": "Cement",
    "Specialty Chemicals": "Specialty Chemicals",
    "Commodity Chemicals": "Commodity Chemicals",
    "Dyes And Pigments": "Dyes & Pigments",
    "Petrochemicals": "Petrochemicals",
    "Trading - Chemicals": "Specialty Chemicals",
    "Explosives": "Specialty Chemicals",
    "Industrial Gases": "Industrial Gases",
    "Carbon Black": "Specialty Chemicals",
    "Printing Inks": "Specialty Chemicals",
    "Household Appliances": "Appliances",
    "Consumer Electronics": "Electronics",
    "Plywood Boards/ Laminates": "Building Materials",
    "Footwear": "Footwear",
    "Ceramics": "Building Materials",
    "Plastic Products - Consumer": "Plastic Products",
    "Furniture Home Furnishing": "Furniture",
    "Furniture, Home Furnishing": "Furniture",
    "Granites & Marbles": "Building Materials",
    "Leather And Leather Products": "Leather",
    "Houseware": "Appliances",
    "Leisure Products": "Other Durables",
    "Sanitary Ware": "Building Materials",
    "Diversified consumer products": "Other Durables",
    "Glass - Consumer": "Other Durables",
    "Cycles": "Other Durables",
    "Paints": "Paints",
    "Stationary": "Stationery",
    "Gems, Jewellery And Watches": "Jewellery",
    "Gems Jewellery And Watches": "Jewellery",
    "Aerospace & Defense": "Aerospace & Defense",
    "Ship Building & Allied Services": "Shipbuilding",
    "Education": "Education",
    "E-Learning": "E-Learning",
    "Heavy Electrical Equipment": "Heavy Electricals",
    "Other Electrical Equipment": "Electrical Equipment",
    "Refineries & Marketing": "Refineries",
    "Lubricants": "Lubricants",
    "LPG/CNG/PNG/LNG Supplier": "Gas Distribution",
    "Oil Exploration & Production": "Oil E&P",
    "Oil Equipment & Services": "Oil Services",
    "Offshore Support Solution Drilling": "Oil Services",
    "Coal": "Coal",
    "Oil Storage & Transportation": "Oil Services",
    "Gas Transmission/Marketing": "Gas Distribution",
    "Trading - Gas": "Gas Distribution",
    "Trading Coal": "Coal",
    "Media & Entertainment": "Media & Entertainment",
    "Film Production, Distribution & Exhibition": "Film & OTT",
    "Film Production Distribution & Exhibition": "Film & OTT",
    "TV Broadcasting & Software Production": "Broadcasting",
    "Advertising & Media Agencies": "Advertising",
    "Print Media": "Print Media",
    "Printing & Publication": "Print Media",
    "Web based media and service": "Digital Media",
    "Digital Entertainment": "Film & OTT",
    "Electronic Media": "Broadcasting",
    "Waste Management": "Waste Management",
    "Water Supply & Management": "Water Management",
    "Water Treatment": "Water Treatment",
    "Other Food Products": "Food Products",
    "Packaged Foods": "Packaged Foods",
    "Breweries & Distilleries": "Beverages",
    "Edible Oil": "Edible Oil",
    "Tea & Coffee": "Tea & Coffee",
    "Personal Care": "Personal Care",
    "Dairy Products": "Dairy",
    "Household Products": "Household Products",
    "Diversified FMCG": "Diversified FMCG",
    "Other Beverages": "Beverages",
    "Cigarettes & Tobacco Products": "Tobacco",
    "Medical Equipment & Supplies": "Med Devices",
    "Healthcare Service Provider": "Healthcare Services",
    "Healthcare Research, Analytics & Technology": "Health Tech",
    "Biotechnology": "Biotech",
    "Hospital": "Hospitals",
    "Computers Hardware & Equipments": "IT Hardware",
    "Computers - Software & Consulting": "IT Consulting",
    "IT Enabled Services": "IT Enabled Services",
    "Software Products": "Software Products",
    "Industrial Products": "Industrial Products",
    "Packaging": "Packaging",
    "Plastic Products - Industrial": "Plastic Products",
    "Other Industrial Products": "Industrial Products",
    "Castings & Forgings": "Castings & Forgings",
    "Electrodes & Refractories": "Electrodes & Refractories",
    "Rubber": "Rubber",
    "Compressors, Pumps & Diesel Engines": "Machinery",
    "Compressors Pumps & Diesel Engines": "Machinery",
    "Abrasives & Bearings": "Machinery",
    "Aluminium Copper & Zinc Products": "Other Industrials",
    "Glass - Industrial": "Other Industrials",
    "Railway Wagons": "Railways",
    "Civil Construction": "Civil Construction",
    "Other Construction Materials": "Construction Materials",
    "Road Assets–Toll, Annuity, Hybrid-Annuity": "Road Assets",
    "Logistics Solution Provider": "3PL Logistics",
    "Shipping": "Shipping",
    "Port & Port services": "Ports",
    "Road Transport": "Road Transport",
    "Airport & Airport services": "Airports",
    "Transport Related Services": "Transport Services",
    "Dredging": "Shipping",
    "Iron & Steel Products": "Steel",
    "Iron & Steel": "Steel",
    "Industrial Minerals": "Minerals & Mining",
    "Aluminium, Copper & Zinc Products": "Non-Ferrous Metals",
    "Aluminium": "Aluminium",
    "Ferro & Silica Manganese": "Steel",
    "Trading - Metals": "Metals Trading",
    "Sponge Iron": "Steel",
    "Diversified Metals": "Non-Ferrous Metals",
    "Copper": "Non-Ferrous Metals",
    "Trading - Minerals": "Minerals & Mining",
    "Zinc": "Non-Ferrous Metals",
    "Pig Iron": "Steel",
    "Precious Metals": "Non-Ferrous Metals",
    "Paper & Paper Products": "Paper",
    "Jute & Jute Products": "Paper",
    "Forest Products": "Paper",
    "Pharmaceuticals": "Pharmaceuticals",
    "Power Generation": "Power Generation",
    "Integrated Power Utilities": "Integrated Power",
    "Power Distribution": "Power Distribution",
    "Power - Transmission": "Power Transmission",
    "Power Trading": "Power Generation",
    "Multi Utilities": "Utilities",
    "Other Utilities": "Utilities",
    "Residential, Commercial Projects": "Real Estate",
    "Residential Commercial Projects": "Real Estate",
    "Real Estate related services": "Real Estate Services",
    "Speciality Retail": "Specialty Retail",
    "Diversified Retail": "Diversified Retail",
    "E-Retail/ E-Commerce": "E-Commerce",
    "Internet & Catalogue Retail": "E-Commerce",
    "Distributors": "Distribution",
    "Pharmacy Retail": "Pharmacy Retail",
    "Telecom -  Equipment & Accessories": "Telecom Equipment",
    "Telecom - Equipment & Accessories": "Telecom Equipment",
    "Telecom - Infrastructure": "Telecom Infra",
    "Telecom - Cellular & Fixed line services": "Telecom Services",
    "Other Telecom Services": "Telecom Services",
    "Other Textile Products": "Textiles",
    "Garments & Apparels": "Garments",
    "Trading - Textile Products": "Textiles",
    "Hotels & Resorts": "Hotels",
    "Restaurants": "Restaurants",
    "Tour, Travel Related Services": "Travel Services",
    "Airline": "Airlines",
    "Amusement Parks/ Other Recreation": "Recreation",
    "Wellness": "Travel Services",
    "Tour Travel Related Services": "Travel Services",
    "Cables - Electricals": "Wires & Cables",
    "Diversified": "Diversified",
}

# =========================================================
# DESCRIPTION-BASED OVERRIDES
# Scans company description for keywords
# Priority: description > sub_industry map
# =========================================================

# Format: ([keywords], sector_group, display_industry)
DESCRIPTION_SECTOR_MAP = [
    (["prepaid payment", "prepaid card", "expense management platform",
      "fintech service", "digital payment", "payment solution",
      "payment gateway", "neobank", "buy now pay later", "bnpl",
      "digital lending", "peer-to-peer lending", "p2p lending"],
     "Financial Services", "Fintech"),

    (["wealth management", "portfolio management service",
      "investment advisory", "estate planning", "family office",
      "private wealth", "wealth and asset management"],
     "Financial Services", "Wealth Mgmt"),

    (["mutual fund", "alternative investment fund", "aif management",
      "asset management company", "fund management"],
     "Financial Services", "Asset Management"),

    (["life insurance", "general insurance", "health insurance",
      "reinsurance", "insurance broker", "insurance distributor"],
     "Financial Services", "Life Insurance"),

    (["stockbroking", "stock broking", "equity broking",
      "commodity broking", "depository participant"],
     "Financial Services", "Stockbroking"),

    (["housing finance", "home loan", "mortgage loan"],
     "Financial Services", "Housing Finance"),

    (["microfinance", "micro finance", "self help group",
      "joint liability group"],
     "Financial Services", "Microfinance"),

    (["solar panel", "solar cell", "solar module", "photovoltaic",
      "wind turbine", "wind energy", "renewable energy developer",
      "green energy developer"],
     "Power", "Power Generation"),

    (["water treatment plant", "wastewater treatment", "sewage treatment",
      "desalination plant", "effluent treatment plant"],
     "Environmental Services", "Water Treatment"),

    (["multispecialty hospital", "hospital chain", "healthcare delivery",
      "tertiary care hospital", "hospital network"],
     "Healthcare", "Hospitals"),
]

# Format: ([keywords], [themes])
DESCRIPTION_THEME_MAP = [
    (["5g", "telecom infrastructure", "optical fibre", "fiber optic",
      "telecom tower", "cellular network", "wireless network"],
     ["Telecom & 5G"]),

    (["data center", "data centre", "artificial intelligence platform",
      "machine learning platform", "cloud infrastructure", "colocation"],
     ["Data Center & AI Infra"]),

    (["power cable", "electrical cable", "winding wire",
      "optical fibre cable", "submarine cable", "cables and wires"],
     ["Cables & Wires"]),

    (["wealth management", "asset management", "portfolio management",
      "mutual fund", "alternative investment fund"],
     ["Wealth & AMC"]),

    (["defence", "defense", "aerospace", "military", "naval vessel",
      "armament", "missile", "radar system", "unmanned aerial vehicle",
      "uav", "surveillance system"],
     ["Defence"]),

    (["solar", "wind energy", "renewable energy", "green energy",
      "clean energy", "photovoltaic", "wind turbine", "hydropower"],
     ["Renewable Energy"]),

    (["electric vehicle", " ev ", "electric mobility", "ev charging",
      "battery management", "electric bus", "electric two wheeler",
      "electric car"],
     ["EV"]),

    (["railway", "rail vikas", "metro rail", "freight corridor",
      "railway signalling", "rail coach", "railway wagon",
      "railway electrification", "locomotive", "dedicated freight"],
     ["Railways"]),

    (["stock exchange", "commodity exchange", "depository services",
      "clearing corporation", "stockbroking", "investment banking",
      "capital market services"],
     ["Capital Markets"]),

    (["semiconductor", "integrated circuit", "pcb assembly",
      "electronic manufacturing service", "chip design",
      "embedded system", "vlsi design"],
     ["Semiconductor", "Electronics Manufacturing"]),

    (["water treatment", "wastewater treatment", "sewage treatment",
      "desalination", "water purification", "effluent treatment"],
     ["Water Treatment"]),

    (["specialty chemical", "agrochemical", "fine chemical",
      "fluorochemical", "dye intermediate", "pigment manufacture",
      "performance chemical"],
     ["Specialty Chemicals"]),

    (["airline", "aviation services", "airport operations",
      "aircraft maintenance", "aerospace component", "mro service",
      "air cargo"],
     ["Aviation"]),

    (["logistics", "supply chain", "warehousing", "freight forwarding",
      "cargo", "last mile delivery", "cold chain logistics", "3pl"],
     ["Logistics"]),

    (["road construction", "highway construction", "bridge construction",
      "infrastructure project", "epc contractor", "civil engineering",
      "toll road"],
     ["Infrastructure"]),

    (["residential project", "commercial project", "real estate developer",
      "township development", "shopping mall", "office space developer"],
     ["Real Estate"]),

    (["power generation", "power distribution", "power transmission",
      "thermal power", "hydroelectric", "nuclear power", "power utility"],
     ["Power"]),

    (["pharmaceutical", "drug formulation", "api manufacturer",
      "generics", "biosimilar", "vaccine manufacturer",
      "active pharmaceutical ingredient"],
     ["Pharma"]),

    (["consumer goods", "packaged food", "personal care product",
      "household product", "fast moving consumer"],
     ["FMCG"]),

    (["quick service restaurant", "food delivery platform",
      "fashion retail", "retail chain", "e-commerce platform",
      "consumer brand"],
     ["Consumption"]),

    (["public sector bank", "nationalised bank", "government owned bank"],
     ["PSU Banks"]),

    (["hotel chain", "hospitality company", "luxury hotel",
      "hotel management", "resort operations"],
     ["Hotels"]),

    (["life insurance", "general insurance", "health insurance",
      "reinsurance", "insurance company"],
     ["Insurance"]),

    (["prepaid", "payment solution", "fintech", "digital payment",
      "neobank", "digital lending", "expense management"],
     ["Capital Markets"]),
]

# =========================================================
# THEMES (manual symbol list — fallback/supplement)
# =========================================================

_THEME_SYMBOLS = {
    "Defence": ['HAL','BEL','BDL','MAZDOCK','GRSE','COCHINSHIP','PARAS','DATAPATTNS','IDEAFORGE','ZENTEC','ASTRAMICRO','AVANTEL','MTARTECH','SOLARINDS','BEML','SWANDEF','ROSSTECH','AXISCADES','MIDHANI','NIBE','SIKA','AEQUS','APOLLO','UNIMECH','DCXINDIA'],
    "Railways": ['RVNL','IRFC','IRCON','RAILTEL','RITES','IRCTC','TITAGARH','TEXRAIL','JWL','KERNEX','BEML','HFCL','NRAIL','TRANSRAILL','E2ERAIL'],
    "Power": ['NTPC','POWERGRID','NHPC','SJVN','NLCINDIA','JSWENERGY','TATAPOWER','TORNTPOWER','ADANIGREEN','ADANIPOWER','KEC','CGPOWER','CESC','RECLTD','PFC','IREDA','JPPOWER','RPOWER'],
    "Renewable Energy": ['SUZLON','INOXWIND','WAAREEENER','PREMIERENE','KPIGREEN','BORORENEW','WEBELSOLAR','NTPCGREEN','JSWENERGY','ACMESOLAR','ADANIGREEN','IREDA','TATAPOWER','SJVN','GENSOL'],
    "Data Center & AI Infra": ['ANANTRAJ','NETWEB','BBOX','TATACOMM','STLTECH','HFCL','TEJASNET','POLYCAB','KEI','SCHNEIDER','ABB','SIEMENS','BLUESTARCO','VOLTAS','CMSINFO','RATEGAIN','KPITTECH','PERSISTENT','LTTS','TATAELXSI'],
    "Telecom & 5G": ['BHARTIARTL','HFCL','TEJASNET','ITI','STLTECH','TATACOMM','INDUSTOWER','IDEA','ROUTE','TANLA','ONMOBILE','VINDHYATEL','GTPL','TTML','MTNL'],
    "EV": ['TMPV','OLECTRA','EXIDEIND','ARE&M','SONACOMS','MOTHERSON','GREAVESCOT','JBMA','WARDINMOBI','LUMAXIND','CRAFTSMAN','TVSMOTOR','BAJAJ-AUTO'],
    "Electronics Manufacturing": ['DIXON','KAYNES','SYRMA','PGEL','MOSCHIP','ASMTECH','DCXINDIA','CYIENTDLM','AMBER','AVALON','CENTUM','VSSL','SANSERA','CGPOWER','APARINDS'],
    "Capital Markets": ['BSE','MCX','CDSL','NSDL','CAMS','KFINTECH','ANGELONE','MOTILALOFS','360ONE','NUVAMA','ISEC','GROWW','5PAISA','GEOJITFSL','EMKAY','ANANDRATHI'],
    "Water Treatment": ['IONEXCHANG','THERMAX','JASH','WPIL','EMSLIMITED','WABAG','TRIVENI'],
    "Specialty Chemicals": ['DEEPAKNTR','SRF','NAVINFLUOR','PIIND','AETHER','FINEORG','VINATIORGA','CLEAN','AARTIIND','GALAXYSURF','SUDARSCHEM','ROSSARI','ALKYLAMINE','TATACHEM','NOCIL','ATUL','PIDILITIND','FLUOROCHEM'],
    "Cables & Wires": ['POLYCAB','KEI','FINCABLES','RRKABEL','APARINDS','HAVELLS','UNIVCABLES','VINDHYATEL','STLTECH','HFCL'],
    "PSU Banks": ['SBIN','BANKBARODA','PNB','CANBK','UNIONBANK','INDIANB','BANKINDIA','CENTRALBK','MAHABANK','UCOBANK','IOB','J&KBANK'],
    "Real Estate": ['DLF','LODHA','GODREJPROP','OBEROIRLTY','PRESTIGE','ANANTRAJ','PHOENIXLTD','BRIGADE','SOBHA','MAHLIFE','KOLTEPATIL','SUNTECK','NESCO','ASHIANA'],
    "Infrastructure": ['LT','NBCC','NCC','KNRCON','PNCINFRA','IRB','ASHOKA','HGINFRA','HCC','JSWINFRA','CAPACITE','GRINFRA','RVNL'],
    "Pharma": ['SUNPHARMA','CIPLA','DIVISLAB','DRREDDY','MANKIND','LUPIN','TORNTPHARM','AUROPHARMA','ALKEM','ZYDUSLIFE','IPCALAB','GLENMARK','BIOCON','LAURUSLABS','AJANTPHARM','GRANULES','ERIS','JBCHEPHARM','CAPLIPOINT'],
    "FMCG": ['ITC','HINDUNILVR','NESTLEIND','VBL','DABUR','BRITANNIA','TATACONSUM'],
    "Consumption": ['TITAN','TRENT','DMART','ZOMATO','SWIGGY','EASEMYTRIP','NYKAA','MANYAVAR','JUBLFOOD','DEVYANI','WESTLIFE','RELAXO','CAMPUS','SAFARI'],
    "Logistics": ['CONCOR','ALLCARGO','TCI','DELHIVERY','BLUEDART','ADANIPORTS','MAHLOG','TVSSCS','AEGISLOG','VRLLOG','SNOWMAN','GATEWAY'],
    "Aviation": ['INDIGO','SPICEJET','TAALENT','GMRAIRPORT','GMRP&UI','AIAENG','BLUEDART'],
    "Hotels": ['INDHOTEL','LEMONTREE','EIHOTEL','CHALET','TAJGVK'],
    "Insurance": ['HDFCLIFE','SBILIFE','ICICIPRULI','LICI','GICRE','NIACL','STARHEALTH','GODIGIT','BAJAJFINSV'],
    "Wealth & AMC": ['NAM-INDIA','360ONE','NUVAMA','MOTILALOFS','HDFCAMC','ICICIAMC','ABSLAMC','UTIAMC','CRAMC','ANGELONE','ANANDRATHI','GEOJITFSL','5PAISA','EMKAY'],
    "Semiconductor": ['CGPOWER','MOSCHIP','SAGILITY','RIR','TATAELXSI','ASMTECH','KAYNES','SYRMA','AVALON','CENTUM','PGEL','VEDL','SASKEN','INTELLECT','HCLTECH'],
}

# Build reverse: symbol → [themes]
SYMBOL_THEMES: dict[str, list[str]] = {}
for _theme, _syms in _THEME_SYMBOLS.items():
    for _sym in dict.fromkeys(_syms):
        SYMBOL_THEMES.setdefault(_sym, []).append(_theme)

# =========================================================
# CLASSIFICATION HELPERS
# =========================================================

def get_description_overrides(description: str):
    """Extract sector_group, display_industry, themes from description."""
    if not description:
        return None, None, []

    desc = description.lower()
    sector_group = None
    display_industry = None

    for keywords, sg, di in DESCRIPTION_SECTOR_MAP:
        if any(kw in desc for kw in keywords):
            sector_group = sg
            display_industry = di
            break

    themes = []
    for keywords, theme_list in DESCRIPTION_THEME_MAP:
        if any(kw in desc for kw in keywords):
            for t in theme_list:
                if t not in themes:
                    themes.append(t)

    return sector_group, display_industry, themes


def get_sector_group(profile: dict) -> str:
    for field in ["sub_industry", "industry", "sector"]:
        value = (profile.get(field) or "").strip()
        if value in SECTOR_GROUP_MAP:
            return SECTOR_GROUP_MAP[value]
    return profile.get("sector", "Other")


def get_display_industry(profile: dict) -> str:
    for field in ["sub_industry", "industry", "sector"]:
        value = (profile.get(field) or "").strip()
        if value in INDUSTRY_MAP:
            return INDUSTRY_MAP[value]
    return (profile.get("sub_industry") or profile.get("sector") or "Other")


def classify(symbol: str, profile: dict) -> tuple[str, str, list[str]]:
    """
    Returns (sector_group, display_industry, themes)
    Priority: description keywords > sub_industry map
    Themes: description keywords + manual symbol list (merged, deduplicated)
    """
    description = profile.get("description", "")

    # Get description-based overrides
    desc_sg, desc_di, desc_themes = get_description_overrides(description)

    # Sector group & display industry
    sector_group     = desc_sg or get_sector_group(profile)
    display_industry = desc_di or get_display_industry(profile)

    # Themes: merge manual list + description-derived (deduplicated)
    manual_themes = SYMBOL_THEMES.get(symbol, [])
    all_themes = list(dict.fromkeys(manual_themes + desc_themes))

    return sector_group, display_industry, all_themes

# =========================================================
# HEADERS
# =========================================================

HEADERS = {"User-Agent": "Mozilla/5.0"}

WORKER_HEADERS = {
    "X-Secret-Token": WORKER_TOKEN,
    "Content-Type": "application/json",
}

PROFILE_URL = "https://data.finedgeapi.com/api/v1/company-profile"

# =========================================================
# R2 HELPERS
# =========================================================

async def r2_download(client, filename):
    url = f"{WORKER_URL}/{filename}"
    r = await client.get(url, headers=WORKER_HEADERS, timeout=120)
    if r.status_code != 200:
        raise RuntimeError(f"{filename} download failed")
    return r.json()


async def r2_upload(client, filename, data):
    url = f"{WORKER_URL}?file={filename}"
    r = await client.post(
        url,
        headers=WORKER_HEADERS,
        content=json.dumps(data).encode(),
        timeout=120,
    )
    if r.status_code != 200:
        raise RuntimeError(f"{filename} upload failed")
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
                    params={"token": FINEDGE_TOKEN},
                    timeout=60,
                )
            except Exception as e:
                print(f"{symbol} Network Error: {e}")
                await asyncio.sleep(2 ** attempt)
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
                print(f"{symbol} -> HTTP {r.status_code}")
                return None
            try:
                return r.json()
            except Exception:
                return None
    return None

# =========================================================
# PROCESS STOCK
# =========================================================

async def process_stock(client, stock, semaphore, fundamentals: dict):
    symbol = stock["symbol"]

    profile = await fetch_profile(client, symbol, semaphore)

    if not profile:
        print(f"✗ {symbol} | profile fail")
        return None

    market_cap = float(profile.get("market_cap") or 0)
    if market_cap < MIN_MARKET_CAP_CR:
        print(f"✗ {symbol} | market cap < {MIN_MARKET_CAP_CR}cr")
        return None

    # Store profile in fundamentals dict (for fundamental.json)
    fundamentals[symbol] = {
        "symbol":      symbol,
        "name":        profile.get("name"),
        "description": profile.get("description", ""),
        "website":     profile.get("website", ""),
        "bse_code":    profile.get("bse_code"),
        "nse_code":    profile.get("nse_code"),
        "macro_sector":profile.get("macro_sector"),
        "sector":      profile.get("sector"),
        "industry":    profile.get("industry"),
        "sub_industry":profile.get("sub_industry"),
        "market_cap":  market_cap,
    }

    sector_group, display_industry, themes = classify(symbol, profile)

    print(f"✓ {symbol} | {market_cap:.0f}cr | {sector_group} | {display_industry}")

    return {
        "symbol":           symbol,
        "name":             stock.get("name"),
        "exchange":         stock.get("exchange"),
        "market_cap_cr":    market_cap,
        "macro_sector":     profile.get("macro_sector"),
        "sector_group":     sector_group,
        "display_industry": display_industry,
        "sector":           profile.get("sector"),
        "industry":         profile.get("industry"),
        "sub_industry":     profile.get("sub_industry"),
        "bse_code":         profile.get("bse_code"),
        "nse_code":         profile.get("nse_code"),
        "consolidated_ind": stock.get("consolidated_ind", False),
        "themes":           themes,
    }

# =========================================================
# MAIN
# =========================================================

async def main():
    semaphore   = asyncio.Semaphore(CONCURRENCY)
    fundamentals: dict = {}

    async with httpx.AsyncClient(headers=HEADERS) as client:

        print("\nDownloading master.json...")
        master = await r2_download(client, "master.json")
        print(f"Loaded {len(master)} stocks")

        results = []
        total   = len(master)

        for i in range(0, total, BATCH_SIZE):
            batch = master[i:i + BATCH_SIZE]
            tasks = [
                process_stock(client, stock, semaphore, fundamentals)
                for stock in batch
            ]
            batch_results = await asyncio.gather(*tasks)
            results.extend(batch_results)
            print(f"\nProcessed {min(i + BATCH_SIZE, total)}/{total}")
            await asyncio.sleep(2)

        classification = [x for x in results if x]
        classification.sort(key=lambda x: x["market_cap_cr"], reverse=True)

        print("\n=== SUMMARY ===")
        print(f"✓ Final Stocks : {len(classification)}")
        print(f"✗ Removed      : {len(master) - len(classification)}")

        # Upload both files
        await r2_upload(client, OUTPUT_FILE, classification)
        await r2_upload(client, FUNDAMENTAL_FILE, list(fundamentals.values()))

        print("\n🎉 Done! classification.json + fundamental.json uploaded")


if __name__ == "__main__":
    asyncio.run(main())
