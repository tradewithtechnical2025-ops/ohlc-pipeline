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

    # ── Banks (standalone) ──
    "Private Sector Bank":                      "Banks",
    "Public Sector Bank":                       "Banks",
    "Other Bank":                               "Banks",

    # ── Financial Services ──
    # (NBFC + Insurance + Capital Markets + Fintech merged)
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
    "Depositories, Clearing Houses and Other Intermediaries":  "Financial Services",
    "Depositories Clearing Houses and Other Intermediaries":   "Financial Services",
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

    # ── Healthcare (Hospitals merged) ──
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

    # ── IT Services (standalone) ──
    "Computers - Software & Consulting":        "IT Services",
    "IT Enabled Services":                      "IT Services",

    # ── Software (standalone) ──
    "Software Products":                        "Software",

    # ── IT Hardware (standalone) ──
    "Computers Hardware & Equipments":          "IT Hardware",

    # ── Power (Utilities merged) ──
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

    # ── FMCG (Tobacco merged) ──
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

    # ── Industrials (Railways merged) ──
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

    # ── Consumer Durables (Jewellery + Paints + Consumer Products merged) ──
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

    # ── Entertainment (Media & Publishing merged) ──
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
# sub_industry → display_industry (clean UI label)
# =========================================================

INDUSTRY_MAP = {

    # Agriculture
    "Other Agricultural Products":          "Agri Products",
    "Sugar":                                "Sugar",
    "Pesticides & Agrochemicals":           "Agrochemicals",
    "Fertilizers":                          "Fertilizers",
    "Animal Feed":                          "Agri Products",
    "Seafood":                              "Agri Products",
    "Meat Products including Poultry":      "Agri Products",

    # Automobiles
    "Auto Components & Equipments":         "Auto Components",
    "Tyres & Rubber Products":              "Tyres & Rubber",
    "2/3 Wheelers":                         "2/3 Wheelers",
    "Passenger Cars & Utility Vehicles":    "Passenger Vehicles",
    "Construction Vehicles":                "Commercial Vehicles",
    "Commercial Vehicles":                  "Commercial Vehicles",
    "Tractors":                             "Commercial Vehicles",
    "Auto -Dealer":                         "Auto Dealers",
    "Trading - Auto Components":            "Auto Components",
    "Dealers-Commercial Vehicles, Tractors, Construction Vehicles": "Commercial Vehicles",

    # Banks
    "Private Sector Bank":                  "Private Banks",
    "Public Sector Bank":                   "PSU Banks",
    "Other Bank":                           "Other Banks",

    # Financial Services
    "Stockbroking & Allied":                "Stockbroking",
    "Asset Management Company":             "Asset Management",
    "Exchange and Data Platform":           "Exchanges",
    "Financial Products Distributor":       "Wealth Mgmt",
    "Depositories, Clearing Houses and Other Intermediaries": "Depositories",
    "Depositories Clearing Houses and Other Intermediaries":  "Depositories",
    "Other Capital Market related Services":"Other Cap Markets",
    "Ratings":                              "Ratings",
    "Financial Technology (Fintech)":       "Fintech",
    "Non Banking Financial Company (NBFC)": "NBFC",
    "Other Financial Services":             "Other Financials",
    "Investment Company":                   "Investment Cos",
    "Holding Company":                      "Holding Cos",
    "Housing Finance Company":              "Housing Finance",
    "Financial Institution":                "Other Financials",
    "Microfinance Institutions":            "Microfinance",
    "Life Insurance":                       "Life Insurance",
    "General Insurance":                    "General Insurance",
    "Insurance Distributors":               "Insurance Broking",

    # Business Services
    "Diversified Commercial Services":      "Commercial Services",
    "Trading & Distributors":               "Trading & Distribution",
    "Consulting Services":                  "Consulting",
    "Business Process Outsourcing (BPO)/ Knowledge Process Outsourcing (KPO)": "BPO/KPO",
    "Other Consumer Services":              "Other Services",
    "Data Processing Services":             "Other Services",

    # Cement
    "Cement & Cement Products":             "Cement",

    # Chemicals
    "Specialty Chemicals":                  "Specialty Chemicals",
    "Commodity Chemicals":                  "Commodity Chemicals",
    "Dyes And Pigments":                    "Dyes & Pigments",
    "Petrochemicals":                       "Petrochemicals",
    "Trading - Chemicals":                  "Specialty Chemicals",
    "Explosives":                           "Specialty Chemicals",
    "Industrial Gases":                     "Industrial Gases",
    "Carbon Black":                         "Specialty Chemicals",
    "Printing Inks":                        "Specialty Chemicals",

    # Consumer Durables
    "Household Appliances":                 "Appliances",
    "Consumer Electronics":                 "Electronics",
    "Plywood Boards/ Laminates":            "Building Materials",
    "Footwear":                             "Footwear",
    "Ceramics":                             "Building Materials",
    "Plastic Products - Consumer":          "Plastic Products",
    "Furniture Home Furnishing":            "Furniture",
    "Furniture, Home Furnishing":           "Furniture",
    "Granites & Marbles":                   "Building Materials",
    "Leather And Leather Products":         "Leather",
    "Houseware":                            "Appliances",
    "Leisure Products":                     "Other Durables",
    "Sanitary Ware":                        "Building Materials",
    "Diversified consumer products":        "Other Durables",
    "Glass - Consumer":                     "Other Durables",
    "Cycles":                               "Other Durables",
    "Paints":                               "Paints",
    "Stationary":                           "Stationery",
    "Gems, Jewellery And Watches":          "Jewellery",
    "Gems Jewellery And Watches":           "Jewellery",

    # Defence
    "Aerospace & Defense":                  "Aerospace & Defense",
    "Ship Building & Allied Services":      "Shipbuilding",

    # Education
    "Education":                            "Education",
    "E-Learning":                           "E-Learning",

    # Electrical Equipment
    "Heavy Electrical Equipment":           "Heavy Electricals",
    "Other Electrical Equipment":           "Electrical Equipment",

    # Energy
    "Refineries & Marketing":               "Refineries",
    "Lubricants":                           "Lubricants",
    "LPG/CNG/PNG/LNG Supplier":             "Gas Distribution",
    "Oil Exploration & Production":         "Oil E&P",
    "Oil Equipment & Services":             "Oil Services",
    "Offshore Support Solution Drilling":   "Oil Services",
    "Coal":                                 "Coal",
    "Oil Storage & Transportation":         "Oil Services",
    "Gas Transmission/Marketing":           "Gas Distribution",
    "Trading - Gas":                        "Gas Distribution",
    "Trading Coal":                         "Coal",

    # Entertainment
    "Media & Entertainment":                "Media & Entertainment",
    "Film Production, Distribution & Exhibition": "Film & OTT",
    "Film Production Distribution & Exhibition":  "Film & OTT",
    "TV Broadcasting & Software Production":"Broadcasting",
    "Advertising & Media Agencies":         "Advertising",
    "Print Media":                          "Print Media",
    "Printing & Publication":               "Print Media",
    "Web based media and service":          "Digital Media",
    "Digital Entertainment":               "Film & OTT",
    "Electronic Media":                     "Broadcasting",

    # Environmental Services
    "Waste Management":                     "Waste Management",
    "Water Supply & Management":            "Water Management",
    "Water Treatment":                      "Water Treatment",

    # FMCG
    "Other Food Products":                  "Food Products",
    "Packaged Foods":                       "Packaged Foods",
    "Breweries & Distilleries":             "Beverages",
    "Edible Oil":                           "Edible Oil",
    "Tea & Coffee":                         "Tea & Coffee",
    "Personal Care":                        "Personal Care",
    "Dairy Products":                       "Dairy",
    "Household Products":                   "Household Products",
    "Diversified FMCG":                     "Diversified FMCG",
    "Other Beverages":                      "Beverages",
    "Cigarettes & Tobacco Products":        "Tobacco",

    # Healthcare
    "Medical Equipment & Supplies":         "Med Devices",
    "Healthcare Service Provider":          "Healthcare Services",
    "Healthcare Research, Analytics & Technology": "Health Tech",
    "Biotechnology":                        "Biotech",
    "Hospital":                             "Hospitals",

    # IT
    "Computers Hardware & Equipments":      "IT Hardware",
    "Computers - Software & Consulting":    "IT Consulting",
    "IT Enabled Services":                  "IT Enabled Services",
    "Software Products":                    "Software Products",

    # Industrials
    "Industrial Products":                  "Industrial Products",
    "Packaging":                            "Packaging",
    "Plastic Products - Industrial":        "Plastic Products",
    "Other Industrial Products":            "Industrial Products",
    "Castings & Forgings":                  "Castings & Forgings",
    "Electrodes & Refractories":            "Electrodes & Refractories",
    "Rubber":                               "Rubber",
    "Compressors, Pumps & Diesel Engines":  "Machinery",
    "Compressors Pumps & Diesel Engines":   "Machinery",
    "Abrasives & Bearings":                 "Machinery",
    "Aluminium Copper & Zinc Products":     "Other Industrials",
    "Glass - Industrial":                   "Other Industrials",
    "Railway Wagons":                       "Railways",

    # Infrastructure
    "Civil Construction":                   "Civil Construction",
    "Other Construction Materials":         "Construction Materials",
    "Road Assets–Toll, Annuity, Hybrid-Annuity": "Road Assets",

    # Logistics
    "Logistics Solution Provider":          "3PL Logistics",
    "Shipping":                             "Shipping",
    "Port & Port services":                 "Ports",
    "Road Transport":                       "Road Transport",
    "Airport & Airport services":           "Airports",
    "Transport Related Services":           "Transport Services",
    "Dredging":                             "Shipping",

    # Metals & Mining
    "Iron & Steel Products":                "Steel",
    "Iron & Steel":                         "Steel",
    "Industrial Minerals":                  "Minerals & Mining",
    "Aluminium, Copper & Zinc Products":    "Non-Ferrous Metals",
    "Aluminium":                            "Aluminium",
    "Ferro & Silica Manganese":             "Steel",
    "Trading - Metals":                     "Metals Trading",
    "Sponge Iron":                          "Steel",
    "Diversified Metals":                   "Non-Ferrous Metals",
    "Copper":                               "Non-Ferrous Metals",
    "Trading - Minerals":                   "Minerals & Mining",
    "Zinc":                                 "Non-Ferrous Metals",
    "Pig Iron":                             "Steel",
    "Precious Metals":                      "Non-Ferrous Metals",

    # Paper & Packaging
    "Paper & Paper Products":               "Paper",
    "Jute & Jute Products":                 "Paper",
    "Forest Products":                      "Paper",

    # Pharma
    "Pharmaceuticals":                      "Pharmaceuticals",

    # Power
    "Power Generation":                     "Power Generation",
    "Integrated Power Utilities":           "Integrated Power",
    "Power Distribution":                   "Power Distribution",
    "Power - Transmission":                 "Power Transmission",
    "Power Trading":                        "Power Generation",
    "Multi Utilities":                      "Utilities",
    "Other Utilities":                      "Utilities",

    # Real Estate
    "Residential, Commercial Projects":     "Real Estate",
    "Residential Commercial Projects":      "Real Estate",
    "Real Estate related services":         "Real Estate Services",

    # Retail
    "Speciality Retail":                    "Specialty Retail",
    "Diversified Retail":                   "Diversified Retail",
    "E-Retail/ E-Commerce":                 "E-Commerce",
    "Internet & Catalogue Retail":          "E-Commerce",
    "Distributors":                         "Distribution",
    "Pharmacy Retail":                      "Pharmacy Retail",

    # Telecom
    "Telecom -  Equipment & Accessories":   "Telecom Equipment",
    "Telecom - Equipment & Accessories":    "Telecom Equipment",
    "Telecom - Infrastructure":             "Telecom Infra",
    "Telecom - Cellular & Fixed line services": "Telecom Services",
    "Other Telecom Services":               "Telecom Services",

    # Textiles
    "Other Textile Products":               "Textiles",
    "Garments & Apparels":                  "Garments",
    "Trading - Textile Products":           "Textiles",

    # Travel & Hospitality
    "Hotels & Resorts":                     "Hotels",
    "Restaurants":                          "Restaurants",
    "Tour, Travel Related Services":        "Travel Services",
    "Airline":                              "Airlines",
    "Amusement Parks/ Other Recreation":    "Recreation",
    "Wellness":                             "Travel Services",
    "Tour Travel Related Services":         "Travel Services",

    # Wires & Cables
    "Cables - Electricals":                 "Wires & Cables",

    # Diversified
    "Diversified":                          "Diversified",
}

# =========================================================
# THEMES
# symbol → [theme1, theme2, ...]  (cross-sector)
# =========================================================

_THEME_SYMBOLS = {
    "Telecom & 5G": [
        "BHARTIARTL","IDEA","TTML","MTNL","TATACOMM","RAILTEL","HFCL","STLTECH",
        "TEJAS","ROUTE","VINDHYATEL","GTLINFRA","GTPL","ONMOBILE","TANLA",
        "NELCO","ITI","MAHANAGAR",
    ],
    "Data Center & AI Infra": [
        "NETWEB","SIFY","STLTECH","HCLTECH","INFY","TCS","WIPRO","MPHASIS",
        "COFORGE","MASTEK","PERSISTENT","LTTS","TATAELXSI","KELLTON","ZENSAR",
        "KPITTECH","CMSINFO","RATEGAIN",
    ],
    "Cables & Wires": [
        "POLYCAB","KEI","RRKABEL","FINCABLES","UNIVCABLES","VINDHYATEL",
        "HAVELLS","FINOLEX","STLTECH","HFCL","INDUCTOTILE","DIAMONDYD",
    ],
    "Wealth & AMC": [
        "ICICIAMC","HDFCAMC","NAM-INDIA","ABSLAMC","UTIAMC","CRAMC",
        "GROWW","MOTILALOFS","360ONE","ANGELONE","ISEC","ICICISEC",
        "NUVAMA","ANANDRATHI","GEOJITFSL","5PAISA","EMKAY",
    ],
    "Pharma": [
        "SUNPHARMA","DRREDDY","CIPLA","LUPIN","AUROPHARMA","ALKEM","ZYDUSLIFE",
        "TORNTPHARM","IPCALAB","GLENMARK","NATCO","BIOCON","GRANULES","LAURUSLABS",
        "AJANTPHARM","PFIZER","ABBOTINDIA","SANOFI","GLAXO","DIVISLAB",
        "SUVEN","SOLARA","ERIS","JBCHEPHARM","CAPLIPOINT",
    ],
    "Defence": [
        "BEL","HAL","BDL","MAZDOCK","COCHINSHIP","GRSE","MTARTECH","DATAPATTNS",
        "ZENTEC","AEQUS","ASTRAMICRO","APOLLO","SWANDEF","AXISCADES","MIDHANI",
        "PARAS","UNIMECH","AVANTEL","ROSSTECH","IDEAFORGE","JAYKAY","DCXINDIA",
        "NIBE","SIKA","KRISHNADEF","APSISAERO","TECHERA",
    ],
    "Power": [
        "NTPC","ADANIPOWER","TATAPOWER","CESC","TORNTPOWER","NHPC","SJVN",
        "NEYVELI","JSPL","POWERGRID","RECLTD","PFC","IREDA","NLCINDIA",
        "RPOWER","JPPOWER","JSWENERGY","KALPATPOWR",
    ],
    "Capital Markets": [
        "BSE","MCX","CDSL","CAMS","KFINTECH","ANANDRATHI","ICICISEC",
        "GROWW","MOTILALOFS","360ONE","ANGELONE","ISEC","NUVAMA",
        "5PAISA","EMKAY","GEOJITFSL","MOFSL",
    ],
    "Renewable Energy": [
        "ADANIGREEN","ACMESOLAR","WEBSOL","INSOLATION","WAAREE","SUZLON",
        "INOXWIND","SJVN","NTPC","TATAPOWER","STERLINWIL","GENSOL",
        "ORIENTGREEN","PREMIER","UJAAS","WINDWORLD","KPIL","TRIL",
    ],
    "EV": [
        "TATAMOTORS","MAHINDRA","BAJAJ-AUTO","TVSMOTOR","HMCL","OLECTRA",
        "GOLDSTONE","SONA","MOTHERSON","EXIDEIND","AMARAJABAT","MINDA",
        "LUMAXIND","CRAFTSMAN",
    ],
    "PSU Banks": [
        "SBIN","PNB","BANKBARODA","BANKINDIA","CANARABANK","UNIONBANK",
        "INDIANB","CENTRALBK","MAHABANK","UCOBANK","IOB","J&KBANK",
        "PSBBANK","BANDHANBNK",
    ],
    "Semiconductor": [
        "KAYNES","SYRMA","AVALON","CENTUM","RUTTONSHA","MOSCHIP","PGEL",
        "CGPOWER","IDEAFORGE","VEDL","TATAELXSI","SASKEN","INTELLECT",
        "SAKSOFT","HCLTECH",
    ],
    "Specialty Chemicals": [
        "NAVINFLUOR","DEEPAKNITRITE","CLEANSCIENCE","AARTIIND","VINATIORGA",
        "FINEORG","GALAXYSURF","SUDARSCHEM","ROSSARI","ALKYLAMINE","TATACHEM",
        "NOCIL","ATUL","PIDILITIND","NEOGEN","GUJALKALI","FLUOROCHEM",
        "LAXCHEM","ANUPAM","OMNICHM",
    ],
    "Water Treatment": [
        "WABAG","THERMAX","IONEXCHANG","ENVIROTECH","EWL","DEWA",
        "WATERBASE","VATECH","PERMIONICS","ROCHEM","PRAJ","TRIVENI",
    ],
    "FMCG": [
        "HINDUNILVR","ITC","NESTLEIND","MARICO","DABUR","COLPAL","EMAMILTD",
        "TATACONSUM","BRITANNIA","GODREJCP","VBL","UBL","RADICO",
        "MCDOWELL-N","JYOTHYLAB","BAJAJCON","ZYDUSWELL",
    ],
    "Electronics Manufacturing": [
        "DIXON","AMBER","PGEL","KAYNES","SYRMA","AVALON","CENTUM","ELINCOIN",
        "VSSL","SANSERA","MOTHERSON","CGPOWER","TATAELXSI","ASTRA","APARINDS",
    ],
    "Aviation": [
        "INDIGO","SPICEJET","GMRAIRPORT","GMRP&UI","AIAENG",
        "BLUEDART","INTERGLOBE","TAALTECH",
    ],
    "Logistics": [
        "ADANIPORTS","CONCOR","MAHLOG","BLUEDART","DELHIVERY","ALLCARGO",
        "GATI","TVSSCS","AEGISLOG","VRLLOG","TCIEXPRESS","SNOWMAN",
        "GATEWAY","CONTAINERC",
    ],
    "Real Estate": [
        "DLF","GODREJPROP","PRESTIGE","OBEROIRLTY","BRIGADE","PHOENIXLTD",
        "SOBHA","MACROTECH","MAHLIFE","KOLTEPATIL","SUNTECK","ANANTRAJ",
        "ARVIND","ASHIANA","NESCO","ELDECO",
    ],
    "Railways": [
        "RVNL","IRFC","IRCON","RITES","RAILTEL","IRCTC","TITAGARH",
        "KERNEX","TEXRAIL","JWL","NRAIL","TRANSRAILL","E2ERAIL","HFCL","BEML",
    ],
    "Consumption": [
        "TITAN","TRENT","DMART","VSTIND","ZOMATO","NYKAA","SAPPHIREF",
        "WESTLIFE","DEVYANI","JUBLFOOD","MANYAVAR","VEDANT","SENCO",
        "METRO","CAMPUS","BATA","RELAXO","VIP","SAFARI",
    ],
    "Infrastructure": [
        "LT","NCC","HCC","PNCINFRA","KNRCON","ASHOKA","IRB","DILIPBUILD",
        "JSWINFRA","AHLUWALIA","PSP","CAPACITE","GRINFRA","HGINFRA","POLYCAB",
    ],
    "Hotels": [
        "INDHOTEL","TAJGVK","LEMONTREE","CHALET","EIHOTEL","ORIENTHOTEL",
        "KAMAT","ROYALORCH","SINCLAIRS","BARBEQUE",
    ],
    "Insurance": [
        "SBILIFE","HDFCLIFE","ICICIPRULI","LICI","NIACL","STARHEALTH",
        "GODIGIT","POLICYBAZ","GICRE","MAXFINSERV","BAJAJFINSV","CHOLAMANDALAM",
    ],
}

# Build reverse: symbol → [themes]
SYMBOL_THEMES: dict[str, list[str]] = {}
for _theme, _syms in _THEME_SYMBOLS.items():
    for _sym in dict.fromkeys(_syms):
        SYMBOL_THEMES.setdefault(_sym, []).append(_theme)

# =========================================================
# HELPERS
# =========================================================

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

async def process_stock(client, stock, semaphore):
    symbol = stock["symbol"]

    profile = await fetch_profile(client, symbol, semaphore)

    if not profile:
        print(f"✗ {symbol} | profile fail")
        return None

    market_cap = float(profile.get("market_cap") or 0)

    if market_cap < MIN_MARKET_CAP_CR:
        print(f"✗ {symbol} | market cap < {MIN_MARKET_CAP_CR}cr")
        return None

    sector_group     = get_sector_group(profile)
    display_industry = get_display_industry(profile)
    themes           = SYMBOL_THEMES.get(symbol, [])

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
    semaphore = asyncio.Semaphore(CONCURRENCY)

    async with httpx.AsyncClient(headers=HEADERS) as client:

        print()
        print("Downloading master.json...")

        master = await r2_download(client, "master.json")

        print(f"Loaded {len(master)} stocks")

        results = []
        total = len(master)

        for i in range(0, total, BATCH_SIZE):
            batch = master[i:i + BATCH_SIZE]
            tasks = [
                process_stock(client, stock, semaphore)
                for stock in batch
            ]
            batch_results = await asyncio.gather(*tasks)
            results.extend(batch_results)

            print()
            print(f"Processed {min(i + BATCH_SIZE, total)}/{total}")

            await asyncio.sleep(2)

        classification = [x for x in results if x]
        classification.sort(key=lambda x: x["market_cap_cr"], reverse=True)

        print()
        print("=== SUMMARY ===")
        print(f"✓ Final Stocks : {len(classification)}")
        print(f"✗ Removed      : {len(master) - len(classification)}")

        await r2_upload(client, OUTPUT_FILE, classification)

        print()
        print("🎉 classification.json uploaded")


if __name__ == "__main__":
    asyncio.run(main())
