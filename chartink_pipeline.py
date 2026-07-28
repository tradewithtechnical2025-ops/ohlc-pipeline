import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime
import time
import pytz
import sys
import traceback

# === Google Sheets Setup ===
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name("./credentials.json", scope)
client = gspread.authorize(creds)

# Sheet references (ensure these exist)
dashboard_sheet = client.open("Chartink Scanner Results").worksheet("PRICEPATTERN")
logger_sheet = client.open("Chartink Scanner Results").worksheet("LOGGER")
marketdata_sheet = client.open("Chartink Scanner Results").worksheet("MarketData")
ep_sheet = client.open("Chartink Scanner Results").worksheet("EP")

try:
    weeklydata_sheet = client.open("Chartink Scanner Results").worksheet("WeeklyData")
except Exception:
    weeklydata_sheet = client.open("Chartink Scanner Results").add_worksheet(title="WeeklyData", rows="1000", cols="20")

# Timezone helper
ist = pytz.timezone('Asia/Kolkata')
def now_ts():
    return datetime.now(ist).strftime('%Y-%m-%d %H:%M:%S')

start_time = time.time()

# ----------------------
# === Scanner definitions
# ----------------------
# Realtime scanners (unchanged)
scanners = [
    {"name": "MCP", "condition": "( {cash} ( ( {cash} ( latest high <= 3 days ago high and 1 day ago high <= 3 days ago high and 2 days ago high <= 3 days ago high and latest low >= 3 days ago low and 1 day ago low >= 3 days ago low and 2 days ago low >= 3 days ago low and latest close > 20 and( {166311} not( latest close > 20 ) ) and( {45603} not( latest close > 20 ) ) ) ) or( {cash} ( latest high <= 4 days ago high and 1 day ago high <= 4 days ago high and 2 days ago high <= 4 days ago high and 3 days ago high <= 4 days ago high and latest low >= 4 days ago low and 1 day ago low >= 4 days ago low and 2 days ago low >= 4 days ago low and 3 days ago low >= 4 days ago low and latest close > 20 and( {166311} not( latest close > 20 ) ) and( {45603} not( latest close > 20 ) ) ) ) or( {cash} ( latest high <= 5 days ago high and 1 day ago high <= 5 days ago high and 2 days ago high <= 5 days ago high and 3 days ago high <= 5 days ago high and 4 days ago high <= 5 days ago high and latest low >= 5 days ago low and 1 day ago low >= 5 days ago low and 2 days ago low >= 5 days ago low and 3 days ago low >= 5 days ago low and 4 days ago low >= 5 days ago low and latest close > 20 and( {166311} not( latest close > 20 ) ) and( {45603} not( latest close > 20 ) ) ) ) or( {cash} ( latest high <= 6 days ago high and 1 day ago high <= 6 days ago high and 2 days ago high <= 6 days ago high and 3 days ago high <= 6 days ago high and 4 days ago high <= 6 days ago high and 5 days ago high <= 6 days ago high and latest low >= 6 days ago low and 1 day ago low >= 6 days ago low and 3 days ago low >= 6 days ago low and 2 days ago low >= 6 days ago low and 4 days ago low >= 6 days ago low and 5 days ago low >= 6 days ago low and latest close > 20 and( {166311} not( latest close > 20 ) ) and( {45603} not( latest close > 20 ) ) ) ) ) ) "},
    {"name": "Above21", "condition": "( {cash} ( latest close > latest ema( latest close , 21 ) ) )"},
    {"name": "Above50", "condition": "( {cash} ( latest close > latest ema( latest close , 50 ) ) )"},
    {"name": "Stocks", "condition": "( {cash} ( latest close * latest sma( latest volume , 50 ) >= 45000000 and latest close > 10 and latest volume > 30000 and( {166311} not( latest close > 0 ) ) and( {45603} not( latest close > 0 ) ) ) ) "},
    {"name": "IB", "condition": "( {cash} ( latest high <= 1 day ago high and latest low >= 1 day ago low and latest volume > 25000 and latest close > 20 and( {166311} not( latest close > 0 ) ) ) ) "},
    {"name": "MACD Hook", "condition": "( {cash} ( latest macd line( 26,12,9 ) > 0 and latest macd signal( 26,12,9 ) > 0 and latest ema( latest macd signal( 26,12,9 ) , 8 ) > latest ema( latest macd signal( 26,12,9 ) , 13 ) and( latest macd line( 26,12,9 ) - latest macd signal( 26,12,9 ) ) / latest macd signal( 26,12,9 ) < 0.10 and( latest macd line( 26,12,9 ) - latest macd signal( 26,12,9 ) ) / latest macd signal( 26,12,9 ) > 0 and latest close > latest ema( latest close , 20 ) and latest ema( latest close , 20 ) > latest ema( latest close , 50 ) and latest close > 20 and latest sma( latest volume , 50 ) > 5000 and latest countstreak( 10, 1 where latest macd line( 26,12,9 ) >= latest macd signal( 26,12,9 ) ) = 10 and latest countstreak( 20, 1 where latest macd signal( 26,12,9 ) >= 0 ) > 12 and market cap > 100 and latest ema( latest close , 20 ) * latest sma( latest volume , 20 ) >= 40000000 ) )"},
    {"name": "GAPUP", "condition": "( {cash} ( ( {cash} ( latest close > 20 and latest open > 1 day ago close * 1.035 and latest volume > 25000 and latest low > 1 day ago high * 1.02 and( {166311} not( latest close > 0 ) ) and( {167068} not( latest close > 0 ) ) and( {45603} not( latest close > 0 ) ) ) ) ) )"},
    {"name": "50>200", "condition": "( {cash} ( latest ema( latest close , 50 ) > latest ema( latest close , 200 ) ) )"},
    {"name": "21>50", "condition": "( {cash} ( latest ema( latest close , 21 ) > latest ema( latest close , 50 ) ) )"},
    {"name": "52WH", "condition": "( {cash} ( latest high >= weekly max( 52 , weekly high ) ) ) "},
    {"name": "NR7", "condition": "( {cash} ( latest high - latest low < 1 day ago high - 1 day ago low and latest high - latest low < 2 days ago high - 2 days ago low and latest high - latest low < 3 days ago high - 3 days ago low and latest high - latest low < 4 days ago high - 4 days ago low and latest high - latest low < 5 days ago high - 5 days ago low and latest high - latest low < 6 days ago high - 6 days ago low and latest close > 20 and( {166311} not( latest close > 0 ) ) and latest volume > 25000 ) ) "},
    {"name": "HVQ", "condition": "( {cash} ( latest volume > 1 day ago max( 63 , latest volume ) and( {166311} not( latest close > 0 ) ) and( {45603} not( latest close > 0 ) ) ) ) "},
    {"name": "HVY", "condition": "( {cash} ( latest volume > 1 day ago max( 252 , latest volume ) and( {166311} not( latest close > 0 ) ) and( {45603} not( latest close > 0 ) ) ) ) "},
    {"name": "BS", "condition": "( {cash} ( latest close > 1 day ago close and latest volume > latest sma( volume,50 ) * 3 and latest close >= ( ( latest high - latest low ) * 0.65 + latest low ) ) ) "},
    {"name": "VD", "condition": "( {cash} ( ( latest sma( latest volume , 50 ) - latest volume ) / latest volume * 100 >= 50 and latest volume > 20 and( {45603} not( latest close > 0 ) ) and( {166311} not( latest close > 0 ) ) and market cap >= 200 and latest close >= 10 ) ) "},
    {"name": "IV", "condition": "( {cash} ( latest volume > 1 day ago max( 10 , latest volume ) * 2 and latest close > 1 day ago close and( latest close - latest low ) / ( latest high - latest low ) * 100 > 50 and( {166311} not( latest close > 0 ) ) and( {45603} not( latest close > 0 ) ) and latest volume > 25000 ) )" },
    {"name": "PP", "condition": "( {cash} ( ( {cash} ( latest volume > latest max( 10 , latest volume * latest count( 1, 1 where latest close < latest open ) ) or( {cash} ( ( {cash} ( ( {cash} ( 1 day ago close > 2 days ago close ) ) or( {cash} ( 1 day ago close < 2 days ago close and 1 day ago volume < latest volume ) ) ) ) and( {cash} ( ( {cash} ( 2 days ago close > 3 days ago close ) ) or( {cash} ( 2 days ago close < 3 days ago close and 2 days ago volume < latest volume ) ) ) ) and( {cash} ( ( {cash} ( 3 days ago close > 4 days ago close ) ) or( {cash} ( 3 days ago close < 4 days ago close and 3 days ago volume < latest volume ) ) ) ) and( {cash} ( ( {cash} ( 4 days ago close > 5 days ago close ) ) or( {cash} ( 4 days ago close < 5 days ago close and 4 days ago volume < latest volume ) ) ) ) and( {cash} ( ( {cash} ( 5 days ago close > 6 days ago close ) ) or( {cash} ( 5 days ago close < 6 days ago close and 5 days ago volume < latest volume ) ) ) ) and( {cash} ( ( {cash} ( 6 days ago close > 7 days ago close ) ) or( {cash} ( 6 days ago close < 7 days ago close and 6 days ago volume < latest volume ) ) ) ) and( {cash} ( ( {cash} ( 7 days ago close > 8 days ago close ) ) or( {cash} ( 7 days ago close < 8 days ago close and 7 days ago volume < latest volume ) ) ) ) and( {cash} ( ( {cash} ( 8 days ago close > 9 days ago close ) ) or( {cash} ( 8 days ago close < 9 days ago close and 8 days ago volume < latest volume ) ) ) ) and( {cash} ( ( {cash} ( 9 days ago close > 10 days ago close ) ) or( {cash} ( 9 days ago close < 10 days ago close and 9 days ago volume < latest volume ) ) ) ) and( {cash} ( ( {cash} ( 10 days ago close > 11 days ago close ) ) or( {cash} ( 10 days ago close < 11 days ago close and 10 days ago volume < latest volume ) ) ) ) ) ) ) ) and latest close >= 1 day ago close and latest close >= 10 and market cap >= 100 and latest volume > 30000 ) )"},
    {"name": "MBO", "condition": "( {cash} ( monthly close > monthly upper bollinger band( 20 , 2 ) and 1 month ago  close <= 1 month ago  upper bollinger band( 20 , 2 ) and 1 month ago close < 1 month ago upper bollinger band( 20 , 2 ) and 2 months ago close < 2 months ago upper bollinger band( 20 , 2 ) and 3 months ago close < 3 months ago upper bollinger band( 20 , 2 ) and 4 months ago close < 4 months ago upper bollinger band( 20 , 2 ) and( {166311} not( monthly close > 0 ) ) and monthly adx di positive( 14 ) > monthly adx di negative( 14 ) and monthly adx( 14 ) > 15 and monthly rsi( 14 ) < 75 and monthly rsi( 14 ) > 55 and monthly volume > 25000 and market cap > 100 and monthly adx di negative( 14 ) < 1 month ago adx di negative( 14 ) and monthly close > 20 ) ) "},
    {"name": "IPO", "condition": "( {cash} ( latest volume > 25000 and( {cash} not( 63 days ago close > 0 ) ) and( {166311} not( latest close > 0 ) ) ) ) "},
    {"name": "IB(D)", "condition": "( {cash} ( latest high <= 1 day ago high and 1 day ago high <= 2 days ago high and latest low >= 1 day ago low and 1 day ago low >= 2 days ago low and latest volume > 25000 ) )"},
    {"name": "LQV", "condition": "( {cash} ( latest volume < 1 day ago min( 63 , latest volume ) ) )"},
    {"name": "LYV", "condition": "( {cash} ( latest volume < 1 day ago min( 252 , latest volume ) ) )"},
    {"name": "LMV", "condition": "( {cash} ( latest volume < 1 day ago min( 21 , latest volume ) ) )"},
    {"name": "ATRTightness", "condition": "( {cash} ( ( latest max( 3 , latest high ) - latest min( 3 , latest low ) ) <= latest avg true range( 14 ) and latest volume > 25000 and latest close > latest ema( latest close , 50 ) ) )"},
    {"name": "IPO-IB", "condition": "( {cash} ( ( {cash} ( ( {cash} not( 63 days ago close > 0 ) ) and( {166311} not( latest close > 0 ) ) and( {45603} not( latest close > 0 ) ) and latest volume > 30000 and latest high <= 1 day ago high and latest low >= 1 day ago low ) ) ) )"},
    {"name": "LAUNCHPAD", "condition": "( {cash} ( ( {cash} ( ( {cash} (  daily high <=  3 days ago high and  1 day ago high <=  3 days ago high and  2 days ago high <=  3 days ago high and  daily low >=  3 days ago low and  1 day ago low >=  3 days ago low and  2 days ago low >=  3 days ago low and(  3 days ago high -  3 days ago low ) /  3 days ago high *  100 <  10 and  daily close >  20 and( {166311} not(  daily close >  20 ) ) and( {45603} not(  daily close >  20 ) ) and  daily close >  daily ema(  daily close , 50 ) and  daily close *  daily sma(  daily volume , 50 ) >=  50000000 and  daily volume >  25000 and  daily ema(  daily close , 50 ) <=  3 days ago high and  daily ema(  daily close , 50 ) >=  3 days ago low ) ) or( {cash} (  daily high <=  4 days ago high and  1 day ago high <=  4 days ago high and  2 days ago high <=  4 days ago high and  3 days ago high <=  4 days ago high and  daily low >=  4 days ago low and  1 day ago low >=  4 days ago low and  2 days ago low >=  4 days ago low and  3 days ago low >=  4 days ago low and(  4 days ago high -  4 days ago low ) /  4 days ago high *  100 <  10 and  daily close >  20 and( {166311} not(  daily close >  20 ) ) and( {45603} not(  daily close >  20 ) ) and  daily close >  daily ema(  daily close , 50 ) and  daily close *  daily sma(  daily volume , 50 ) >=  50000000 and  daily volume >  25000 and  daily ema(  daily close , 50 ) <=  4 days ago high and  daily ema(  daily close , 50 ) >=  4 days ago low ) ) or( {cash} (  daily high <=  5 days ago high and  1 day ago high <=  5 days ago high and  2 days ago high <=  5 days ago high and  3 days ago high <=  5 days ago high and  4 days ago high <=  5 days ago high and  daily low >=  5 days ago low and  1 day ago low >=  5 days ago low and  2 days ago low >=  5 days ago low and  3 days ago low >=  5 days ago low and  4 days ago low >=  5 days ago low and(  5 days ago high -  5 days ago low ) /  5 days ago high *  100 <  10 and  daily close >  20 and( {166311} not(  daily close >  20 ) ) and( {45603} not(  daily close >  20 ) ) and  daily close >  daily ema(  daily close , 50 ) and  daily close *  daily sma(  daily volume , 50 ) >=  50000000 and  daily volume >  25000 and  daily ema(  daily close , 50 ) <=  5 days ago high and  daily ema(  daily close , 50 ) >=  5 days ago low ) ) or( {cash} (  daily high <=  6 days ago high and  1 day ago high <=  6 days ago high and  2 days ago high <=  6 days ago high and  3 days ago high <=  6 days ago high and  4 days ago high <=  6 days ago high and  5 days ago high <=  6 days ago high and  daily low >=  6 days ago low and  1 day ago low >=  6 days ago low and  3 days ago low >=  6 days ago low and  2 days ago low >=  6 days ago low and  4 days ago low >=  6 days ago low and  5 days ago low >=  6 days ago low and(  6 days ago high -  6 days ago low ) /  6 days ago high *  100 <  10 and  daily close >  20 and( {166311} not(  daily close >  20 ) ) and( {45603} not(  daily close >  20 ) ) and  daily close >  daily ema(  daily close , 50 ) and  daily close *  daily sma(  daily volume , 50 ) >=  50000000 and  daily volume >  25000 and  daily ema(  daily close , 50 ) <=  6 days ago high and  daily ema(  daily close , 50 ) >=  6 days ago low ) ) or( {cash} (  daily high <=  7 days ago high and  1 day ago high <=  7 days ago high and  2 days ago high <=  7 days ago high and  3 days ago high <=  7 days ago high and  4 days ago high <=  7 days ago high and  5 days ago high <=  7 days ago high and  6 days ago high <=  7 days ago high and  daily low >=  7 days ago low and  1 day ago low >=  7 days ago low and  3 days ago low >=  7 days ago low and  2 days ago low >=  7 days ago low and  4 days ago low >=  7 days ago low and  5 days ago low >=  7 days ago low and  6 days ago low >=  7 days ago low and(  7 days ago high -  7 days ago low ) /  7 days ago high *  100 <  10 and  daily close >  20 and  daily ema(  daily close , 50 ) <=  7 days ago high and  daily ema(  daily close , 50 ) >=  7 days ago low and( {166311} not(  daily close >  20 ) ) and( {45603} not(  daily close >  20 ) ) and  daily close >  daily ema(  daily close , 50 ) and  daily close *  daily sma(  daily volume , 50 ) >=  50000000 and  daily volume >  25000 ) ) ) ) ) )"},
    {"name": "VOLUME FOOTPRINT", "condition": "( {cash} (  daily max( 20 ,  daily volume ) =  daily max( 63 ,  daily volume ) and( {1468504} not(  daily close >  1 ) ) and( {166311} not(  daily close >  1 ) ) and  daily close >=  daily ema(  daily close , 50 ) and  daily ema(  daily close , 21 ) >=  daily ema(  daily close , 50 ) and  daily volume >  20000 and  daily close *  daily sma(  daily volume , 20 ) >=  40000000 ) )"},
    {"name": "Above200", "condition": "( {cash} ( latest close > latest ema( latest close , 200 ) ) )"},
    {"name": "HommaPBC", "condition": "( {cash} ( ( {cash} (  daily close >=  20 and  market cap >=  1000 and( {cash} (  daily sma(  daily volume , 20 ) *  daily sma(  daily close , 20 ) >=  10000000 or  daily sma(  daily volume , 50 ) *  daily sma(  daily close , 50 ) >=  10000000 ) ) and( {cash} (  daily {custom_indicator_139136_start}\"( (  close - 20 candles ago close ) * 100 / 20 candles ago close / 20 ) + ( (  close - 50 candles ago close ) * 100 / 50 candles ago close / 50 )\"{custom_indicator_139136_end} >=  0 ) ) and( {cash} (  daily sma(  daily close , 50 ) /  50 days ago sma(  daily close , 50 ) >=  1 and  daily sma(  daily close , 200 ) /  50 days ago sma(  daily close , 200 ) >=  1 ) ) and( {cash} (  daily close >=  (  weekly max( 52 ,  weekly close *  0.75 ) ) or  daily close >=  (  daily max( 50 ,  daily close *  0.75 ) ) ) ) and( {cash} ( ( {cash} (  daily close /  daily ema(  daily close , 21 ) <=  1.05 and  daily close /  daily ema(  daily close , 21 ) >=  0.95 ) ) or( {cash} (  daily close /  daily sma(  daily close , 50 ) <=  1.05 and  daily close /  daily sma(  daily close , 50 ) >=  0.95 ) ) ) ) and( {cash} ( ( {cash} (  (  daily high -  daily low ) /  daily close <=  0.04 and  daily \"close - 1 candle ago close / 1 candle ago close * 100\" <=  2 ) ) and( {cash} (  (  1 day ago high -  1 day ago low ) /  1 day ago close <=  0.04 and  1 day ago \"close - 1 candle ago close / 1 candle ago close * 100\" <=  2 ) ) and( {cash} (  (  2 days ago high -  2 days ago low ) /  2 days ago close <=  0.04 and  2 days ago \"close - 1 candle ago close / 1 candle ago close * 100\" <=  2 ) ) and( {cash} (  (  3 days ago high -  3 days ago low ) /  3 days ago close <=  0.04 and  3 days ago \"close - 1 candle ago close / 1 candle ago close * 100\" <=  2 ) ) ) ) and  daily avg true range( 14 ) /  daily sma(  daily close , 14 ) >=  3 /  100 and( {cash} (  daily high >=  daily sma(  daily close , 50 ) and  daily close >=  weekly min( 52 ,  weekly low ) *  1.3 ) ) ) ) or( {cash} ( ( {cash} (  daily close >=  10 and  market cap >=  5000 and  daily sma(  daily volume , 20 ) *  daily sma(  daily close , 20 ) >=  10000000 ) ) and( {cash} (  daily {custom_indicator_139136_start}\"( (  close - 20 candles ago close ) * 100 / 20 candles ago close / 20 ) + ( (  close - 50 candles ago close ) * 100 / 50 candles ago close / 50 )\"{custom_indicator_139136_end} >=  0 ) ) and( {cash} (  (  daily max( 3 ,  daily high ) -  daily min( 3 ,  daily low ) *  100 ) /  daily sma(  daily close , 3 ) <=  daily avg true range( 10 ) *  100 /  daily sma(  daily close , 10 ) *  1.25 or(  daily max( 3 ,  daily high ) -  daily min( 3 ,  daily low ) *  100 ) /  daily sma(  daily close , 3 ) <=  daily avg true range( 14 ) *  100 /  daily sma(  daily close , 14 ) *  1.25 or(  daily max( 3 ,  daily high ) -  daily min( 3 ,  daily low ) *  100 ) /  daily sma(  daily close , 3 ) <=  daily avg true range( 20 ) *  100 /  daily sma(  daily close , 20 ) *  1.25 ) ) and( {cash} ( ( {cash} (  daily close /  daily sma(  daily close , 10 ) <=  1.025 and  daily close /  daily sma(  daily close , 10 ) >=  0.975 ) ) or( {cash} (  daily close /  daily ema(  daily close , 21 ) <=  1.025 and  daily close /  daily ema(  daily close , 21 ) >=  0.975 ) ) or( {cash} (  daily close /  daily sma(  daily close , 50 ) <=  1.025 and  daily close /  daily sma(  daily close , 50 ) >=  0.975 ) ) ) ) and( {cash} (  daily close >=  weekly min( 52 ,  weekly low ) *  1.3 and  daily high >=  daily sma(  daily close , 50 ) ) ) ) ) ) )"},
    {"name": "WIB", "condition": "( {cash} (  weekly high <=  1 week ago high and  weekly low >=  1 week ago low and  weekly sma(  weekly close , 10 ) >  weekly sma(  weekly close , 30 ) ) )"},
    {"name": "3WTC", "condition": "( {cash} (  weekly max( 3 ,  weekly close ) /  weekly min( 3 ,  weekly close ) <  1.021 and( {166311} not(  weekly close >  0 ) ) and( {45603} not(  weekly close >  0 ) ) and( {167068} not(  weekly close >  0 ) ) and  weekly volume >  50000 and  weekly sma(  weekly close , 10 ) >  weekly sma(  weekly close , 30 ) ) )"},
    {"name": "W-MCP", "condition": "( {cash} ( ( {cash} (  weekly high <=  3 weeks ago high and  1 week ago high <=  3 weeks ago high and  2 weeks ago high <=  3 days ago high and  weekly low >=  3 weeks ago low and  1 week ago low >=  3 weeks ago low and  2 weeks ago low >=  3 weeks ago low and  weekly close >  20 and( {166311} not(  weekly close >  0 ) ) and( {45603} not(  weekly close >  0 ) ) ) ) or( {cash} (  weekly high <=  4 weeks ago high and  1 week ago high <=  4 weeks ago high and  2 weeks ago high <=  4 weeks ago high and  3 weeks ago high <=  4 weeks ago high and  weekly low >=  4 weeks ago low and  1 week ago low >=  4 weeks ago low and  2 weeks ago low >=  4 weeks ago low and  3 weeks ago low >=  4 weeks ago low and  weekly close >  20 and( {166311} not(  weekly close >  0 ) ) and( {45603} not(  weekly close >  0 ) ) ) ) or( {cash} (  weekly high <=  5 weeks ago high and  1 week ago high <=  5 weeks ago high and  2 weeks ago high <=  5 weeks ago high and  3 weeks ago high <=  5 weeks ago high and  4 weeks ago high <=  5 weeks ago high and  weekly low >=  5 weeks ago low and  1 week ago low >=  5 weeks ago low and  2 weeks ago low >=  5 weeks ago low and  3 weeks ago low >=  5 weeks ago low and  4 weeks ago low >=  5 weeks ago low and  weekly close >  20 and( {166311} not(  weekly close >  0 ) ) and( {45603} not(  weekly close >  0 ) ) ) ) ) )"},
]

# Backtest scanners (counts -> MarketData) (kept as the "older two backtest sets" from previous code)
counts_backtest_scanners = {
    "Total stocks": "( {166311} not(  1 > 0 ) ) and( {cash} (  daily close >  0 and  market cap >  1 ) )",
    "Above21SMA": "( {166311} not(  1 > 0 ) ) and( {cash} (  daily close >  0 and  market cap >  1 and  daily close >  daily sma(  daily close , 20 ) ) )",
    "Below21SMA": "( {166311} not(  1 > 0 ) ) and( {cash} (  daily close >  0 and  market cap >  1 and  daily close <  daily sma(  daily close , 20 ) ) )",
    "Above50SMA": "( {166311} not(  1 > 0 ) ) and( {cash} (  daily close >  0 and  market cap >  1 and  daily close >  daily sma(  daily close , 50 ) ) )",
    "Below50SMA": "( {166311} not(  1 > 0 ) ) and( {cash} (  daily close >  0 and  market cap >  1 and  daily close <  daily sma(  daily close , 50 ) ) )",
    "Above200SMA": "( {166311} not(  1 > 0 ) ) and( {cash} (  daily close >  0 and  market cap >  1 and  daily close >  daily sma(  daily close , 200 ) ) )",
    "Below200SMA": "( {166311} not(  1 > 0 ) ) and( {cash} (  daily close >  0 and  market cap >  1 and  daily close <  daily sma(  daily close , 200 ) ) )",
    "Above21EMA": "( {166311} not(  1 > 0 ) ) and( {cash} (  daily close >  0 and  market cap >  1 and  daily close >  daily ema(  daily close , 20 ) ) )",
    "Below21EMA": "( {166311} not(  1 > 0 ) ) and( {cash} (  daily close >  0 and  market cap >  1 and  daily close <  daily ema(  daily close , 20 ) ) )",
    "Above10EMA": "( {166311} not(  1 > 0 ) ) and( {cash} (  daily close >  0 and  market cap >  1 and  daily close >  daily ema(  daily close , 10 ) ) )",
    "Below10EMA": "( {166311} not(  1 > 0 ) ) and( {cash} (  daily close >  0 and  market cap >  1 and  daily close <  daily ema(  daily close , 10 ) ) )",
    "Above50EMA": "( {166311} not(  1 > 0 ) ) and( {cash} (  daily close >  0 and  market cap >  1 and  daily close >  daily ema(  daily close , 50 ) ) )",
    "Below50EMA": "( {166311} not(  1 > 0 ) ) and( {cash} (  daily close >  0 and  market cap >  1 and  daily close <  daily ema(  daily close , 50 ) ) )",
    "Above200EMA": "( {166311} not(  1 > 0 ) ) and( {cash} (  daily close >  0 and  market cap >  1 and  daily close >  daily ema(  daily close , 200 ) ) )",
    "Below200EMA": "( {166311} not(  1 > 0 ) ) and( {cash} (  daily close >  0 and  market cap >  1 and  daily close <  daily ema(  daily close , 20 ) ) )",
    "DCR70+": "( {cash} ( ( latest close - latest low ) / ( latest high - latest low ) * 100 > 70 and( {166311} not( latest close > 0 ) ) and( {45603} not( latest close > 0 ) ) and( {167068} not( latest close > 0 ) ) ) )",
    "DCR30-": "( {cash} ( ( latest close - latest low ) / ( latest high - latest low ) * 100 < 30 and( {166311} not( latest close > 0 ) ) and( {45603} not( latest close > 0 ) ) and( {167068} not( latest close > 0 ) ) ) )",
    "DCR30-70": "( {cash} ( ( latest close - latest low ) / ( latest high - latest low ) * 100 <= 70 and( latest close - latest low ) / ( latest high - latest low ) * 100 >= 30 and( {166311} not( latest close > 0 ) ) and( {45603} not( latest close > 0 ) ) and( {167068} not( latest close > 0 ) ) ) )",
    "4.5+": "( {166311} not(  1 > 0 ) ) and( {1468504} not( 1 > 0 ) ) and( {cash} ( ( {cash} (  daily close >=  0 and  market cap >=  1 and(  daily close -  1 day ago close ) /  1 day ago close *  100 >  4.5 ) ) ) )",
    "4.5-": "( {166311} not(  1 > 0 ) ) and( {1468504} not( 1 > 0 ) ) and( {cash} ( ( {cash} (  daily close >=  0 and  market cap >=  1 and(  daily close -  1 day ago close ) /  1 day ago close *  100 <  4.5 ) ) ) )",
    "PP": "( {cash} ( ( {cash} ( latest volume > latest max( 10 , latest volume * latest count( 1, 1 where latest close < latest open ) ) or( {cash} ( ( {cash} ( ( {cash} ( 1 day ago close > 2 days ago close ) ) or( {cash} ( 1 day ago close < 2 days ago close and 1 day ago volume < latest volume ) ) ) ) and( {cash} ( ( {cash} ( 2 days ago close > 3 days ago close ) ) or( {cash} ( 2 days ago close < 3 days ago close and 2 days ago volume < latest volume ) ) ) ) and( {cash} ( ( {cash} ( 3 days ago close > 4 days ago close ) ) or( {cash} ( 3 days ago close < 4 days ago close and 3 days ago volume < latest volume ) ) ) ) and( {cash} ( ( {cash} ( 4 days ago close > 5 days ago close ) ) or( {cash} ( 4 days ago close < 5 days ago close and 4 days ago volume < latest volume ) ) ) ) and( {cash} ( ( {cash} ( 5 days ago close > 6 days ago close ) ) or( {cash} ( 5 days ago close < 6 days ago close and 5 days ago volume < latest volume ) ) ) ) and( {cash} ( ( {cash} ( 6 days ago close > 7 days ago close ) ) or( {cash} ( 6 days ago close < 7 days ago close and 6 days ago volume < latest volume ) ) ) ) and( {cash} ( ( {cash} ( 7 days ago close > 8 days ago close ) ) or( {cash} ( 7 days ago close < 8 days ago close and 7 days ago volume < latest volume ) ) ) ) and( {cash} ( ( {cash} ( 8 days ago close > 9 days ago close ) ) or( {cash} ( 8 days ago close < 9 days ago close and 8 days ago volume < latest volume ) ) ) ) and( {cash} ( ( {cash} ( 9 days ago close > 10 days ago close ) ) or( {cash} ( 9 days ago close < 10 days ago close and 9 days ago volume < latest volume ) ) ) ) and( {cash} ( ( {cash} ( 10 days ago close > 11 days ago close ) ) or( {cash} ( 10 days ago close < 11 days ago close and 10 days ago volume < latest volume ) ) ) ) ) ) ) ) and latest close >= 1 day ago close and latest close >= 10 and market cap >= 100 and latest volume > 30000 ) )",
    "Above10SMA": "( {166311} not(  1 > 0 ) ) and( {cash} (  daily close >  0 and  market cap >  1 and  daily close >  daily sma(  daily close , 10 ) ) )",
    "Bulls Snort": "( {cash} ( latest close > 1 day ago close and latest volume > latest sma( volume,50 ) * 3 and latest close >= ( ( latest high - latest low ) * 0.65 + latest low ) ) )",
    "52WH": "( {cash} (  daily high =  daily max( 252 ,  daily high ) and  daily close >  1 and  market cap >  10 ) )",
    "52WL": "( {cash} (  daily low =  daily min( 252 ,  daily low ) and  daily close >  1 and  market cap >  10 ) )",
    "Mswing": "( {cash} (  (   close -  20 candles ago close ) *  100 /  20 candles ago close /  20 ) +  (  (   close -  50 candles ago close ) *  100 /  50 candles ago close /  50 ) >  0 and  daily close >  10 ) ",
    "Stage2": "( {cash} (  daily close >  daily sma(  daily close , 50 ) and  daily close >  daily sma(  daily close , 150 ) and  daily close >  daily sma(  daily close , 200 ) and  daily sma(  daily close , 50 ) >  daily sma(  daily close , 150 ) and  daily sma(  daily close , 50 ) >  daily sma(  daily close , 200 ) and  daily sma(  daily close , 150 ) >  daily sma(  daily close , 200 ) and  daily count( 20, 1 where  daily sma(  daily close , 200 ) >  1 day ago sma(  daily close , 200 ) ) >=  20 and  daily close >  daily max( 252 ,  daily high ) *  0.75 ) )",
    "Above40SMA": "( {166311} not(  1 > 0 ) ) and( {cash} (  daily close >  0 and  market cap >  1 and  daily close >  daily sma(  daily close , 40 ) ) )",
    "5DayCh": "( {166311} not(  1 > 0 ) ) and( {1468504} not( 1 > 0 ) ) and( {cash} ( ( {cash} (  daily close >=  0 and  market cap >=  1 and(  daily close -  5 days ago close ) /  5 days ago close *  100 >  20 ) ) ) )",
}

# Weekly-periodicity scanner(s) — kept separate from daily counts so dates don't mismatch
weekly_backtest_scanners = {
    "52WD": "( {cash} (  weekly close /  52 weeks ago close >=  2 ) )",
}

# EP single scanner (GapUp_Strong) — will write Date/Scanner/Stock rows into EP
ep_backtest_scanner = {
    "GapUp_Strong": """( {cash} ( ( {cash} ( latest close > 20
    and latest open > 1 day ago close * 1.035
    and latest volume > 25000
    and latest low > 1 day ago high * 1.02
    and( {166311} not( latest close > 0 ) )
    and( {167068} not( latest close > 0 ) )
    and( {45603} not( latest close > 0 ) ) ) ) ) )"""
}

# ----------------------
# === Robust CSRF fetch (browser-like headers)
# ----------------------
def get_chartink_csrf(session, url="https://chartink.com"):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "Referer": "https://chartink.com/",
        "DNT": "1",
    }
    r = session.get(url, headers=headers, timeout=30)
    soup = bs(r.text, "lxml")
    meta_tag = soup.find("meta", {"name": "csrf-token"})
    if not meta_tag:
        # try again with /screener path (some pages have token there)
        r2 = session.get("https://chartink.com/screener", headers=headers, timeout=30)
        soup2 = bs(r2.text, "lxml")
        meta_tag = soup2.find("meta", {"name": "csrf-token"})
    if not meta_tag:
        raise RuntimeError("Unable to find Chartink CSRF token on the page.")
    return meta_tag["content"]

# ----------------------
# === Helpers: log status to LOGGER sheet
# ----------------------
def log_status(module_name, status_message):
    ts = now_ts()
    try:
        logger_sheet.append_row([ts, f"{module_name} - STATUS", status_message])
    except Exception as e:
        print(f"Failed to write status to LOGGER: {e}")

# ----------------------
# === Run Realtime Scanners
# ----------------------
def run_realtime_scanners():
    module = "Realtime Scanners"
    start = now_ts()
    try:
        print("=== Running realtime scanners ===")
        logger_sheet.batch_clear(["A2:C2000"])
        headers = []
        all_scanner_data = []

        url = "https://chartink.com/screener/process"
        with requests.session() as s:
            try:
                csrf = get_chartink_csrf(s, "https://chartink.com/screener")
            except Exception:
                csrf = get_chartink_csrf(s)
            headers_for_request = {"x-csrf-token": csrf, "User-Agent": "Mozilla/5.0"}

            for scanner in scanners:
                name = scanner["name"]
                clause = scanner["condition"]
                print(f"🔍 Processing realtime: {name}")
                try:
                    response = s.post(url, headers=headers_for_request, data={"scan_clause": clause}, timeout=30)
                    data = response.json()
                except Exception as e:
                    print(f"❌ Error for realtime scanner {name}: {e}")
                    headers.append(name)
                    all_scanner_data.append([])
                    continue

                headers.append(name)
                if not data.get("data"):
                    all_scanner_data.append([])
                    print(f"   -> No rows for {name}")
                    continue

                df = pd.DataFrame(data["data"]).astype(str)
                try:
                    nse_codes = df.iloc[:, 1].tolist()
                except Exception:
                    nse_codes = df.iloc[:, 0].tolist()
                all_scanner_data.append(nse_codes)
                print(f"   ✅ {name}: {len(nse_codes)} rows")

        # Write headers and data
        try:
            dashboard_sheet.update(range_name="A1", values=[headers])
            max_len = max((len(col) for col in all_scanner_data), default=0)
            data_to_write = []
            for row in range(max_len):
                row_data = []
                for col in all_scanner_data:
                    row_data.append(col[row] if row < len(col) else "")
                data_to_write.append(row_data)
            dashboard_sheet.batch_clear(["A2:Z2000"])
            if data_to_write:
                dashboard_sheet.update(range_name="A2", values=data_to_write)
        except Exception as e:
            print("❌ Failed writing PRICEPATTERN:", e)

        # Log counts to LOGGER (same format as before)
        for i, header in enumerate(headers):
            count_val = len(all_scanner_data[i]) if i < len(all_scanner_data) else 0
            try:
                logger_sheet.append_row([now_ts(), header, count_val])
            except Exception as e:
                print("❌ Failed appending to LOGGER:", e)

        log_status(module, f"Completed successfully at {now_ts()}")
    except Exception as e:
        tb = traceback.format_exc()
        print(f"❌ {module} failed:", e)
        log_status(module, f"ERROR: {e}")
        print(tb)

# ----------------------
# === Run Backtest Scanners -> MarketData (counts per date)
# ----------------------
def run_backtest_scanners_counts():
    module = "Backtest Counts -> MarketData"
    try:
        print("=== Running backtest scanners (counts -> MarketData) ===")
        backtest_url = "https://chartink.com/backtest/process"
        results = {}

        with requests.session() as s:
            try:
                csrf = get_chartink_csrf(s)
            except Exception as e:
                raise RuntimeError(f"Could not fetch CSRF token for backtest counts: {e}")

            header = {"x-csrf-token": csrf, "User-Agent": "Mozilla/5.0"}

            for name, clause in counts_backtest_scanners.items():
                print(f"📊 Backtesting (counts): {name}")
                try:
                    response = s.post(backtest_url, headers=header, data={"scan_clause": clause}, timeout=60)
                    data = response.json()
                    dates = data["metaData"][0]["tradeTimes"]
                    stocks = data["aggregatedStockList"]

                    for i, ts in enumerate(dates):
                        dt = datetime.fromtimestamp(ts / 1000)
                        if dt not in results:
                            results[dt] = {}
                        try:
                            count_val = len([stocks[i][j] for j in range(0, len(stocks[i]), 3)])
                        except Exception:
                            count_val = 0
                        results[dt][f"{name}_Count"] = count_val
                except Exception as e:
                    print(f"❌ Error for backtest counts {name}: {e}")

        if not results:
            raise RuntimeError("No backtest count results to write.")

        df_bt = pd.DataFrame.from_dict(results, orient='index').sort_index(ascending=False)
        df_bt.index.name = "Date"
        df_reset = df_bt.reset_index()
        df_reset["Date"] = df_reset["Date"].astype(str)

        marketdata_sheet.clear()
        marketdata_sheet.update([df_reset.columns.tolist()] + df_reset.values.tolist())
        print("✅ MarketData updated.")
        log_status(module, f"Completed successfully at {now_ts()}")
    except Exception as e:
        tb = traceback.format_exc()
        print(f"❌ {module} failed:", e)
        log_status(module, f"ERROR: {e}")
        print(tb)

# ----------------------
# === Run Backtest Scanners (weekly) -> WeeklyData (counts per week)
# ----------------------
def run_backtest_scanners_weekly():
    module = "Backtest Weekly -> WeeklyData"
    try:
        print("=== Running backtest scanners (weekly counts -> WeeklyData) ===")
        backtest_url = "https://chartink.com/backtest/process"
        results = {}

        with requests.session() as s:
            try:
                csrf = get_chartink_csrf(s)
            except Exception as e:
                raise RuntimeError(f"Could not fetch CSRF token for weekly backtest: {e}")

            header = {"x-csrf-token": csrf, "User-Agent": "Mozilla/5.0"}

            for name, clause in weekly_backtest_scanners.items():
                print(f"📊 Backtesting (weekly): {name}")
                try:
                    response = s.post(backtest_url, headers=header, data={"scan_clause": clause}, timeout=60)
                    data = response.json()
                    dates = data["metaData"][0]["tradeTimes"]
                    stocks = data["aggregatedStockList"]

                    for i, ts in enumerate(dates):
                        dt = datetime.fromtimestamp(ts / 1000)
                        if dt not in results:
                            results[dt] = {}
                        try:
                            count_val = len([stocks[i][j] for j in range(0, len(stocks[i]), 3)])
                        except Exception:
                            count_val = 0
                        results[dt][f"{name}_Count"] = count_val
                except Exception as e:
                    print(f"❌ Error for weekly backtest {name}: {e}")

        if not results:
            raise RuntimeError("No weekly backtest results to write.")

        df_wk = pd.DataFrame.from_dict(results, orient='index').sort_index(ascending=False)
        df_wk.index.name = "Date"
        df_reset = df_wk.reset_index()
        df_reset["Date"] = df_reset["Date"].astype(str)

        weeklydata_sheet.clear()
        weeklydata_sheet.update([df_reset.columns.tolist()] + df_reset.values.tolist())
        print("✅ WeeklyData updated.")
        log_status(module, f"Completed successfully at {now_ts()}")
    except Exception as e:
        tb = traceback.format_exc()
        print(f"❌ {module} failed:", e)
        log_status(module, f"ERROR: {e}")
        print(tb)

# ----------------------
# === Run EP backtest scanner -> EP (Date, Scanner, Stock) append
# ----------------------
def run_ep_backtest_and_append():
    module = "EP Backtest (GapUp_Strong) -> EP"
    try:
        print("=== Running EP backtest (GapUp_Strong) and appending to EP sheet ===")
        backtest_url = "https://chartink.com/backtest/process"
        new_rows = []

        # Read existing EP sheet to avoid duplicates
        existing_vals = ep_sheet.get_all_values()
        if existing_vals and len(existing_vals) > 0:
            header_row = existing_vals[0]
            existing_df = pd.DataFrame(existing_vals[1:], columns=header_row) if len(existing_vals) > 1 else pd.DataFrame(columns=header_row)
            expected_cols = ["Date", "Scanner", "Stock"]
            for c in expected_cols:
                if c not in existing_df.columns:
                    existing_df[c] = ""
            existing_df = existing_df[expected_cols]
        else:
            existing_df = pd.DataFrame(columns=["Date", "Scanner", "Stock"])

        with requests.session() as s:
            try:
                csrf = get_chartink_csrf(s)
            except Exception as e:
                raise RuntimeError(f"Could not fetch CSRF token for EP backtest: {e}")

            header = {"x-csrf-token": csrf, "User-Agent": "Mozilla/5.0"}

            for name, clause in ep_backtest_scanner.items():
                print(f"📊 Backtesting (EP): {name}")
                try:
                    response = s.post(backtest_url, headers=header, data={"scan_clause": clause}, timeout=60)
                    data = response.json()
                    dates = data["metaData"][0]["tradeTimes"]
                    stocks = data["aggregatedStockList"]

                    for i, ts in enumerate(dates):
                        dt = datetime.fromtimestamp(ts / 1000).strftime("%Y-%m-%d")
                        try:
                            stock_list = [stocks[i][j] for j in range(0, len(stocks[i]), 3)]
                        except Exception:
                            stock_list = []
                        for stock in stock_list:
                            new_rows.append([dt, name, stock])
                except Exception as e:
                    print(f"❌ Error for EP backtest {name}: {e}")

        if not new_rows:
            print("No new EP rows found.")
            log_status(module, "No new rows")
            return

        new_df = pd.DataFrame(new_rows, columns=["Date", "Scanner", "Stock"])

        # Combine with existing and drop duplicates
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
        combined_df = combined_df.drop_duplicates(subset=["Date", "Scanner", "Stock"], keep="first")
        combined_df = combined_df.sort_values(by=["Date", "Scanner", "Stock"], ascending=[False, True, True])

        ep_sheet.clear()
        ep_sheet.update([combined_df.columns.tolist()] + combined_df.values.tolist())
        print("✅ EP sheet updated (appended new rows).")
        log_status(module, f"Completed successfully at {now_ts()}")
    except Exception as e:
        tb = traceback.format_exc()
        print(f"❌ {module} failed:", e)
        log_status(module, f"ERROR: {e}")
        print(tb)
import requests
import pandas as pd
import numpy as np

def run_custom_list_scanner():
    module = "Custom Scanner -> List"
    try:
        print("=== Running Custom Scanner (List) ===")

        url = "https://chartink.com/screener/process"

        scanners = {
            "List": {
                "scan_clause": r"( {166311} not( 1 > 0 ) ) and( {cash} ( daily close > 0 and( {166311} not( daily close > 0 ) ) and( {45603} not( daily close > 0 ) ) ) )",

                "column_clause": r""" Daily Close as 'scan-column-default-close',
                Daily "close - 1 candle ago close / 1 candle ago close * 100" as 'scan-column-default-percent-change',
                filternumber( daily close > 1 day ago close,1) as 'default-percent-change-conditional-filters-color',
                Daily Volume as 'scan-column-default-volume',
                Daily Ema( Daily Close , 20 ) as 'scan-column-_fa42d',
                Daily Ema( Daily Close , 50 ) as 'scan-column-_67415',
                Daily Rsi( 14 ) as 'scan-column-_8e331',
                ( Daily Close - 63 days ago Close ) / 63 days ago Close * 100 as 'scan-column-_5f5d7',
                ( Daily Close - 126 days ago Close ) / 126 days ago Close * 100 as 'scan-column-_b7891',
                ( Daily Close -  189 days ago Close ) /  189 days ago Close *  100 as 'scan-column-_db637',
                ( Daily Close -  252 days ago Close ) /  252 days ago Close *  100 as 'scan-column-_5fa89',
                ( Daily Max( 252 , Daily High ) - Daily Close ) / Daily Max( 252 , Daily High ) * 100 as 'scan-column-_4f03a',
                Daily Max( 252 , Daily High ) as 'scan-column-_7de18',
                Market Cap as 'scan-column-_0a2a2',
                Daily Sma( Daily Volume , 20 ) as 'scan-column-_238e1',
                Daily Sma( Daily Volume , 50 ) as 'scan-column-_c7c6b',
                ( Daily Volume / Daily Sma( Daily Volume , 50 ) ) * 100 as 'scan-column-_8096e',
                ( Daily Close * Daily Sma( Daily Volume , 50 ) ) / 10000000 as 'scan-column-_8c63c',
                Daily Ema(  Daily Close , 10 ) as 'scan-column-_a8ae8',
                Daily Ema(  Daily Close , 200 ) as 'scan-column-_7ab73',
                Quarterly Net sales as 'scan-column-_cdef1',
                (  Quarterly Net sales -  4 quarters ago Net sales ) /  4 quarters ago Net sales *  100 as 'scan-column-_863d4',
                (  Quarterly Eps after extraordinary items basic -  4 quarters ago Eps after extraordinary items basic ) /  4 quarters ago Eps after extraordinary items basic *  100 as 'scan-column-_74ea4',
                (  Daily Upper Bollinger band( 20 , 2 ) -  Daily Lower Bollinger band( 20 , 2 ) ) /  Daily Sma(  Daily Close , 20 ) *  100 as 'scan-column-_90e4e',
                Daily ^6405('length'='14','output'='val')^ as 'scan-column-_53946',
                (  Daily High -  Daily Ema(  Daily Close , 50 ) ) /  Daily Ema(  Daily Close , 50 ) *  100 as 'scan-column-_dad5f',
                (  Daily Close -  Daily Ema(  Daily Close , 50 ) ) /  Daily Ema(  Daily Close , 50 ) *  100 as 'scan-column-_75de1',
                (  Daily Close -  Daily Ema(  Daily Close , 21 ) ) /  Daily Ema(  Daily Close , 21 ) *  100 as 'scan-column-_e42e1',
                (  Daily Close -  Daily Ema(  Daily Close , 10 ) ) /  Daily Ema(  Daily Close , 10 ) *  100 as 'scan-column-_28cfc',
                (  Daily Close -  21 days ago Close ) /  21 days ago Close *  100 as 'scan-column-_c6643',
                Weekly Rsi( 14 ) as 'scan-column-_5d6b3',
                Monthly Rsi( 14 ) as 'scan-column-_28ed3',
                (  (  Daily Close -  20 days ago Close ) *  100 /  20 days ago Close /  20 ) +  (  (  Daily Close -  50 days ago Close ) *  100 /  50 days ago Close /  50 ) as 'scan-column-_72c36',
                (  Daily Max( 5 ,  Daily High ) -  Daily Min( 5 ,  Daily Low ) ) /  Daily Close *  100 as 'scan-column-_60040',
                (  Daily Max( 3 ,  Daily High ) -  Daily Min( 3 ,  Daily Low ) ) /  Daily Close *  100 as 'scan-column-_cb122'"""

            }
        }

        with requests.session() as s:
            csrf = get_chartink_csrf(s)

            headers = {
                "x-csrf-token": csrf,
                "User-Agent": "Mozilla/5.0"
            }

            for name, scan in scanners.items():

                payload = {
                    "scan_clause": scan["scan_clause"],
                    "column_clause": scan["column_clause"]
                }

                response = s.post(url, headers=headers, data=payload, timeout=30)
                data = response.json()

                if not data.get("data"):
                    print("No data found")
                    return

                df = pd.DataFrame(data["data"])

                # Rename NSE code column
                if "nsecode" in df.columns:
                    df.rename(columns={"nsecode": "Stock"}, inplace=True)

                # ✅ CLEAN COLUMN NAMES
                df.rename(columns={
                    "scan-column-default-close": "Close",
                    "scan-column-default-percent-change": "% Change",
                    "scan-column-default-volume": "Volume",
                    "scan-column-_fa42d": "EMA 21",
                    "scan-column-_67415": "EMA 50",
                    "scan-column-_8e331": "RSI",
                    "scan-column-_5f5d7": "63D Return",
                    "scan-column-_b7891": "6M Return",
                    "scan-column-_db637": "9M Return",
                    "scan-column-_5fa89": "12M Return",
                    "scan-column-_4f03a": "From High %",
                    "scan-column-_7de18": "52W High",
                    "scan-column-_0a2a2": "Market Cap",
                    "scan-column-_238e1": "Vol SMA 20",
                    "scan-column-_c7c6b": "Vol SMA 50",
                    "scan-column-_8096e": "Volume %",
                    "scan-column-_8c63c": "Liquidity",
                    "scan-column-_a8ae8": "10EMA",
                    "scan-column-_7ab73": "200EMA",
                    "scan-column-_cdef1": "Quarterly Net sales",
                    "scan-column-_863d4": "Quarterly Net sales Change",
                    "scan-column-_74ea4": "Quarterly EPS Change",
                    "scan-column-_90e4e": "BB Width",
                    "scan-column-_53946": "%ATR",
                    "scan-column-_dad5f": "%50EMAD",
                    "scan-column-_75de1": "50EMAD",
                    "scan-column-_e42e1": "21EMAD",
                    "scan-column-_28cfc": "10EMAD",
                    "scan-column-_c6643": "1M Return",
                    "scan-column-_5d6b3": "W RSI",
                    "scan-column-_28ed3": "M RSI",
                    "scan-column-_72c36": "M Swing",
                    "scan-column-_cb122": "3DR",
                    "scan-column-_60040": "5DR"


                }, inplace=True)

                # 🔥 SAFE DATA CLEANING — pandas 2.x/3.x compatible
                for col in df.columns:
                    try:
                        s = pd.to_numeric(df[col])
                    except (ValueError, TypeError):
                        continue  # non-numeric column, chhod do
                    s = s.replace([np.inf, -np.inf], np.nan)
                    s = s.mask(s.abs() > 1e10)   # abnormal values -> NaN
                    df[col] = s

                # NaN ko blank karo — object dtype pe pehle switch karke, taaki
                # float column mein "" daalne wala dtype error na aaye
                df = df.astype(object).where(df.notna(), "")

                # Reorder columns
                cols = ["Stock"] + [c for c in df.columns if c != "Stock"]
                df = df[cols]

                df = df.astype(str)

                # Write to Google Sheet
                sheet = client.open("Chartink Scanner Results")

                try:
                    list_sheet = sheet.worksheet("List")
                except:
                    list_sheet = sheet.add_worksheet(title="List", rows="1000", cols="20")

                list_sheet.clear()
                list_sheet.update([df.columns.tolist()] + df.values.tolist())

                print(f"✅ List sheet updated with {len(df)} stocks")
                log_status(module, f"Success - {len(df)} rows")

    except Exception as e:
        print("❌ Error in custom scanner:", e)
        log_status(module, f"ERROR: {e}")

# ----------------------
# === MAIN: run all modules sequentially
# ----------------------
if __name__ == "__main__":

    try:
        run_realtime_scanners()
    except Exception as e:
        print("❌ Realtime scanner module failed:", e)

    try:
        run_backtest_scanners_counts()
    except Exception as e:
        print("❌ Backtest counts module failed:", e)

    try:
        run_backtest_scanners_weekly()
    except Exception as e:
        print("❌ Weekly backtest module failed:", e)

    try:
        run_ep_backtest_and_append()
    except Exception as e:
        print("❌ EP backtest module failed:", e)
    try:
        run_custom_list_scanner()
    except Exception as e:
        print("❌ List scanner failed:", e)
    duration = round(time.time() - start_time, 2)
    print(f"\n✅ All done in {duration}s at {now_ts()}")
    log_status("Full Run", f"Finished in {duration}s at {now_ts()}")
