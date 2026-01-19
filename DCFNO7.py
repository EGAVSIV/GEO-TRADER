import pandas as pd
import os, logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from tvDatafeed import TvDatafeed, Interval

# =====================================================
# CONFIG
# =====================================================
MAX_WORKERS = 12
BARS = 300

BASE_DIR = "market_data"
FNO_PATH = os.path.join(BASE_DIR, "fno")

# =====================================================
# LOGGING
# =====================================================
logging.basicConfig(
    filename="tv_fno.log",
    level=logging.INFO,
    format="%(asctime)s | %(message)s"
)

# =====================================================
# TIMEFRAMES
# =====================================================
TIMEFRAMES = {
    "15m": Interval.in_15_minute,
    "30m": Interval.in_30_minute,
    "1H":  Interval.in_1_hour,
    "4H":  Interval.in_4_hour,
    "D":   Interval.in_daily,
    "W":   Interval.in_weekly,
    "M":   Interval.in_monthly,
}

for tf in TIMEFRAMES:
    os.makedirs(os.path.join(FNO_PATH, tf), exist_ok=True)

fno_symbols = [ 

    'PPLPHARMA','PRESTIGE','RBLBANK','RECLTD','RELIANCE','RVNL','SAIL','SAMMAANCAP','SBICARD','SBILIFE',
    'SBIN','SHREECEM','SHRIRAMFIN','SIEMENS','SOLARINDS','SONACOMS','SRF','SUNPHARMA','SUPREMEIND','SUZLON']
# =====================================================
# INDICATORS
# =====================================================
def calc_rsi(close, period=14):
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    rs = gain.ewm(alpha=1/period).mean() / loss.ewm(alpha=1/period).mean()
    return 100 - (100 / (1 + rs))

def calc_bollinger(close, period=20, std=2):
    mid = close.rolling(period).mean()
    dev = close.rolling(period).std()
    return mid + std*dev, mid, mid - std*dev

# =====================================================
# WORKER
# =====================================================
def process_fno(symbol):
    tv = TvDatafeed()

    for tf, interval in TIMEFRAMES.items():
        try:
            df = tv.get_hist(symbol, "NSE", interval, n_bars=BARS)
            if df is None or df.empty:
                continue

            df = df.sort_index().tail(BARS)

            df["rsi_14"] = calc_rsi(df["close"])
            df["bb_upper"], df["bb_mid"], df["bb_lower"] = calc_bollinger(df["close"])

            save = os.path.join(FNO_PATH, tf, f"{symbol}.parquet")

            if os.path.exists(save):
                old = pd.read_parquet(save)
                df = (
                    pd.concat([old, df])
                    .drop_duplicates()
                    .sort_index()
                    .tail(BARS)
                )

            df.to_parquet(save)

        except Exception as e:
            logging.info(f"{symbol} | {tf} | {e}")

    return f"✅ FNO {symbol}"

# =====================================================
# MAIN (RUN ONCE & EXIT)
# =====================================================
def main():
    print("🚀 FNO Collector Started")

    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as exe:
        futures = [exe.submit(process_fno, s) for s in fno_symbols]

        for f in as_completed(futures):
            print(f.result())

    print("✅ FNO Collection Cycle Completed")

if __name__ == "__main__":
    main()



    

