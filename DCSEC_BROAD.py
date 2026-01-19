import pandas as pd
import os, time, logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from tvDatafeed import TvDatafeed, Interval

# =====================================================
# CONFIG
# =====================================================
UPDATE_INTERVAL_SECONDS = 3
MAX_WORKERS = 6
BARS = 300

BASE_DIR = "market_data"
BROADER_PATH = os.path.join(BASE_DIR, "broader_index")
SECTOR_PATH  = os.path.join(BASE_DIR, "sector_index")

# =====================================================
# LOGGING
# =====================================================
logging.basicConfig(
    filename="tv_indices.log",
    level=logging.INFO,
    format="%(asctime)s | %(message)s"
)

# =====================================================
# TIMEFRAMES
# =====================================================
TIMEFRAMES = {
    "D": Interval.in_daily,
    "W": Interval.in_weekly,
    "M": Interval.in_monthly,
}

for path in [BROADER_PATH, SECTOR_PATH]:
    for tf in TIMEFRAMES:
        os.makedirs(os.path.join(path, tf), exist_ok=True)

# =====================================================
# SYMBOLS
# =====================================================
broader_index = [
    'NIFTY','BANKNIFTY','CNXMIDCAP','CNXSMALLCAP','CNX500',
    'CNXFINANCE','NIFTYJR','CNX100','NIFTY_TOP_10_EW'
]
sector_index = [
    'CNXREALTY','CNXPSUBANK','CNXMETAL','CNXIT','CNXSERVICE',
    'CNXPSE','CNXCONSUMPTION','CNXINFRA','CNXENERGY','CNXAUTO',
    'CNXFMCG','CNXPHARMA','NIFTY_IND_DEFENCE','NIFTY_CAPITAL_MKT',
    'NIFTYPVTBANK','NIFTY_INDIA_MFG','NIFTY_OIL_AND_GAS',
    'NIFTY_HEALTHCARE','NIFTY_CHEMICALS','NIFTY_CONSR_DURBL',
    'NIFTY_MS_IT_TELCM'
]

# =====================================================
# WORKER
# =====================================================
def process_index(symbol, bucket_path):
    tv = TvDatafeed()
    for tf, interval in TIMEFRAMES.items():
        try:
            df = tv.get_hist(symbol, "NSE", interval, n_bars=BARS)
            if df is None or df.empty:
                continue

            df = df.sort_index().tail(BARS)
            save = os.path.join(bucket_path, tf, f"{symbol}.parquet")

            if os.path.exists(save):
                old = pd.read_parquet(save)
                df = pd.concat([old, df]).drop_duplicates().tail(BARS)

            df.to_parquet(save)

        except Exception as e:
            logging.info(f"{symbol} | {tf} | {e}")

    return f"✅ INDEX {symbol}"

# =====================================================
# MAIN LOOP
# =====================================================
if __name__ == "__main__":
    print("🚀 Index Collector Started")

    while True:
        start = time.time()

        tasks = []
        with ProcessPoolExecutor(MAX_WORKERS) as exe:
            for s in broader_index:
                tasks.append(exe.submit(process_index, s, BROADER_PATH))
            for s in sector_index:
                tasks.append(exe.submit(process_index, s, SECTOR_PATH))

            for f in as_completed(tasks):
                print(f.result())

        time.sleep(max(0, UPDATE_INTERVAL_SECONDS - (time.time() - start)))
