import asyncio
import os
import time
from fastapi import FastAPI
from otcharts import Client

app = FastAPI()

# Candle buffer for your Supabase Edge Function
CANDLE_STORE = {
    "EUR/USD OTC": [],
    "GBP/USD OTC": [],
    "USD/JPY OTC": []
}

# Mapping OTCharts symbols to your engine names
PAIR_MAP = {
    "EURUSD_otc": "EUR/USD OTC",
    "GBPUSD_otc": "GBP/USD OTC",
    "USDJPY_otc": "USD/JPY OTC"
}

# Temporary tracking for building live 1-minute bars
current_candles = {}

def update_candle(symbol_key, price, timestamp):
    display_pair = PAIR_MAP.get(symbol_key)
    if not display_pair:
        return

    # Calculate 60-second bar boundary
    candle_time = (int(timestamp) // 60) * 60

    if display_pair not in current_candles or current_candles[display_pair]["time"] != candle_time:
        # A new minute started - archive the finished candle into store
        if display_pair in current_candles:
            CANDLE_STORE[display_pair].append(current_candles[display_pair])
            if len(CANDLE_STORE[display_pair]) > 50:
                CANDLE_STORE[display_pair] = CANDLE_STORE[display_pair][-50:]
        
        # Start new 1-minute candle
        current_candles[display_pair] = {
            "time": candle_time,
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "payout": 88
        }
    else:
        # Update current ongoing minute bar
        c = current_candles[display_pair]
        c["high"] = max(c["high"], price)
        c["low"] = min(c["low"], price)
        c["close"] = price

def stream_worker():
    """Runs synchronous OTCharts streaming in a dedicated background thread."""
    while True:
        try:
            api_key = os.getenv("OTCHARTS_API_KEY", "")
            if not api_key:
                print("OTCHARTS_API_KEY is missing. Waiting 10s...")
                time.sleep(10)
                continue

            otc = Client(api_key=api_key)
            print("Connected to OTCharts client. Preloading seed history...")

            # 1. Fetch initial 50 historical candles for EURUSD_otc, GBPUSD_otc, USDJPY_otc
            for sym, display_name in PAIR_MAP.items():
                try:
                    bars = otc.candles("quotex", sym, tf=60, limit=50)
                    if bars:
                        CANDLE_STORE[display_name] = [
                            {
                                "time": b.time,
                                "open": float(b.open),
                                "high": float(b.high),
                                "low": float(b.low),
                                "close": float(b.close),
                                "payout": 88
                            }
                            for b in bars
                        ]
                        print(f"Preloaded {len(CANDLE_STORE[display_name])} candles for {display_name}")
                except Exception as ex:
                    print(f"Error loading initial bars for {sym}: {ex}")

            # 2. Hold 1 live persistent stream connection (Free plan allows 1 stream)
            print("Opening live Quotex OTC tick stream...")
            for tick in otc.stream("quotex", "EURUSD_otc"):
                now = time.time()
                update_candle(tick.symbol, float(tick.price), now)

        except Exception as e:
            print(f"OTCharts stream error: {e}. Reconnecting in 5s...")
            time.sleep(5)

@app.on_event("startup")
async def startup_event():
    # Run the stream listener in a background worker thread
    asyncio.get_event_loop().run_in_executor(None, stream_worker)

@app.get("/candles")
def get_candles():
    return CANDLE_STORE

@app.get("/")
def root():
    return {"status": "running", "source": "OTCharts Free Quotex Stream"}
