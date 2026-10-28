import asyncio
import os
import time
from fastapi import FastAPI
from quotexpy import Quotex

app = FastAPI()

# Buffer storing the last 50 closed candles for Supabase
CANDLE_STORE = {
    "EUR/USD OTC": [],
    "GBP/USD OTC": [],
    "USD/JPY OTC": []
}

# Mapping Quotex internal asset names to your strategy pair names
PAIRS = {
    "EURUSD_otc": "EUR/USD OTC",
    "GBPUSD_otc": "GBP/USD OTC",
    "USDJPY_otc": "USD/JPY OTC"
}

# Optional credentials (leave empty if testing public guest/demo stream)
QUOTEX_EMAIL = os.getenv("QUOTEX_EMAIL", "")
QUOTEX_PASSWORD = os.getenv("QUOTEX_PASSWORD", "")

async def stream_quotex_otc():
    """
    Background worker that connects to Quotex using TLS impersonation
    to avoid Cloudflare 403 blocks, continuously updating CANDLE_STORE.
    """
    client = Quotex(
        email=QUOTEX_EMAIL,
        password=QUOTEX_PASSWORD,
        lang="en"
    )

    while True:
        try:
            print("Connecting to Quotex OTC stream via client...")
            connected = await client.connect()
            
            if not connected:
                print("Failed to authenticate or connect. Retrying in 10s...")
                await asyncio.sleep(10)
                continue

            print("Connected successfully. Listening for OTC candles...")

            # Polling loop to fetch live 1-minute (60s) candle snapshots
            while True:
                now = int(time.time())
                
                for qx_symbol, display_name in PAIRS.items():
                    try:
                        # get_candles fetches historical / live bars: (symbol, timeframe_seconds, count, end_time)
                        candles = await client.get_candles(qx_symbol, 60, 50, now)
                        
                        if candles and isinstance(candles, list):
                            formatted = []
                            for c in candles[-50:]:
                                formatted.append({
                                    "time": c.get("time", now),
                                    "open": float(c.get("open", 0.0)),
                                    "high": float(c.get("high", 0.0)),
                                    "low": float(c.get("low", 0.0)),
                                    "close": float(c.get("close", 0.0)),
                                    "payout": 88
                                })
                            CANDLE_STORE[display_name] = formatted
                    except Exception as e:
                        print(f"Error fetching candles for {qx_symbol}: {e}")

                # Sleep 3 seconds before next snapshot pass
                await asyncio.sleep(3)

        except Exception as err:
            print(f"Quotex stream error: {err}. Reconnecting in 5s...")
            await asyncio.sleep(5)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(stream_quotex_otc())

@app.get("/candles")
def get_candles():
    return CANDLE_STORE

@app.get("/")
def health_check():
    return {"status": "running", "pairs": list(CANDLE_STORE.keys())}
