import asyncio
import os
import time
from fastapi import FastAPI
from quotexpy import Quotex

app = FastAPI()

CANDLE_STORE = {
    "EUR/USD OTC": [],
    "GBP/USD OTC": [],
    "USD/JPY OTC": []
}

PAIRS = {
    "EURUSD_otc": "EUR/USD OTC",
    "GBPUSD_otc": "GBP/USD OTC",
    "USDJPY_otc": "USD/JPY OTC"
}

QUOTEX_EMAIL = os.getenv("QUOTEX_EMAIL", "")
QUOTEX_PASSWORD = os.getenv("QUOTEX_PASSWORD", "")

async def stream_quotex_otc():
    client = Quotex(
        email=QUOTEX_EMAIL,
        password=QUOTEX_PASSWORD,
        lang="en"
    )

    while True:
        try:
            print("Connecting to Quotex OTC stream...")
            # connect() will now find Chrome installed by the build command
            connected = await client.connect()
            
            if not connected:
                print("Connection failed. Retrying in 10s...")
                await asyncio.sleep(10)
                continue

            print("Connected successfully! Streaming OTC candles...")

            while True:
                now = int(time.time())
                for qx_symbol, display_name in PAIRS.items():
                    try:
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
                        pass
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
