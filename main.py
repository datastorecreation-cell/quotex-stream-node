import asyncio
import json
import os
import time
from fastapi import FastAPI
import httpx
from httpx_sse import aconnect_sse

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

current_candles = {}

def update_candle(symbol_key, price, timestamp):
    display_pair = PAIR_MAP.get(symbol_key)
    if not display_pair:
        return

    candle_time = (int(timestamp) // 60) * 60

    if display_pair not in current_candles or current_candles[display_pair]["time"] != candle_time:
        if display_pair in current_candles:
            CANDLE_STORE[display_pair].append(current_candles[display_pair])
            if len(CANDLE_STORE[display_pair]) > 50:
                CANDLE_STORE[display_pair] = CANDLE_STORE[display_pair][-50:]
        
        current_candles[display_pair] = {
            "time": candle_time,
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "payout": 88
        }
    else:
        c = current_candles[display_pair]
        c["high"] = max(c["high"], price)
        c["low"] = min(c["low"], price)
        c["close"] = price

async def stream_worker():
    """Streams live Quotex OTC ticks via Server-Sent Events (SSE)."""
    api_key = os.getenv("OTCHARTS_API_KEY", "").strip()
    headers = {"Authorization": f"Bearer {api_key}"}

    # 1. Preload recent 50 bars via REST API
    async with httpx.AsyncClient(timeout=15.0) as client:
        for sym, display_name in PAIR_MAP.items():
            try:
                url = f"https://otcharts.com/v1/candles?venue=quotex&symbol={sym}&tf=60&limit=50"
                resp = await client.get(url, headers=headers)
                if resp.status_code == 200:
                    bars = resp.json().get("candles", [])
                    CANDLE_STORE[display_name] = [
                        {
                            "time": b.get("time"),
                            "open": float(b.get("open")),
                            "high": float(b.get("high")),
                            "low": float(b.get("low")),
                            "close": float(b.get("close")),
                            "payout": 88
                        }
                        for b in bars
                    ]
                    print(f"Preloaded {len(CANDLE_STORE[display_name])} bars for {display_name}")
                else:
                    print(f"Failed to preload {sym}: HTTP {resp.status_code}")
            except Exception as e:
                print(f"Preload error for {sym}: {e}")

    # 2. Hold 1 continuous live stream connection (Free plan quota)
    while True:
        try:
            print("Connecting to live Quotex OTC SSE stream...")
            stream_url = "https://otcharts.com/v1/stream?venue=quotex&symbol=EURUSD_otc"
            
            async with httpx.AsyncClient(timeout=None) as client:
                async with aconnect_sse(client, "GET", stream_url, headers=headers) as event_source:
                    print("Live OTC SSE stream connected!")
                    async for sse in event_source.aiter_sse():
                        if sse.data:
                            data = json.loads(sse.data)
                            price = float(data.get("price", 0.0))
                            sym = data.get("symbol", "EURUSD_otc")
                            update_candle(sym, price, time.time())
        except Exception as err:
            print(f"Stream interrupted: {err}. Reconnecting in 5s...")
            await asyncio.sleep(5)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(stream_worker())

@app.get("/candles")
def get_candles():
    return CANDLE_STORE

@app.get("/")
def health_check():
    return {"status": "ok", "provider": "OTCharts SSE Live Stream"}
