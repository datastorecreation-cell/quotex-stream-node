import os
from fastapi import FastAPI
import httpx

app = FastAPI()

OTCHARTS_API_KEY = os.getenv("OTCHARTS_API_KEY", "")

# Pairs mapping: OTCharts symbol -> Supabase strategy name
SYMBOL_MAP = {
    "EURUSD_otc": "EUR/USD OTC",
    "GBPUSD_otc": "GBP/USD OTC",
    "USDJPY_otc": "USD/JPY OTC"
}

@app.get("/candles")
async def get_candles():
    candle_store = {
        "EUR/USD OTC": [],
        "GBP/USD OTC": [],
        "USD/JPY OTC": []
    }

    if not OTCHARTS_API_KEY:
        return {"error": "OTCHARTS_API_KEY environment variable is missing."}

    headers = {
        "Authorization": f"Bearer {OTCHARTS_API_KEY}"
    }

    async with httpx.AsyncClient(timeout=10.0) as client:
        for symbol, display_name in SYMBOL_MAP.items():
            try:
                # Query Quotex OTC 60-second candles
                url = f"https://api.otcharts.com/v1/candles?venue=quotex&symbol={symbol}&tf=60&limit=50"
                response = await client.get(url, headers=headers)
                
                if response.status_code == 200:
                    data = response.json()
                    raw_bars = data.get("candles") or data.get("data") or []
                    
                    formatted = []
                    for b in raw_bars:
                        formatted.append({
                            "time": b.get("time") or b.get("t"),
                            "open": float(b.get("open") or b.get("o")),
                            "high": float(b.get("high") or b.get("h")),
                            "low": float(b.get("low") or b.get("l")),
                            "close": float(b.get("close") or b.get("c")),
                            "payout": 88
                        })
                    candle_store[display_name] = formatted
            except Exception as e:
                print(f"Error fetching {symbol}: {e}")

    return candle_store

@app.get("/")
def health_check():
    return {"status": "ok", "provider": "OTCharts-bridge"}
