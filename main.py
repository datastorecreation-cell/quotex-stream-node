from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import time
import random

app = FastAPI(title="Quotex Data Bridge")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def root():
    return {"status": "online", "message": "Quotex Stream Node Active"}

@app.get("/candles")
def get_candles(pair: str = "EURUSD_otc"):
    now = int(time.time())
    base_price = 1.0850
    candles = []

    # Generates 50 sequential closed 1-minute candles
    for i in range(50, 0, -1):
        c_time = now - (i * 60)
        variation = random.uniform(-0.0004, 0.0004)
        o = round(base_price + variation, 5)
        c = round(o + random.uniform(-0.0003, 0.0003), 5)
        h = max(o, c) + round(random.uniform(0.00005, 0.00015), 5)
        l = min(o, c) - round(random.uniform(0.00005, 0.00015), 5)

        candles.append({
            "time": c_time,
            "open": o,
            "high": round(h, 5),
            "low": round(l, 5),
            "close": c,
            "payout": 88
        })
        base_price = c

    return candles
