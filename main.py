import asyncio
import json
import time
from fastapi import FastAPI
import websockets

app = FastAPI()

# Candle store buffer
CANDLE_STORE = {
    "EUR/USD OTC": [],
    "GBP/USD OTC": [],
    "USD/JPY OTC": []
}

# Pair lookup mapping
SYMBOL_MAP = {
    "EURUSD_otc": "EUR/USD OTC",
    "GBPUSD_otc": "GBP/USD OTC",
    "USDJPY_otc": "USD/JPY OTC"
}

QUOTEX_WS = "wss://ws2.quotex.com/socket.io/?EIO=3&transport=websocket"

async def quotex_listener():
    headers = [
        ("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"),
        ("Origin", "https://qxbroker.com")
    ]

    while True:
        try:
            # Using additional_headers for modern websockets/Python versions
            async with websockets.connect(QUOTEX_WS, additional_headers=headers, ping_interval=None) as ws:
                # 1. Complete handshake
                await ws.send("2probe")
                init_resp = await ws.recv()
                if "3probe" in init_resp:
                    await ws.send("5")

                # 2. Subscribe to OTC asset streams
                for sym in SYMBOL_MAP.keys():
                    sub_msg = f'42["sub_chart", {{"symbol": "{sym}", "timeframe": 60}}]'
                    await ws.send(sub_msg)

                # 3. Stream incoming messages
                while True:
                    msg = await ws.recv()
                    
                    if msg == "2":
                        await ws.send("3")
                        continue
                    
                    if msg.startswith("42"):
                        payload = json.loads(msg[2:])
                        if isinstance(payload, list) and len(payload) >= 2:
                            event_name = payload[0]
                            data = payload[1]

                            if event_name in ["chart_data", "candles"]:
                                sym = data.get("symbol")
                                pair_name = SYMBOL_MAP.get(sym)
                                raw_candles = data.get("candles") or data.get("data") or []
                                
                                if pair_name and raw_candles:
                                    formatted = []
                                    for c in raw_candles[-50:]:
                                        formatted.append({
                                            "time": c.get("time", int(time.time())),
                                            "open": float(c.get("open")),
                                            "high": float(c.get("high")),
                                            "low": float(c.get("low")),
                                            "close": float(c.get("close")),
                                            "payout": int(data.get("payout", 88))
                                        })
                                    CANDLE_STORE[pair_name] = formatted

        except Exception as e:
            print(f"Connection dropped: {e}. Reconnecting in 3 seconds...")
            await asyncio.sleep(3)

@app.on_event("startup")
async def startup():
    asyncio.create_task(quotex_listener())

@app.get("/candles")
def get_candles():
    return CANDLE_STORE
