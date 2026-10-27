import asyncio
import json
import time
from fastapi import FastAPI
import websockets

app = FastAPI()

CANDLE_STORE = {
    "EUR/USD OTC": [],
    "GBP/USD OTC": [],
    "USD/JPY OTC": []
}

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
            print("Connecting to Quotex WebSocket gateway...")
            async with websockets.connect(QUOTEX_WS, additional_headers=headers, ping_interval=None) as ws:
                # 1. Engine.io handshake
                await ws.send("2probe")
                init_resp = await ws.recv()
                print(f"Handshake response received: {init_resp}")
                
                if "3probe" in init_resp:
                    await ws.send("5")
                    print("Handshake upgraded with code 5.")

                # 2. Subscribe to OTC charts
                for sym in SYMBOL_MAP.keys():
                    sub_msg = f'42["sub_chart", {{"symbol": "{sym}", "timeframe": 60}}]'
                    await ws.send(sub_msg)
                    print(f"Subscribed to {sym}")

                # 3. Read stream
                while True:
                    msg = await ws.recv()
                    
                    if msg == "2":
                        await ws.send("3")
                        continue
                    
                    # Print raw incoming message snippet to Render logs
                    print(f"Raw Quotex message: {msg[:120]}")

                    if msg.startswith("42"):
                        payload = json.loads(msg[2:])
                        if isinstance(payload, list) and len(payload) >= 2:
                            event_name = payload[0]
                            data = payload[1]

                            if event_name in ["chart_data", "candles", "live_data"]:
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
                                    print(f"Updated store for {pair_name}: {len(formatted)} candles")

        except Exception as e:
            print(f"Connection error: {e}. Reconnecting in 3s...")
            await asyncio.sleep(3)

@app.on_event("startup")
async def startup():
    asyncio.create_task(quotex_listener())

@app.get("/candles")
def get_candles():
    return CANDLE_STORE
