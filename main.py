import asyncio
import json
import time
from fastapi import FastAPI
import websockets

app = FastAPI()

# Buffer storing the last 50 candles per asset
CANDLE_STORE = {
    "EUR/USD OTC": [],
    "GBP/USD OTC": [],
    "USD/JPY OTC": []
}

# Mapping our display names to Quotex OTC internal identifiers
SYMBOL_MAP = {
    "EURUSD_otc": "EUR/USD OTC",
    "GBPUSD_otc": "GBP/USD OTC",
    "USDJPY_otc": "USD/JPY OTC"
}

QUOTEX_WS_URL = "wss://ws2.quotex.com/socket.io/?EIO=3&transport=websocket"

async def quotex_feed_listener():
    """Persistent background task maintaining the Quotex WebSocket connection."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Origin": "https://qxbroker.com"
    }

    while True:
        try:
            async with websockets.connect(QUOTEX_WS_URL, extra_headers=headers, ping_interval=20, ping_timeout=20) as ws:
                # 1. Complete socket.io handshake
                await ws.send('2probe')
                response = await ws.recv()
                if '3probe' in response:
                    await ws.send('5')

                # 2. Subscribe to each OTC asset stream
                for qx_symbol in SYMBOL_MAP.keys():
                    sub_payload = f'42["sub_chart", {{"symbol": "{qx_symbol}", "timeframe": 60}}]'
                    await ws.send(sub_payload)

                # 3. Stream and parse live incoming messages
                while True:
                    msg = await ws.recv()
                    if msg.startswith("42"):
                        raw_json = msg[2:]
                        data = json.loads(raw_json)
                        
                        # Handle live candle updates from Quotex
                        if isinstance(data, list) and len(data) >= 2:
                            event_type, payload = data[0], data[1]
                            
                            if event_type == "chart_data":
                                symbol = payload.get("symbol")
                                pair_name = SYMBOL_MAP.get(symbol)
                                candles = payload.get("candles", [])
                                
                                if pair_name and candles:
                                    formatted = []
                                    for c in candles[-50:]:
                                        formatted.append({
                                            "time": c.get("time", int(time.time())),
                                            "open": float(c.get("open")),
                                            "high": float(c.get("high")),
                                            "low": float(c.get("low")),
                                            "close": float(c.get("close")),
                                            "payout": int(payload.get("payout", 85))
                                        })
                                    CANDLE_STORE[pair_name] = formatted

                    elif msg == "2":
                        # Engine.io heartbeat keep-alive
                        await ws.send("3")

        except Exception as e:
            print(f"Quotex stream disconnected: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)

@app.on_event("startup")
async def startup_event():
    # Spin off persistent stream in the background on Render startup
    asyncio.create_task(quotex_feed_listener())

@app.get("/candles")
def get_candles():
    return CANDLE_STORE
