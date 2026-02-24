import asyncio
import json
from collections import defaultdict
from datetime import datetime, timezone, timedelta
import os
import websockets
from questdb.ingress import Sender

# ================== ตั้งค่า ==================
HOST = os.getenv("QUESTDB_HOST", "metro.proxy.rlwy.net")
PORT = os.getenv("QUESTDB_PORT", "25708")
BANGKOK_TZ = timedelta(hours=7)
DEPTH = 5          # 5 levels (พอสำหรับ momentum)
AGG_INTERVAL = 30  # 30 วินาที (ตามที่คุณต้องการ)
# ===========================================

TICKERS = ['BTC', 'ETH', 'BNB', 'SOL', 'XRP', 'ADA', 'DOT', 'AVAX', 'LINK', 'LTC', 'BCH', 'TRX', 'MATIC', 'SUI', 'APT', 'NEAR', 'TIA', 'ATOM', 'ALGO', 'STX', 'EGLD', 'KAS', 'USDT', 'USDC', 'FDUSD', 'DAI', 'FRAX', 'RLUSD', 'DOGE', 'SHIB', 'PEPE', 'FLOKI', 'BONK', 'WIF', 'MEW', 'NEIRO', 'BRETT', 'FARTCOIN', 'POPCAT', 'MOODENG', 'GOAT', 'FET', 'TAO', 'RENDER', 'HYPE', 'AKT', 'AR', 'FIL', 'GRT', 'ICP', 'THETA', 'LPT', 'JASMY', 'IO', 'NOS', 'PLANCK', 'AAVE', 'UNI', 'SUSHI', 'CRV', 'MKR', 'DYDX', 'SNX', '1INCH', 'PYTH', 'API3', 'JUP', 'ENA', 'PENDLE', 'RAY', 'BREV', 'ZENT', 'ATH', 'CGPT', 'COOKIE', 'ZKC', 'KAITO', 'MORPHO', 'SOMI', 'TURTLE', 'SENT', 'HYPER', 'ZAMA', 'GALA', 'AXS', 'SAND', 'MANA', 'ILV', 'IMX', 'BEAM']

SYMBOLS = [f"{t}USDT" for t in TICKERS if t not in ['USDT','USDC','DAI','FDUSD','FRAX','RLUSD']]
SYMBOLS = list(dict.fromkeys(SYMBOLS))

buffer = defaultdict(lambda: {'bids': [], 'asks': []})
last_flush = datetime.now(timezone.utc) + BANGKOK_TZ

async def handle_l2(data):
    global last_flush
    symbol = data['s']
    bids = [[float(p), float(q)] for p, q in data['b'][:DEPTH]]
    asks = [[float(p), float(q)] for p, q in data['a'][:DEPTH]]

    now = datetime.now(timezone.utc) + BANGKOK_TZ
    ts_floor = now.replace(microsecond=0)

    buffer[symbol] = {'bids': bids, 'asks': asks}

    if (now - last_flush).total_seconds() >= AGG_INTERVAL:
        await flush_to_questdb(ts_floor)
        last_flush = now
        buffer.clear()

async def flush_to_questdb(ts):
    try:
        with Sender.from_conf(f"tcp::addr={HOST}:{PORT};") as sender:
            for symbol, book in buffer.items():
                if not book: continue
                bids_price, bids_qty = zip(*book['bids']) if book['bids'] else ([], [])
                asks_price, asks_qty = zip(*book['asks']) if book['asks'] else ([], [])

                sender.row(
                    'book_l2',
                    symbols={'symbol': symbol},
                    columns={
                        'bids_price': list(bids_price),
                        'bids_qty': list(bids_qty),
                        'asks_price': list(asks_price),
                        'asks_qty': list(asks_qty)
                    },
                    at=ts
                )
            sender.flush()
            print(f"✅ L2 Flushed {len(buffer)} symbols @ {ts} (Bangkok time, 30s agg)")
    except Exception as e:
        print(f"❌ L2 Flush error: {e}")

async def main():
    stream_names = [f"{s.lower()}@depth{DEPTH}" for s in SYMBOLS]
    url = f"wss://fstream.binance.com/stream?streams={'/'.join(stream_names)}"

    print(f"🚀 เริ่มดึง L2 ({DEPTH} levels) agg ทุก {AGG_INTERVAL} วินาที สำหรับ {len(SYMBOLS)} symbols")

    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                print("L2 WebSocket connected")
                while True:
                    msg = await ws.recv()
                    data = json.loads(msg)
                    if 'data' in data:
                        await handle_l2(data['data'])
        except Exception as e:
            print(f"L2 WebSocket disconnected: {e}. Reconnecting in 5s...")
            await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())
