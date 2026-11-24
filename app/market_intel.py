"""Public market-intelligence monitors. Only public market data is used."""
import asyncio
import json
import logging
import os
import time
from collections import defaultdict, deque
import websockets

log = logging.getLogger("bogdan-intel.market")
SYMBOLS = [s.strip().upper() for s in os.getenv("BINANCE_SYMBOLS", "btcusdt,ethusdt,solusdt,xrpusdt").split(",") if s.strip()]
COINS = [s.strip().upper() for s in os.getenv("HYPERLIQUID_COINS", "BTC,ETH,SOL,XRP").split(",") if s.strip()]
WHALE_USD = float(os.getenv("WHALE_TRADE_USD", "250000"))
OI_MOVE_PCT = float(os.getenv("OI_MOVE_ALERT_PCT", "1.5"))
COOLDOWN = int(os.getenv("INTEL_ALERT_COOLDOWN", "900"))

class Gate:
    def __init__(self):
        self.last = {}
    def allow(self, key):
        now = time.time()
        if now - self.last.get(key, 0) < COOLDOWN:
            return False
        self.last[key] = now
        return True

async def send(session, message):
    from app.mail import telegram_send
    await telegram_send(session, message)

async def derivatives_monitor(session, symbols=None):
    watch_symbols = [s.upper() for s in symbols] if symbols else SYMBOLS
    gate = Gate()
    history = defaultdict(lambda: deque(maxlen=7))
    base = "https://fapi.binance.com"
    while True:
        for symbol in watch_symbols:
            try:
                async with session.get(base + "/fapi/v1/openInterest", params={"symbol": symbol}, timeout=12) as r:
                    r.raise_for_status()
                    oi = float((await r.json())["openInterest"])
                async with session.get(base + "/fapi/v1/premiumIndex", params={"symbol": symbol}, timeout=12) as r:
                    r.raise_for_status()
                    premium = await r.json()
                funding = float(premium.get("lastFundingRate", 0))
                mark = float(premium.get("markPrice", 0))
                hist = history[symbol]
                hist.append((time.time(), oi))
                if len(hist) >= 2:
                    old_time, old_oi = hist[0]
                    elapsed = time.time() - old_time
                    if elapsed >= 240 and old_oi > 0:
                        change = (oi / old_oi - 1) * 100
                        if abs(change) >= OI_MOVE_PCT and gate.allow("oi:" + symbol):
                            await send(session,
                                "🐋 DERIVATIVES WATCH | " + symbol + "\n"
                                + "Open Interest: " + format(change, "+.2f") + "% за " + format(elapsed / 60, ".1f") + " мин.\n"
                                + "Mark: " + str(mark) + " | Funding: " + format(funding * 100, ".4f") + "%\n"
                                + "OI показывает изменение открытых позиций, но не их направление.")
                if abs(funding) >= 0.0008 and gate.allow("funding:" + symbol + (":pos" if funding > 0 else ":neg")):
                    bias = "лонги платят шортам" if funding > 0 else "шорты платят лонгам"
                    await send(session, "⚠️ FUNDING EXTREME | " + symbol + "\nFunding: "
                        + format(funding * 100, ".4f") + "% (" + bias + ")\n"
                        + "Маркер перегруженности, не самостоятельный сигнал.")
                await asyncio.sleep(0.15)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Binance derivatives monitor failed: %s", symbol)
        await asyncio.sleep(60)

async def hyperliquid_whale_monitor(session, coins=None):
    watch_coins = [s.upper() for s in coins] if coins else COINS
    if not watch_coins:
        return
    gate = Gate()
    url = "wss://api.hyperliquid.xyz/ws"
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=60, close_timeout=10, open_timeout=20) as ws:
                for coin in watch_coins:
                    await ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "trades", "coin": coin}}))
                log.info("Hyperliquid public trade tape connected: %s", watch_coins)
                async for raw in ws:
                    packet = json.loads(raw)
                    if packet.get("channel") != "trades":
                        continue
                    trades = packet.get("data", [])
                    if isinstance(trades, dict):
                        trades = [trades]
                    for trade in trades:
                        try:
                            coin = str(trade.get("coin", ""))
                            side = str(trade.get("side", "?"))
                            price = float(trade.get("px", 0))
                            size = float(trade.get("sz", 0))
                            usd = price * size
                            if usd < WHALE_USD:
                                continue
                            key = "trade:" + str(trade.get("hash")) + ":" + coin + ":" + side
                            if not gate.allow(key):
                                continue
                            users = trade.get("users") or []
                            parties = ", ".join(str(x)[:10] for x in users[:2]) or "н/д"
                            side_text = "BUY" if side == "B" else "SELL" if side == "A" else side
                            await send(session,
                                "🐳 LARGE EXECUTION | Hyperliquid | " + coin + "\n"
                                + "Taker: " + side_text + "\nЦена: " + str(price) + "\n"
                                + "Размер: " + str(size) + " " + coin + "\n"
                                + "Номинал: $" + format(usd, ",.0f") + "\n"
                                + "Обрезанные адреса участников: " + parties + "\n"
                                + "Публичная крупная сделка. Это не доказательство инсайда и не сигнал копировать.")
                        except (TypeError, ValueError):
                            log.debug("Malformed Hyperliquid trade: %r", trade)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Hyperliquid stream disconnected; reconnecting")
            await asyncio.sleep(10)

async def run_market_intelligence(session, symbols=None):
    selected = [s.upper() for s in symbols] if symbols else SYMBOLS
    # Hyperliquid's public trade channel uses its own coin universe.
    # Keep the verified major-coin list separate from Binance-discovered contracts.
    await asyncio.gather(
        derivatives_monitor(session, selected),
        hyperliquid_whale_monitor(session, COINS),
    )
