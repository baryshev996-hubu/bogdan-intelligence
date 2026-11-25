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
WHALE_USD = float(os.getenv("WHALE_TRADE_USD", "500000"))
WHALE_WINDOW_SECONDS = max(10, int(os.getenv("WHALE_FLOW_WINDOW_SECONDS", "30")))
WHALE_COOLDOWN = max(60, int(os.getenv("WHALE_FLOW_COOLDOWN_SECONDS", "180")))
ORDER_FLOW_DEFAULT = "BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,XRPUSDT,DOGEUSDT,ADAUSDT,LINKUSDT,TRXUSDT,SUIUSDT,AVAXUSDT,1000PEPEUSDT"
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

async def get_binance_flow_context(session, coin, side, reference_price):
    """Confirm a Hyperliquid taker-flow burst against Binance Futures price/OI data."""
    symbol = str(coin).upper() + "USDT"
    base = "https://fapi.binance.com"
    result = {
        "symbol": symbol, "price_change_1m": None, "price_change_5m": None,
        "binance_price": None, "oi_change_5m": None, "funding": None,
        "reaction": "НЕТ ПОДТВЕРЖДЕНИЯ", "details": [],
    }
    async def get_json(path, params):
        async with session.get(base + path, params=params, timeout=10) as response:
            response.raise_for_status()
            return await response.json(content_type=None)
    try:
        rows = await get_json("/fapi/v1/klines", {"symbol": symbol, "interval": "1m", "limit": 6})
        closed = [row for row in rows if int(row[6]) < int(time.time() * 1000)]
        if len(closed) >= 2:
            first = float(closed[-2][4])
            last = float(closed[-1][4])
            if first > 0:
                result["price_change_1m"] = (last / first - 1) * 100
            if len(closed) >= 6:
                first5 = float(closed[-6][4])
                if first5 > 0:
                    result["price_change_5m"] = (last / first5 - 1) * 100
            result["binance_price"] = last
    except Exception as exc:
        log.warning("FLOW CONTEXT KLINES FAILED | %s | %s", symbol, str(exc)[:120])
    try:
        premium = await get_json("/fapi/v1/premiumIndex", {"symbol": symbol})
        result["funding"] = float(premium.get("lastFundingRate", 0))
        result["binance_price"] = result["binance_price"] or float(premium.get("markPrice", 0))
    except Exception as exc:
        log.warning("FLOW CONTEXT PREMIUM FAILED | %s | %s", symbol, str(exc)[:120])
    try:
        oi_rows = await get_json("/futures/data/openInterestHist", {
            "symbol": symbol, "period": "5m", "limit": 2
        })
        if len(oi_rows) >= 2:
            old_oi = float(oi_rows[0].get("sumOpenInterest", 0))
            new_oi = float(oi_rows[-1].get("sumOpenInterest", 0))
            if old_oi > 0:
                result["oi_change_5m"] = (new_oi / old_oi - 1) * 100
    except Exception as exc:
        log.warning("FLOW CONTEXT OI FAILED | %s | %s", symbol, str(exc)[:120])

    p1 = result["price_change_1m"]
    p5 = result["price_change_5m"]
    px = result["binance_price"]
    vwap = float(reference_price or 0)
    if side == "SELL":
        confirmed = p1 is not None and p5 is not None and p1 <= -0.08 and p5 < 0 and (not px or not vwap or px < vwap)
        absorbed = p1 is not None and p1 >= 0.08 and (not px or not vwap or px > vwap)
        if confirmed:
            result["reaction"] = "ПРОДАВЦЫ ПОЛУЧАЮТ ПОДТВЕРЖДЕНИЕ"
        elif absorbed:
            result["reaction"] = "ВОЗМОЖНОЕ ПОГЛОЩЕНИЕ ПРОДАЖ"
    else:
        confirmed = p1 is not None and p5 is not None and p1 >= 0.08 and p5 > 0 and (not px or not vwap or px > vwap)
        absorbed = p1 is not None and p1 <= -0.08 and (not px or not vwap or px < vwap)
        if confirmed:
            result["reaction"] = "ПОКУПАТЕЛИ ПОЛУЧАЮТ ПОДТВЕРЖДЕНИЕ"
        elif absorbed:
            result["reaction"] = "ВОЗМОЖНОЕ ПОГЛОЩЕНИЕ ПОКУПОК"
    return result


def format_flow_context(context):
    def pct(value):
        return "н/д" if value is None else format(value, "+.3f") + "%"
    lines = [
        "Binance Futures 1m: " + pct(context.get("price_change_1m")),
        "Binance Futures 5m: " + pct(context.get("price_change_5m")),
        "Реакция рынка: " + context.get("reaction", "НЕТ ПОДТВЕРЖДЕНИЯ"),
    ]
    if context.get("oi_change_5m") is not None:
        lines.append("Open Interest 5m: " + pct(context["oi_change_5m"]))
    if context.get("funding") is not None:
        lines.append("Funding: " + format(context["funding"] * 100, "+.4f") + "%")
    return "\n".join(lines)


async def hyperliquid_whale_monitor(session, coins=None):
    """Aggregate large public taker flow to avoid one Telegram alert per print."""
    watch_coins = [s.upper() for s in (coins if coins is not None else COINS)]
    if not watch_coins:
        return
    url = "wss://api.hyperliquid.xyz/ws"
    while True:
        flow = defaultdict(lambda: deque())
        last_alert = {}
        try:
            async with websockets.connect(
                url, ping_interval=20, ping_timeout=60,
                close_timeout=10, open_timeout=20, max_size=2**20
            ) as ws:
                for coin in watch_coins:
                    await ws.send(json.dumps({
                        "method": "subscribe",
                        "subscription": {"type": "trades", "coin": coin}
                    }))
                log.info("HYPERLIQUID FLOW CONNECTED | coins=%s | aggregate_window=%ss | threshold=$%s",
                         watch_coins, WHALE_WINDOW_SECONDS, int(WHALE_USD))
                async for raw in ws:
                    packet = json.loads(raw)
                    if packet.get("channel") != "trades":
                        continue
                    trades = packet.get("data", [])
                    if isinstance(trades, dict):
                        trades = [trades]
                    now = time.time()
                    for trade in trades:
                        try:
                            coin = str(trade.get("coin", "")).upper()
                            side = "BUY" if trade.get("side") == "B" else "SELL" if trade.get("side") == "A" else ""
                            price = float(trade.get("px", 0))
                            size = float(trade.get("sz", 0))
                            usd = price * size
                            if coin not in watch_coins or not side or price <= 0 or usd < 25000:
                                continue
                            key = (coin, side)
                            bucket = flow[key]
                            bucket.append((now, usd, price))
                            while bucket and now - bucket[0][0] > WHALE_WINDOW_SECONDS:
                                bucket.popleft()
                            total = sum(row[1] for row in bucket)
                            if total < WHALE_USD or now - last_alert.get(key, 0) < WHALE_COOLDOWN:
                                continue
                            largest = max(row[1] for row in bucket)
                            avg_price = sum(row[1] * row[2] for row in bucket) / max(total, 1)
                            last_alert[key] = now
                            context = await get_binance_flow_context(session, coin, side, avg_price)
                            alert_tier = (
                                "🟢 FLOW CONFIRMED" if "ПОЛУЧАЮТ ПОДТВЕРЖДЕНИЕ" in context["reaction"]
                                else "🟡 POSSIBLE ABSORPTION" if "ПОГЛОЩЕНИЕ" in context["reaction"]
                                else "⚪ RAW FLOW, NO CONFIRMATION"
                            )
                            await send(session,
                                "🐳 BOGDAN INTELLIGENCE | " + alert_tier + " | " + coin + "\n"
                                + "Направление потока: " + side + (" 🟢" if side == "BUY" else " 🔴") + "\n"
                                + "Объём за " + str(WHALE_WINDOW_SECONDS) + " сек.: $" + format(total, ",.0f") + "\n"
                                + "Крупнейшее исполнение: $" + format(largest, ",.0f") + "\n"
                                + "VWAP потока: $" + format(avg_price, ",.6f").rstrip("0").rstrip(".") + "\n"
                                + "Количество исполнений: " + str(len(bucket)) + "\n\n"
                                + "ПОДТВЕРЖДЕНИЕ BINANCE FUTURES\n"
                                + format_flow_context(context) + "\n\n"
                                + "Как читать: подтверждение = цена движется в сторону taker-потока; поглощение = цена идёт против потока; без подтверждения = наблюдать, не входить.\n"
                                + "Это аналитическое уведомление, не торговый сигнал. Hyperliquid показывает только одну площадку."
                            )
                            log.info("HYPERLIQUID FLOW ALERT | %s %s | usd=%.0f | prints=%d",
                                     coin, side, total, len(bucket))
                        except (TypeError, ValueError):
                            log.debug("Malformed Hyperliquid trade: %r", trade)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Hyperliquid stream disconnected; reconnecting in 10s")
            await asyncio.sleep(10)

async def run_market_intelligence(session, symbols=None):
    # Keep whale executions and order-book intelligence as independent monitors.
    from app.order_flow import run_order_flow_monitor
    tasks = [hyperliquid_whale_monitor(session, COINS)]
    if os.getenv("ENABLE_ORDER_FLOW", "true").lower() == "true":
        requested = [s.strip().upper() for s in os.getenv("ORDER_FLOW_SYMBOLS", ORDER_FLOW_DEFAULT).split(",") if s.strip()]
        available = set(symbols or SYMBOLS)
        order_flow_symbols = [s for s in requested if s in available]
        if not order_flow_symbols:
            order_flow_symbols = (symbols or SYMBOLS)[:12]
        tasks.append(run_order_flow_monitor(session, order_flow_symbols))
    await asyncio.gather(*tasks)
