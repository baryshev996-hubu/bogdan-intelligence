"""Binance USD-M Futures order-flow monitor.

Uses public partial depth snapshots and aggregate trades. It never places orders.
"""
import asyncio
import json
import logging
import os
import time
from collections import defaultdict, deque
from html import escape

import websockets

log = logging.getLogger("bogdan-intel.order-flow")

MIN_WALL_USD = float(os.getenv("ORDER_FLOW_MIN_WALL_USD", "750000"))
MIN_PERSISTENCE = float(os.getenv("ORDER_FLOW_MIN_PERSISTENCE_SEC", "5"))
MAX_DISTANCE_PCT = float(os.getenv("ORDER_FLOW_MAX_DISTANCE_PCT", "0.35"))
MIN_AGGRESSOR_USD = float(os.getenv("ORDER_FLOW_MIN_AGGRESSOR_USD", "250000"))
COOLDOWN = int(os.getenv("ORDER_FLOW_ALERT_COOLDOWN", "900"))
FLOW_WINDOW = 10
MISSING_GRACE = 1.5
latest_order_flow = {}


def get_order_flow_confirmation(symbol, side, max_age=2.0):
    """Return a fresh, side-aligned wall/absorption snapshot or None."""
    row = latest_order_flow.get(str(symbol).upper())
    if not row or time.time() - row.get("ts", 0) > max_age:
        return None
    if row.get("side") != str(side).upper():
        return None
    return dict(row)


def _fmt_price(value):
    return format(value, ",.8f").rstrip("0").rstrip(".")


def _fmt_usd(value):
    return "$" + format(value, ",.0f")


def format_order_flow_alert(symbol, side, level, notional, distance, age, buy_usd, sell_usd):
    """Format a possible absorption event without implying a trade entry."""
    bid = side == "BID"
    context = "LONG" if bid else "SHORT"
    icon = "🟢" if bid else "🔴"
    wall = "BID-СТЕНА" if bid else "ASK-СТЕНА"
    absorption = "продаж" if bid else "покупок"
    total = buy_usd + sell_usd
    separator = "━━━━━━━━━━━━━━━━━━"
    lines = [
        f"🧭 ORDER FLOW INTELLIGENCE | {escape(str(symbol))}",
        "",
        f"<b>{escape(str(symbol))} | ORDER FLOW</b>",
        f"🟡 {wall} + возможное поглощение {absorption}",
        f"{icon} <b>{context}-КОНТЕКСТ</b>",
        separator,
        "<i>ОБЪЁМ ЛИМИТНОЙ ЗАЯВКИ</i>",
        f"<b>{_fmt_usd(notional)}</b>",
        f"📍 Уровень: <code>{_fmt_price(level)}</code>",
        f"Расстояние до цены: {distance:.3f}%",
        f"В стакане: {age:.1f} сек.",
        separator,
        f"📊 <b>Агрессивные исполнения · {FLOW_WINDOW}с</b>",
        f"🟢 BUY: <b>{_fmt_usd(buy_usd)}</b>",
        f"🔴 SELL: <b>{_fmt_usd(sell_usd)}</b>",
    ]
    if total > 0:
        share = 100 * buy_usd / total
        blocks = max(0, min(10, int(share / 10 + 0.5)))
        lines.extend([
            "🟩" * blocks + "🟥" * (10 - blocks),
            f"Покупки {share:.1f}% | Продажи {100-share:.1f}%",
        ])
    lines.extend([
        separator,
        "🧠 <b>Интерпретация рынка</b>",
        ("Покупатели удерживают наблюдаемый BID-уровень на фоне агрессивных продаж."
         if bid else "Продавцы удерживают наблюдаемый ASK-уровень на фоне агрессивных покупок."),
        f"🟡 Возможное поглощение {absorption}. Разворот цены не подтверждён.",
        "",
        "⚠️ Дождаться реакции цены и подтверждения структуры. Не входить только по стене.",
        separator,
        "<i>Binance USDⓈ-M Futures | Стакан + исполнения</i>",
        "<i>Режим: наблюдение, не торговый сигнал.</i>",
    ])
    return "\n".join(lines)


def _levels(rows):
    result = []
    for row in rows or []:
        try:
            price, qty = float(row[0]), float(row[1])
            if price > 0 and qty > 0:
                result.append((price, qty, price * qty))
        except (TypeError, ValueError, IndexError):
            continue
    return result


async def merged_events(depth, fills):
    streams = {depth: "depth", fills: "aggTrade"}
    pending = {asyncio.create_task(ws.recv()): ws for ws in streams}
    counts = {name: 0 for name in streams.values()}
    report_at = time.monotonic() + 60
    try:
        while True:
            ready, _ = await asyncio.wait(pending, timeout=30, return_when=asyncio.FIRST_COMPLETED)
            if not ready:
                raise TimeoutError("order flow streams idle")
            for task in ready:
                ws = pending.pop(task)
                raw = task.result()
                counts[streams[ws]] += 1
                pending[asyncio.create_task(ws.recv())] = ws
                yield raw
            if time.monotonic() >= report_at:
                log.info("ORDER FLOW HEALTH | depth=%d | aggTrade=%d", counts["depth"], counts["aggTrade"])
                if not all(counts.values()):
                    raise TimeoutError("one order flow stream idle")
                counts = {name: 0 for name in streams.values()}
                report_at = time.monotonic() + 60
    finally:
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)


async def run_order_flow_monitor(session, symbols):
    """Watch persistent large walls and aggressive trade flow on Binance Futures."""
    watch = sorted({str(s).upper() for s in (symbols or []) if str(s).upper().endswith("USDT")})
    if not watch:
        log.warning("ORDER FLOW disabled: no USDT perpetual symbols")
        return

    base = "wss://fstream.binance.com"
    depth_url = base + "/public/stream?streams=" + "/".join(s.lower() + "@depth20@100ms" for s in watch)
    trade_url = base + "/market/stream?streams=" + "/".join(s.lower() + "@aggTrade" for s in watch)
    options = dict(ping_interval=20, ping_timeout=60, close_timeout=10, open_timeout=20, max_size=2**20)
    cooldowns = {}
    while True:
        walls = {}
        trades = defaultdict(deque)
        latest_order_flow.clear()
        try:
            async with websockets.connect(depth_url, **options) as depth, websockets.connect(trade_url, **options) as fills:
                log.info("ORDER FLOW connected | symbols=%d", len(watch))
                async for raw in merged_events(depth, fills):
                    packet = json.loads(raw)
                    data = packet.get("data", packet)
                    event = data.get("e")
                    symbol = str(data.get("s", "")).upper()
                    if symbol not in watch:
                        continue
                    now = time.time()

                    event_time = float(data.get("E", now * 1000)) / 1000
                    if now - event_time > 2:
                        continue
                    if event == "aggTrade":
                        try:
                            price = float(data.get("p", 0))
                            qty = float(data.get("q", 0))
                            usd = price * qty
                            # m=true means buyer was maker, so the aggressor was a seller.
                            side = "SELL" if data.get("m") else "BUY"
                            trades[symbol].append((event_time, side, usd))
                            while trades[symbol] and now - trades[symbol][0][0] > FLOW_WINDOW:
                                trades[symbol].popleft()
                        except (TypeError, ValueError):
                            continue
                        continue

                    if event not in ("depthUpdate", None) or "b" not in data or "a" not in data:
                        continue

                    bids = _levels(data.get("b"))
                    asks = _levels(data.get("a"))
                    if not bids or not asks:
                        continue
                    best_bid = max(x[0] for x in bids)
                    best_ask = min(x[0] for x in asks)
                    mid = (best_bid + best_ask) / 2
                    if mid <= 0:
                        continue

                    while trades[symbol] and now - trades[symbol][0][0] > FLOW_WINDOW:
                        trades[symbol].popleft()
                    current = {}
                    for side, levels in (("BID", bids), ("ASK", asks)):
                        for price, qty, notional in levels:
                            distance = abs(price - mid) / mid * 100
                            if notional >= MIN_WALL_USD and distance <= MAX_DISTANCE_PCT:
                                key = (symbol, side, price)
                                current[key] = (notional, distance)

                    # Track persistent walls and send a single initial observation.
                    for key, (notional, distance) in current.items():
                        state = walls.get(key)
                        if state is None:
                            state = {"first": now, "last": now, "max_usd": notional,
                                     "watch_sent": False, "flow_sent": False}
                            walls[key] = state
                        state["last"] = now
                        state["max_usd"] = max(state["max_usd"], notional)
                        age = now - state["first"]
                        if age < MIN_PERSISTENCE or state["watch_sent"]:
                            continue

                        side = key[1]
                        recent = trades[symbol]
                        buy_usd = sum(v for ts, taker, v in recent if taker == "BUY")
                        sell_usd = sum(v for ts, taker, v in recent if taker == "SELL")
                        opposing = sell_usd if side == "BID" else buy_usd
                        supporting = buy_usd if side == "BID" else sell_usd
                        absorption = (
                            opposing >= MIN_AGGRESSOR_USD
                            and opposing >= max(1.0, supporting) * 1.4
                        )
                        action = "ПОТЕНЦИАЛЬНАЯ ЗОНА СПРОСА" if side == "BID" else "ПОТЕНЦИАЛЬНАЯ ЗОНА ПРЕДЛОЖЕНИЯ"
                        confirmation = ""
                        # A wall alone is not actionable. Alert only when aggressive
                        # opposite-side executions show meaningful absorption.
                        if not absorption:
                            continue
                        action = "ВОЗМОЖНОЕ ПОГЛОЩЕНИЕ ПРОДАЖ, LONG-КОНТЕКСТ" if side == "BID" else "ВОЗМОЖНОЕ ПОГЛОЩЕНИЕ ПОКУПОК, SHORT-КОНТЕКСТ"
                        confirmation = (
                            "\nАгрессивные BUY за 10с: " + _fmt_usd(buy_usd)
                            + "\nАгрессивные SELL за 10с: " + _fmt_usd(sell_usd)
                        )
                        alert_key = "side:" + symbol + ":" + side
                        if now - cooldowns.get(alert_key, 0) < COOLDOWN:
                            continue
                        cooldowns[alert_key] = now
                        state["watch_sent"] = True
                        state["flow_sent"] = True
                        state["alerted_at"] = now
                        await _send(session, format_order_flow_alert(
                            symbol, side, key[2], notional, distance, age, buy_usd, sell_usd
                        ))

                    # Publish a fresh side-specific snapshot for the isolated PAPER experiment.
                    buy_usd = sum(v for ts, taker, v in trades[symbol] if taker == "BUY")
                    sell_usd = sum(v for ts, taker, v in trades[symbol] if taker == "SELL")
                    candidates = []
                    for key, (notional, distance) in current.items():
                        state = walls.get(key, {})
                        age = now - state.get("first", now)
                        if age < MIN_PERSISTENCE:
                            continue
                        wall_side = key[1]
                        opposing = sell_usd if wall_side == "BID" else buy_usd
                        supporting = buy_usd if wall_side == "BID" else sell_usd
                        if opposing < MIN_AGGRESSOR_USD or opposing < max(1.0, supporting) * 1.4:
                            continue
                        trade_side = "LONG" if wall_side == "BID" else "SHORT"
                        imbalance = opposing / max(1.0, supporting)
                        strength = min(100.0, 55.0 + min(20.0, (notional / MIN_WALL_USD - 1) * 8)
                                       + min(15.0, (age - MIN_PERSISTENCE) * 1.5)
                                       + min(10.0, (imbalance - 1.4) * 8))
                        candidates.append((strength, {
                            "ts": now, "side": trade_side, "wall_side": wall_side,
                            "wall_price": key[2], "wall_usd": notional,
                            "distance_pct": distance, "persistence_sec": age,
                            "buy_usd_10s": buy_usd, "sell_usd_10s": sell_usd,
                            "strength": strength, "mid": mid,
                        }))
                    aligned = [row for _, row in candidates]
                    if aligned:
                        latest_order_flow[symbol] = max(aligned, key=lambda row: row["strength"])
                    else:
                        latest_order_flow.pop(symbol, None)

                    # If a tracked level leaves the visible top-20, invalidate the watch.
                    for key, state in list(walls.items()):
                        if key[0] != symbol or key in current:
                            continue
                        if now - state["last"] < MISSING_GRACE:
                            continue
                        # A disappearing top-20 level is not sent separately.
                        del walls[key]

        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("ORDER FLOW stream disconnected; reconnecting")
            await asyncio.sleep(5)


async def _send(session, message):
    from app.mail import telegram_send
    await telegram_send(session, message)
