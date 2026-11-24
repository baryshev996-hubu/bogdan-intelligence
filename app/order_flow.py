"""Binance USD-M Futures order-flow monitor.

Uses public partial depth snapshots and aggregate trades. It never places orders.
"""
import asyncio
import json
import logging
import os
import time
from collections import defaultdict, deque

import websockets

log = logging.getLogger("bogdan-intel.order-flow")

MIN_WALL_USD = float(os.getenv("ORDER_FLOW_MIN_WALL_USD", "500000"))
MIN_PERSISTENCE = float(os.getenv("ORDER_FLOW_MIN_PERSISTENCE_SEC", "3"))
MAX_DISTANCE_PCT = float(os.getenv("ORDER_FLOW_MAX_DISTANCE_PCT", "0.70"))
MIN_AGGRESSOR_USD = float(os.getenv("ORDER_FLOW_MIN_AGGRESSOR_USD", "150000"))
COOLDOWN = int(os.getenv("ORDER_FLOW_ALERT_COOLDOWN", "600"))
FLOW_WINDOW = 10
MISSING_GRACE = 1.5


def _fmt_price(value):
    return format(value, ",.8f").rstrip("0").rstrip(".")


def _fmt_usd(value):
    return "$" + format(value, ",.0f")


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


async def run_order_flow_monitor(session, symbols):
    """Watch persistent large walls and aggressive trade flow on Binance Futures."""
    watch = sorted({str(s).upper() for s in (symbols or []) if str(s).upper().endswith("USDT")})
    if not watch:
        log.warning("ORDER FLOW disabled: no USDT perpetual symbols")
        return

    streams = []
    for symbol in watch:
        base = symbol.lower()
        streams.extend((base + "@depth20@100ms", base + "@aggTrade"))
    url = "wss://fstream.binance.com/market/stream?streams=" + "/".join(streams)

    while True:
        walls = {}
        trades = defaultdict(lambda: deque())
        cooldowns = {}
        try:
            async with websockets.connect(
                url, ping_interval=20, ping_timeout=60, close_timeout=10,
                open_timeout=20, max_size=2**20
            ) as ws:
                log.info("ORDER FLOW connected to Binance USD-M Futures: %s symbols", len(watch))
                async for raw in ws:
                    packet = json.loads(raw)
                    data = packet.get("data", packet)
                    event = data.get("e")
                    symbol = str(data.get("s", "")).upper()
                    if symbol not in watch:
                        continue
                    now = time.time()

                    if event == "aggTrade":
                        try:
                            price = float(data.get("p", 0))
                            qty = float(data.get("q", 0))
                            usd = price * qty
                            # m=true means buyer was maker, so the aggressor was a seller.
                            side = "SELL" if data.get("m") else "BUY"
                            trades[symbol].append((now, side, usd))
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
                        recent = list(trades[symbol])
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
                        if absorption:
                            action = "ВОЗМОЖНОЕ ПОГЛОЩЕНИЕ ПРОДАЖ, СЛЕДИТЬ ЗА LONG" if side == "BID" else "ВОЗМОЖНОЕ ПОГЛОЩЕНИЕ ПОКУПОК, СЛЕДИТЬ ЗА SHORT"
                            confirmation = (
                                "\nАгрессивные BUY за 10с: " + _fmt_usd(buy_usd)
                                + "\nАгрессивные SELL за 10с: " + _fmt_usd(sell_usd)
                            )
                            state["flow_sent"] = True

                        alert_key = "wall:" + symbol + ":" + side + ":" + str(round(key[2], 8))
                        if now - cooldowns.get(alert_key, 0) < COOLDOWN:
                            state["watch_sent"] = True
                            continue
                        cooldowns[alert_key] = now
                        state["watch_sent"] = True
                        state["alerted_at"] = now
                        await _send(session,
                            "🧭 ORDER FLOW INTELLIGENCE | " + symbol + "\n"
                            + "Событие: " + ("КРУПНАЯ BID-СТЕНА" if side == "BID" else "КРУПНАЯ ASK-СТЕНА") + "\n"
                            + "Уровень: " + _fmt_price(key[2]) + "\n"
                            + "Объём заявки: " + _fmt_usd(notional) + "\n"
                            + "Расстояние до цены: " + format(distance, ".3f") + "%\n"
                            + "В стакане держится: " + format(age, ".1f") + " сек.\n"
                            + "Оценка: " + action
                            + confirmation
                            + "\nУсловие: дождаться реакции цены и подтверждения структуры. Не входить только по стене."
                        )

                    # If a tracked level leaves the visible top-20, invalidate the watch.
                    for key, state in list(walls.items()):
                        if key[0] != symbol or key in current:
                            continue
                        if now - state["last"] < MISSING_GRACE:
                            continue
                        if state.get("watch_sent"):
                            await _send(session,
                                "🧭 ORDER FLOW INTELLIGENCE | " + symbol + "\n"
                                + "Событие: УРОВЕНЬ БОЛЬШЕ НЕ ВИДЕН В TOP-20\n"
                                + "Сторона: " + key[1] + "\n"
                                + "Уровень: " + _fmt_price(key[2]) + "\n"
                                + "Последний объём: " + _fmt_usd(state["max_usd"]) + "\n"
                                + "Статус: прежняя зона больше не подтверждается стаканом. Пересмотреть вход/удержание позиции."
                            )
                        del walls[key]

        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("ORDER FLOW stream disconnected; reconnecting")
            await asyncio.sleep(5)


async def _send(session, message):
    from app.mail import telegram_send
    await telegram_send(session, message)
