"""Historical, conservative backtest for the live Futures signal rules.

Diagnostic only. It never places orders or changes account state.
"""
import asyncio
import bisect
import logging
import os
import time
from collections import defaultdict

import aiohttp

from app.signal_engine import INTERVALS, evaluate


log = logging.getLogger("bogdan-backtest")
SYMBOLS = [s.strip().upper() for s in os.getenv(
    "BINANCE_SYMBOLS", "btcusdt,ethusdt,solusdt,xrpusdt"
).split(",") if s.strip()]
LIMIT = 1500
HORIZON_BARS = 144  # 12 hours of 5m candles
SAME_SIDE_COOLDOWN_MS = 6 * 60 * 60 * 1000
FLIP_COOLDOWN_MS = 60 * 60 * 1000
TAKER_FEE_RATE = float(os.getenv("BACKTEST_TAKER_FEE", "0.0005"))
SLIPPAGE_RATE = float(os.getenv("BACKTEST_SLIPPAGE", "0.0001"))


async def fetch_candles(session, symbol, interval):
    url = "https://fapi.binance.com/fapi/v1/klines"
    params = {"symbol": symbol, "interval": interval, "limit": LIMIT}
    async with session.get(url, params=params, timeout=30) as response:
        response.raise_for_status()
        rows = await response.json(content_type=None)

    candles = []
    now_ms = int(time.time() * 1000)
    for row in rows:
        if int(row[6]) >= now_ms:
            continue
        candles.append({
            "open_time": int(row[0]),
            "open": float(row[1]),
            "high": float(row[2]),
            "low": float(row[3]),
            "close": float(row[4]),
            "volume": float(row[5]),
            "close_time": int(row[6]),
        })
    return candles


def simulate_trade(signal, future_candles):
    """Partial exits: 50% at 1R, 30% at 2R, 20% at 3R.
    After TP1 stop moves to entry; after TP2 it moves to TP1.
    If stop and target are inside the same candle, stop is assumed first.
    """
    if not future_candles:
        return None, None

    side = signal["side"]
    entry = signal["entry"]
    initial_stop = signal["stop"]
    risk = abs(entry - initial_stop)
    if risk <= 0:
        return None, None

    t1, t2, t3 = signal["targets"]
    remaining = 1.0
    realized_r = 0.0
    active_stop = initial_stop
    tp1_hit = tp2_hit = False

    for index, candle in enumerate(future_candles[:HORIZON_BARS]):
        if side == "LONG":
            if candle["low"] <= active_stop:
                realized_r += remaining * ((active_stop - entry) / risk)
                return realized_r, index
            if not tp1_hit and candle["high"] >= t1:
                realized_r += 0.5
                remaining -= 0.5
                tp1_hit = True
                active_stop = entry
                if candle["low"] <= active_stop:
                    return realized_r, index
            if tp1_hit and not tp2_hit and candle["high"] >= t2:
                realized_r += 0.3 * 2
                remaining -= 0.3
                tp2_hit = True
                active_stop = t1
                if candle["low"] <= active_stop:
                    realized_r += remaining * 1.0
                    return realized_r, index
            if tp2_hit and candle["high"] >= t3:
                realized_r += remaining * 3.0
                return realized_r, index
        else:
            if candle["high"] >= active_stop:
                realized_r += remaining * ((entry - active_stop) / risk)
                return realized_r, index
            if not tp1_hit and candle["low"] <= t1:
                realized_r += 0.5
                remaining -= 0.5
                tp1_hit = True
                active_stop = entry
                if candle["high"] >= active_stop:
                    return realized_r, index
            if tp1_hit and not tp2_hit and candle["low"] <= t2:
                realized_r += 0.3 * 2
                remaining -= 0.3
                tp2_hit = True
                active_stop = t1
                if candle["high"] >= active_stop:
                    realized_r += remaining * 1.0
                    return realized_r, index
            if tp2_hit and candle["low"] <= t3:
                realized_r += remaining * 3.0
                return realized_r, index

    last_price = future_candles[min(len(future_candles), HORIZON_BARS) - 1]["close"]
    direction = 1.0 if side == "LONG" else -1.0
    realized_r += remaining * direction * (last_price - entry) / risk
    return realized_r, min(len(future_candles), HORIZON_BARS) - 1


def backtest_symbol(symbol, data):
    five = data["5m"]
    fifteen = data["15m"]
    hourly = data["1h"]
    if min(len(five), len(fifteen), len(hourly)) < 100:
        log.warning("%s: insufficient historical candles, skipping", symbol)
        return []

    close15 = [c["close_time"] for c in fifteen]
    close1h = [c["close_time"] for c in hourly]
    results = []
    last_signal = None
    index = 59

    while index < len(five) - 1:
        candle = five[index]
        as_of = candle["close_time"]
        j15 = bisect.bisect_right(close15, as_of)
        j1h = bisect.bisect_right(close1h, as_of)
        if j15 < 60 or j1h < 60:
            index += 1
            continue

        signal = evaluate(symbol, {
            "5m": five[:index + 1],
            "15m": fifteen[:j15],
            "1h": hourly[:j1h],
        }, as_of_ms=as_of)
        if not signal:
            index += 1
            continue

        if last_signal:
            cooldown = (SAME_SIDE_COOLDOWN_MS
                        if last_signal["side"] == signal["side"]
                        else FLIP_COOLDOWN_MS)
            if as_of - last_signal["time"] < cooldown:
                index += 1
                continue

        future = five[index + 1:index + 1 + HORIZON_BARS]
        gross_r, exit_offset = simulate_trade(signal, future)
        if gross_r is None:
            break

        # Approximate round-trip taker fees and slippage, configurable per environment.
        cost_fraction = 2 * (TAKER_FEE_RATE + SLIPPAGE_RATE)
        cost_r = cost_fraction * signal["entry"] / abs(signal["entry"] - signal["stop"])
        net_r = gross_r - cost_r
        results.append({
            "side": signal["side"],
            "gross_r": gross_r,
            "net_r": net_r,
            "time": as_of,
        })
        last_signal = {"side": signal["side"], "time": as_of}
        index += exit_offset + 2

    return results


def report(all_results):
    trades = [trade for rows in all_results.values() for trade in rows]
    if not trades:
        log.info("BACKTEST: no qualifying signals in the loaded sample.")
        return

    wins = [t["net_r"] for t in trades if t["net_r"] > 0]
    losses = [t["net_r"] for t in trades if t["net_r"] < 0]
    total_r = sum(t["net_r"] for t in trades)
    avg_r = total_r / len(trades)
    profit_factor = sum(wins) / abs(sum(losses)) if losses else float("inf")

    equity = peak = max_drawdown = 0.0
    for trade in trades:
        equity += trade["net_r"]
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, peak - equity)

    log.info("BACKTEST COSTS | taker_fee_each_side=%.3f%% slippage_each_side=%.3f%%; funding excluded",
             TAKER_FEE_RATE * 100, SLIPPAGE_RATE * 100)
    log.info(
        "BACKTEST | trades=%s wins=%s losses=%s win_rate=%.1f%% "
        "avg_net_R=%.3f total_net_R=%.2f profit_factor=%.2f max_drawdown_R=%.2f",
        len(trades), len(wins), len(losses), 100 * len(wins) / len(trades),
        avg_r, total_r, profit_factor, max_drawdown
    )
    for symbol, rows in all_results.items():
        if not rows:
            log.info("BACKTEST | %s: no signals", symbol)
            continue
        symbol_wins = sum(1 for t in rows if t["net_r"] > 0)
        log.info("BACKTEST | %s: trades=%s win_rate=%.1f%% net_R=%.2f",
                 symbol, len(rows), 100 * symbol_wins / len(rows),
                 sum(t["net_r"] for t in rows))

    if len(trades) < 30:
        log.warning("BACKTEST SAMPLE TOO SMALL: fewer than 30 trades; do not treat metrics as reliable.")
    if avg_r <= 0:
        log.warning("BACKTEST FILTER: strategy did not show positive average net R in this sample.")


async def main():
    timeout = aiohttp.ClientTimeout(total=45)
    connector = aiohttp.TCPConnector(limit=12)
    all_results = defaultdict(list)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        for symbol in SYMBOLS:
            data = {}
            for interval in INTERVALS:
                try:
                    data[interval] = await fetch_candles(session, symbol, interval)
                    log.info("BACKTEST DATA | %s %s candles=%s",
                             symbol, interval, len(data[interval]))
                except Exception:
                    log.exception("BACKTEST DATA FAILED | %s %s", symbol, interval)
                    data[interval] = []
            all_results[symbol] = backtest_symbol(symbol, data)

    report(all_results)


if __name__ == "__main__":
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(message)s")
    asyncio.run(main())
