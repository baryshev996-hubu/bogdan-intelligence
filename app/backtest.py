"""Historical, conservative backtest for the live Futures signal rules.

Diagnostic only. It never places orders or changes account state.
"""
import asyncio
import logging
import os
import time
import bisect

import aiohttp

from app.signal_engine import INTERVALS, evaluate


log = logging.getLogger("bogdan-backtest")
SYMBOLS = [s.strip().upper() for s in os.getenv(
    "BINANCE_SYMBOLS", "btcusdt,ethusdt,solusdt,xrpusdt"
).split(",") if s.strip()]
LIMIT = 1500
LOOKBACK_DAYS = 45
INTERVAL_MS = {"5m": 300_000, "15m": 900_000, "1h": 3_600_000}
HORIZON_BARS = 144  # 12 hours of 5m candles
SAME_SIDE_COOLDOWN_MS = 6 * 60 * 60 * 1000
FLIP_COOLDOWN_MS = 60 * 60 * 1000
TAKER_FEE_RATE = float(os.getenv("BACKTEST_TAKER_FEE", "0.0005"))
SLIPPAGE_RATE = float(os.getenv("BACKTEST_SLIPPAGE", "0.0001"))


async def fetch_candles(session, symbol, interval):
    url = "https://fapi.binance.com/fapi/v1/klines"
    end_ms = int(time.time() * 1000)
    cursor = end_ms - LOOKBACK_DAYS * 24 * 60 * 60 * 1000
    candles = []

    while cursor < end_ms:
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": cursor,
            "endTime": end_ms,
            "limit": LIMIT,
        }
        async with session.get(url, params=params, timeout=30) as response:
            response.raise_for_status()
            rows = await response.json(content_type=None)
        if not rows:
            break

        for row in rows:
            close_time = int(row[6])
            if close_time >= end_ms:
                continue
            candles.append({
                "open_time": int(row[0]),
                "open": float(row[1]),
                "high": float(row[2]),
                "low": float(row[3]),
                "close": float(row[4]),
                "volume": float(row[5]),
                "close_time": close_time,
            })

        next_cursor = int(rows[-1][0]) + INTERVAL_MS[interval]
        if next_cursor <= cursor or len(rows) < LIMIT:
            break
        cursor = next_cursor

    # De-duplicate by open time and preserve chronological order.
    unique = {c["open_time"]: c for c in candles}
    return [unique[key] for key in sorted(unique)]


def simulate_trade(signal, future_candles):
    """Mirror the live paper exit plan: 70% at 0.8R, 20% at 1.5R,
    then trail the final 10%; 2.2R is an optional full runner exit.
    Conservative same-candle ordering assumes the active stop fills first.
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
    trail_buffer = 0.15 * risk
    lock_profit = 0.10 * risk

    for index, candle in enumerate(future_candles[:HORIZON_BARS]):
        stop_hit = (candle["low"] <= active_stop if side == "LONG"
                    else candle["high"] >= active_stop)
        if stop_hit:
            direction = 1.0 if side == "LONG" else -1.0
            realized_r += remaining * direction * (active_stop - entry) / risk
            return realized_r, index

        target1_hit = candle["high"] >= t1 if side == "LONG" else candle["low"] <= t1
        target2_hit = candle["high"] >= t2 if side == "LONG" else candle["low"] <= t2
        target3_hit = candle["high"] >= t3 if side == "LONG" else candle["low"] <= t3

        if not tp1_hit and target1_hit:
            realized_r += 0.70 * 0.80
            remaining -= 0.70
            tp1_hit = True
            active_stop = entry + lock_profit if side == "LONG" else entry - lock_profit

        if tp1_hit and not tp2_hit and target2_hit:
            realized_r += 0.20 * 1.50
            remaining -= 0.20
            tp2_hit = True
            active_stop = t1

        if tp2_hit and target3_hit:
            realized_r += remaining * 2.20
            return realized_r, index

        # Trail behind the latest completed candle, with the update taking
        # effect from the next candle. Never loosen the protected stop.
        if tp2_hit and remaining > 0:
            if side == "LONG":
                active_stop = max(active_stop, candle["low"] - trail_buffer)
            else:
                active_stop = min(active_stop, candle["high"] + trail_buffer)

    last_price = future_candles[min(len(future_candles), HORIZON_BARS) - 1]["close"]
    direction = 1.0 if side == "LONG" else -1.0
    realized_r += remaining * direction * (last_price - entry) / risk
    return realized_r, min(len(future_candles), HORIZON_BARS) - 1


def backtest_symbol(symbol, data):
    five, fifteen, hourly = data["5m"], data["15m"], data["1h"]
    if min(len(five), len(fifteen), len(hourly)) < 100:
        log.warning("%s: insufficient historical candles, skipping", symbol)
        return {"trades": [], "candidate_signals": 0, "blocked_signals": 0, "filter_reasons": {}}
    close15, close1h = [c["close_time"] for c in fifteen], [c["close_time"] for c in hourly]
    results, candidate_signals, last_signal = [], 0, None
    index = 59
    while index < len(five) - 2:
        as_of = five[index]["close_time"]
        j15, j1h = bisect.bisect_right(close15, as_of), bisect.bisect_right(close1h, as_of)
        if j15 < 60 or j1h < 60:
            index += 1
            continue
        signal = evaluate(symbol, {
            "5m": five[max(0, index + 1 - 240):index + 1],
            "15m": fifteen[max(0, j15 - 240):j15],
            "1h": hourly[max(0, j1h - 240):j1h],
        }, as_of_ms=as_of)
        if not signal:
            index += 1
            continue
        candidate_signals += 1
        if last_signal:
            cooldown = SAME_SIDE_COOLDOWN_MS if last_signal["side"] == signal["side"] else FLIP_COOLDOWN_MS
            if as_of - last_signal["time"] < cooldown:
                index += 1
                continue
        gross_r, exit_offset = simulate_trade(signal, five[index + 1:index + 1 + HORIZON_BARS])
        if gross_r is None:
            break
        cost_fraction = 2 * (TAKER_FEE_RATE + SLIPPAGE_RATE)
        cost_r = cost_fraction * signal["entry"] / abs(signal["entry"] - signal["stop"])
        results.append({"side": signal["side"], "gross_r": gross_r, "net_r": gross_r - cost_r, "time": as_of})
        last_signal = {"side": signal["side"], "time": as_of}
        index += exit_offset + 2
    return {"trades": results, "candidate_signals": candidate_signals, "blocked_signals": 0, "filter_reasons": {}}

def report(all_results, label="FILTERED"):
    trades = [trade for result in all_results.values() for trade in result["trades"]]
    candidates = sum(result["candidate_signals"] for result in all_results.values())
    log.info("BACKTEST SIGNAL COUNTS | %s | candidates=%d", label, candidates)
    if not trades:
        log.info("BACKTEST %s: no qualifying trades in the loaded sample.", label)
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
        "BACKTEST %s | trades=%s wins=%s losses=%s win_rate=%.1f%% "
        "avg_net_R=%.3f total_net_R=%.2f profit_factor=%.2f max_drawdown_R=%.2f",
        label, len(trades), len(wins), len(losses), 100 * len(wins) / len(trades),
        avg_r, total_r, profit_factor, max_drawdown
    )
    for symbol, result in all_results.items():
        rows = result["trades"]
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
    datasets = {}
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
            datasets[symbol] = data

    baseline = {symbol: backtest_symbol(symbol, data) for symbol, data in datasets.items()}
    report(baseline, label="MTF BASELINE 5m 15m 1h")


if __name__ == "__main__":
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                        format="%(asctime)s %(levelname)s %(message)s")
    asyncio.run(main())
