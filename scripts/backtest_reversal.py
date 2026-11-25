"""Historical A/B signal-quality backtest. Research only; never imported by the bot.

Downloads Binance USD-M 5m klines, derives closed 15m/1h candles, evaluates the
production signal engine, and compares baseline vs reversal-filtered signals.
Trade outcome model: next-bar-open entry, 70/20/10 exits at 0.8R/1.5R/2.2R,
stop-first on ambiguous OHLC bars, fee+slippage, 12h time exit.
"""
import argparse
import asyncio
import json
import statistics
import time
from collections import defaultdict

import aiohttp

from app.reversal_filter import reversal_warning
from app.signal_engine import evaluate

DEFAULT_SYMBOLS = (
    "BTCUSDT ETHUSDT BNBUSDT SOLUSDT XRPUSDT DOGEUSDT ADAUSDT AVAXUSDT "
    "LINKUSDT DOTUSDT LTCUSDT BCHUSDT TRXUSDT ETCUSDT XLMUSDT SUIUSDT "
    "NEARUSDT APTUSDT ARBUSDT OPUSDT INJUSDT UNIUSDT FILUSDT AAVEUSDT "
    "RENDERUSDT TAOUSDT HBARUSDT 1000PEPEUSDT 1000SHIBUSDT ATOMUSDT"
).split()
BASE_URL = "https://fapi.binance.com/fapi/v1/klines"
INTERVAL_MS = 300_000
FEE_RATE = 0.0005
SLIPPAGE = 0.0001
MAX_HOLD_BARS = 144


async def fetch_top_symbols(session, count):
    async with session.get("https://fapi.binance.com/fapi/v1/exchangeInfo") as response:
        response.raise_for_status()
        info = await response.json()
    async with session.get("https://fapi.binance.com/fapi/v1/ticker/24hr") as response:
        response.raise_for_status()
        tickers = await response.json()
    eligible = {
        x["symbol"] for x in info["symbols"]
        if x.get("status") == "TRADING" and x.get("contractType") == "PERPETUAL"
        and x.get("quoteAsset") == "USDT"
    }
    ranked = sorted(
        (x for x in tickers if x.get("symbol") in eligible),
        key=lambda x: float(x.get("quoteVolume", 0)), reverse=True
    )
    return [x["symbol"] for x in ranked[:count]]


async def fetch_klines(session, symbol, start_ms, end_ms, semaphore):
    rows, cursor = [], start_ms
    while cursor < end_ms:
        async with semaphore:
            for attempt in range(5):
                try:
                    async with session.get(
                        BASE_URL,
                        params={"symbol": symbol, "interval": "5m", "startTime": cursor,
                                "endTime": end_ms, "limit": 1500},
                        timeout=aiohttp.ClientTimeout(total=30),
                    ) as response:
                        if response.status in (418, 429):
                            await asyncio.sleep(1.5 * (attempt + 1))
                            continue
                        response.raise_for_status()
                        batch = await response.json()
                        break
                except (aiohttp.ClientError, asyncio.TimeoutError):
                    if attempt == 4:
                        raise
                    await asyncio.sleep(1.0 * (attempt + 1))
            else:
                raise RuntimeError(f"Binance rate limit persisted for {symbol}")
        if not batch:
            break
        rows.extend(batch)
        cursor = int(batch[-1][0]) + INTERVAL_MS
        if len(batch) < 1500:
            break
    candles = []
    for x in rows:
        candles.append({
            "open_time": int(x[0]), "open": float(x[1]), "high": float(x[2]),
            "low": float(x[3]), "close": float(x[4]), "volume": float(x[5]),
            "close_time": int(x[6]),
        })
    return candles


def aggregate(candles, interval_ms):
    groups = {}
    for c in candles:
        bucket = c["open_time"] // interval_ms * interval_ms
        if bucket not in groups:
            groups[bucket] = {
                "open_time": bucket, "open": c["open"], "high": c["high"],
                "low": c["low"], "close": c["close"], "volume": c["volume"],
                "close_time": c["close_time"],
            }
        else:
            a = groups[bucket]
            a["high"] = max(a["high"], c["high"])
            a["low"] = min(a["low"], c["low"])
            a["close"] = c["close"]
            a["volume"] += c["volume"]
            a["close_time"] = c["close_time"]
    return [groups[k] for k in sorted(groups)]


def simulate_trade(signal, candles, index):
    """Return net R and exit index using conservative stop-first OHLC logic."""
    if index + 1 >= len(candles):
        return None
    direction = 1 if signal["side"] == "LONG" else -1
    raw_entry = candles[index + 1]["open"]
    entry = raw_entry * (1 + SLIPPAGE * direction)
    risk = abs(float(signal["entry"]) - float(signal["stop"]))
    if risk <= 0:
        return None
    stop = float(signal["stop"])
    targets = [float(x) for x in signal["targets"]]
    remaining = 1.0
    realized = 0.0
    fee_cost = 0.0
    fee_cost += entry * FEE_RATE / risk
    exit_index = min(index + MAX_HOLD_BARS, len(candles) - 1)
    tp1 = tp2 = False
    active_stop = stop

    def close_fraction(price, fraction):
        nonlocal remaining, realized, fee_cost
        qty = min(remaining, fraction)
        fill = price * (1 - SLIPPAGE * direction)
        realized += qty * direction * (fill - entry) / risk
        fee_cost += qty * fill * FEE_RATE / risk
        remaining -= qty

    for j in range(index + 1, exit_index + 1):
        c = candles[j]
        hi, lo = c["high"], c["low"]
        stop_hit = lo <= active_stop if direction == 1 else hi >= active_stop
        if stop_hit:
            close_fraction(active_stop, remaining)
            exit_index = j
            break
        hit = (lambda p: hi >= p) if direction == 1 else (lambda p: lo <= p)
        if not tp1 and hit(targets[0]):
            close_fraction(targets[0], 0.70)
            tp1 = True
            buffer = max(risk * 0.10, entry * (2 * FEE_RATE + 0.0001))
            active_stop = entry + direction * buffer
        if tp1 and not tp2 and hit(targets[1]):
            close_fraction(targets[1], 0.20)
            tp2 = True
            active_stop = targets[0]
        if tp2 and hit(targets[2]):
            close_fraction(targets[2], remaining)
            exit_index = j
            break
        if tp2 and remaining > 0:
            trail = c["low"] - 0.15 * risk if direction == 1 else c["high"] + 0.15 * risk
            active_stop = max(active_stop, trail) if direction == 1 else min(active_stop, trail)

    if remaining > 1e-9:
        close_fraction(candles[exit_index]["close"], remaining)
    return realized - fee_cost, exit_index


def metrics(rows):
    if not rows:
        return {"trades": 0, "win_rate_pct": 0, "net_R": 0, "avg_R": 0,
                "profit_factor": 0, "max_drawdown_R": 0}
    values = [x["net_R"] for x in rows]
    wins = [x for x in values if x > 0]
    losses = [x for x in values if x < 0]
    equity = peak = drawdown = 0.0
    for value in values:
        equity += value
        peak = max(peak, equity)
        drawdown = max(drawdown, peak - equity)
    gross_loss = abs(sum(losses))
    return {
        "trades": len(values),
        "win_rate_pct": round(100 * len(wins) / len(values), 2),
        "net_R": round(sum(values), 3),
        "avg_R": round(statistics.mean(values), 4),
        "profit_factor": round(sum(wins) / gross_loss, 3) if gross_loss else "inf",
        "max_drawdown_R": round(drawdown, 3),
    }


def evaluate_symbol(symbol, candles):
    c15_all = aggregate(candles, 900_000)
    c1h_all = aggregate(candles, 3_600_000)
    idx15 = idx1h = 0
    histories = {"5m": [], "15m": [], "1h": []}
    results = {"baseline": [], "filtered": []}
    blocked = defaultdict(int)
    cooldown = {"baseline": {}, "filtered": {}}
    busy_until = {"baseline": -1, "filtered": -1}

    for i in range(200, len(candles) - 1):
        current = candles[i]
        t = current["close_time"]
        while idx15 < len(c15_all) and c15_all[idx15]["close_time"] <= t:
            idx15 += 1
        while idx1h < len(c1h_all) and c1h_all[idx1h]["close_time"] <= t:
            idx1h += 1
        c15 = c15_all[max(0, idx15 - 200):idx15]
        c1h = c1h_all[max(0, idx1h - 200):idx1h]
        if len(c15) < 60 or len(c1h) < 60:
            continue
        c5 = candles[max(0, i - 199):i + 1]
        by_tf = {"5m": c5, "15m": c15, "1h": c1h}
        signal = evaluate(symbol, by_tf, as_of_ms=t)
        if not signal:
            continue
        blocked_now, reasons = reversal_warning(signal["side"], c5)
        if blocked_now:
            blocked[",".join(reasons)] += 1

        for mode in ("baseline", "filtered"):
            if mode == "filtered" and blocked_now:
                continue
            if i <= busy_until[mode]:
                continue
            prior = cooldown[mode].get(signal["side"])
            if prior is not None and t - prior < 90 * 60 * 1000:
                continue
            opposite = cooldown[mode].get("opposite")
            if opposite is not None and t - opposite < 15 * 60 * 1000:
                continue
            trade = simulate_trade(signal, candles, i)
            if trade is None:
                continue
            net_r, exit_i = trade
            results[mode].append({"net_R": net_r, "symbol": symbol,
                                  "side": signal["side"], "signal_time": t})
            busy_until[mode] = exit_i
            cooldown[mode][signal["side"]] = t
            cooldown[mode]["opposite"] = t
    return results, dict(blocked)


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=60)
    parser.add_argument("--symbols", type=int, default=30, choices=(30, 100))
    parser.add_argument("--output", default="reversal_backtest.json")
    args = parser.parse_args()
    symbols = DEFAULT_SYMBOLS if args.symbols == 30 else DEFAULT_SYMBOLS
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - args.days * 86_400_000
    semaphore = asyncio.Semaphore(5)
    connector = aiohttp.TCPConnector(limit=10)
    timeout = aiohttp.ClientTimeout(total=45)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        fetched = await asyncio.gather(*[
            fetch_klines(session, s, start_ms, end_ms, semaphore) for s in symbols
        ], return_exceptions=True)

    all_rows = {"baseline": [], "filtered": []}
    rejected = defaultdict(int)
    failures = {}
    for symbol, data in zip(symbols, fetched):
        if isinstance(data, Exception):
            failures[symbol] = str(data)
            continue
        if len(data) < 500:
            failures[symbol] = f"insufficient candles: {len(data)}"
            continue
        result, blocked = evaluate_symbol(symbol, data)
        for mode in all_rows:
            all_rows[mode].extend(result[mode])
        for reason, count in blocked.items():
            rejected[reason] += count

    report = {
        "period_days": args.days, "symbols_requested": len(symbols),
        "symbols_tested": len(symbols) - len(failures),
        "failures": failures, "blocked_by_reversal_reasons": dict(rejected),
        "baseline": metrics(all_rows["baseline"]),
        "reversal_filter": metrics(all_rows["filtered"]),
        "method": {
            "entry": "next 5m candle open",
            "exits": "70% at 0.8R, 20% at 1.5R, 10% at 2.2R; stop-first; 12h time exit",
            "fees_each_side": FEE_RATE, "slippage_each_side": SLIPPAGE,
            "timeframes": ["5m", "15m", "1h"], "closed_candles_only": True,
            "warning_rule": "block only when at least 2 of structure break, MACD fade, micro-structure shift agree",
            "limitations": "Independent per-symbol trade streams; no portfolio-wide max-open constraint or funding-rate debits. Research comparison, not a live performance guarantee."
        }
    }
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
