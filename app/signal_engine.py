"""Conservative, closed-candle multi-timeframe signal engine.

Signals are informational only. No orders are placed.
"""
from collections import deque
from datetime import datetime, timezone
from statistics import mean
from html import escape

from app.analyst import analyst_lines


INTERVALS = ("5m", "15m", "1h")
INTERVAL_MS = {"5m": 300_000, "15m": 900_000, "1h": 3_600_000}
MAX_CANDLES = 240


def _reject(diagnostics, reason):
    """Record why a closed-candle evaluation did not qualify, when requested."""
    if diagnostics is not None:
        diagnostics[reason] = diagnostics.get(reason, 0) + 1
    return None


def ema(values, period):
    if len(values) < period:
        return None
    alpha = 2.0 / (period + 1)
    value = sum(values[:period]) / period
    for item in values[period:]:
        value = alpha * item + (1 - alpha) * value
    return value


def rsi(values, period=14):
    if len(values) < period + 1:
        return None
    changes = [values[i] - values[i - 1] for i in range(1, len(values))]
    gains = [max(x, 0.0) for x in changes]
    losses = [max(-x, 0.0) for x in changes]
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(changes)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 50.0 if avg_gain == 0 else 100.0
    if avg_gain == 0:
        return 0.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def atr(candles, period=14):
    if len(candles) < period + 1:
        return None
    ranges = []
    for prev, cur in zip(candles[:-1], candles[1:]):
        ranges.append(max(
            cur["high"] - cur["low"],
            abs(cur["high"] - prev["close"]),
            abs(cur["low"] - prev["close"]),
        ))
    value = sum(ranges[:period]) / period
    for item in ranges[period:]:
        value = (value * (period - 1) + item) / period
    return value


def macd_histogram(values):
    fast = ema(values, 12)
    slow = ema(values, 26)
    if fast is None or slow is None:
        return None
    alpha_fast, alpha_slow = 2 / 13, 2 / 27
    ef = sum(values[:12]) / 12
    es = sum(values[:26]) / 26
    macd_values = []
    for i, item in enumerate(values):
        if i >= 11:
            if i > 11:
                ef = alpha_fast * item + (1 - alpha_fast) * ef
            if i >= 25:
                if i > 25:
                    es = alpha_slow * item + (1 - alpha_slow) * es
                macd_values.append(ef - es)
    signal = ema(macd_values, 9)
    if signal is None:
        return None
    return macd_values[-1] - signal


def _trend(candles):
    closes = [c["close"] for c in candles]
    e20, e50 = ema(closes, 20), ema(closes, 50)
    if e20 is None or e50 is None:
        return None
    last = closes[-1]
    if last > e20 > e50:
        return "LONG"
    if last < e20 < e50:
        return "SHORT"
    return "NEUTRAL"



def _fmt_price(value):
    if value >= 1000:
        return f"{value:,.2f}"
    if value >= 1:
        return f"{value:.4f}"
    return f"{value:.6f}"


def evaluate(symbol, candles_by_interval, as_of_ms=None, diagnostics=None):
    """Return a signal dict only when strict conditions align."""
    candles = {tf: list(candles_by_interval.get(tf, ())) for tf in INTERVALS}
    required = ("5m", "15m", "1h")
    if any(len(candles[tf]) < 60 for tf in required):
        return _reject(diagnostics, "history_short")
    c5, c15, c1h = candles["5m"], candles["15m"], candles["1h"]
    now_ms = as_of_ms or int(datetime.now(timezone.utc).timestamp() * 1000)
    freshness_limits = {"5m": 12 * 60 * 1000, "15m": 35 * 60 * 1000, "1h": 2 * 60 * 60 * 1000}
    for timeframe, limit_ms in freshness_limits.items():
        latest = candles[timeframe][-1].get("close_time", 0)
        if not latest or now_ms - latest > limit_ms:
            return _reject(diagnostics, "stale_candles")
    closes5 = [c["close"] for c in c5]
    last = c5[-1]
    price = last["close"]
    atr5 = atr(c5)
    atr15 = atr(c15)
    if not atr5 or not atr15 or price <= 0:
        return _reject(diagnostics, "invalid_atr_or_price")

    trend15, trend1h = _trend(c15), _trend(c1h)
    if trend15 not in ("LONG", "SHORT") or trend15 != trend1h:
        return _reject(diagnostics, "trend_misaligned")

    rsi5, rsi15 = rsi(closes5), rsi([c["close"] for c in c15])
    hist = macd_histogram(closes5)
    if rsi5 is None or rsi15 is None or hist is None:
        return _reject(diagnostics, "indicator_unavailable")

    ema20_5 = ema(closes5, 20)
    if ema20_5 is None or abs(price - ema20_5) > 1.25 * atr5:
        return _reject(diagnostics, "price_too_far_from_ema")

    body = last["close"] - last["open"]
    candle_range = max(last["high"] - last["low"], price * 1e-9)
    body_ratio = abs(body) / candle_range
    if body_ratio < 0.45:
        return _reject(diagnostics, "weak_candle_body")

    volumes = [c["volume"] for c in c5[-21:-1]]
    avg_volume = mean(volumes) if volumes else 0
    volume_ratio = last["volume"] / avg_volume if avg_volume > 0 else 0
    if volume_ratio < 1.15:
        return _reject(diagnostics, "low_volume")

    if trend15 == "LONG":
        if not (body > 0 and 48 <= rsi5 <= 68 and 50 <= rsi15 <= 70 and hist > 0):
            return _reject(diagnostics, "momentum_not_confirmed")
        side = "LONG"
    else:
        if not (body < 0 and 32 <= rsi5 <= 52 and 30 <= rsi15 <= 50 and hist < 0):
            return _reject(diagnostics, "momentum_not_confirmed")
        side = "SHORT"

    recent = c5[-12:-1]
    if side == "LONG":
        structural = min(c["low"] for c in recent) - 0.10 * atr5
        stop = min(price - 1.5 * atr5, structural)
        risk = price - stop
    else:
        structural = max(c["high"] for c in recent) + 0.10 * atr5
        stop = max(price + 1.5 * atr5, structural)
        risk = stop - price

    if risk <= 0 or risk > 2.5 * atr5:
        return _reject(diagnostics, "risk_out_of_range")

    # Bring the first cash-out closer and avoid relying on a distant 3R target.
    target_r = (0.80, 1.50, 2.20)
    targets = ([price + risk * n for n in target_r] if side == "LONG"
               else [price - risk * n for n in target_r])
    confidence = 65
    confidence += 5 if volume_ratio >= 1.5 else 0
    confidence += 5 if body_ratio >= 0.7 else 0
    confidence += 5 if 52 <= rsi5 <= 63 and side == "LONG" else 0
    confidence += 5 if 37 <= rsi5 <= 48 and side == "SHORT" else 0
    confidence = min(confidence, 90)
    if confidence < 75:
        return _reject(diagnostics, "confidence_below_75")

    close_time = last.get("close_time")
    if close_time:
        timestamp = datetime.fromtimestamp(close_time / 1000, tz=timezone.utc)
        candle_label = timestamp.strftime("%Y-%m-%d %H:%M UTC")
    else:
        candle_label = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    reasons = [
        f"Тренд 15m и 1h: {side}",
        f"RSI: 5m {rsi5:.1f}, 15m {rsi15:.1f}",
        f"Объём: {volume_ratio:.2f}x среднего",
        f"MACD histogram: {'+' if hist > 0 else '-'}",
        "Закрытая 5m свеча подтвердила направление",
    ]
    return {
        "symbol": symbol,
        "side": side,
        "entry": price,
        "stop": stop,
        "targets": targets,
        "target_r": list(target_r),
        "confidence": confidence,
        "volume_ratio": volume_ratio,
        "risk_pct": risk / price * 100,
        "entry_zone": [price - 0.15 * atr5, price + 0.15 * atr5],
        "reasons": reasons,
        "candle_time": candle_label,
        "candle_id": last.get("open_time"),
    }



SIGNAL_ALERT_SUFFIX = " ‼️🚨‼️ 🚨‼️ 🚨‼️ 🚨‼️"


def format_signal(signal):
    """Render a Telegram HTML card, preserving the allowlisted plain header."""
    side = signal["side"]
    icon = "🟢" if side == "LONG" else "🔴"
    direction = 1 if side == "LONG" else -1
    arrow = "↗" if side == "LONG" else "↘"
    square = "🟩" if side == "LONG" else "🟥"
    entry = signal["entry"]
    stop = signal["stop"]
    score = signal["confidence"]
    filled = max(0, min(10, int(score / 10 + 0.5)))
    symbol = signal["symbol"]
    pair = symbol[:-4] + "/USDT" if symbol.endswith("USDT") else symbol
    separator = "━━━━━━━━━━━━━━━━━━"
    candle_time = signal["candle_time"]
    try:
        from zoneinfo import ZoneInfo
        utc = datetime.strptime(candle_time, "%Y-%m-%d %H:%M UTC").replace(tzinfo=timezone.utc)
        local_time = utc.astimezone(ZoneInfo("Europe/Kyiv")).strftime("%d.%m.%Y · %H:%M")
        time_label = local_time + " (Киев)"
    except (ValueError, KeyError):
        time_label = candle_time

    def price(value):
        return "<code>" + escape(_fmt_price(value)) + "</code>"

    lines = [
        f"{icon} BOGDAN INTELLIGENCE | {side}{SIGNAL_ALERT_SUFFIX}",
        "",
        f"<b>{escape(pair)} · {side} {arrow}</b>",
        f"<i>BINANCE FUTURES | {escape(time_label)}</i>",
        separator,
        f"📊 <b>Оценка сигнала: {score}/100</b>",
        square * filled + "⬜" * (10 - filled),
        separator,
        "📍 <b>Точка входа</b>",
        f"Цена: {price(entry)}",
        f"Зона: {price(signal['entry_zone'][0])} – {price(signal['entry_zone'][1])}",
        separator,
        "🎯 <b>Цели позиции</b>",
    ]
    for i, (target, multiple, portion) in enumerate(zip(
            signal["targets"], signal.get("target_r", (0.8, 1.5, 2.2)), (70, 20, 10)), 1):
        move = direction * (target - entry) / entry * 100
        trailing = " / trailing" if i == 3 else ""
        lines.append(f"<b>Take Profit {i}</b> {price(target)} · {move:+.2f}% · {portion}% ({multiple:g}R{trailing})")
    lines.extend([
        "",
        f"🛑 <b>Stop Loss:</b> {price(stop)} · −{signal['risk_pct']:.2f}%",
        separator,
        "📈 <b>Анализ рынка</b>",
    ])
    lines.extend("• " + escape(reason) for reason in signal["reasons"])
    lines.append("")
    buy = signal.get("market_context", {}).get("buy_share")
    if buy is not None:
        buy_blocks = max(0, min(10, int(buy / 10 + 0.5)))
        lines.extend([
            "<b>Поток исполнений · 5m</b>",
            "🟩" * buy_blocks + "🟥" * (10 - buy_blocks),
            f"Покупки {buy:.1f}% | Продажи {100-buy:.1f}%",
            "",
        ])
    for line in analyst_lines(signal):
        if line.startswith("Исполнения 5m:"):
            continue
        prefix = "⚠️ " if line.startswith(("Сигналы расходятся:", "Оба фактора против")) else ""
        lines.append(prefix + escape(line))
    boundary = "или ниже" if side == "LONG" else "или выше"
    lines.extend([
        separator,
        "⚠️ Вход только внутри рабочей зоны.",
        f"Отмена сценария: {price(stop)} {boundary}.",
        f"<i>Свеча закрылась: {escape(candle_time)}</i>",
        "<i>🔒 Режим: наблюдение, без открытия сделок</i>",
    ])
    return "\n".join(lines)
