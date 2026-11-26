"""Production-only directional confirmation. No order placement.

Score is a heuristic setup-quality index, not a probability of profit.
Reject uncertain or missing data rather than reversing a signal.
"""
from math import isfinite
from app.analyst import net_move
from app.signal_engine import macd_histogram

VERSION = "quality-v2"
MIN_SCORE = 75


def assess_quality(signal, context, histories):
    """Return (score, rejection_reason); reason=None means approved."""
    side = signal["side"]
    if side not in ("LONG", "SHORT"):
        return 0, "quality_invalid_side"
    direction = 1 if side == "LONG" else -1
    buy, btc, relative = (context.get(k) for k in ("buy_share", "btc_change", "relative"))
    is_btc = signal["symbol"] == "BTCUSDT"
    if (buy is None or btc is None or not isfinite(buy) or not isfinite(btc)
            or not 0 <= buy <= 100 or
            (not is_btc and (relative is None or not isfinite(relative)))):
        return 0, "quality_missing_context"

    # Signed aggressive volume: mirror rules for long and short.
    flow = direction * (buy - 50)
    rel = direction * (btc if is_btc else relative)
    if flow < 1.5:
        return 0, "quality_flow_unconfirmed"
    if rel < 0.03:
        return 0, "quality_relative_unconfirmed"
    if direction * btc < -0.50:
        return 0, "quality_btc_opposition"

    bars = list(histories.get("5m", ()))
    if len(bars) < 36 or bars[-1].get("open_time") != signal["candle_id"]:
        return 0, "quality_missing_history"
    last = bars[-1]
    close_time = last["close_time"]
    # Require the correct completed slow candles; no future or stale context.
    for tf, interval in (("15m", 900000), ("1h", 3600000)):
        slow = histories.get(tf, ())
        expected = (close_time + 1) // interval * interval - 1
        if not slow or slow[-1].get("close_time") != expected:
            return 0, "quality_unaligned_" + tf

    recent = bars[-3:]
    if any(b["open_time"] - a["open_time"] != 300000 for a, b in zip(recent, recent[1:])):
        return 0, "quality_history_gap"
    total = taker = 0.0
    for bar in recent:
        volume, aggressive_buy = bar.get("volume"), bar.get("taker_buy_volume")
        if (volume is None or aggressive_buy is None or not isfinite(volume)
                or not isfinite(aggressive_buy) or not 0 <= aggressive_buy <= volume
                or volume <= 0):
            return 0, "quality_missing_flow_history"
        total += volume
        taker += aggressive_buy
    flow_15m = direction * (100 * taker / total - 50)
    if flow_15m < 0:
        return 0, "quality_flow_persistence"

    span = last["high"] - last["low"]
    if span <= 0:
        return 0, "quality_invalid_candle"
    close_position = (last["close"] - last["low"]) / span
    directional_close = close_position if direction == 1 else 1 - close_position
    if directional_close < 0.65:
        return 0, "quality_rejection_wick"

    closes = [bar["close"] for bar in bars]
    hist, prev = macd_histogram(closes), macd_histogram(closes[:-1])
    if hist is None or prev is None or direction * hist <= 0:
        return 0, "quality_macd_unconfirmed"
    if direction * (hist - prev) < 0:
        return 0, "quality_momentum_fading"

    entry, stop, tp = signal["entry"], signal["stop"], signal["targets"][0]
    if not (entry > 0 and stop > 0 and tp > 0
            and direction * (entry - stop) > 0 and direction * (tp - entry) > 0):
        return 0, "quality_invalid_risk"
    loss = -net_move(side, entry, stop)
    reward = net_move(side, entry, tp)
    if loss <= 0 or reward / loss < 0.50:
        return 0, "quality_cost_adjusted_reward"

    # 50 technical + 20 aggressive flow + 10 relative strength +
    # 10 candle structure + 5 momentum + 5 net reward = 100 maximum.
    technical = max(0, min(50, signal["confidence"] - 30))
    flow_points = 20 if flow >= 5 and flow_15m >= 2 else 15 if flow >= 3 else 12
    relative_points = 10 if rel >= 0.20 else 8 if rel >= 0.10 else 6
    candle_points = 10 if directional_close >= 0.85 else 8 if directional_close >= 0.75 else 5
    score = int(technical + flow_points + relative_points + candle_points + 5 + 5)
    if score < MIN_SCORE:
        return score, "quality_score_below_threshold"
    return score, None
