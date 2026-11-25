"""Research-only reversal warning filter. Not imported by production."""
from app.signal_engine import macd_histogram


def _confirmed_pivots(candles, side, left=2, right=2):
    """Return pivots confirmed using only candles available at evaluation."""
    end = len(candles) - right
    if end <= left:
        return []
    pivots = []
    for i in range(left, end):
        window = candles[i-left:i+right+1]
        value = candles[i]["low"] if side == "LONG" else candles[i]["high"]
        values = [c["low"] if side == "LONG" else c["high"] for c in window]
        if side == "LONG" and value == min(values) and values.count(value) == 1:
            pivots.append((i, value))
        elif side == "SHORT" and value == max(values) and values.count(value) == 1:
            pivots.append((i, value))
    return pivots


def reversal_warning(side, candles):
    """Return (blocked, reasons); block only when >=2 warnings agree."""
    if side not in ("LONG", "SHORT") or len(candles) < 40:
        return False, []
    closes = [float(c["close"]) for c in candles]
    last = candles[-1]
    reasons = []

    pivots = _confirmed_pivots(candles, side)
    if pivots:
        _, pivot = pivots[-1]
        if side == "LONG" and float(last["close"]) < pivot:
            reasons.append("bearish_structure_break")
        elif side == "SHORT" and float(last["close"]) > pivot:
            reasons.append("bullish_structure_break")

    hist = [macd_histogram(closes[:-i] if i else closes) for i in (2, 1, 0)]
    if all(x is not None for x in hist):
        if side == "LONG" and hist[0] > hist[1] > hist[2]:
            reasons.append("bearish_momentum_fade")
        elif side == "SHORT" and hist[0] < hist[1] < hist[2]:
            reasons.append("bullish_momentum_fade")

    prev, current = candles[-2], candles[-1]
    if side == "LONG":
        shifted = (float(current["high"]) < float(prev["high"])
                   and float(current["low"]) < float(prev["low"]))
    else:
        shifted = (float(current["high"]) > float(prev["high"])
                   and float(current["low"]) > float(prev["low"]))
    if shifted:
        reasons.append("micro_structure_shift")

    return len(reasons) >= 2, reasons
