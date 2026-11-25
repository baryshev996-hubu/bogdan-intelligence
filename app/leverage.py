def build_leverage_paper_signal(signal, balance):
    """Size a virtual $20-margin position while capping planned loss at 0.25% equity."""
    entry = float(signal["entry"])
    if entry <= 0:
        return None
    confidence = int(signal.get("confidence", 0))
    leverage = 20 if confidence >= 80 else 15 if signal.get("volume_ratio", 0) >= 1.5 else 10
    direction = 1.0 if signal["side"] == "LONG" else -1.0
    fee, slip = 0.0005, 0.0001
    filled_entry = entry * (1 + direction * slip)
    quantity = 20.0 * leverage / filled_entry
    budget = float(balance) * 0.0025
    # Solve net PnL at the slipped exit price including entry and exit fees.
    exit_fill = (filled_entry * (direction + fee) - budget / quantity) / (direction - fee)
    stop = exit_fill / (1 - direction * slip)
    if stop <= 0 or direction * (entry - stop) <= 0:
        return None
    prepared = dict(signal, stop=stop, paper_qty=quantity)
    prepared["targets"] = [
        entry + direction * 0.0100 * entry,
        entry + direction * 0.0125 * entry,
        entry + direction * 0.0150 * entry,
    ]
    prepared["paper_management_version"] = 3
    prepared["paper_max_gross_leverage"] = 20.0
    prepared["paper_slippage_rate"] = 0.0001
    prepared["paper_leverage"] = leverage
    return prepared
