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


def build_structural_paper_signal(signal, balance):
    """Paper-only V2: preserve market invalidation, then size to all-in risk.

    $20 margin is a ceiling, not a requirement. Nominal 10x is used only
    for that ceiling; effective exposure is determined by stop distance.
    Keep the existing 1/1.25/1.5% target policy for comparison.
    """
    from math import isfinite
    from app.analyst import net_move

    side = signal.get("side")
    if side not in ("LONG", "SHORT"):
        return None
    direction = 1 if side == "LONG" else -1
    entry, stop, balance = float(signal["entry"]), float(signal["stop"]), float(balance)
    if not all(isfinite(v) and v > 0 for v in (entry, stop, balance)):
        return None
    if direction * (entry - stop) <= 0:
        return None
    fee, slip = 0.0005, 0.0001
    filled_entry = entry * (1 + direction * slip)
    loss_per_unit = -net_move(side, entry, stop, fee, slip)
    if not isfinite(loss_per_unit) or loss_per_unit <= 0:
        return None
    targets = [entry * (1 + direction * fraction) for fraction in (0.01, 0.0125, 0.015)]
    # Reject positions whose first fixed target cannot pay at least 0.5R net.
    if net_move(side, entry, targets[0], fee, slip) < 0.5 * loss_per_unit:
        return None
    # Do not amplify the new heuristic score into larger leverage.
    budget = balance * 0.0025
    leverage = 10
    qty = min(budget / loss_per_unit, 20 * leverage / filled_entry)
    if not isfinite(qty) or qty <= 0:
        return None
    return dict(signal, paper_qty=qty, targets=targets,
                paper_management_version=3, paper_max_gross_leverage=10.0,
                paper_slippage_rate=slip, paper_leverage=leverage,
                paper_entry_policy="structural-risk-v2",
                paper_margin=qty * filled_entry / leverage,
                paper_planned_loss=qty * loss_per_unit)


def recent_paper_loss(signal, closed_trades, now, cooldown_seconds=3600):
    """Do not immediately repeat the same losing setup in the paper book."""
    return any(
        trade.get("symbol") == signal.get("symbol")
        and trade.get("side") == signal.get("side")
        and float(trade.get("net_pnl", 0)) < 0
        and 0 <= now - float(trade.get("closed_at") or 0) < cooldown_seconds
        for trade in closed_trades
    )
