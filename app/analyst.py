"""Market context and modeled execution costs for a closed-candle setup."""


def market_context(candle, hourly, btc_hourly):
    volume = candle.get("volume", 0)
    buy_volume = candle.get("taker_buy_volume")
    buy_share = None
    if buy_volume is not None and volume > 0 and 0 <= buy_volume <= volume:
        buy_share = 100 * buy_volume / volume
    # Compare the same completed UTC hour, even if polling is delayed.
    hour_close = (candle["close_time"] + 1) // 3600000 * 3600000 - 1

    def hourly_return(history):
        for bar in reversed(history):
            if bar["close_time"] == hour_close and bar["open"] > 0:
                return 100 * (bar["close"] / bar["open"] - 1)
        return None

    asset = hourly_return(hourly)
    btc = hourly_return(btc_hourly)
    relative = asset - btc if asset is not None and btc is not None else None
    return {"buy_share": buy_share, "btc_change": btc, "relative": relative}


def net_move(side, entry, exit_price, fee=0.0005, slip=0.0001):
    """Modeled PnL in quote currency for one unit at taker entry and exit."""
    direction = 1 if side == "LONG" else -1
    fill_in = entry * (1 + direction * slip)
    fill_out = exit_price * (1 - direction * slip)
    return direction * (fill_out - fill_in) - fee * (fill_in + fill_out)


def analyst_lines(signal):
    context = signal.get("market_context", {})
    side, entry, stop = signal["side"], signal["entry"], signal["stop"]
    lines = []
    buy = context.get("buy_share")
    if buy is not None:
        lines.append(f"Исполнения 5m: покупки {buy:.1f}%, продажи {100-buy:.1f}% объёма.")
    btc = context.get("btc_change")
    if btc is not None:
        lines.append(f"BTC за закрытый час: {btc:+.2f}%.")
    relative = context.get("relative")
    if relative is not None and signal["symbol"] != "BTCUSDT":
        lines.append(f"Монета относительно BTC за тот же час: {relative:+.2f} п.п.")
    if buy is not None and relative is not None and signal["symbol"] != "BTCUSDT":
        direction = 1 if side == "LONG" else -1
        flow_support = direction * (buy - 50)
        relative_support = direction * relative
        if flow_support > 0 and relative_support > 0:
            lines.append(f"Поток сделок и сила относительно BTC поддерживают {side}.")
        elif flow_support < 0 and relative_support < 0:
            lines.append(f"Оба фактора против {side}: относительное движение и поток сделок.")
        elif flow_support * relative_support < 0:
            lines.append("Сигналы расходятся: поток сделок и движение относительно BTC указывают в разные стороны.")
    loss = -net_move(side, entry, stop)
    if loss > 0:
        tp1 = net_move(side, entry, signal["targets"][0])
        lines.append(f"До TP1 после затрат: {tp1/loss:.2f}R на закрываемую часть "
                     "(комиссия 0,05% и проскальзывание 0,01% на сторону).")
    return lines
