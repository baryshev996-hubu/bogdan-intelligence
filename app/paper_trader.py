"""Ten-day virtual portfolio for BOGDAN INTELLIGENCE.

No exchange orders are ever placed. State is persisted on Railway's /data volume.
"""
import json
import logging
import os
import tempfile
import time

log = logging.getLogger("bogdan-intel.paper")
PATH = os.getenv("PAPER_LEDGER_PATH", "/data/paper_trading.json")
START_BALANCE = float(os.getenv("PAPER_START_BALANCE", "100"))
RISK_FRACTION = float(os.getenv("PAPER_RISK_FRACTION", "0.01"))
MAX_OPEN = int(os.getenv("PAPER_MAX_OPEN", "3"))
MAX_GROSS_LEVERAGE = float(os.getenv("PAPER_MAX_GROSS_LEVERAGE", "2"))
FEE_RATE = float(os.getenv("PAPER_TAKER_FEE", "0.0005"))
DURATION_SECONDS = int(os.getenv("PAPER_DURATION_DAYS", "10")) * 86400

# New exit plan: bank 70% at 0.8R, another 20% at 1.5R,
# and let the final 10% run with a protected/trailing stop.
TP1_FRACTION = 0.70
TP2_FRACTION = 0.20
TP1_R = 0.80
TP2_R = 1.50
TP3_R = 2.20
LOCK_PROFIT_R = 0.10
TRAIL_BUFFER_R = 0.15


def _now():
    return int(time.time())


class PaperBook:
    def __init__(self, *, path=None, start_balance=None, risk_fraction=None,
                 max_open=None, max_gross_leverage=None, fee_rate=None,
                 ends_at=None, label="10-day", breakeven_trigger_r=0.5,
                 impulse_exit_r=0.55, impulse_body_ratio=0.65,
                 impulse_close_position=0.20):
        self.path = path or PATH
        self.breakeven_trigger_r = float(breakeven_trigger_r)
        self.impulse_exit_r = float(impulse_exit_r)
        self.impulse_body_ratio = float(impulse_body_ratio)
        self.impulse_close_position = float(impulse_close_position)
        self.start_balance = START_BALANCE if start_balance is None else float(start_balance)
        self.risk_fraction = RISK_FRACTION if risk_fraction is None else float(risk_fraction)
        self.max_open = MAX_OPEN if max_open is None else int(max_open)
        self.max_gross_leverage = MAX_GROSS_LEVERAGE if max_gross_leverage is None else float(max_gross_leverage)
        self.fee_rate = FEE_RATE if fee_rate is None else float(fee_rate)
        self.ends_at = ends_at
        self.label = label
        self.state = self._load()

    def _fresh(self):
        now = _now()
        return {
            "version": 1, "started_at": now, "ends_at": self.ends_at or now + DURATION_SECONDS,
            "starting_balance": self.start_balance, "balance": self.start_balance,
            "equity_peak": self.start_balance, "max_drawdown": 0.0,
            "closed_trades": [], "open_trades": {}, "summary_logged": False
        }

    def _load(self):
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if data.get("version") != 1:
                raise ValueError("Unknown paper ledger version")
            # Old ledgers have no per-position cursor. The last atomic ledger
            # write is the latest checkpoint available without reapplying exits.
            checkpoint = int(os.stat(self.path).st_mtime) // 300 * 300000 - 300000
            for trade in data["open_trades"].values():
                trade.setdefault("last_candle_open", max(int(trade.get("entry_time", 0)), checkpoint))
            return data
        except FileNotFoundError:
            os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
            data = self._fresh()
            self._save_data(data)
            log.info("PAPER START | account=%s | balance=$%.2f | ends_at=%s | risk=%.2f%% | max_gross=%.2fx",
                     self.label, self.start_balance, data["ends_at"], self.risk_fraction * 100,
                     self.max_gross_leverage)
            return data

    def _save_data(self, data=None):
        data = self.state if data is None else data
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        fd, tmp = tempfile.mkstemp(prefix="paper-", dir=os.path.dirname(self.path) or ".")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self.path)
        finally:
            if os.path.exists(tmp):
                os.unlink(tmp)

    def active(self):
        return _now() < self.state["ends_at"]

    def open_trade(self, signal):
        symbol = signal["symbol"]
        if not self.active():
            return False
        if symbol in self.state["open_trades"] or len(self.state["open_trades"]) >= self.max_open:
            return False
        equity = float(self.state["balance"])
        if equity <= 0:
            return False
        entry, stop = float(signal["entry"]), float(signal["stop"])
        slippage_rate = max(0.0, float(signal.get("paper_slippage_rate", 0.0)))
        entry *= (1.0 + slippage_rate) if signal["side"] == "LONG" else (1.0 - slippage_rate)
        distance = abs(entry - stop)
        if entry <= 0 or distance <= 0:
            return False
        risk_budget = equity * self.risk_fraction
        qty = float(signal.get("paper_qty", risk_budget / distance))
        current_gross = sum(float(t["remaining"]) * float(t["entry"])
                            for t in self.state["open_trades"].values())
        leverage_cap = float(signal.get("paper_max_gross_leverage", self.max_gross_leverage))
        max_gross = equity * min(self.max_gross_leverage, max(1.0, leverage_cap))
        remaining_capacity = max(0.0, max_gross - current_gross)
        qty = min(qty, remaining_capacity / entry)
        notional = qty * entry
        if qty <= 0:
            return False
        fee = notional * self.fee_rate
        self.state["balance"] -= fee
        self.state["open_trades"][symbol] = {
            "symbol": symbol, "side": signal["side"], "entry": entry,
            "initial_stop": stop, "active_stop": stop,
            "targets": [float(x) for x in signal["targets"]],
            "qty": qty, "remaining": qty, "risk_budget": risk_budget,
            "entry_fee": fee, "exit_fees": 0.0, "gross_realized": 0.0,
            "exit_notional": 0.0, "last_mark": entry,
            "slippage_rate": slippage_rate,
            "tp1": False, "tp2": False, "opened_at": _now(),
            "signal_confidence": int(signal.get("confidence", 0)),
            "paper_leverage_cap": min(self.max_gross_leverage, max(1.0, float(signal.get("paper_max_gross_leverage", self.max_gross_leverage)))),
            "entry_time": int(signal.get("entry_time", signal.get("candle_id", 0)) or 0),
            "management_version": int(signal.get("paper_management_version", 2)),

            "last_candle_open": int(signal.get("candle_id") or 0),
            "risk_distance": distance,
            "tp1_qty": qty * TP1_FRACTION,
            "tp2_qty": qty * TP2_FRACTION,
            "tp3_qty": qty * (1.0 - TP1_FRACTION - TP2_FRACTION),
        }
        self._save_data()
        log.info("PAPER OPEN | %s %s entry=%.8f stop=%.8f qty=%.8f notional=$%.2f effective_leverage=%.2fx risk=$%.2f fee=$%.4f targets=%s",
                 signal["side"], symbol, entry, stop, qty, notional, notional / equity,
                 risk_budget, fee, signal["targets"])
        return True

    def _exit_qty(self, trade, price, qty, reason):
        qty = min(float(trade["remaining"]), max(0.0, float(qty)))
        if qty <= 0:
            return
        direction = 1 if trade["side"] == "LONG" else -1
        slip = float(trade.get("slippage_rate", 0.0))
        price = price * (1.0 - slip) if direction == 1 else price * (1.0 + slip)
        gross = (price - trade["entry"]) * qty * direction
        fee = price * qty * self.fee_rate
        trade["gross_realized"] += gross
        trade["exit_fees"] += fee
        trade["exit_notional"] += price * qty
        trade["remaining"] = max(0.0, float(trade["remaining"]) - qty)
        self.state["balance"] += gross - fee
        log.info("PAPER EXIT | %s %s reason=%s price=%.8f qty=%.8f gross=$%.4f fee=$%.4f",
                 trade["side"], trade["symbol"], reason, price, qty, gross, fee)

    def _exit(self, trade, price, fraction, reason):
        self._exit_qty(trade, price, trade["qty"] * fraction, reason)

    def on_candle(self, symbol, candle):
        trade = self.state["open_trades"].get(symbol)
        if not trade:
            self._maybe_finish()
            return
        bar_time = int(candle.get("close_time", _now() * 1000)) // 1000
        bar_open = candle.get("open_time")
        if bar_open is not None and int(bar_open) <= trade.get("last_candle_open", -1):
            return
        trade["last_bar_at"] = bar_time
        if bar_time >= self.state["ends_at"]:
            close = float(candle["close"])
            self._exit(trade, close, 1.0, "CHALLENGE_END")
            self._close_trade(symbol, "CHALLENGE_END")
            return
        # A 5m candle that began before a delayed entry must not
        # manage the new position using price action from before the entry.
        if int(candle.get("open_time", 0)) < int(trade.get("entry_time", 0)):
            return
        if bar_open is not None:
            trade["last_candle_open"] = int(bar_open)
        high, low = float(candle["high"]), float(candle["low"])
        trade["last_mark"] = float(candle["close"])
        side = trade["side"]
        stop = float(trade["active_stop"])
        open_price = float(candle.get("open", stop))
        stop_fill = min(stop, open_price) if side == "LONG" else max(stop, open_price)
        stop_hit = low <= stop if side == "LONG" else high >= stop

        # Keep the original exit rules for positions opened before this release.
        if int(trade.get("management_version", 1)) < 2:
            if stop_hit:
                self._exit(trade, stop_fill, 1.0, "STOP")
                self._close_trade(symbol, "STOP")
                return
            t1, t2, t3 = trade["targets"]
            touched = (lambda target: high >= target) if side == "LONG" else (lambda target: low <= target)
            if not trade["tp1"] and touched(t1):
                self._exit(trade, t1, 0.50, "TP1")
                trade["tp1"] = True
                trade["active_stop"] = trade["entry"]
                if (low <= trade["active_stop"] if side == "LONG" else high >= trade["active_stop"]):
                    self._exit(trade, trade["active_stop"], 1.0, "BREAKEVEN")
                    self._close_trade(symbol, "BREAKEVEN")
                    return
            if trade["tp1"] and not trade["tp2"] and touched(t2):
                self._exit(trade, t2, 0.30, "TP2")
                trade["tp2"] = True
                trade["active_stop"] = t1
                if (low <= t1 if side == "LONG" else high >= t1):
                    self._exit(trade, t1, 1.0, "TRAILING_STOP")
                    self._close_trade(symbol, "TRAILING_STOP")
                    return
            if trade["tp2"] and touched(t3):
                self._exit(trade, t3, 1.0, "TP3")
                self._close_trade(symbol, "TP3")
                return
        elif int(trade.get("management_version", 1)) == 2:
            t1, t2, t3 = trade["targets"]
            touched = (lambda target: high >= target) if side == "LONG" else (lambda target: low <= target)
            if stop_hit:
                self._exit(trade, stop_fill, 1.0, "PROTECTED_STOP" if trade["tp1"] else "STOP")
                self._close_trade(symbol, "PROTECTED_STOP" if trade["tp1"] else "STOP")
                return

            if not trade["tp1"] and touched(t1):
                self._exit_qty(trade, t1, trade["tp1_qty"], "TP1_70_PERCENT")
                trade["tp1"] = True
                risk = float(trade["risk_distance"])
                fee_buffer = float(trade["entry"]) * (2 * self.fee_rate + 0.0001)
                lock = max(risk * LOCK_PROFIT_R, fee_buffer)
                trade["active_stop"] = trade["entry"] + lock if side == "LONG" else trade["entry"] - lock
                log.info("PAPER PROTECT | %s stop moved into profit zone: %.8f",
                         symbol, trade["active_stop"])

            if trade["tp1"] and not trade["tp2"] and touched(t2):
                self._exit_qty(trade, t2, trade["tp2_qty"], "TP2_20_PERCENT")
                trade["tp2"] = True
                # The final 10% is protected at TP1 and then trailed behind
                # the latest completed 5m candle. The new trail applies next candle.
                trade["active_stop"] = t1
                log.info("PAPER TRAIL | %s stop advanced to TP1: %.8f", symbol, t1)

            if trade["tp2"] and touched(t3):
                self._exit(trade, t3, 1.0, "TP3_RUNNER")
                self._close_trade(symbol, "TP3_RUNNER")
                return

            if trade["tp2"] and trade["remaining"] > 0:
                risk = float(trade["risk_distance"])
                if side == "LONG":
                    trail = float(candle["low"]) - TRAIL_BUFFER_R * risk
                    trade["active_stop"] = max(float(trade["active_stop"]), trail)
                else:
                    trail = float(candle["high"]) + TRAIL_BUFFER_R * risk
                    trade["active_stop"] = min(float(trade["active_stop"]), trail)

        else:
            direction = 1 if side == "LONG" else -1
            risk = float(trade["risk_distance"])
            entry = float(trade["entry"])
            close = float(candle["close"])
            candle_open = float(candle["open"])
            candle_range = max(high - low, entry * 1e-12)
            body = close - candle_open
            body_ratio = abs(body) / candle_range
            close_near_favorable_extreme = (
                close >= high - self.impulse_close_position * candle_range
                if side == "LONG"
                else close <= low + self.impulse_close_position * candle_range
            )
            favorable_close_r = direction * (close - entry) / risk

            if stop_hit:
                reason = "BREAKEVEN_STOP" if trade.get("breakeven_moved") else "STOP"
                self._exit(trade, stop_fill, 1.0, reason)
                self._close_trade(symbol, reason)
                return

            favorable_excursion = (
                (high - entry) if side == "LONG" else (entry - low)
            )
            if (not trade.get("breakeven_moved")
                    and favorable_excursion >= self.breakeven_trigger_r * risk):
                # Include both entry/exit fees and modeled slippage so the protected
                # stop is near net-zero, not merely equal to the raw entry price.
                friction = entry * (2 * self.fee_rate + 2 * float(trade.get("slippage_rate", 0.0)))
                new_stop = entry + direction * friction
                improves = new_stop > float(trade["active_stop"]) if side == "LONG" else new_stop < float(trade["active_stop"])
                if improves and direction * (close - new_stop) >= 0:
                    trade["active_stop"] = new_stop
                    trade["breakeven_moved"] = True
                    log.info("PAPER SPRINT BREAKEVEN | %s %s trigger=%.2fR stop=%.8f",
                             side, symbol, self.breakeven_trigger_r, new_stop)

            impulse_candle = (
                direction * body > 0
                and body_ratio >= self.impulse_body_ratio
                and close_near_favorable_extreme
                and favorable_close_r >= self.impulse_exit_r
            )
            if impulse_candle:
                self._exit(trade, close, 1.0, "IMPULSE_PROFIT")
                self._close_trade(symbol, "IMPULSE_PROFIT")
                return

            t1, t2, t3 = trade["targets"]
            touched = (lambda target: high >= target) if side == "LONG" else (lambda target: low <= target)
            if not trade["tp1"] and touched(t1):
                self._exit_qty(trade, t1, trade["tp1_qty"], "TP1_70_PERCENT")
                trade["tp1"] = True
            if trade["tp1"] and not trade["tp2"] and touched(t2):
                self._exit_qty(trade, t2, trade["tp2_qty"], "TP2_20_PERCENT")
                trade["tp2"] = True
                trade["active_stop"] = t1
            if trade["tp2"] and touched(t3):
                self._exit(trade, t3, 1.0, "TP3_RUNNER")
                self._close_trade(symbol, "TP3_RUNNER")
                return

        if bar_time - trade["opened_at"] >= 12 * 3600:
            close = float(candle["close"])
            self._exit(trade, close, 1.0, "TIME_EXIT")
            self._close_trade(symbol, "TIME_EXIT")
            return
        if trade["remaining"] <= max(1e-12, trade["qty"] * 1e-10):
            self._close_trade(symbol, "TARGETS_COMPLETED")
            return
        self._save_data()

    def _close_trade(self, symbol, reason):
        trade = self.state["open_trades"].pop(symbol)
        net = trade["gross_realized"] - trade["entry_fee"] - trade["exit_fees"]
        exit_qty = max(0.0, float(trade["qty"]) - float(trade["remaining"]))
        row = {
            "symbol": symbol, "side": trade["side"], "opened_at": trade["opened_at"],
            "closed_at": trade.get("last_bar_at", _now()), "reason": reason,
            "entry": round(float(trade["entry"]), 10),
            "exit_avg_price": round(float(trade.get("exit_notional", 0.0)) / exit_qty, 10) if exit_qty else None,
            "entry_fee": round(float(trade["entry_fee"]), 8),
            "exit_fee": round(float(trade["exit_fees"]), 8),
            "total_fees": round(float(trade["entry_fee"]) + float(trade["exit_fees"]), 8),
            "gross_pnl": round(float(trade["gross_realized"]), 8),
            "net_pnl": round(net, 8),
            "risk_budget": round(trade["risk_budget"], 8),
            "net_R": round(net / trade["risk_budget"], 4) if trade["risk_budget"] else 0
        }
        self.state["closed_trades"].append(row)
        self.state["equity_peak"] = max(float(self.state.get("equity_peak", self.start_balance)), float(self.state["balance"]))
        peak = float(self.state["equity_peak"])
        drawdown = (peak - float(self.state["balance"])) / peak if peak > 0 else 0.0
        self.state["max_drawdown"] = max(float(self.state.get("max_drawdown", 0.0)), drawdown)
        self._save_data()
        log.info("PAPER CLOSED | %s %s net=$%.4f net_R=%.3f balance=$%.2f",
                 row["side"], symbol, net, row["net_R"], self.state["balance"])
        self._maybe_finish()

    def _maybe_finish(self):
        if self.active() or self.state["summary_logged"]:
            return
        if self.state["open_trades"]:
            return
        trades = self.state["closed_trades"]
        pnl = self.state["balance"] - self.state["starting_balance"]
        wins = sum(1 for x in trades if x["net_pnl"] > 0)
        log.info("PAPER FINAL | start=$%.2f end=$%.2f pnl=$%.2f return=%.2f%% trades=%d wins=%d losses=%d",
                 self.state["starting_balance"], self.state["balance"], pnl,
                 pnl / self.state["starting_balance"] * 100, len(trades), wins, len(trades) - wins)
        self.state["summary_logged"] = True
        self._save_data()

    def status(self):
        unrealized = 0.0
        for trade in self.state["open_trades"].values():
            mark = float(trade.get("last_mark", trade["entry"]))
            direction = 1 if trade["side"] == "LONG" else -1
            unrealized += (mark - float(trade["entry"])) * float(trade["remaining"]) * direction
        return {
            "started_at": self.state["started_at"], "ends_at": self.state["ends_at"],
            "balance": round(self.state["balance"], 2),
            "unrealized_pnl": round(unrealized, 8),
            "equity": round(float(self.state["balance"]) + unrealized, 8),
            "max_drawdown": round(float(self.state.get("max_drawdown", 0.0)) * 100, 4),
            "open_trades": len(self.state["open_trades"]),
            "closed_trades": len(self.state["closed_trades"]),
            "active": self.active()
        }


paper_book = PaperBook()
