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
                 ends_at=None, label="10-day"):
        self.path = path or PATH
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
            "closed_trades": [], "open_trades": {}, "summary_logged": False
        }

    def _load(self):
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if data.get("version") != 1:
                raise ValueError("Unknown paper ledger version")
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
        distance = abs(entry - stop)
        if entry <= 0 or distance <= 0:
            return False
        risk_budget = equity * self.risk_fraction
        qty = risk_budget / distance
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
            "tp1": False, "tp2": False, "opened_at": _now(),
            "signal_confidence": int(signal.get("confidence", 0)),
            "paper_leverage_cap": min(self.max_gross_leverage, max(1.0, float(signal.get("paper_max_gross_leverage", self.max_gross_leverage)))),
            "entry_time": int(signal.get("entry_time", signal.get("candle_id", 0)) or 0),
            "management_version": 2,
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
        gross = (price - trade["entry"]) * qty * direction
        fee = price * qty * self.fee_rate
        trade["gross_realized"] += gross
        trade["exit_fees"] += fee
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
        if not self.active():
            close = float(candle["close"])
            self._exit(trade, close, 1.0, "CHALLENGE_END")
            self._close_trade(symbol, "CHALLENGE_END")
            return
        # A 5m candle that began before a delayed 1m-confirmed entry must not
        # manage the new position using price action from before the entry.
        if int(candle.get("open_time", 0)) < int(trade.get("entry_time", 0)):
            return
        high, low = float(candle["high"]), float(candle["low"])
        side = trade["side"]
        stop = float(trade["active_stop"])
        stop_hit = low <= stop if side == "LONG" else high >= stop

        # Keep the original exit rules for positions opened before this release.
        if int(trade.get("management_version", 1)) < 2:
            if stop_hit:
                self._exit(trade, stop, 1.0, "STOP")
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
        else:
            t1, t2, t3 = trade["targets"]
            touched = (lambda target: high >= target) if side == "LONG" else (lambda target: low <= target)
            if stop_hit:
                self._exit(trade, stop, 1.0, "PROTECTED_STOP" if trade["tp1"] else "STOP")
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

        if _now() - trade["opened_at"] >= 12 * 3600:
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
        row = {
            "symbol": symbol, "side": trade["side"], "opened_at": trade["opened_at"],
            "closed_at": _now(), "reason": reason, "net_pnl": round(net, 8),
            "risk_budget": round(trade["risk_budget"], 8),
            "net_R": round(net / trade["risk_budget"], 4) if trade["risk_budget"] else 0
        }
        self.state["closed_trades"].append(row)
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
        return {
            "started_at": self.state["started_at"], "ends_at": self.state["ends_at"],
            "balance": round(self.state["balance"], 2),
            "open_trades": len(self.state["open_trades"]),
            "closed_trades": len(self.state["closed_trades"]),
            "active": self.active()
        }


paper_book = PaperBook()
