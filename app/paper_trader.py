"""Ten-day virtual portfolio for BOGDAN INTELLIGENCE.

No exchange orders are ever placed. State is persisted on Railway's /data volume.
"""
import json
import logging
import os
import tempfile
import time
from datetime import datetime, timezone

log = logging.getLogger("bogdan-intel.paper")
PATH = os.getenv("PAPER_LEDGER_PATH", "/data/paper_trading.json")
START_BALANCE = float(os.getenv("PAPER_START_BALANCE", "100"))
RISK_FRACTION = float(os.getenv("PAPER_RISK_FRACTION", "0.01"))
MAX_OPEN = int(os.getenv("PAPER_MAX_OPEN", "3"))
MAX_GROSS_LEVERAGE = float(os.getenv("PAPER_MAX_GROSS_LEVERAGE", "2"))
FEE_RATE = float(os.getenv("PAPER_TAKER_FEE", "0.0005"))
DURATION_SECONDS = int(os.getenv("PAPER_DURATION_DAYS", "10")) * 86400


def _now():
    return int(time.time())


class PaperBook:
    def __init__(self):
        self.state = self._load()

    def _fresh(self):
        now = _now()
        return {
            "version": 1, "started_at": now, "ends_at": now + DURATION_SECONDS,
            "starting_balance": START_BALANCE, "balance": START_BALANCE,
            "closed_trades": [], "open_trades": {}, "summary_logged": False
        }

    def _load(self):
        try:
            with open(PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
            if data.get("version") != 1:
                raise ValueError("Unknown paper ledger version")
            return data
        except FileNotFoundError:
            os.makedirs(os.path.dirname(PATH) or ".", exist_ok=True)
            data = self._fresh()
            self._save_data(data)
            log.info("PAPER START | balance=$%.2f | duration=10 days | risk=%.2f%% per trade",
                     START_BALANCE, RISK_FRACTION * 100)
            return data

    def _save_data(self, data=None):
        data = self.state if data is None else data
        os.makedirs(os.path.dirname(PATH) or ".", exist_ok=True)
        fd, tmp = tempfile.mkstemp(prefix="paper-", dir=os.path.dirname(PATH) or ".")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, PATH)
        finally:
            if os.path.exists(tmp):
                os.unlink(tmp)

    def active(self):
        return _now() < self.state["ends_at"]

    def open_trade(self, signal):
        symbol = signal["symbol"]
        if not self.active():
            return False
        if symbol in self.state["open_trades"] or len(self.state["open_trades"]) >= MAX_OPEN:
            return False
        equity = float(self.state["balance"])
        if equity <= 0:
            return False
        entry, stop = float(signal["entry"]), float(signal["stop"])
        distance = abs(entry - stop)
        if entry <= 0 or distance <= 0:
            return False
        risk_budget = equity * RISK_FRACTION
        qty = risk_budget / distance
        max_notional = equity * MAX_GROSS_LEVERAGE
        qty = min(qty, max_notional / entry)
        notional = qty * entry
        if qty <= 0:
            return False
        fee = notional * FEE_RATE
        self.state["balance"] -= fee
        self.state["open_trades"][symbol] = {
            "symbol": symbol, "side": signal["side"], "entry": entry,
            "initial_stop": stop, "active_stop": stop,
            "targets": [float(x) for x in signal["targets"]],
            "qty": qty, "remaining": qty, "risk_budget": risk_budget,
            "entry_fee": fee, "exit_fees": 0.0, "gross_realized": 0.0,
            "tp1": False, "tp2": False, "opened_at": _now(),
            "signal_confidence": int(signal.get("confidence", 0))
        }
        self._save_data()
        log.info("PAPER OPEN | %s %s entry=%.8f stop=%.8f qty=%.8f notional=$%.2f risk=$%.2f fee=$%.4f",
                 signal["side"], symbol, entry, stop, qty, notional, risk_budget, fee)
        return True

    def _exit(self, trade, price, fraction, reason):
        qty = min(trade["remaining"], trade["qty"] * fraction)
        if qty <= 0:
            return
        direction = 1 if trade["side"] == "LONG" else -1
        gross = (price - trade["entry"]) * qty * direction
        fee = price * qty * FEE_RATE
        trade["gross_realized"] += gross
        trade["exit_fees"] += fee
        trade["remaining"] -= qty
        self.state["balance"] += gross - fee
        log.info("PAPER EXIT | %s %s reason=%s price=%.8f qty=%.8f gross=$%.4f fee=$%.4f",
                 trade["side"], trade["symbol"], reason, price, qty, gross, fee)

    def on_candle(self, symbol, candle):
        trade = self.state["open_trades"].get(symbol)
        if not trade:
            self._maybe_finish()
            return
        high, low = float(candle["high"]), float(candle["low"])
        side = trade["side"]
        stop = trade["active_stop"]
        # Conservative intrabar ordering: if stop and target are both touched,
        # assume the stop filled first.
        stop_hit = low <= stop if side == "LONG" else high >= stop
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
            self._exit(trade, t2, 0.60, "TP2")  # 60% of remaining = 30% of original
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

        if _now() - trade["opened_at"] >= 12 * 3600:
            close = float(candle["close"])
            self._exit(trade, close, 1.0, "TIME_EXIT")
            self._close_trade(symbol, "TIME_EXIT")
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
