import json
import tempfile
import time
import unittest
from pathlib import Path

from app.paper_trader import PaperBook


class PaperSprintV3Tests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = str(Path(self.tmp.name) / "ledger.json")
        self.book = PaperBook(
            path=self.path,
            start_balance=100.0,
            risk_fraction=0.005,
            max_open=2,
            max_gross_leverage=5.0,
            fee_rate=0.0005,
            ends_at=int(time.time()) + 3600,
            label="test sprint v3",
            breakeven_trigger_r=0.5,
            impulse_exit_r=0.55,
            impulse_body_ratio=0.65,
            impulse_close_position=0.20,
        )

    def tearDown(self):
        self.tmp.cleanup()

    def signal(self, side="LONG"):
        entry = 100.0
        stop = 99.0 if side == "LONG" else 101.0
        targets = [100.8, 101.5, 102.2] if side == "LONG" else [99.2, 98.5, 97.8]
        return {
            "symbol": "TESTUSDT",
            "side": side,
            "entry": entry,
            "stop": stop,
            "targets": targets,
            "candle_id": 0,
            "confidence": 80,
            "paper_management_version": 3,
            "paper_max_gross_leverage": 5.0,
            "paper_slippage_rate": 0.0001,
        }

    def test_favorable_move_moves_stop_to_net_breakeven(self):
        self.assertTrue(self.book.open_trade(self.signal()))
        trade = self.book.state["open_trades"]["TESTUSDT"]
        original_stop = trade["active_stop"]
        self.book.on_candle("TESTUSDT", {
            "open_time": 300000, "open": 100.10, "high": 100.60,
            "low": 100.05, "close": 100.40,
        })
        updated = self.book.state["open_trades"]["TESTUSDT"]
        self.assertTrue(updated["breakeven_moved"])
        self.assertGreater(updated["active_stop"], updated["entry"])
        self.assertGreater(updated["active_stop"], original_stop)

    def test_strong_favorable_impulse_closes_before_target(self):
        self.assertTrue(self.book.open_trade(self.signal()))
        self.book.on_candle("TESTUSDT", {
            "open_time": 300000, "open": 100.05, "high": 100.75,
            "low": 100.04, "close": 100.71,
        })
        self.assertNotIn("TESTUSDT", self.book.state["open_trades"])
        row = self.book.state["closed_trades"][-1]
        self.assertEqual(row["reason"], "IMPULSE_PROFIT")
        self.assertGreater(row["net_pnl"], 0)
        self.assertGreater(row["total_fees"], 0)
        self.assertIsNotNone(row["exit_avg_price"])

    def test_short_favorable_move_also_moves_stop_to_net_breakeven(self):
        self.assertTrue(self.book.open_trade(self.signal("SHORT")))
        self.book.on_candle("TESTUSDT", {
            "open_time": 300000, "open": 99.90, "high": 99.95,
            "low": 99.40, "close": 99.60,
        })
        trade = self.book.state["open_trades"]["TESTUSDT"]
        self.assertTrue(trade["breakeven_moved"])
        self.assertLess(trade["active_stop"], trade["entry"])

    def test_closed_ledger_contains_fees_and_exit_details(self):
        self.assertTrue(self.book.open_trade(self.signal()))
        self.book.on_candle("TESTUSDT", {
            "open_time": 300000, "open": 100.05, "high": 100.75,
            "low": 100.04, "close": 100.71,
        })
        with open(self.path, "r", encoding="utf-8") as f:
            ledger = json.load(f)
        row = ledger["closed_trades"][-1]
        for key in ("entry", "exit_avg_price", "reason", "entry_fee",
                    "exit_fee", "total_fees", "gross_pnl", "net_pnl"):
            self.assertIn(key, row)


if __name__ == "__main__":
    unittest.main()
