import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app import signal_delivery
from app.leverage import build_leverage_paper_signal
from app.paper_trader import PaperBook


class SignalPipelineTests(unittest.TestCase):
    def test_stop_includes_fees_and_slippage_both_directions_all_tiers(self):
        with tempfile.TemporaryDirectory() as tmp:
            for side in ("LONG", "SHORT"):
                for score, volume, leverage in ((75, 1.2, 10), (75, 1.6, 15), (80, 1.6, 20)):
                    book = PaperBook(path=str(Path(tmp)/f"{side}{leverage}"), start_balance=200,
                                     risk_fraction=.0025, max_gross_leverage=20)
                    signal = build_leverage_paper_signal(dict(symbol="X", side=side, entry=100,
                        confidence=score, volume_ratio=volume, candle_id=300000), 200)
                    self.assertEqual(signal["paper_leverage"], leverage)
                    self.assertTrue(book.open_trade(signal))
                    trade = book.state["open_trades"]["X"]
                    self.assertAlmostEqual(trade["entry"] * trade["qty"], 20*leverage)
                    book.on_candle("X",dict(open_time=600000,open=100,high=102,low=98,close=100))
                    self.assertAlmostEqual(book.state["closed_trades"][0]["net_pnl"], -.5)

    def test_success_for_one_chat_does_not_cancel_other_or_next_candle(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(signal_delivery,"PATH",str(Path(tmp)/"outbox")), patch.object(signal_delivery,"format_signal",return_value="signal"):
            signal = dict(symbol="X", candle_id=300000)
            signal_delivery.queue_signal(signal, ["a","b"])
            with sqlite3.connect(signal_delivery.PATH) as db:
                db.execute("UPDATE deliveries SET done=1 WHERE recipient='a'")
            signal_delivery.queue_signal(signal, ["a","b"])
            signal_delivery.queue_signal(dict(signal,candle_id=600000), ["a","b"])
            with sqlite3.connect(signal_delivery.PATH) as db:
                self.assertEqual(db.execute("SELECT recipient,candle,done FROM deliveries ORDER BY candle,recipient").fetchall(),
                                 [("a",300000,1),("b",300000,0),("a",600000,0),("b",600000,0)])
