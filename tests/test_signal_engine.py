import unittest
from datetime import datetime, timezone

from app.signal_engine import atr, ema, evaluate, format_signal, macd_histogram, rsi
from app.backtest import simulate_trade


class SignalEngineTests(unittest.TestCase):
    def test_ema(self):
        self.assertAlmostEqual(ema([1, 2, 3, 4, 5], 3), 4.0)

    def test_rsi_edge_cases(self):
        self.assertEqual(rsi([10] * 20), 50.0)
        self.assertEqual(rsi(list(range(1, 21))), 100.0)
        self.assertEqual(rsi(list(range(20, 0, -1))), 0.0)

    def test_atr(self):
        candles = [
            {"high": 11.0, "low": 9.0, "close": 10.0},
            {"high": 12.0, "low": 10.0, "close": 11.0},
            {"high": 13.0, "low": 11.0, "close": 12.0},
        ]
        self.assertAlmostEqual(atr(candles, 2), 2.0)

    def test_macd_returns_value_with_enough_data(self):
        values = [100 + i * 0.1 + (i % 4) * 0.03 for i in range(80)]
        self.assertIsNotNone(macd_histogram(values))

    def test_insufficient_history_never_emits_signal(self):
        diagnostics = {}
        self.assertIsNone(evaluate("BTCUSDT", {"5m": [], "15m": [], "1h": []},
                                   diagnostics=diagnostics))
        self.assertEqual(diagnostics, {"history_short": 1})

    def test_stale_history_never_emits_signal(self):
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        old_time = now_ms - 10 * 60 * 60 * 1000
        candle = {
            "open_time": old_time - 300000,
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.5,
            "volume": 1000.0,
            "close_time": old_time,
        }
        history = {tf: [dict(candle) for _ in range(60)] for tf in ("5m", "15m", "1h")}
        diagnostics = {}
        self.assertIsNone(evaluate("BTCUSDT", history, as_of_ms=now_ms,
                                   diagnostics=diagnostics))
        self.assertEqual(diagnostics, {"stale_candles": 1})


    def test_backtest_long_full_targets(self):
        signal = {"side": "LONG", "entry": 100.0, "stop": 98.0,
                  "targets": [102.0, 104.0, 106.0]}
        future = [{"low": 103.0, "high": 107.0, "close": 106.5}]
        result, _ = simulate_trade(signal, future)
        self.assertAlmostEqual(result, 1.08)

    def test_backtest_stop_first_when_stop_and_target_share_candle(self):
        signal = {"side": "LONG", "entry": 100.0, "stop": 98.0,
                  "targets": [102.0, 104.0, 106.0]}
        future = [{"low": 97.5, "high": 103.0, "close": 100.0}]
        result, _ = simulate_trade(signal, future)
        self.assertAlmostEqual(result, -1.0)

    def test_backtest_short_partial_exit(self):
        signal = {"side": "SHORT", "entry": 100.0, "stop": 102.0,
                  "targets": [98.0, 96.0, 94.0]}
        future = [{"low": 97.5, "high": 100.5, "close": 98.0}]
        result, _ = simulate_trade(signal, future)
        self.assertAlmostEqual(result, 0.86)

    def test_signal_message_has_trade_levels_and_no_execution_claim(self):
        signal = {
            "symbol": "BTCUSDT",
            "side": "LONG",
            "entry": 100.0,
            "entry_zone": [99.5, 100.5],
            "stop": 98.0,
            "targets": [102.0, 104.0, 106.0],
            "confidence": 80,
            "risk_pct": 2.0,
            "reasons": ["Trend aligned", "Volume confirmed"],
            "candle_time": "2026-11-24 20:00 UTC",
        }
        message = format_signal(signal)
        self.assertIn("LONG", message)
        self.assertIn("Stop Loss", message)
        self.assertIn("Take Profit 3", message)
        self.assertIn("без открытия сделок", message)


if __name__ == "__main__":
    unittest.main()


from app.upcoming_listings import _market_type, _parse_explicit_utc, _extract_ticker, _event_score


class UpcomingListingTests(unittest.TestCase):
    def test_spot_and_futures_classification(self):
        self.assertEqual(_market_type("Binance will list Example (EXM)"), "SPOT")
        self.assertEqual(_market_type("Binance Futures will launch EXMUSDT perpetual contract"), "FUTURES")

    def test_excludes_stocks_and_delistings(self):
        self.assertIsNone(_market_type("Binance adds TSLA tokenized stock"))
        self.assertIsNone(_market_type("Notice of removal of trading pairs"))

    def test_explicit_utc_only(self):
        parsed = _parse_explicit_utc("Trading starts at 12:30 UTC on 2026-12-01")
        self.assertEqual(parsed, datetime(2026, 12, 1, 12, 30, tzinfo=timezone.utc))
        self.assertIsNone(_parse_explicit_utc("Trading starts at 12:30 Kyiv time on 2026-12-01"))

    def test_ticker_formats(self):
        self.assertEqual(_extract_ticker("Example Token (EXM)"), "EXM")
        self.assertEqual(_extract_ticker("EXM/USDT trading"), "EXM")
        self.assertEqual(_extract_ticker("EXMUSDT perpetual"), "EXM")

    def test_event_score_cap(self):
        event = {"ticker": "EXM", "when": datetime.now(timezone.utc)}
        self.assertLessEqual(_event_score(event, 3), 95)
