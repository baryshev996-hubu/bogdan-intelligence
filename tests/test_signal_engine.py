import unittest
from datetime import datetime, timezone

from app.signal_engine import atr, ema, evaluate, format_signal, macd_histogram, rsi, _reversal_conflict, confirm_signal_with_1m
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

    def test_1m_confirmation_reprices_short_entry(self):
        signal = {"side": "SHORT", "entry": 100.0, "stop": 102.0,
                  "targets": [98.0, 97.0, 95.0], "entry_zone": [99.5, 100.5],
                  "risk_pct": 2.0, "reasons": []}
        candle = {"open": 100.2, "high": 100.3, "low": 99.8,
                  "close": 99.9, "close_time": 1795585259999}
        confirmed = confirm_signal_with_1m(signal, candle)
        self.assertIsNotNone(confirmed)
        self.assertAlmostEqual(confirmed["entry"], 99.9)
        self.assertAlmostEqual(confirmed["stop"], 101.9)
        self.assertAlmostEqual(confirmed["targets"][0], 97.9)

    def test_weak_opposite_1m_candle_does_not_cancel_short(self):
        signal = {"side": "SHORT", "entry": 100.0, "stop": 102.0,
                  "targets": [98.0, 97.0, 95.0], "entry_zone": [99.5, 100.5],
                  "risk_pct": 2.0, "reasons": []}
        candle = {"open": 99.8, "high": 100.1, "low": 99.7,
                  "close": 99.9, "close_time": 1795585259999}
        confirmed = confirm_signal_with_1m(signal, candle)
        self.assertIsNotNone(confirmed)
        self.assertAlmostEqual(confirmed["entry"], 99.9)

    def test_1m_reversal_cancels_short_entry(self):
        signal = {"side": "SHORT", "entry": 100.0, "stop": 102.0,
                  "targets": [98.0, 97.0, 95.0], "entry_zone": [99.5, 100.5],
                  "risk_pct": 2.0, "reasons": []}
        candle = {"open": 100.0, "high": 100.5, "low": 99.9,
                  "close": 100.4, "close_time": 1795585259999}
        self.assertIsNone(confirm_signal_with_1m(signal, candle))

    def test_weak_bullish_1m_close_above_short_entry_is_vetoed(self):
        signal = {"side": "SHORT", "entry": 100.0, "stop": 102.0,
                  "targets": [98.0, 97.0, 95.0], "entry_zone": [99.5, 100.5],
                  "risk_pct": 2.0, "reasons": []}
        candle = {"open": 100.0, "high": 100.2, "low": 99.9,
                  "close": 100.1, "close_time": 1795585259999}
        self.assertIsNone(confirm_signal_with_1m(signal, candle))

    def test_short_allows_favorable_move_within_half_r(self):
        signal = {"side": "SHORT", "entry": 100.0, "stop": 102.0,
                  "targets": [98.0, 97.0, 95.0], "entry_zone": [99.5, 100.5],
                  "risk_pct": 2.0, "reasons": []}
        candle = {"open": 99.3, "high": 99.4, "low": 99.1,
                  "close": 99.2, "close_time": 1795585259999}
        self.assertIsNotNone(confirm_signal_with_1m(signal, candle))

    def test_short_rejects_adverse_move_beyond_quarter_r(self):
        signal = {"side": "SHORT", "entry": 100.0, "stop": 102.0,
                  "targets": [98.0, 97.0, 95.0], "entry_zone": [99.5, 100.5],
                  "risk_pct": 2.0, "reasons": []}
        candle = {"open": 100.5, "high": 100.7, "low": 100.4,
                  "close": 100.6, "close_time": 1795585259999}
        self.assertIsNone(confirm_signal_with_1m(signal, candle))

    def test_1m_confirmation_reprices_long_entry(self):
        signal = {"side": "LONG", "entry": 100.0, "stop": 98.0,
                  "targets": [102.0, 103.0, 105.0], "entry_zone": [99.5, 100.5],
                  "risk_pct": 2.0, "reasons": []}
        candle = {"open": 100.0, "high": 100.3, "low": 99.8,
                  "close": 100.2, "close_time": 1795585259999}
        confirmed = confirm_signal_with_1m(signal, candle)
        self.assertIsNotNone(confirmed)
        self.assertAlmostEqual(confirmed["entry"], 100.2)
        self.assertAlmostEqual(confirmed["stop"], 98.2)
        self.assertAlmostEqual(confirmed["targets"][0], 102.2)

    def test_confirmed_upward_reversal_blocks_short(self):
        candles = []
        for i in range(30):
            low = 99.0 + i * 0.02 if i >= 26 else 99.0
            candles.append({"open": 99.8, "high": 100.5, "low": low,
                            "close": 100.0, "volume": 100.0})
        candles[-1] = {"open": 100.0, "high": 102.5, "low": 100.0,
                       "close": 102.0, "volume": 200.0}
        blocked, _ = _reversal_conflict(candles, "SHORT")
        self.assertTrue(blocked)

    def test_confirmed_downward_reversal_blocks_long(self):
        candles = []
        for i in range(30):
            high = 101.0 - i * 0.02 if i >= 26 else 101.0
            candles.append({"open": 100.2, "high": high, "low": 99.5,
                            "close": 100.0, "volume": 100.0})
        candles[-1] = {"open": 100.0, "high": 100.0, "low": 97.5,
                       "close": 98.0, "volume": 200.0}
        blocked, _ = _reversal_conflict(candles, "LONG")
        self.assertTrue(blocked)

    def test_high_volume_countertrend_impulse_blocks_entry(self):
        candles = [{"open": 100.0, "high": 101.0, "low": 99.0,
                    "close": 100.0, "volume": 100.0} for _ in range(30)]
        candles[-1] = {"open": 100.0, "high": 100.8, "low": 99.9,
                       "close": 100.7, "volume": 160.0}
        blocked, _ = _reversal_conflict(candles, "SHORT")
        self.assertTrue(blocked)

    def test_single_opposite_candle_does_not_confirm_reversal(self):
        candles = [{"open": 100.0, "high": 101.0, "low": 99.0,
                    "close": 100.2, "volume": 100.0} for _ in range(30)]
        candles[-1] = {"open": 100.0, "high": 100.5, "low": 99.8,
                       "close": 100.4, "volume": 110.0}
        blocked, _ = _reversal_conflict(candles, "SHORT")
        self.assertFalse(blocked)

    def test_insufficient_history_never_emits_signal(self):
        self.assertIsNone(evaluate("BTCUSDT", {"5m": [], "15m": [], "1h": []}))

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
        self.assertIsNone(evaluate("BTCUSDT", history, as_of_ms=now_ms))


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
