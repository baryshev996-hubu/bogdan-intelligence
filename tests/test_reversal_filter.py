import unittest

from app.reversal_filter import reversal_warning


class ReversalFilterTests(unittest.TestCase):
    def test_insufficient_history_never_blocks(self):
        candles = [{"open": 1, "high": 2, "low": 0.5, "close": 1.5} for _ in range(20)]
        self.assertEqual(reversal_warning("LONG", candles), (False, []))

    def test_invalid_side_never_blocks(self):
        candles = [{"open": 1, "high": 2, "low": 0.5, "close": 1.5} for _ in range(50)]
        self.assertEqual(reversal_warning("FLAT", candles), (False, []))

    def test_ordered_uptrend_has_no_reversal_confirmation(self):
        candles = []
        for i in range(60):
            p = 100 + i
            candles.append({
                "open": p - 0.2, "high": p + 0.5, "low": p - 0.5,
                "close": p, "volume": 100, "open_time": i * 300000,
                "close_time": (i + 1) * 300000 - 1,
            })
        blocked, reasons = reversal_warning("LONG", candles)
        self.assertFalse(blocked)
        self.assertLess(len(reasons), 2)


if __name__ == "__main__":
    unittest.main()
