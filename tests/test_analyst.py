import unittest

from app.analyst import analyst_lines, market_context, net_move


class AnalystTests(unittest.TestCase):
    def test_uses_only_matching_closed_hour(self):
        candle = dict(close_time=3899999, volume=80, taker_buy_volume=60)
        closed = dict(close_time=3599999, open=100, close=102)
        future = dict(close_time=7199999, open=99, close=10)
        btc = dict(closed, close=101)
        context = market_context(candle, [closed, future], [btc, future])
        self.assertAlmostEqual(context["relative"], 1.)
        self.assertEqual(context["buy_share"], 75.)
        self.assertIsNone(market_context(candle, [closed], [future])["btc_change"])

    def test_bad_volume_and_short_costs(self):
        bar = dict(close_time=299999, volume=5, taker_buy_volume=6)
        self.assertIsNone(market_context(bar, [], [])["buy_share"])
        for side, exit_price in (("LONG", 101), ("SHORT", 99)):
            self.assertLess(net_move(side, 100, exit_price), 1.)
            self.assertGreater(net_move(side, 100, exit_price), .87)
        signal = dict(symbol="ETHUSDT", side="SHORT", entry=100, stop=102, targets=[98.4],
                      market_context=dict(buy_share=60, btc_change=1, relative=2))
        self.assertIn("Оба фактора против SHORT", " ".join(analyst_lines(signal)))
