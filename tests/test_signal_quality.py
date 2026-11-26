import unittest
from copy import deepcopy
from unittest.mock import patch

from app.signal_quality import assess_quality


class DirectionQualityTests(unittest.TestCase):
    def setup_case(self, side="LONG", symbol="1000BONKUSDT"):
        long = side == "LONG"
        end = 3600000 * 100 + 299999
        bars = []
        for i in range(60):
            close_time = end - (59-i) * 300000
            bars.append(dict(open_time=close_time-299999, close_time=close_time,
                             high=101, low=99, close=100.8 if long else 99.2,
                             volume=100, taker_buy_volume=56 if long else 44))
        histories = {"5m": bars,
                     "15m": [{"close_time": (end+1)//900000*900000-1}],
                     "1h": [{"close_time": (end+1)//3600000*3600000-1}]}
        s = dict(side=side, symbol=symbol, confidence=75, entry=100,
                 stop=99 if long else 101, targets=[100.8 if long else 99.2],
                 candle_id=bars[-1]["open_time"])
        c = dict(buy_share=56 if long else 44,
                 btc_change=0.33 if long else -0.33,
                 relative=0.2 if long else -0.2)
        return s,c,histories

    def assess(self, s,c,h, fading=False):
        sign = 1 if s["side"] == "LONG" else -1
        with patch("app.signal_quality.macd_histogram", side_effect=[sign*0.2, sign*(0.3 if fading else 0.1)]):
            return assess_quality(s,c,h)

    def test_bonk_regression(self):
        s,c,h = self.setup_case()
        c.update(buy_share=48.6294974183, btc_change=0.3250113241, relative=-0.1429448699)
        self.assertEqual(self.assess(s,c,h), (0,"quality_flow_unconfirmed"))

    def test_mirrored_short_regression(self):
        s,c,h = self.setup_case("SHORT")
        c.update(buy_share=51.3705025817, btc_change=-0.3250113241, relative=0.1429448699)
        self.assertEqual(self.assess(s,c,h), (0,"quality_flow_unconfirmed"))

    def test_confirmed_long_and_short(self):
        scores = []
        for side in ("LONG","SHORT"):
            s,c,h = self.setup_case(side)
            score,reason = self.assess(s,c,h)
            self.assertIsNone(reason)
            self.assertGreaterEqual(score,75)
            self.assertLessEqual(score,100)
            scores.append(score)
        self.assertEqual(scores[0],scores[1])

    def test_relative_disagreement(self):
        for side in ("LONG","SHORT"):
            s,c,h = self.setup_case(side)
            c["relative"] *= -1
            self.assertEqual(self.assess(s,c,h)[1], "quality_relative_unconfirmed")

    def test_missing_and_nonfinite_context(self):
        for key in ("buy_share","relative","btc_change"):
            for val in (None,float("nan")):
                s,c,h = self.setup_case()
                c[key]=val
                self.assertEqual(self.assess(s,c,h)[1],"quality_missing_context")

    def test_btc_itself_does_not_require_relative_strength(self):
        s,c,h = self.setup_case(symbol="BTCUSDT")
        c["relative"]=None
        self.assertIsNone(self.assess(s,c,h)[1])

    def test_strong_btc_opposition(self):
        s,c,h = self.setup_case()
        c["btc_change"]=-0.8
        self.assertEqual(self.assess(s,c,h)[1],"quality_btc_opposition")

    def test_fading_impulse_both_directions(self):
        for side in ("LONG","SHORT"):
            s,c,h = self.setup_case(side)
            self.assertEqual(self.assess(s,c,h,True)[1],"quality_momentum_fading")

    def test_rejection_wick_both_directions(self):
        for side in ("LONG","SHORT"):
            s,c,h = self.setup_case(side)
            h["5m"][-1]["close"]=100
            self.assertEqual(self.assess(s,c,h)[1],"quality_rejection_wick")

    def test_persistent_opposite_flow(self):
        s,c,h = self.setup_case()
        for bar in h["5m"][-3:-1]:
            bar["taker_buy_volume"]=10
        self.assertEqual(self.assess(s,c,h)[1],"quality_flow_persistence")

    def test_missing_flow_and_candle_gap(self):
        s,c,h = self.setup_case()
        h["5m"][-2]["taker_buy_volume"]=None
        self.assertEqual(self.assess(s,c,h)[1],"quality_missing_flow_history")
        s,c,h = self.setup_case()
        h["5m"][-2]["open_time"]-=300000
        self.assertEqual(self.assess(s,c,h)[1],"quality_history_gap")

    def test_stale_and_future_context(self):
        for tf in ("15m","1h"):
            for shift in (-900000,900000):
                s,c,h = self.setup_case()
                h[tf][-1]["close_time"]+=shift
                self.assertEqual(self.assess(s,c,h)[1],"quality_unaligned_"+tf)

    def test_costs_block_tiny_targets(self):
        s,c,h = self.setup_case()
        s["stop"]=99.9
        s["targets"]=[100.08]
        self.assertEqual(self.assess(s,c,h)[1],"quality_cost_adjusted_reward")

    def test_maximum_score(self):
        s,c,h = self.setup_case()
        s["confidence"]=80
        self.assertEqual(self.assess(s,c,h),(100,None))


if __name__ == "__main__":
    unittest.main()
