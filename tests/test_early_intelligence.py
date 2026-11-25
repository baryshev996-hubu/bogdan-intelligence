import unittest
from app.early_intelligence import binance_events, bybit_events, okx_events, anomaly, format_prelaunch, format_anomaly


class EarlyIntelligenceTests(unittest.TestCase):
    def test_binance_pending_only(self):
        row = dict(symbol="EXMUSDT", baseAsset="EXM", quoteAsset="USDT", status="PENDING_TRADING",
                   contractType="PERPETUAL", underlyingType="COIN", onboardDate=1799999999000)
        events = binance_events({"symbols": [row, dict(row, status="TRADING"),
                                              dict(row, underlyingType="STOCK"), dict(row, baseAsset="JD")]})
        self.assertEqual(len(events), 1)
        message = format_prelaunch(events[0])
        self.assertIn("не подтверждение обычного листинга", message)
        self.assertIn("пока без позиции", message)

    def test_bybit_premarket(self):
        row = dict(symbol="EXMUSDT", baseCoin="EXM", quoteCoin="USDT", status="PreLaunch",
                   contractType="LinearPerpetual", symbolType="", preListingInfo={"curAuctionPhase": "CallAuction"})
        events = bybit_events([row, dict(row, symbolType="xstocks"), dict(row, status="Trading")])
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["phase"], "CallAuction")

    def test_okx_crypto_only(self):
        row = dict(instId="EXM-USDT-SWAP", state="preopen", instCategory="1")
        self.assertEqual(len(okx_events({"code": "0", "data": [row, dict(row, instCategory="3")]})), 1)
        with self.assertRaises(ValueError):
            okx_events({"code": "500", "data": []})

    def test_anomaly_and_btc_alignment(self):
        history = [dict(open=100, close=100, volume=2000, close_time=i) for i in range(20)]
        final = dict(open=100, close=102, volume=7000, taker_buy_volume=5600, close_time=20)
        btc = [dict(open=100, close=100.1, close_time=20)]
        result = anomaly("EXMUSDT", history + [final], btc)
        self.assertIsNotNone(result)
        message = format_anomaly(result)
        self.assertIn("не LONG/SHORT сигнал", message)
        self.assertIn("Я бы рассматривал LONG", message)
        self.assertIn("агрессивные покупки 80.0%", message)
        self.assertIn("Подтверждение:", message)
        self.assertIn("Отмена:", message)
        short = anomaly("EXMUSDT", history + [dict(final, close=98, taker_buy_volume=1400)], btc)
        self.assertIsNotNone(short)
        message = format_anomaly(short)
        self.assertIn("Я бы рассматривал SHORT", message)
        self.assertIn("агрессивные продажи 80.0%", message)
        self.assertIn("удержания сопротивления", message)
        self.assertIsNone(anomaly("EXMUSDT", history + [final], []))
        self.assertIsNone(anomaly("EXMUSDT", history + [dict(final, taker_buy_volume=3500)], btc))
        self.assertIsNone(anomaly("EXMUSDT", history + [final], [dict(btc[0], close=102)]))
