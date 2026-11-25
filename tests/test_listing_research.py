import unittest
from datetime import datetime, timezone, timedelta
from app.listing_research import is_crypto_instrument, excluded_ticker, choose_coin, _market_summary, format_research
from app.upcoming_listings import _format_alert


class ListingResearchTests(unittest.TestCase):
    def test_crypto_category_and_equities(self):
        self.assertTrue(is_crypto_instrument({"instCategory": "1"}))
        for value in ("3", "4", "5", "6", "7", "", None):
            self.assertFalse(is_crypto_instrument({"instCategory": value}))
        for ticker in ("TENCENT", "MEITUAN", "KUAISHOU", "JD"):
            self.assertTrue(excluded_ticker(ticker))

    def test_identity_not_ticker_only(self):
        coin = {"id": "example", "symbol": "EXM", "name": "Example Protocol"}
        self.assertIsNone(choose_coin({"ticker": "EXM", "title": "EXM-USDT-SWAP"}, [coin]))
        self.assertEqual(choose_coin({"ticker": "EXM", "title": "Binance will list Example Protocol (EXM)"}, [coin]), coin)
        self.assertIsNone(choose_coin({"ticker": "EXM", "title": "Example Protocol"}, [coin, dict(coin, id="other")]))

    def test_missing_data_and_risk_flags(self):
        self.assertEqual(_market_summary({})["coverage"], 0)
        summary = _market_summary(dict(market_cap=10000000, fully_diluted_valuation=100000000,
                   total_volume=500000, circulating_supply=1000000, max_supply=10000000))
        self.assertEqual(summary["coverage"], 4)
        self.assertAlmostEqual(summary["float_pct"], 10)
        self.assertEqual(len(summary["flags"]), 3)
        self.assertIn("Инвесторы: н/д; не подтверждены", format_research({}))

    def test_html_alert_and_disclaimer(self):
        event = dict(ticker="EXM", exchange="Binance", market_type="SPOT",
                     when=datetime.now(timezone.utc)+timedelta(hours=1),
                     source="official & public", url="https://www.binance.com/")
        message = _format_alert(event, 75, {"Binance"})
        self.assertTrue(message.startswith("🚨 BOGDAN INTELLIGENCE | UPCOMING LISTING\n\n<b>"))
        self.assertIn("official &amp; public", message)
        self.assertIn("не вероятность прибыли", message)
        self.assertIn("Капитализация: н/д", message)
