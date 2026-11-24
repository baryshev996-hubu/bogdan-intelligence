import unittest
from datetime import datetime, timezone

from app.upcoming_listings import (
    _market_type, _parse_explicit_utc, _extract_ticker, _event_score
)


class UpcomingListingTests(unittest.TestCase):
    def test_spot_listing_classification(self):
        self.assertEqual(_market_type("Binance will list Example (EXM)"), "SPOT")

    def test_futures_listing_classification(self):
        self.assertEqual(_market_type("Binance Futures will launch EXMUSDT perpetual contract"), "FUTURES")

    def test_excludes_tokenized_stocks_and_delistings(self):
        self.assertIsNone(_market_type("Binance adds TSLA tokenized stock"))
        self.assertIsNone(_market_type("Notice of removal of trading pairs"))

    def test_requires_explicit_utc_timestamp(self):
        parsed = _parse_explicit_utc("Trading starts at 12:30 UTC on 2026-12-01")
        self.assertEqual(parsed, datetime(2026, 12, 1, 12, 30, tzinfo=timezone.utc))
        self.assertIsNone(_parse_explicit_utc("Trading starts at 12:30 Kyiv time on 2026-12-01"))

    def test_extracts_common_ticker_formats(self):
        self.assertEqual(_extract_ticker("Example Token (EXM)"), "EXM")
        self.assertEqual(_extract_ticker("EXM/USDT trading"), "EXM")
        self.assertEqual(_extract_ticker("EXMUSDT perpetual"), "EXM")

    def test_event_score_is_capped(self):
        event = {"ticker": "EXM", "when": datetime.now(timezone.utc)}
        self.assertLessEqual(_event_score(event, 3), 95)


if __name__ == "__main__":
    unittest.main()
