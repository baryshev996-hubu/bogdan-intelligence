import unittest
from app.upcoming_listings import _telegram_listing_events
from app.listing_research import official_funding_excerpt

LINK = "https://www.binance.com/en/support/announcement/detail/" + "a" * 32


def post(title, body, link=LINK):
    return ('<div class="tgme_widget_message_text js-message_text" dir="auto"><b>'
            + title + '</b><br>' + body + '<br><a href="' + link
            + '">Learn more</a></div>')


class BinanceSourceTests(unittest.TestCase):
    def test_spot_and_explicit_time(self):
        events, count = _telegram_listing_events(post("Binance Will List Example (EXM)",
            "Binance will open trading at 2026-12-01 12:30 (UTC)."))
        self.assertEqual(count, 1)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["ticker"], "EXM")
        self.assertEqual(events[0]["id"], "binance:" + "a" * 32)
        self.assertEqual(events[0]["when"].hour, 12)

    def test_excludes_equities_and_nonlisting(self):
        page = post("Binance Futures Will Launch TradFi ABCUSDT Perpetual Contract",
                    "Trading starts at 2026-12-01 12:30 UTC.")
        page += post("Binance Will List Example (EXM) bStocks",
                     "Trading starts at 2026-12-01 12:30 UTC.")
        page += post("Binance Updates Example (EXM)",
                     "Trading starts at 2026-12-01 12:30 UTC.")
        self.assertEqual(_telegram_listing_events(page), ([], 3))

    def test_missing_time_and_links(self):
        page = post("Binance Will List Example (EXM)", "Deposits open 2026-12-01 12:30 UTC.")
        page += post("Binance Will List Example (EXM)", "Trading starts at 2026-12-01 12:30 UTC.",
                     "https://example.com/")
        self.assertEqual(_telegram_listing_events(page), ([], 2))
        with self.assertRaises(RuntimeError):
            _telegram_listing_events("<html>blocked</html>")

    def test_funding_excerpt(self):
        self.assertIsNone(official_funding_excerpt({"official_text": "No data about funds."}))
        self.assertEqual(official_funding_excerpt({"official_text": "The project is backed by Example Ventures."}),
                         "The project is backed by Example Ventures.")
