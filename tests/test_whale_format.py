import unittest

from app.market_intel import format_whale_alert


class WhaleFormatTests(unittest.TestCase):
    def test_absorption_buy(self):
        context = dict(reaction="ВОЗМОЖНОЕ ПОГЛОЩЕНИЕ ПОКУПОК", price_change_1m=-.097,
                       price_change_5m=-.129, oi_change_5m=-.241, funding=.000056)
        message = format_whale_alert("BTC", "BUY", 1129101, 112139, 58798.1056, 25,
                                     context, "🟡 POSSIBLE ABSORPTION")
        self.assertTrue(message.startswith("🐳 BOGDAN INTELLIGENCE | 🟡 POSSIBLE ABSORPTION | BTC\n\n<b>"))
        for value in ("$1,129,101", "$112,139", "$58,798.1056", "25 исполнений",
                      "-0.097%", "-0.129%", "-0.241%", "+0.0056%", "🟢 <b>BUY FLOW",
                      "Цена 1m: снижается", "не торговый сигнал"):
            self.assertIn(value, message)
        self.assertNotIn("Цена подтверждает направление потока.", message)

    def test_confirmed_sell_and_missing_data(self):
        context = dict(reaction="ПРОДАВЦЫ ПОЛУЧАЮТ ПОДТВЕРЖДЕНИЕ", price_change_1m=-.2)
        message = format_whale_alert("A&B", "SELL", 600000, 90000, 100, 12,
                                     context, "🟢 FLOW CONFIRMED")
        self.assertIn("A&amp;B", message)
        self.assertIn("🔴 <b>SELL FLOW", message)
        self.assertIn("Цена подтверждает направление потока.", message)
        self.assertIn("Изменение 5m: н/д", message)
        self.assertIn("Funding: н/д", message)

    def test_absorption_sell(self):
        message = format_whale_alert("ETH", "SELL", 600000, 90000, 100, 12,
                    dict(reaction="ВОЗМОЖНОЕ ПОГЛОЩЕНИЕ ПРОДАЖ", price_change_1m=.2),
                    "🟡 POSSIBLE ABSORPTION")
        self.assertIn("Цена 1m: растёт", message)
        self.assertIn("Возможно поглощение", message)
