import unittest

from app.order_flow import format_order_flow_alert


class OrderFlowFormatTests(unittest.TestCase):
    def test_bid_absorption_example(self):
        message = format_order_flow_alert("SOLUSDT", "BID", 67.68, 999607,
                                          .037, 101.3, 533158, 774835)
        self.assertTrue(message.startswith("🧭 ORDER FLOW INTELLIGENCE | SOLUSDT\n\n<b>"))
        for value in ("BID-СТЕНА", "LONG-КОНТЕКСТ", "$999,607", "<code>67.68</code>",
                      "0.037%", "101.3 сек.", "$533,158", "$774,835",
                      "🟩🟩🟩🟩🟥🟥🟥🟥🟥🟥", "Разворот цены не подтверждён",
                      "не торговый сигнал"):
            self.assertIn(value, message)

    def test_ask_absorption(self):
        message = format_order_flow_alert("ETHUSDT", "ASK", 1650, 1200000,
                                          .05, 15, 900000, 300000)
        self.assertIn("ASK-СТЕНА", message)
        self.assertIn("SHORT-КОНТЕКСТ", message)
        self.assertIn("поглощение покупок", message)
        self.assertIn("Покупки 75.0% | Продажи 25.0%", message)

    def test_zero_flow_and_escaping(self):
        message = format_order_flow_alert("A&B", "BID", 1, 1000000, 0, 10, 0, 0)
        self.assertIn("A&amp;B", message)
        self.assertNotIn("Покупки 0.0%", message)
