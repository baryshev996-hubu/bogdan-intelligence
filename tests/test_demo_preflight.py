import unittest
from decimal import Decimal
from app.demo_preflight import plan_entry, signed_query, DemoPreflight, DEMO_URL


class DemoPreflightTests(unittest.TestCase):
    def setUp(self):
        self.signal = dict(symbol="EXMUSDT", side="LONG", entry_zone=[99, 101], stop=99)

    def test_long_risk_and_margin_caps(self):
        result = plan_entry(self.signal, 100, ".001", ".01", ".001", 5)
        self.assertEqual(result["side"], "BUY")
        self.assertLessEqual(Decimal(result["margin_model"]), Decimal("20"))
        self.assertLessEqual(Decimal(result["stop_risk_model"]), Decimal("1"))

    def test_short_stop_rounds_outward(self):
        result = plan_entry(dict(self.signal, side="SHORT", stop="101.003"),
                            100, ".001", ".01", ".001", 5)
        self.assertEqual(result["side"], "SELL")
        self.assertEqual(result["stop_trigger"], "101.01")

    def test_rejects_stale_price_wrong_stop_and_minimum(self):
        with self.assertRaises(ValueError):
            plan_entry(self.signal, 102, ".001", ".01", ".001", 5)
        with self.assertRaises(ValueError):
            plan_entry(dict(self.signal, stop=101), 100, ".001", ".01", ".001", 5)
        with self.assertRaises(ValueError):
            plan_entry(self.signal, 100, ".001", ".01", ".001", 500)

    def test_signing(self):
        value = signed_query({"symbol": "EXMUSDT"}, "test-secret", 123)
        self.assertTrue(value.startswith("symbol=EXMUSDT&timestamp=123&recvWindow=5000&signature="))
        self.assertEqual(len(value.rsplit("=", 1)[-1]), 64)


class DemoEndpointTests(unittest.IsolatedAsyncioTestCase):
    async def test_no_execution_endpoints(self):
        client = DemoPreflight(None, "dummy", "dummy")
        self.assertEqual(DEMO_URL, "https://demo-fapi.binance.com")
        for path in ("/fapi/v1/order", "/fapi/v1/algoOrder", "/fapi/v1/leverage"):
            with self.assertRaises(ValueError):
                await client.request("POST", path, signed=True)
