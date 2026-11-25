"""Demo-only preflight for semi-automatic futures execution.

No live endpoint, no real or demo order placement, no integration with the
signal dispatcher until credential and order-lifecycle tests are completed.
"""
import argparse
import asyncio
import hashlib
import hmac
import json
import os
import time
from decimal import Decimal, ROUND_DOWN
from urllib.parse import urlencode

import aiohttp

DEMO_URL = "https://demo-fapi.binance.com"
D = Decimal


def dec(value):
    x = D(str(value))
    if not x.is_finite():
        raise ValueError("non-finite value")
    return x


def plan_entry(signal, price, step_size, tick_size, min_qty, min_notional, max_qty=None):
    """Conservative quantity: $20 margin cap, 5x, $1 modeled stop risk incl fees.

    The result is a preview, not a guarantee of execution price or loss.
    """
    price, step, tick = map(dec, (price, step_size, tick_size))
    lo, hi = map(dec, signal["entry_zone"])
    stop = dec(signal["stop"])
    side = signal["side"]
    if side not in ("LONG", "SHORT") or min(price, step, tick, lo, hi, stop) <= 0:
        raise ValueError("invalid order data")
    if not lo <= price <= hi:
        raise ValueError("price outside signal zone")
    # Use the actual proposed entry and rounded stop when sizing.
    # Round stop away from entry so rounding never understates modeled risk.
    rounding = "ROUND_FLOOR" if side == "LONG" else "ROUND_CEILING"
    stop = (stop / tick).to_integral_value(rounding=rounding) * tick
    if not (stop < price if side == "LONG" else stop > price):
        raise ValueError("stop on wrong side")
    fee = D("0.0006")
    risk_per_unit = abs(price - stop) + fee * (price + stop)
    raw = min(D("100") / price, D("1") / risk_per_unit)
    if max_qty is not None:
        raw = min(raw, dec(max_qty))
    qty = (raw / step).to_integral_value(rounding=ROUND_DOWN) * step
    if qty <= 0 or qty < dec(min_qty) or qty * price < dec(min_notional):
        raise ValueError("exchange minimum exceeds risk or margin limit")
    return {
        "symbol": signal["symbol"],
        "side": "BUY" if side == "LONG" else "SELL",
        "quantity": format(qty, "f"),
        "stop_trigger": format(stop, "f"),
        "entry_reference": format(price, "f"),
        "leverage": 5,
        "margin_model": format(qty * price / 5, ".4f"),
        "stop_risk_model": format(qty * risk_per_unit, ".4f"),
    }


def signed_query(params, secret, timestamp):
    payload = urlencode({**params, "timestamp": timestamp, "recvWindow": 5000})
    signature = hmac.new(secret.encode(), payload.encode(), hashlib.sha256).hexdigest()
    return payload + "&signature=" + signature


class DemoPreflight:
    def __init__(self, session, key, secret):
        if not key or not secret:
            raise ValueError("separate BINANCE_DEMO_API_KEY and BINANCE_DEMO_API_SECRET required")
        self.session, self.key, self.secret = session, key, secret

    async def request(self, method, path, params=None, signed=False):
        # Explicit allowlist. No executable order endpoint, no redirects.
        allowed = {
            ("GET", "/fapi/v1/time"),
            ("GET", "/fapi/v1/exchangeInfo"),
            ("GET", "/fapi/v1/ticker/price"),
            ("GET", "/fapi/v3/account"),
            ("GET", "/fapi/v1/positionSide/dual"),
            ("POST", "/fapi/v1/order/test"),
        }
        if (method, path) not in allowed:
            raise ValueError("endpoint not permitted in demo preflight")
        headers = {"X-MBX-APIKEY": self.key} if signed else {}
        query = urlencode(params or {})
        if signed:
            server = await self.request("GET", "/fapi/v1/time")
            query = signed_query(params or {}, self.secret, int(server["serverTime"]))
        url = DEMO_URL + path + ("?" + query if query else "")
        async with self.session.request(method, url, headers=headers, timeout=12,
                                        allow_redirects=False) as response:
            try:
                data = await response.json(content_type=None)
            except Exception:
                raise RuntimeError("Demo API returned non-JSON") from None
            if response.status != 200:
                code = data.get("code", "unknown") if isinstance(data, dict) else "unknown"
                raise RuntimeError(f"Demo API status={response.status} code={code}")
            return data

    async def verify(self, signal):
        account = await self.request("GET", "/fapi/v3/account", signed=True)
        mode = await self.request("GET", "/fapi/v1/positionSide/dual", signed=True)
        if mode.get("dualSidePosition") is not False:
            raise ValueError("preflight requires One-way Mode")
        if account.get("canTrade") is not True:
            raise ValueError("demo trading permission not confirmed")
        info = await self.request("GET", "/fapi/v1/exchangeInfo")
        symbol = signal["symbol"]
        row = next((x for x in info["symbols"] if x["symbol"] == symbol), None)
        if not row or row.get("status") != "TRADING" or row.get("contractType") != "PERPETUAL" or row.get("quoteAsset") != "USDT":
            raise ValueError("symbol not available in demo USDT-M perpetual market")
        filters = {f["filterType"]: f for f in row["filters"]}
        lot = filters["MARKET_LOT_SIZE"]
        price_filter = filters["PRICE_FILTER"]
        current = await self.request("GET", "/fapi/v1/ticker/price", {"symbol": symbol})
        plan = plan_entry(signal, current["price"], lot["stepSize"], price_filter["tickSize"],
                          lot["minQty"], filters["MIN_NOTIONAL"]["notional"], lot["maxQty"])
        if dec(account.get("availableBalance", "0")) < dec(plan["margin_model"]):
            raise ValueError("insufficient demo balance")
        # The endpoint validates a request without sending it to the matching engine.
        result = await self.request("POST", "/fapi/v1/order/test", {
            "symbol": symbol, "side": plan["side"], "type": "MARKET",
            "quantity": plan["quantity"],
        }, signed=True)
        return {"demo_order_test": "accepted", "order_placed": False,
                "stop_placed": False, "plan": plan, "test_response": result}


async def main():
    parser = argparse.ArgumentParser(description="Non-executing Binance Futures Demo preflight")
    parser.add_argument("--signal-json", required=True, help="JSON signal object, not credentials")
    args = parser.parse_args()
    signal = json.loads(args.signal_json)
    try:
        async with aiohttp.ClientSession() as session:
            client = DemoPreflight(session, os.getenv("BINANCE_DEMO_API_KEY"),
                                   os.getenv("BINANCE_DEMO_API_SECRET"))
            result = await client.verify(signal)
        print(json.dumps(result, ensure_ascii=False))
    except Exception as exc:
        # Never print URLs or credential-bearing exceptions.
        if isinstance(exc, (ValueError, RuntimeError)):
            print("DEMO PREFLIGHT FAILED: " + str(exc))
        else:
            print("DEMO PREFLIGHT FAILED: " + type(exc).__name__)
        raise SystemExit(1) from None


if __name__ == "__main__":
    asyncio.run(main())
