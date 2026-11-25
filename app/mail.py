"""BOGDAN INTELLIGENCE production entrypoint.

Telegram is fail-closed: only LONG, SHORT, Hyperliquid large executions,
and Binance Futures order-flow alerts can be delivered. No spot ticker,
SEC, RSS, funding, OI, or generic market notifications are sent.
"""
import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from collections import deque

import aiohttp
import websockets
from dotenv import load_dotenv

from app.signal_engine import INTERVALS, MAX_CANDLES, evaluate, format_signal
from app.market_intel import run_market_intelligence
from app.arkham_intel import run_arkham_monitor
from app.paper_trader import PaperBook, paper_book
from app.upcoming_listings import run_upcoming_listings_monitor

# Separate short sprint. 09:00 Kyiv time on 25 Nov 2026 = 07:00 UTC.
paper_sprint_book = PaperBook(
    path="/data/paper_sprint_20261125.json",
    start_balance=100.0,
    risk_fraction=0.01,
    max_open=3,
    max_gross_leverage=3.0,
    ends_at=int(datetime(2026, 11, 25, 7, 0, tzinfo=timezone.utc).timestamp()),
    label="09:00 sprint",
)

# Isolated order-book PAPER challenge. Kyiv 20:00 on 25 Nov 2026 = 18:00 UTC.
orderflow_paper_book = PaperBook(
    path="/data/orderflow_paper_20261125.json",
    start_balance=200.0,
    risk_fraction=0.01,
    max_open=2,
    max_gross_leverage=20.0,
    fee_rate=0.0005,
    ends_at=int(datetime(2026, 11, 25, 18, 0, tzinfo=timezone.utc).timestamp()),
    label="ORDERFLOW 200 USD challenge",
)

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("bogdan-intel")

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

BINANCE_SYMBOLS = [
    item.strip().upper()
    for item in os.getenv("BINANCE_SYMBOLS", "BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,XRPUSDT,DOGEUSDT,ADAUSDT,AVAXUSDT,LINKUSDT,DOTUSDT,LTCUSDT,BCHUSDT,TRXUSDT,ETCUSDT,XLMUSDT,SUIUSDT,NEARUSDT,APTUSDT,ARBUSDT,OPUSDT,INJUSDT,UNIUSDT,FILUSDT,AAVEUSDT,RENDERUSDT,TAOUSDT,HBARUSDT,1000PEPEUSDT,1000SHIBUSDT,ATOMUSDT").split(",")
    if item.strip()
]
SIGNAL_DELIVERY_SYMBOLS = {
    item.strip().upper()
    for item in os.getenv("SIGNAL_DELIVERY_SYMBOLS", "BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,XRPUSDT,DOGEUSDT,ADAUSDT,AVAXUSDT,LINKUSDT,DOTUSDT,LTCUSDT,BCHUSDT,TRXUSDT,ETCUSDT,XLMUSDT,SUIUSDT,NEARUSDT,APTUSDT,ARBUSDT,OPUSDT,INJUSDT,UNIUSDT,FILUSDT,AAVEUSDT,RENDERUSDT,TAOUSDT,HBARUSDT,1000PEPEUSDT,1000SHIBUSDT,ATOMUSDT").split(",")
    if item.strip()
}
ENABLE_SIGNAL_DELIVERY = os.getenv("ENABLE_SIGNAL_DELIVERY", "true").lower() == "true"
ENABLE_MARKET_INTEL = os.getenv("ENABLE_MARKET_INTEL", "true").lower() == "true"
ENABLE_ORDER_FLOW = os.getenv("ENABLE_ORDER_FLOW", "true").lower() == "true"
ENABLE_ARKHAM = os.getenv("ENABLE_ARKHAM", "false").lower() == "true"
ENABLE_UPCOMING_LISTINGS = os.getenv("ENABLE_UPCOMING_LISTINGS", "true").lower() == "true"
AUTO_DISCOVER_ALTS = os.getenv("AUTO_DISCOVER_ALTS", "true").lower() == "true"
MAX_DISCOVERED_ALTS = max(0, min(30, int(os.getenv("MAX_DISCOVERED_ALTS", "0"))))
SIGNAL_SAME_SIDE_COOLDOWN = max(0, int(os.getenv("SIGNAL_SAME_SIDE_COOLDOWN", "21600")))
SIGNAL_FLIP_COOLDOWN = max(0, int(os.getenv("SIGNAL_FLIP_COOLDOWN", "3600")))

last_signal_sent = {}
telegram_send_lock = asyncio.Lock()
telegram_last_sent = 0.0
telegram_auth_valid = None
telegram_auth_checked_at = 0.0
TELEGRAM_AUTH_RECHECK_SECONDS = 300


def refresh_telegram_token_from_environment() -> bool:
    """Pick up a rotated Railway variable after a process restart/reconfiguration."""
    global BOT_TOKEN, telegram_auth_valid, telegram_auth_checked_at
    candidate = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if candidate and candidate != BOT_TOKEN:
        BOT_TOKEN = candidate
        telegram_auth_valid = None
        telegram_auth_checked_at = 0.0
        log.warning("TELEGRAM TOKEN CHANGED | revalidating without logging the token")
        return True
    return False



async def validate_telegram_auth(session: aiohttp.ClientSession) -> bool:
    """Validate token and remember the result. Never log the secret."""
    global telegram_auth_valid, telegram_auth_checked_at
    refresh_telegram_token_from_environment()
    telegram_auth_checked_at = time.monotonic()

    if not BOT_TOKEN:
        telegram_auth_valid = False
        log.critical("TELEGRAM AUTH FAILED | TELEGRAM_BOT_TOKEN is empty")
        return False

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/getMe"
    try:
        async with session.get(url, timeout=15) as response:
            body = await response.json(content_type=None)
            telegram_auth_valid = response.status == 200 and body.get("ok") is True
            if telegram_auth_valid:
                bot = body.get("result") or {}
                log.info("TELEGRAM AUTH OK | bot_username=@%s", bot.get("username", "unknown"))
            elif response.status == 401:
                log.critical(
                    "TELEGRAM AUTH INVALID (401) | token is rejected by Telegram. "
                    "Automatic token regeneration is impossible. Replace TELEGRAM_BOT_TOKEN "
                    "in Railway with a fresh BotFather token; the service will revalidate on restart."
                )
            else:
                log.error("TELEGRAM AUTH CHECK FAILED | HTTP %s | %s",
                          response.status, body.get("description", "unknown Telegram API error"))
            return telegram_auth_valid
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        telegram_auth_valid = False
        log.error("TELEGRAM AUTH CHECK INCONCLUSIVE | %s: %s",
                  type(exc).__name__, str(exc)[:180])
        return False


async def telegram_auth_monitor(session: aiohttp.ClientSession) -> None:
    """Periodically recheck auth and recover automatically after a token rotation."""
    while True:
        await asyncio.sleep(TELEGRAM_AUTH_RECHECK_SECONDS)
        token_changed = refresh_telegram_token_from_environment()
        stale = time.monotonic() - telegram_auth_checked_at >= TELEGRAM_AUTH_RECHECK_SECONDS
        if token_changed or (telegram_auth_valid is not True and stale):
            log.info("TELEGRAM AUTH RECOVERY CHECK | checking configured token")
            await validate_telegram_auth(session)



def is_allowed_telegram_alert(message: str) -> bool:
    """Allow only the four explicitly approved alert families."""
    lines = [line.strip() for line in str(message).splitlines() if line.strip()]
    if not lines:
        return False

    header = lines[0].upper()
    # Remove only the decorative icons used by our own formatters.
    normalized = header.lstrip("🟢🔴🐳🧭⚡️🔥🚨 ")

    if normalized in {
        "BOGDAN INTELLIGENCE | LONG",
        "BOGDAN INTELLIGENCE | SHORT",
    }:
        return True

    if normalized.startswith("LARGE EXECUTION | HYPERLIQUID |"):
        return any(
            line.strip().upper() in {"TAKER: BUY 🟢", "TAKER: SELL 🔴", "TAKER: BUY", "TAKER: SELL"}
            for line in lines[1:]
        )

    if normalized.startswith("ORDER FLOW INTELLIGENCE |"):
        return True

    if normalized.startswith("🚨 BOGDAN INTELLIGENCE | UPCOMING LISTING") or normalized.startswith("BOGDAN INTELLIGENCE | UPCOMING LISTING"):
        return True

    return False


async def telegram_send(session: aiohttp.ClientSession, message: str) -> bool:
    """Send only allowlisted alerts, with serialization and bounded retries."""
    global telegram_last_sent, telegram_auth_valid, telegram_auth_checked_at

    if not is_allowed_telegram_alert(message):
        first_line = next((line.strip() for line in str(message).splitlines() if line.strip()), "<empty>")
        log.info("TELEGRAM BLOCKED | header=%r", first_line[:120])
        return False

    refresh_telegram_token_from_environment()
    if not BOT_TOKEN or not CHAT_ID:
        log.error("Telegram is not configured. Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID.")
        return False
    if telegram_auth_valid is not True:
        age = time.monotonic() - telegram_auth_checked_at
        if telegram_auth_valid is None or age >= TELEGRAM_AUTH_RECHECK_SECONDS:
            if not await validate_telegram_auth(session):
                return False
        else:
            log.warning("TELEGRAM BLOCKED | authentication unavailable; waiting for next recovery check")
            return False

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": message,
        "disable_web_page_preview": True,
    }

    async with telegram_send_lock:
        for attempt in range(4):
            delay_to_rate_limit = 1.1 - (time.monotonic() - telegram_last_sent)
            if delay_to_rate_limit > 0:
                await asyncio.sleep(delay_to_rate_limit)

            try:
                async with session.post(url, json=payload, timeout=20) as response:
                    body = await response.json(content_type=None)
                    telegram_last_sent = time.monotonic()

                    if response.status == 200 and body.get("ok") is True:
                        log.info("TELEGRAM SENT | header=%r", message.splitlines()[0][:100])
                        return True

                    if response.status == 429:
                        retry_after = body.get("parameters", {}).get("retry_after", 2)
                        try:
                            retry_after = max(1, min(int(retry_after), 60))
                        except (TypeError, ValueError):
                            retry_after = 2
                        if attempt < 3:
                            log.warning("Telegram rate limited; retrying after %ss", retry_after)
                            await asyncio.sleep(retry_after)
                            continue

                    if response.status >= 500 and attempt < 3:
                        backoff = min(2 ** attempt, 15)
                        log.warning("Telegram HTTP %s; retrying in %ss", response.status, backoff)
                        await asyncio.sleep(backoff)
                        continue

                    if response.status == 401:
                        telegram_auth_valid = False
                        telegram_auth_checked_at = time.monotonic()
                        log.critical(
                            "TELEGRAM SEND AUTH FAILED (401) | token rejected. "
                            "Stopped all further sends to prevent retry storms. "
                            "A fresh BotFather token must be saved as TELEGRAM_BOT_TOKEN in Railway."
                        )
                        return False

                    log.error("Telegram rejected message: HTTP %s, response=%s", response.status, body)
                    return False

            except (aiohttp.ClientError, asyncio.TimeoutError):
                if attempt == 3:
                    log.exception("Telegram delivery failed after retries")
                    return False
                backoff = min(2 ** attempt, 15)
                log.warning("Telegram network error; retrying in %ss", backoff, exc_info=True)
                await asyncio.sleep(backoff)
            except Exception:
                log.exception("Unexpected Telegram delivery error")
                return False

    return False


async def load_binance_history(session, symbol: str, interval: str):
    url = "https://fapi.binance.com/fapi/v1/klines"
    params = {"symbol": symbol, "interval": interval, "limit": 200}
    async with session.get(url, params=params, timeout=20) as response:
        response.raise_for_status()
        rows = await response.json(content_type=None)

    now_ms = int(time.time() * 1000)
    candles = []
    for row in rows:
        candle = {
            "open_time": int(row[0]),
            "open": float(row[1]),
            "high": float(row[2]),
            "low": float(row[3]),
            "close": float(row[4]),
            "volume": float(row[5]),
            "close_time": int(row[6]),
        }
        if candle["close_time"] < now_ms:
            candles.append(candle)
    return candles


def store_closed_candle(history, candle):
    if history and history[-1]["open_time"] == candle["open_time"]:
        history[-1] = candle
    elif not history or history[-1]["open_time"] < candle["open_time"]:
        history.append(candle)


async def discover_trading_universe(session):
    """Keep configured markets and optionally add the most liquid USDT perpetuals."""
    pinned = set(BINANCE_SYMBOLS)
    if not pinned:
        return []
    if not AUTO_DISCOVER_ALTS:
        return sorted(pinned)

    try:
        async with session.get("https://fapi.binance.com/fapi/v1/exchangeInfo", timeout=20) as response:
            response.raise_for_status()
            exchange = await response.json(content_type=None)
        async with session.get("https://fapi.binance.com/fapi/v1/ticker/24hr", timeout=20) as response:
            response.raise_for_status()
            tickers = await response.json(content_type=None)

        eligible = {
            row["symbol"]
            for row in exchange.get("symbols", [])
            if row.get("status") == "TRADING"
            and row.get("contractType") == "PERPETUAL"
            and row.get("quoteAsset") == "USDT"
            and row.get("symbol", "").endswith("USDT")
            # Match quote-asset suffixes only. Substring checks wrongly reject
            # BNBUSDT, APTUSDT, ARBUSDT, DOTUSDT and 1000SHIBUSDT.
            and not row.get("symbol", "").endswith(("USDC", "BUSD", "TUSD", "FDUSD"))
        }

        valid_pinned = pinned & eligible
        rejected = sorted(pinned - eligible)
        if rejected:
            log.warning("Configured symbols not currently tradable as USDT perpetuals: %s", rejected)

        ranked = []
        for row in tickers:
            symbol = row.get("symbol")
            if symbol in eligible and symbol not in pinned:
                try:
                    ranked.append((float(row.get("quoteVolume", 0)), symbol))
                except (TypeError, ValueError):
                    continue

        additions = [symbol for _, symbol in sorted(ranked, reverse=True)[:MAX_DISCOVERED_ALTS]]
        universe = sorted(valid_pinned | set(additions))
        if len(universe) < 21:
            log.error("Trading universe below requested 21 markets | valid=%d", len(universe))
        log.info("Trading universe ready | configured=%d valid=%d discovered=%d total=%d",
                 len(pinned), len(valid_pinned), len(additions), len(universe))
        return universe
    except asyncio.CancelledError:
        raise
    except Exception:
        log.exception("Altcoin discovery failed; using configured symbols only")
        return sorted(pinned)


async def binance_signal_monitor(session, symbols):
    if not symbols:
        log.error("Signal engine stopped: no Binance USD-M Futures symbols configured.")
        return

    histories = {
        symbol: {timeframe: deque(maxlen=MAX_CANDLES) for timeframe in INTERVALS}
        for symbol in symbols
    }

    async def dispatch_signal(signal):
        symbol = signal["symbol"]
        now = time.time()
        previous = last_signal_sent.get(symbol)
        if previous:
            cooldown = (
                SIGNAL_SAME_SIDE_COOLDOWN
                if previous["side"] == signal["side"]
                else SIGNAL_FLIP_COOLDOWN
            )
            if now - previous["sent_at"] < cooldown:
                log.info("SIGNAL COOLDOWN | %s %s", signal["side"], symbol)
                return

        if symbol not in SIGNAL_DELIVERY_SYMBOLS:
            log.info("SHADOW SIGNAL | %s %s | excluded from delivery universe",
                     signal["side"], symbol)
            last_signal_sent[symbol] = {
                "side": signal["side"],
                "sent_at": now,
                "candle_id": signal["candle_id"],
            }
            return

        if paper_book.active():
            opened = paper_book.open_trade(signal)
            if opened:
                log.info("PAPER STATUS | %s", paper_book.status())
        if paper_sprint_book.active():
            sprint_opened = paper_sprint_book.open_trade(signal)
            if sprint_opened:
                log.info("PAPER SPRINT STATUS | %s", paper_sprint_book.status())

        # Separate experiment: base signal must be confirmed by a fresh persistent
        # order-book wall plus opposing aggressive taker flow (absorption).
        if orderflow_paper_book.active():
            from app.order_flow import get_order_flow_confirmation
            flow = get_order_flow_confirmation(symbol, signal["side"], max_age=2.0)
            if not flow or float(flow.get("strength", 0)) < 70:
                log.info("ORDERFLOW PAPER SKIP | %s %s | no fresh aligned wall/absorption confirmation",
                         signal["side"], symbol)
            else:
                entry = float(flow["mid"])
                original_risk = abs(float(signal["entry"]) - float(signal["stop"]))
                if entry > 0 and original_risk > 0:
                    direction = 1.0 if signal["side"] == "LONG" else -1.0
                    test_signal = dict(signal)
                    test_signal["entry"] = entry
                    test_signal["stop"] = entry - direction * original_risk
                    test_signal["targets"] = [
                        entry + direction * original_risk * 0.45,
                        entry + direction * original_risk * 0.85,
                        entry + direction * original_risk * 1.25,
                    ]
                    strength = float(flow["strength"])
                    leverage = max(10, min(20, 10 + int((strength - 70) / 3)))
                    test_signal["paper_max_gross_leverage"] = leverage
                    opened = orderflow_paper_book.open_trade(test_signal)
                    if opened:
                        log.info("ORDERFLOW PAPER OPENED | %s %s | flow=%.1f | leverage_cap=%dx | wall=$%.0f | persistence=%.1fs | balance=%s",
                                 signal["side"], symbol, strength, leverage,
                                 float(flow["wall_usd"]), float(flow["persistence_sec"]),
                                 orderflow_paper_book.status())
                else:
                    log.warning("ORDERFLOW PAPER SKIP | invalid entry/risk | %s", symbol)

        if not ENABLE_SIGNAL_DELIVERY:
            log.info("SIGNAL SUPPRESSED BY CONFIG | %s %s", signal["side"], symbol)
            return

        delivered = await telegram_send(session, format_signal(signal))
        if delivered:
            last_signal_sent[symbol] = {
                "side": signal["side"],
                "sent_at": now,
                "candle_id": signal["candle_id"],
            }
            log.info("SIGNAL DELIVERED | %s %s | confidence=%s",
                     signal["side"], symbol, signal["confidence"])
        else:
            log.warning("SIGNAL NOT DELIVERED | %s %s", signal["side"], symbol)

    # Load only completed candles. Never evaluate an unfinished candle.
    for symbol in symbols:
        for timeframe in INTERVALS:
            try:
                rows = await load_binance_history(session, symbol, timeframe)
                histories[symbol][timeframe].extend(rows)
                log.info("HISTORY READY | %s | %s | %d closed candles",
                         symbol, timeframe, len(rows))
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("History load failed | %s | %s", symbol, timeframe)

    streams = "/".join(
        f"{symbol.lower()}@kline_{timeframe}"
        for symbol in symbols
        for timeframe in INTERVALS
    )
    url = f"wss://fstream.binance.com/market/stream?streams={streams}"

    while True:
        try:
            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=60,
                close_timeout=10,
                open_timeout=20,
                max_size=2**20,
            ) as websocket:
                log.info("BINANCE FUTURES CONNECTED | symbols=%d | intervals=%s",
                         len(symbols), ",".join(INTERVALS))

                async for raw in websocket:
                    packet = json.loads(raw)
                    item = packet.get("data", {})
                    kline = item.get("k")
                    if not kline or not kline.get("x"):
                        continue

                    symbol = str(item.get("s", "")).upper()
                    timeframe = kline.get("i")
                    if symbol not in histories or timeframe not in INTERVALS:
                        continue

                    candle = {
                        "open_time": int(kline["t"]),
                        "open": float(kline["o"]),
                        "high": float(kline["h"]),
                        "low": float(kline["l"]),
                        "close": float(kline["c"]),
                        "volume": float(kline["v"]),
                        "close_time": int(kline["T"]),
                    }
                    store_closed_candle(histories[symbol][timeframe], candle)

                    if timeframe != "5m":
                        continue

                    paper_book.on_candle(symbol, candle)
                    paper_sprint_book.on_candle(symbol, candle)
                    orderflow_paper_book.on_candle(symbol, candle)

                    signal = evaluate(symbol, histories[symbol])
                    if signal:
                        await dispatch_signal(signal)

        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Binance signal stream disconnected; reconnecting in 10s")
            await asyncio.sleep(10)


async def main():
    if not BOT_TOKEN or not CHAT_ID:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID in Railway variables.")

    timeout = aiohttp.ClientTimeout(total=30)
    connector = aiohttp.TCPConnector(limit=40)

    log.info("BOOT | strict Telegram allowlist enabled")
    log.info("ALLOWED ALERTS | LONG, SHORT, LARGE EXECUTION (Hyperliquid), ORDER FLOW, UPCOMING LISTINGS")
    log.info("DISABLED ALERTS | CRYPTO MOVE, SEC, RSS, ticker, funding, OI, derivatives chatter")
    log.info("SIGNAL DELIVERY | enabled=%s | symbols=%d | %s",
             ENABLE_SIGNAL_DELIVERY, len(SIGNAL_DELIVERY_SYMBOLS), sorted(SIGNAL_DELIVERY_SYMBOLS))
    log.info("PAPER TRADING | %s", paper_book.status())
    log.info("PAPER SPRINT | %s", paper_sprint_book.status())

    log.info("ORDERFLOW PAPER CHALLENGE | %s", orderflow_paper_book.status())

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        await validate_telegram_auth(session)
        universe = await discover_trading_universe(session)
        tasks = [
            binance_signal_monitor(session, universe),
            telegram_auth_monitor(session),
        ]

        if ENABLE_MARKET_INTEL:
            # Public Hyperliquid executions and Binance order-book flow.
            tasks.append(run_market_intelligence(session, universe))

        if ENABLE_UPCOMING_LISTINGS:
            tasks.append(run_upcoming_listings_monitor(session))
        else:
            log.info("UPCOMING LISTINGS disabled by ENABLE_UPCOMING_LISTINGS=false")

        if ENABLE_ARKHAM:
            # Optional Arkham API monitor remains disabled unless explicitly enabled.
            tasks.append(run_arkham_monitor(session))

        if not ENABLE_ORDER_FLOW:
            log.info("ORDER FLOW disabled by ENABLE_ORDER_FLOW=false")

        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
