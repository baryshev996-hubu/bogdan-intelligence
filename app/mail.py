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

# Isolated 10-hour sprint v2. Uses the baseline MTF setup without the
# underperforming 1m reversal veto. Separate ledger; no effect on main PAPER.
SPRINT_STARTED_AT = int(time.time())
SPRINT_ENDS_AT = SPRINT_STARTED_AT + 10 * 60 * 60
paper_sprint_book = PaperBook(
    path="/data/paper_sprint_v2_20261125.json",
    start_balance=100.0,
    risk_fraction=0.005,
    max_open=2,
    max_gross_leverage=3.0,
    fee_rate=0.0005,
    ends_at=SPRINT_ENDS_AT,
    label="10-hour sprint v2",
)

# Independent 10-hour PAPER sprint v3: same setup and risk, 5x notional cap,
# automatic net breakeven protection, and early profit capture on strong impulses.
SPRINT_V3_STARTED_AT = int(time.time())
SPRINT_V3_ENDS_AT = SPRINT_V3_STARTED_AT + 10 * 60 * 60
paper_sprint_v3_book = PaperBook(
    path="/data/paper_sprint_v3_5x_20261125.json",
    start_balance=100.0,
    risk_fraction=0.005,
    max_open=2,
    max_gross_leverage=5.0,
    fee_rate=0.0005,
    ends_at=SPRINT_V3_ENDS_AT,
    label="10-hour sprint v3 5x",
    breakeven_trigger_r=0.50,
    impulse_exit_r=0.55,
    impulse_body_ratio=0.65,
    impulse_close_position=0.20,
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
# Empty means deliver signals for every symbol in the dynamically selected universe.
# Set SIGNAL_DELIVERY_SYMBOLS explicitly only when a restricted watchlist is desired.
SIGNAL_DELIVERY_SYMBOLS = {
    item.strip().upper()
    for item in os.getenv("SIGNAL_DELIVERY_SYMBOLS", "").split(",")
    if item.strip()
}
ENABLE_SIGNAL_DELIVERY = os.getenv("ENABLE_SIGNAL_DELIVERY", "true").lower() == "true"
ENABLE_MARKET_INTEL = os.getenv("ENABLE_MARKET_INTEL", "true").lower() == "true"
ENABLE_ORDER_FLOW = os.getenv("ENABLE_ORDER_FLOW", "true").lower() == "true"
ENABLE_ARKHAM = os.getenv("ENABLE_ARKHAM", "false").lower() == "true"
ENABLE_UPCOMING_LISTINGS = os.getenv("ENABLE_UPCOMING_LISTINGS", "true").lower() == "true"
AUTO_DISCOVER_ALTS = os.getenv("AUTO_DISCOVER_ALTS", "true").lower() == "true"
MAX_FUTURES_SYMBOLS = max(1, min(100, int(os.getenv("MAX_FUTURES_SYMBOLS", "100"))))
# Paper experiments remain on their original configured 30-symbol universe.
PAPER_TEST_SYMBOLS = set(BINANCE_SYMBOLS)
SIGNAL_SAME_SIDE_COOLDOWN = max(0, int(os.getenv("SIGNAL_SAME_SIDE_COOLDOWN", "5400")))
SIGNAL_FLIP_COOLDOWN = max(0, int(os.getenv("SIGNAL_FLIP_COOLDOWN", "900")))

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
        # Recheck after acquiring the lock. If a concurrent request discovered
        # a revoked token, queued messages must not generate a 401 storm.
        if telegram_auth_valid is not True:
            log.warning("TELEGRAM BLOCKED | token authentication is invalid; awaiting credential rotation")
            return False
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
    """Select the 100 most liquid, currently tradable Binance USDT-M perpetuals."""
    pinned = set(BINANCE_SYMBOLS)
    if not AUTO_DISCOVER_ALTS:
        log.warning("AUTO_DISCOVER_ALTS=false | using configured fallback universe (%d symbols)", len(pinned))
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
        }

        ranked = []
        seen = set()
        for row in tickers:
            symbol = str(row.get("symbol", "")).upper()
            if symbol not in eligible or symbol in seen:
                continue
            try:
                quote_volume = float(row.get("quoteVolume", 0))
            except (TypeError, ValueError):
                continue
            if quote_volume > 0:
                ranked.append((quote_volume, symbol))
                seen.add(symbol)

        ranked.sort(key=lambda item: (-item[0], item[1]))
        universe = [symbol for _, symbol in ranked[:MAX_FUTURES_SYMBOLS]]
        if not universe:
            raise RuntimeError("Binance returned no eligible USDT-M perpetual tickers")

        missing_pinned = sorted(pinned - set(universe))
        log.info(
            "BINANCE FUTURES UNIVERSE | selected_top=%d | eligible=%d | configured_fallback=%d | pinned_outside_top=%d",
            len(universe), len(eligible), len(pinned), len(missing_pinned)
        )
        if missing_pinned:
            log.info("Configured symbols outside top %d by quote volume: %s",
                     MAX_FUTURES_SYMBOLS, missing_pinned)
        log.info("SIGNAL SCAN UNIVERSE | top %d USDT-M perpetuals by 24h quote volume",
                 len(universe))
        return universe
    except asyncio.CancelledError:
        raise
    except Exception:
        log.exception("Binance top-100 discovery failed; using configured symbols only")
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

        if SIGNAL_DELIVERY_SYMBOLS and symbol not in SIGNAL_DELIVERY_SYMBOLS:
            log.info("SHADOW SIGNAL | %s %s | excluded by explicit delivery whitelist",
                     signal["side"], symbol)
            last_signal_sent[symbol] = {
                "side": signal["side"],
                "sent_at": now,
                "candle_id": signal["candle_id"],
            }
            return

        if symbol in PAPER_TEST_SYMBOLS and paper_book.active():
            opened = paper_book.open_trade(signal)
            if opened:
                log.info("PAPER STATUS | %s", paper_book.status())
        # Separate experiment: base signal must be confirmed by a fresh persistent
        # order-book wall plus opposing aggressive taker flow (absorption).
        if symbol in PAPER_TEST_SYMBOLS and orderflow_paper_book.active() and orderflow_paper_book.state["balance"] > orderflow_paper_book.state["starting_balance"] * 0.95:
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
                    test_signal["paper_slippage_rate"] = 0.0001
                    opened = orderflow_paper_book.open_trade(test_signal)
                    if opened:
                        log.info("ORDERFLOW PAPER OPENED | %s %s | flow=%.1f | leverage_cap=%dx | wall=$%.0f | persistence=%.1fs | balance=%s",
                                 signal["side"], symbol, strength, leverage,
                                 float(flow["wall_usd"]), float(flow["persistence_sec"]),
                                 orderflow_paper_book.status())
                else:
                    log.warning("ORDERFLOW PAPER SKIP | invalid entry/risk | %s", symbol)

        if symbol in PAPER_TEST_SYMBOLS and orderflow_paper_book.active() and orderflow_paper_book.state["balance"] <= orderflow_paper_book.state["starting_balance"] * 0.95:
            log.error("ORDERFLOW PAPER DAILY LOSS GUARD | 5%% drawdown reached; no new challenge entries")
        
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

    # Load the expanded 100-symbol history concurrently with a strict cap.
    # Binance klines are public market data; no API key or trading permission is used.
    history_semaphore = asyncio.Semaphore(12)

    async def load_symbol_history(symbol, timeframe):
        async with history_semaphore:
            try:
                rows = await load_binance_history(session, symbol, timeframe)
                histories[symbol][timeframe].extend(rows)
                log.info("HISTORY READY | %s | %s | %d closed candles",
                         symbol, timeframe, len(rows))
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("History load failed | %s | %s", symbol, timeframe)

    # Load only completed candles. Never evaluate an unfinished candle.
    await asyncio.gather(*(
        load_symbol_history(symbol, timeframe)
        for symbol in symbols
        for timeframe in INTERVALS
    ))

    streams = [
        f"{symbol.lower()}@kline_{timeframe}"
        for symbol in symbols
        for timeframe in INTERVALS
    ]
    # Subscribe over a raw stream connection to avoid oversized subscription frames
    # while scanning 300 kline streams (100 symbols x 3 intervals).
    url = "wss://fstream.binance.com/ws"

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
                # Binance limits WebSocket control-message payloads. A single
                # 300-stream SUBSCRIBE frame can be rejected as "Payload too long".
                # Send small batches over the same connection and pace them.
                subscription_batch_size = 40
                for batch_index in range(0, len(streams), subscription_batch_size):
                    batch = streams[batch_index:batch_index + subscription_batch_size]
                    await websocket.send(json.dumps({
                        "method": "SUBSCRIBE",
                        "params": batch,
                        "id": 1 + batch_index // subscription_batch_size,
                    }, separators=(",", ":")))
                    await asyncio.sleep(0.15)

                log.info(
                    "BINANCE FUTURES CONNECTED | symbols=%d | streams=%d | batches=%d | batch_size=%d | intervals=%s",
                    len(symbols), len(streams),
                    (len(streams) + subscription_batch_size - 1) // subscription_batch_size,
                    subscription_batch_size, ",".join(INTERVALS)
                )

                async for raw in websocket:
                    packet = json.loads(raw)
                    # Raw /ws subscriptions return the event directly.
                    # Ignore the subscription acknowledgement.
                    if "id" in packet and "result" in packet:
                        continue
                    item = packet.get("data", packet)
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

                    # Keep existing PAPER challenges on their original 30-symbol
                    # sample while production signal delivery scans the expanded top 100.
                    if symbol in PAPER_TEST_SYMBOLS:
                        paper_book.on_candle(symbol, candle)
                        paper_sprint_book.on_candle(symbol, candle)
                        paper_sprint_v3_book.on_candle(symbol, candle)
                        orderflow_paper_book.on_candle(symbol, candle)

                    # Sprint V3 uses the same unfiltered MTF signal as V2,
                    # but its exits and 5x cap are isolated in a separate ledger.
                    if symbol in PAPER_TEST_SYMBOLS and paper_sprint_v3_book.active():
                        sprint_v3_signal = evaluate(symbol, histories[symbol])
                        if sprint_v3_signal:
                            sprint_v3_signal = dict(sprint_v3_signal)
                            sprint_v3_signal["paper_slippage_rate"] = 0.0001
                            sprint_v3_signal["paper_management_version"] = 3
                            sprint_v3_signal["paper_max_gross_leverage"] = 5.0
                            sprint_v3_opened = paper_sprint_v3_book.open_trade(sprint_v3_signal)
                            if sprint_v3_opened:
                                log.info(
                                    "PAPER SPRINT V3 OPENED | %s %s | risk=0.50%% | leverage_cap=5x | breakeven=0.50R | impulse_exit=0.55R | status=%s",
                                    sprint_v3_signal["side"], symbol, paper_sprint_v3_book.status()
                                )

                    # The sprint tests the proven baseline MTF setup independently.
                    # The main PAPER book also uses the baseline MTF setup without the 1m reversal veto.
                    if symbol in PAPER_TEST_SYMBOLS and paper_sprint_book.active():
                        sprint_signal = evaluate(symbol, histories[symbol])
                        if sprint_signal:
                            sprint_signal = dict(sprint_signal)
                            sprint_signal["paper_slippage_rate"] = 0.0001
                            sprint_opened = paper_sprint_book.open_trade(sprint_signal)
                            if sprint_opened:
                                log.info(
                                    "PAPER SPRINT V2 OPENED | %s %s | risk=0.50%% | slippage=0.010%% | status=%s",
                                    sprint_signal["side"], symbol, paper_sprint_book.status()
                                )

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
    log.info("SIGNAL DELIVERY | enabled=%s | scope=%s | same_side_cooldown=%ss | flip_cooldown=%ss",
             ENABLE_SIGNAL_DELIVERY,
             sorted(SIGNAL_DELIVERY_SYMBOLS) if SIGNAL_DELIVERY_SYMBOLS else "all discovered top-100 symbols",
             SIGNAL_SAME_SIDE_COOLDOWN, SIGNAL_FLIP_COOLDOWN)
    log.info("PAPER TRADING | %s", paper_book.status())
    log.info("PAPER SPRINT V2 | %s | risk=0.50%% | max_open=2 | leverage_cap=3x | ends_at=%s",
             paper_sprint_book.status(), SPRINT_ENDS_AT)
    log.info("PAPER SPRINT V3 5X | %s | risk=0.50%% | max_open=2 | leverage_cap=5x | breakeven=0.50R | impulse_exit=0.55R | ends_at=%s",
             paper_sprint_v3_book.status(), SPRINT_V3_ENDS_AT)

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
