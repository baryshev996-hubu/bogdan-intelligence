"""BOGDAN INTELLIGENCE production entrypoint.

Telegram is fail-closed: only LONG, SHORT, Hyperliquid large executions,
and Binance Futures order-flow alerts can be delivered. No spot ticker,
SEC, RSS, funding, OI, or generic market notifications are sent.
"""
import asyncio
import logging
import os
import sys
import time
from datetime import datetime, timezone
from collections import Counter, deque

import aiohttp
from dotenv import load_dotenv

# Auxiliary monitors import app.mail for delivery. Share one process-wide sender.
if __name__ == "__main__":
    sys.modules["app.mail"] = sys.modules[__name__]

from app.signal_engine import INTERVAL_MS, INTERVALS, MAX_CANDLES, evaluate, format_signal
from app.analyst import market_context
from app.leverage import build_leverage_paper_signal
from app.signal_delivery import queue_signal, deliver_signals
from app.market_intel import run_market_intelligence
from app.arkham_intel import run_arkham_monitor
from app.paper_trader import PaperBook, paper_book
from app.upcoming_listings import run_upcoming_listings_monitor

# Isolated 10-hour sprint v2. Uses the baseline MTF setup without the
# baseline multi-timeframe setup. Separate ledger; no effect on main PAPER.
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

# Independent 10-day leverage paper challenge. $20 margin per trade,
# 10-20x leverage, <=0.25% account risk, five concurrent positions.
leverage_paper_book = PaperBook(
    path="/data/paper_leverage_200_20261125.json",
    start_balance=200.0,
    risk_fraction=0.0025,
    max_open=5,
    max_gross_leverage=20.0,
    fee_rate=0.0005,
    label="LEVERAGE 200 USD / 20 USD margin challenge",
    breakeven_trigger_r=0.50,
    impulse_exit_r=100.0,
    impulse_body_ratio=0.99,
    impulse_close_position=0.01,
)

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("bogdan-intel")

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
# Support multiple Telegram recipients while preserving the legacy single-chat variable.
CHAT_IDS = list(dict.fromkeys(
    item.strip()
    for item in (
        os.getenv("TELEGRAM_CHAT_IDS", "") + "," + os.getenv("TELEGRAM_CHAT_ID", "")
    ).split(",")
    if item.strip()
))

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
telegram_send_lock = asyncio.Lock()
signal_waiting = False
telegram_last_sent = 0.0
telegram_auth_valid = None
telegram_auth_checked_at = 0.0
telegram_auth_lock = asyncio.Lock()
telegram_hard_auth_failure = False
TELEGRAM_AUTH_RECHECK_SECONDS = 300


def refresh_telegram_token_from_environment() -> bool:
    """Pick up a rotated Railway variable after a process restart/reconfiguration."""
    global BOT_TOKEN, telegram_auth_valid, telegram_auth_checked_at, telegram_hard_auth_failure
    candidate = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if candidate and candidate != BOT_TOKEN:
        BOT_TOKEN = candidate
        telegram_auth_valid = None
        telegram_auth_checked_at = 0.0
        telegram_hard_auth_failure = False
        log.warning("TELEGRAM TOKEN CHANGED | revalidating without logging the token")
        return True
    return False



async def validate_telegram_auth(session: aiohttp.ClientSession) -> bool:
    """Single-flight token validation with a hard circuit breaker for rejected credentials."""
    global telegram_auth_valid, telegram_auth_checked_at, telegram_hard_auth_failure

    async with telegram_auth_lock:
        refresh_telegram_token_from_environment()
        if telegram_auth_valid is True:
            return True
        if telegram_hard_auth_failure:
            return False
        telegram_auth_checked_at = time.monotonic()

        if not BOT_TOKEN:
            telegram_auth_valid = False
            telegram_hard_auth_failure = True
            log.critical("TELEGRAM AUTH DISABLED | bot token is not configured")
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
                    telegram_hard_auth_failure = True
                    log.critical(
                        "TELEGRAM AUTH CIRCUIT OPEN | Telegram rejected the configured bot credential. "
                        "All sends and automatic rechecks are stopped until the token is rotated."
                    )
                else:
                    log.error("TELEGRAM AUTH CHECK FAILED | HTTP %s | %s",
                              response.status, body.get("description", "unknown Telegram API error"))
                return telegram_auth_valid
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            telegram_auth_valid = False
            log.error("TELEGRAM AUTH CHECK INCONCLUSIVE | %s", type(exc).__name__)
            return False


async def telegram_auth_monitor(session: aiohttp.ClientSession) -> None:
    """Periodically recheck auth and recover automatically after a token rotation."""
    while True:
        await asyncio.sleep(TELEGRAM_AUTH_RECHECK_SECONDS)
        token_changed = refresh_telegram_token_from_environment()
        stale = time.monotonic() - telegram_auth_checked_at >= TELEGRAM_AUTH_RECHECK_SECONDS
        if token_changed:
            log.info("TELEGRAM AUTH RECOVERY CHECK | token rotation detected")
            await validate_telegram_auth(session)
        elif telegram_auth_valid is not True and not telegram_hard_auth_failure and stale:
            log.info("TELEGRAM AUTH RECOVERY CHECK | retrying after transient failure")
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
        "BOGDAN INTELLIGENCE | SYSTEM STATUS",
    }:
        return True

    if normalized.startswith("LARGE EXECUTION | HYPERLIQUID |"):
        return any(
            line.strip().upper() in {"TAKER: BUY 🟢", "TAKER: SELL 🔴", "TAKER: BUY", "TAKER: SELL"}
            for line in lines[1:]
        )

    if normalized.startswith("ORDER FLOW INTELLIGENCE |"):
        return True

    if normalized.startswith("BOGDAN INTELLIGENCE | 🟢 FLOW CONFIRMED |"):
        return True
    if normalized.startswith("BOGDAN INTELLIGENCE | 🟡 POSSIBLE ABSORPTION |"):
        return True

    if normalized.startswith("🚨 BOGDAN INTELLIGENCE | UPCOMING LISTING") or normalized.startswith("BOGDAN INTELLIGENCE | UPCOMING LISTING"):
        return True

    return False


async def telegram_send(session: aiohttp.ClientSession, message: str, *, recipient_only=None) -> bool:
    """Send an allowlisted alert to every configured Telegram recipient."""
    global telegram_last_sent, telegram_auth_valid, telegram_auth_checked_at, telegram_hard_auth_failure

    if not is_allowed_telegram_alert(message):
        first_line = next((line.strip() for line in str(message).splitlines() if line.strip()), "<empty>")
        log.info("TELEGRAM BLOCKED | header=%r", first_line[:120])
        return False

    refresh_telegram_token_from_environment()
    recipients = list(dict.fromkeys(
        item.strip()
        for item in (
            os.getenv("TELEGRAM_CHAT_IDS", "") + "," + os.getenv("TELEGRAM_CHAT_ID", "")
        ).split(",")
        if item.strip()
    ))
    if recipient_only is not None:
        recipients = [recipient_only] if recipient_only in recipients else []
    if not BOT_TOKEN or not recipients:
        log.error("Telegram is not configured. Set TELEGRAM_BOT_TOKEN and at least one Telegram chat ID.")
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
    any_delivered = False

    global signal_waiting
    for recipient in recipients:
        if recipient_only:
            signal_waiting = True
        else:
            while signal_waiting:
                await asyncio.sleep(0.05)
        async with telegram_send_lock:
            if recipient_only:
                signal_waiting = False
            if telegram_auth_valid is not True:
                log.warning("TELEGRAM BLOCKED | token authentication is invalid; awaiting credential rotation")
                return any_delivered
            payload = {
                "chat_id": recipient,
                "text": message,
                "disable_web_page_preview": True,
            }
            # New signal, whale and order-flow cards only; other alerts remain plaintext.
            header = message.splitlines()[0] if message else ""
            is_signal_card = header in {"🟢 BOGDAN INTELLIGENCE | LONG", "🔴 BOGDAN INTELLIGENCE | SHORT"}
            is_whale_card = header.startswith((
                "🐳 BOGDAN INTELLIGENCE | 🟢 FLOW CONFIRMED | ",
                "🐳 BOGDAN INTELLIGENCE | 🟡 POSSIBLE ABSORPTION | ",
            ))
            is_order_flow_card = header.startswith("🧭 ORDER FLOW INTELLIGENCE | ")
            is_listing_card = header == "🚨 BOGDAN INTELLIGENCE | UPCOMING LISTING"
            if (is_signal_card or is_whale_card or is_order_flow_card or is_listing_card) and "\n\n<b>" in message:
                payload["parse_mode"] = "HTML"
            recipient_delivered = False
            for attempt in range(4):
                delay_to_rate_limit = 1.1 - (time.monotonic() - telegram_last_sent)
                if delay_to_rate_limit > 0:
                    await asyncio.sleep(delay_to_rate_limit)

                try:
                    async with session.post(url, json=payload, timeout=20) as response:
                        body = await response.json(content_type=None)
                        telegram_last_sent = time.monotonic()

                        if response.status == 200 and body.get("ok") is True:
                            log.info("TELEGRAM SENT | recipient_suffix=%s | header=%r",
                                     recipient[-4:], message.splitlines()[0][:100])
                            recipient_delivered = True
                            any_delivered = True
                            break

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
                            telegram_hard_auth_failure = True
                            log.critical(
                                "TELEGRAM AUTH CIRCUIT OPEN | send endpoint rejected the configured credential. "
                                "Queued sends are blocked and automatic retries are disabled until token rotation."
                            )
                            return any_delivered

                        description = body.get("description", "unknown Telegram API error")
                        log.error("Telegram delivery failed | recipient_suffix=%s | HTTP %s | %s",
                                  recipient[-4:], response.status, description)
                        break

                except (aiohttp.ClientError, asyncio.TimeoutError):
                    if attempt == 3:
                        log.error("Telegram delivery failed after retries | recipient_suffix=%s",
                                      recipient[-4:])
                        break
                    backoff = min(2 ** attempt, 15)
                    log.warning("Telegram network error; retrying in %ss", backoff)
                    await asyncio.sleep(backoff)
                except Exception:
                    log.error("Unexpected Telegram delivery error | recipient_suffix=%s",
                                  recipient[-4:])
                    break

            if not recipient_delivered:
                log.warning("TELEGRAM RECIPIENT NOT DELIVERED | recipient_suffix=%s", recipient[-4:])

    return any_delivered


async def load_binance_history(session, symbol: str, interval: str, start=None, limit=200):
    url = "https://fapi.binance.com/fapi/v1/klines"
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    if start is not None:
        params["startTime"] = start
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
            "taker_buy_volume": float(row[9]) if len(row) > 9 else None,
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

    signal_symbols = set(symbols)
    books = (paper_book, paper_sprint_book, paper_sprint_v3_book, orderflow_paper_book, leverage_paper_book)
    held_symbols = {symbol for book in books for symbol in book.state["open_trades"]}
    symbols = list(dict.fromkeys([*symbols, *sorted(held_symbols)]))

    async def recover_positions(symbol, through):
        for book in books:
            trade = book.state["open_trades"].get(symbol)
            if not trade:
                continue
            cursor = trade["last_candle_open"] + 300000
            while cursor <= through and symbol in book.state["open_trades"]:
                rows = await load_binance_history(session, symbol, "5m", start=cursor, limit=499)
                if not rows or rows[0]["open_time"] != cursor:
                    raise ValueError(f"missing paper candle {symbol} at {cursor}")
                for row in rows:
                    if row["open_time"] > through or symbol not in book.state["open_trades"]:
                        break
                    if row["open_time"] != cursor:
                        raise ValueError(f"gap in paper candles for {symbol} at {cursor}")
                    book.on_candle(symbol, row)
                    cursor += 300000
    histories = {
        symbol: {timeframe: deque(maxlen=MAX_CANDLES) for timeframe in INTERVALS}
        for symbol in symbols
    }
    scan_rejections = Counter()
    scan_evaluations = 0
    scan_signals = 0
    last_scan_report = time.monotonic()

    async def dispatch_signal(signal):
        symbol = signal["symbol"]
        if SIGNAL_DELIVERY_SYMBOLS and symbol not in SIGNAL_DELIVERY_SYMBOLS:
            return
        if ENABLE_SIGNAL_DELIVERY:
            queue_signal(signal, CHAT_IDS)

        if symbol in PAPER_TEST_SYMBOLS and paper_book.active():
            opened = paper_book.open_trade(signal)
            if opened:
                log.info("PAPER STATUS | %s", paper_book.status())
        if leverage_paper_book.active():
            leverage_signal = build_leverage_paper_signal(signal, leverage_paper_book.state["balance"])
            if leverage_signal:
                leverage_opened = leverage_paper_book.open_trade(leverage_signal)
                if leverage_opened:
                    log.info(
                        "PAPER LEVERAGE CHALLENGE OPENED | %s %s | margin=$20 | leverage=%sx | modeled_stop_risk<=0.25%% | TP=1.00/1.25/1.50%% | status=%s",
                        leverage_signal["side"], symbol, leverage_signal["paper_leverage"],
                        leverage_paper_book.status()
                    )
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

    # Binance WebSocket sessions in this Railway region establish but deliver
    # zero market events. Use public REST klines as the reliable market-data source.
    # 300 limit=3 requests every 20 seconds remain within Binance rate limits.
    async def process_closed_5m_candle(symbol, candle):
        nonlocal scan_evaluations, scan_signals, last_scan_report
        now_mono = time.monotonic()
        # Keep existing PAPER challenges on their original 30-symbol
        # sample while production signal delivery scans the expanded top 100.
        await recover_positions(symbol, candle["open_time"] - 300000)
        for book in books:
            book.on_candle(symbol, candle)
        if symbol not in signal_symbols or int(time.time() * 1000) - candle["close_time"] >= 300000:
            return

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
        # The main PAPER book uses the same baseline MTF setup.
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

        scan_evaluations += 1
        signal = evaluate(symbol, histories[symbol], diagnostics=scan_rejections)
        if signal:
            scan_signals += 1
            signal["market_context"] = market_context(
                candle, histories[symbol]["1h"], histories.get("BTCUSDT", {}).get("1h", ())
            )
            context = signal["market_context"]
            log.info("SIGNAL ANALYSIS | %s %s | buy_share=%s | btc_change=%s | relative=%s",
                     signal["side"], symbol, context["buy_share"], context["btc_change"], context["relative"])
            await dispatch_signal(signal)

        if now_mono - last_scan_report >= 60:
            top_rejections = scan_rejections.most_common(6)
            log.info(
                "SIGNAL SCAN HEALTH | symbols=%d | evaluations=%d | generated=%d | top_rejections=%s | intervals=%s",
                len(symbols), scan_evaluations, scan_signals,
                top_rejections, ",".join(INTERVALS)
            )
            scan_rejections.clear()
            scan_evaluations = 0
            scan_signals = 0
            last_scan_report = now_mono

    poll_semaphore = asyncio.Semaphore(20)
    poll_interval_seconds = 20
    closed_5m_candles = 0
    last_poll_health = time.monotonic()

    async def poll_symbol_timeframe(symbol, timeframe):
        nonlocal closed_5m_candles
        async with poll_semaphore:
            try:
                url = "https://fapi.binance.com/fapi/v1/klines"
                params = {"symbol": symbol, "interval": timeframe, "limit": 3}
                async with session.get(url, params=params, timeout=15) as response:
                    response.raise_for_status()
                    rows = await response.json(content_type=None)

                now_ms = int(time.time() * 1000)
                history = histories[symbol][timeframe]
                latest_open_time = history[-1]["open_time"] if history else -1
                # Retry recovery even when no new signal candle is available.
                if timeframe == "5m" and history:
                    await recover_positions(symbol, latest_open_time)
                if not history or int(rows[0][0]) > latest_open_time + INTERVAL_MS[timeframe]:
                    rebuilt = await load_binance_history(session, symbol, timeframe)
                    if rebuilt:
                        if timeframe == "5m":
                            await recover_positions(symbol, rebuilt[-1]["open_time"])
                        history.clear()
                        history.extend(rebuilt)
                    return True, 0
                added = 0
                for row in rows:
                    candle = {
                        "open_time": int(row[0]),
                        "open": float(row[1]),
                        "high": float(row[2]),
                        "low": float(row[3]),
                        "close": float(row[4]),
                        "volume": float(row[5]),
                        "taker_buy_volume": float(row[9]) if len(row) > 9 else None,
                        "close_time": int(row[6]),
                    }
                    if candle["close_time"] >= now_ms or candle["open_time"] <= latest_open_time:
                        continue
                    store_closed_candle(history, candle)
                    latest_open_time = candle["open_time"]
                    added += 1
                    if timeframe == "5m":
                        closed_5m_candles += 1
                        await process_closed_5m_candle(symbol, candle)
                return True, added
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("BINANCE REST KLINE FAILED | %s | %s | %s: %s",
                            symbol, timeframe, type(exc).__name__, str(exc)[:140])
                return False, 0

    log.info("BINANCE REST SCANNER STARTED | symbols=%d | requests_per_cycle=%d | concurrency=20 | cycle=%ss | intervals=%s",
             len(symbols), len(symbols) * len(INTERVALS), poll_interval_seconds, ",".join(INTERVALS))

    while True:
        cycle_started = time.monotonic()
        # Update slower context first so each 5m evaluation sees the latest
        # fully closed 15m and 1h candles from the same polling cycle.
        poll_order = tuple(tf for tf in ("15m", "1h", "5m") if tf in INTERVALS)
        results = []
        for timeframe in poll_order:
            results.extend(await asyncio.gather(*(poll_symbol_timeframe(symbol, timeframe) for symbol in symbols)))
        successful = sum(1 for ok, _ in results if ok)
        failed = len(results) - successful
        new_candles = sum(count for _, count in results)
        elapsed = time.monotonic() - cycle_started

        if time.monotonic() - last_poll_health >= 60:
            log.info("BINANCE REST POLL HEALTH | symbols=%d | requests=%d | ok=%d | errors=%d | new_closed_candles=%d | new_5m=%d | cycle_seconds=%.1f",
                     len(symbols), len(results), successful, failed, new_candles,
                     closed_5m_candles, elapsed)
            last_poll_health = time.monotonic()

        await asyncio.sleep(max(1.0, poll_interval_seconds - elapsed))


async def main():
    if not BOT_TOKEN or not CHAT_IDS:
        raise RuntimeError("Missing TELEGRAM_BOT_TOKEN or Telegram chat ID in Railway variables.")

    timeout = aiohttp.ClientTimeout(total=30)
    connector = aiohttp.TCPConnector(limit=40)

    log.info("BOOT | strict Telegram allowlist enabled | telegram_recipients=%d", len(CHAT_IDS))
    log.info("ALLOWED ALERTS | LONG, SHORT, LARGE EXECUTION (Hyperliquid), ORDER FLOW, UPCOMING LISTINGS")
    log.info("DISABLED ALERTS | CRYPTO MOVE, SEC, RSS, ticker, funding, OI, derivatives chatter")
    log.info("SIGNAL DELIVERY | enabled=%s | one signal per closed candle | recipients=%d",
             ENABLE_SIGNAL_DELIVERY, len(CHAT_IDS))
    log.info("PAPER TRADING | %s", paper_book.status())
    log.info("PAPER SPRINT V2 | %s | risk=0.50%% | max_open=2 | leverage_cap=3x | ends_at=%s",
             paper_sprint_book.status(), paper_sprint_book.state["ends_at"])
    log.info("PAPER SPRINT V3 5X | %s | risk=0.50%% | max_open=2 | leverage_cap=5x | breakeven=0.50R | impulse_exit=0.55R | ends_at=%s",
             paper_sprint_v3_book.status(), paper_sprint_v3_book.state["ends_at"])

    log.info("ORDERFLOW PAPER CHALLENGE | %s", orderflow_paper_book.status())
    log.info("PAPER LEVERAGE CHALLENGE | %s | margin_per_trade=$20 | leverage=10-20x | risk=0.25%% | max_open=5 | TP=1.00/1.25/1.50%%",
             leverage_paper_book.status())

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        auth_ok = await validate_telegram_auth(session)
        if auth_ok:
            startup_message = (
                "🟢 BOGDAN INTELLIGENCE | SYSTEM STATUS\n\n"
                "Telegram: подключение подтверждено.\n"
                "Binance Futures: сканер запускается.\n"
                "Рынок: 100 USDT-M perpetual пар, закрытые свечи 5m / 15m / 1h.\n"
                "Сигналы LONG / SHORT: включены.\n"
                "Автоматические сделки: выключены."
            )
            startup_sent = await telegram_send(session, startup_message)
            log.info("TELEGRAM STARTUP CHECK | delivered=%s", startup_sent)
        else:
            log.error("TELEGRAM STARTUP CHECK SKIPPED | authentication failed")
        universe = await discover_trading_universe(session)
        tasks = [
            binance_signal_monitor(session, universe),
            telegram_auth_monitor(session),
            deliver_signals(session, telegram_send),
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
