import asyncio
from collections import deque
import hashlib
import json
import logging
import os
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import aiohttp
import feedparser
import websockets
from dotenv import load_dotenv

from app.signal_engine import INTERVALS, MAX_CANDLES, evaluate, format_signal
from app.market_intel import run_market_intelligence

load_dotenv()
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bogdan-intel")

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
SEC_USER_AGENT = os.getenv("SEC_USER_AGENT", "BogdanIntelligence/0.1 contact@example.com")
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "45"))
PRICE_CHANGE_PCT = float(os.getenv("PRICE_CHANGE_PCT", "2.0"))
WATCH_TICKERS = [x.strip().upper() for x in os.getenv(
    "SEC_TICKERS", "AAPL,MSFT,NVDA,TSLA,AMZN,GOOGL,META,COIN").split(",") if x.strip()]
BINANCE_SYMBOLS = [x.strip().lower() for x in os.getenv(
    "BINANCE_SYMBOLS", "btcusdt,ethusdt,solusdt,xrpusdt").split(",") if x.strip()]
RSS_FEEDS = [x.strip() for x in os.getenv("RSS_FEEDS", "").split(",") if x.strip()]
ENABLE_SEC_ALERTS = os.getenv("ENABLE_SEC_ALERTS", "false").lower() == "true"
ENABLE_RSS_ALERTS = os.getenv("ENABLE_RSS_ALERTS", "false").lower() == "true"
SIGNAL_SAME_SIDE_COOLDOWN = int(os.getenv("SIGNAL_SAME_SIDE_COOLDOWN", "21600"))
SIGNAL_FLIP_COOLDOWN = int(os.getenv("SIGNAL_FLIP_COOLDOWN", "3600"))
ENABLE_SIGNAL_DELIVERY = os.getenv("ENABLE_SIGNAL_DELIVERY", "true").lower() == "true"
ENABLE_MARKET_INTEL = os.getenv("ENABLE_MARKET_INTEL", "true").lower() == "true"
SIGNAL_DELIVERY_SYMBOLS = {x.strip().upper() for x in os.getenv(
    "SIGNAL_DELIVERY_SYMBOLS", "BTCUSDT,SOLUSDT,XRPUSDT"
).split(",") if x.strip()}

AUTO_DISCOVER_ALTS = os.getenv("AUTO_DISCOVER_ALTS", "true").lower() == "true"
MAX_DISCOVERED_ALTS = max(0, min(30, int(os.getenv("MAX_DISCOVERED_ALTS", "12"))))
INTEL_POLL_SECONDS = max(60, int(os.getenv("INTEL_POLL_SECONDS", "180")))
INTEL_MIN_ALERT_SCORE = max(2, min(3, int(os.getenv("INTEL_MIN_ALERT_SCORE", "3"))))
intel_last_alert = {}

seen = set()
last_prices = {}
last_sec_check = {}
ticker_to_cik = {}
last_signal_sent = {}

telegram_send_lock = asyncio.Lock()
telegram_last_sent = 0.0

def utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

def fingerprint(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8", errors="ignore")).hexdigest()

def is_allowed_telegram_alert(message):
    """Only deliver actionable LONG/SHORT signals and large BUY/SELL executions."""
    normalized = message.strip().upper()
    signal = (
        "BOGDAN INTELLIGENCE | LONG" in normalized
        or "BOGDAN INTELLIGENCE | SHORT" in normalized
    )
    large_execution = (
        "LARGE EXECUTION | HYPERLIQUID |" in normalized
        and ("\\nTAKER: BUY\\n" in normalized or "\\nTAKER: SELL\\n" in normalized)
    )
    return signal or large_execution


async def telegram_send(session, text):
    global telegram_last_sent
    if not is_allowed_telegram_alert(text):
        log.info("Telegram alert suppressed by strict BUY/SELL/LONG/SHORT filter")
        return False
    if not BOT_TOKEN or not CHAT_ID:
        log.warning("Telegram not configured. Alert was not sent:\n%s", text)
        return False

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text, "disable_web_page_preview": True}

    # Serialize all senders and respect Telegram's practical per-chat rate limit.
    async with telegram_send_lock:
        for attempt in range(4):
            wait_for = 1.1 - (time.monotonic() - telegram_last_sent)
            if wait_for > 0:
                await asyncio.sleep(wait_for)

            try:
                async with session.post(url, json=payload, timeout=20) as resp:
                    telegram_last_sent = time.monotonic()
                    body = await resp.json(content_type=None)

                    if resp.status == 200 and body.get("ok"):
                        return True

                    if resp.status == 429:
                        retry_after = body.get("parameters", {}).get("retry_after", 2)
                        try:
                            retry_after = max(1, min(int(retry_after), 60))
                        except (TypeError, ValueError):
                            retry_after = 2
                        log.warning("Telegram rate limited; retrying after %s seconds", retry_after)
                        if attempt < 3:
                            await asyncio.sleep(retry_after)
                            continue

                    if resp.status >= 500 and attempt < 3:
                        delay = min(2 ** attempt, 15)
                        log.warning("Telegram server error %s; retrying in %s seconds", resp.status, delay)
                        await asyncio.sleep(delay)
                        continue

                    log.error("Telegram API error (HTTP %s): %s", resp.status, body)
                    return False

            except (aiohttp.ClientError, asyncio.TimeoutError):
                if attempt == 3:
                    log.exception("Telegram delivery failed after retries")
                    return False
                delay = min(2 ** attempt, 15)
                log.warning("Telegram network error; retrying in %s seconds", delay, exc_info=True)
                await asyncio.sleep(delay)
            except Exception:
                log.exception("Unexpected Telegram delivery error")
                return False

async def load_ticker_map(session):
    url = "https://www.sec.gov/files/company_tickers.json"
    headers = {"User-Agent": SEC_USER_AGENT, "Accept-Encoding": "gzip, deflate"}
    async with session.get(url, headers=headers, timeout=25) as resp:
        resp.raise_for_status()
        data = await resp.json(content_type=None)
    mapping = {}
    for row in data.values():
        mapping[str(row["ticker"]).upper()] = str(row["cik_str"]).zfill(10)
    return mapping

async def sec_monitor(session):
    global ticker_to_cik
    while True:
        try:
            if not ticker_to_cik:
                ticker_to_cik = await load_ticker_map(session)
            for ticker in WATCH_TICKERS:
                cik = ticker_to_cik.get(ticker)
                if not cik:
                    continue
                url = f"https://data.sec.gov/submissions/CIK{cik}.json"
                headers = {"User-Agent": SEC_USER_AGENT, "Accept-Encoding": "gzip, deflate"}
                async with session.get(url, headers=headers, timeout=25) as resp:
                    if resp.status == 429:
                        log.warning("SEC rate limited; backing off")
                        await asyncio.sleep(10)
                        continue
                    resp.raise_for_status()
                    data = await resp.json(content_type=None)
                recent = data.get("filings", {}).get("recent", {})
                forms = recent.get("form", [])
                dates = recent.get("filingDate", [])
                accessions = recent.get("accessionNumber", [])
                docs = recent.get("primaryDocument", [])
                descriptions = recent.get("primaryDocDescription", [])
                for i, form in enumerate(forms[:30]):
                    if form not in {"8-K", "10-K", "10-Q", "S-1", "SC 13D", "SC 13G", "4", "6-K", "20-F"}:
                        continue
                    accession = accessions[i]
                    key = fingerprint("sec:" + accession)
                    if key in seen:
                        continue
                    seen.add(key)
                    doc = docs[i] if i < len(docs) else ""
                    accession_clean = accession.replace("-", "")
                    link = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession_clean}/{doc}" if doc else f"https://www.sec.gov/edgar/browse/?CIK={cik}"
                    desc = descriptions[i] if i < len(descriptions) else ""
                    msg = (
                        f"🟠 BOGDAN INTELLIGENCE | SEC FILING\n"
                        f"Компания: {ticker}\nФорма: {form}\n"
                        f"Дата подачи: {dates[i]}\n"
                        f"Описание: {desc or 'Смотри первичный документ'}\n"
                        f"Время обнаружения: {utc_now()}\n"
                        f"Источник: {link}\n\n"
                        f"Статус: новое официальное раскрытие. Направление влияния не определено автоматически."
                    )
                    await telegram_send(session, msg)
                await asyncio.sleep(0.25)
        except Exception:
            log.exception("SEC monitor error")
        await asyncio.sleep(POLL_SECONDS)

async def rss_monitor(session):
    while True:
        for feed_url in RSS_FEEDS:
            try:
                async with session.get(feed_url, timeout=20) as resp:
                    resp.raise_for_status()
                    raw = await resp.read()
                parsed = feedparser.parse(raw)
                for entry in parsed.entries[:30]:
                    title = entry.get("title", "").strip()
                    link = entry.get("link", "")
                    summary = entry.get("summary", "")
                    if not title or not link:
                        continue
                    key = fingerprint("rss:" + link)
                    if key in seen:
                        continue
                    seen.add(key)
                    # RSS is ingested, but MVP sends only headlines with configured high-impact keywords.
                    haystack = (title + " " + summary).lower()
                    keywords = ("fed", "rate decision", "sec", "lawsuit", "bankruptcy",
                                "acquisition", "merger", "hack", "exploit", "halted",
                                "sanction", "default", "emergency", "earnings warning")
                    if any(word in haystack for word in keywords):
                        await telegram_send(session,
                            f"📰 BOGDAN INTELLIGENCE | NEWS CANDIDATE\n"
                            f"{title}\n{link}\n"
                            f"Источник: RSS, требует проверки первоисточника.\n"
                            f"Обнаружено: {utc_now()}")
            except Exception:
                log.exception("RSS feed error: %s", feed_url)
        await asyncio.sleep(max(POLL_SECONDS, 60))

async def load_binance_history(session, symbol, interval):
    url = "https://fapi.binance.com/fapi/v1/klines"
    params = {"symbol": symbol, "interval": interval, "limit": 200}
    async with session.get(url, params=params, timeout=20) as resp:
        if resp.status == 451:
            raise RuntimeError("Binance market-data REST endpoint returned HTTP 451")
        resp.raise_for_status()
        rows = await resp.json(content_type=None)

    now_ms = int(time.time() * 1000)
    result = []
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
            result.append(candle)
    return result


def store_closed_candle(history, candle):
    if history and history[-1]["open_time"] == candle["open_time"]:
        history[-1] = candle
    elif not history or history[-1]["open_time"] < candle["open_time"]:
        history.append(candle)


async def discover_trading_universe(session):
    """Keep configured majors and add liquid USDT perpetuals."""
    pinned = {s.upper() for s in BINANCE_SYMBOLS}
    if not AUTO_DISCOVER_ALTS or MAX_DISCOVERED_ALTS == 0:
        return sorted(pinned)
    try:
        async with session.get("https://fapi.binance.com/fapi/v1/exchangeInfo", timeout=20) as resp:
            resp.raise_for_status()
            exchange = await resp.json(content_type=None)
        async with session.get("https://fapi.binance.com/fapi/v1/ticker/24hr", timeout=20) as resp:
            resp.raise_for_status()
            tickers = await resp.json(content_type=None)
        eligible = {row["symbol"] for row in exchange.get("symbols", [])
                    if row.get("status") == "TRADING" and row.get("contractType") == "PERPETUAL"
                    and row.get("quoteAsset") == "USDT" and row.get("symbol", "").endswith("USDT")
                    and not any(x in row.get("symbol", "") for x in ("USDC", "BUSD", "TUSD", "FDUSD"))}
        ranked = []
        for row in tickers:
            if row.get("symbol") in eligible and row["symbol"] not in pinned:
                try:
                    ranked.append((float(row.get("quoteVolume", 0)), row["symbol"]))
                except (TypeError, ValueError):
                    pass
        additions = [s for _, s in sorted(ranked, reverse=True)[:MAX_DISCOVERED_ALTS]]
        log.info("Trading universe: %s configured + %s liquid discovered alts", len(pinned), len(additions))
        return sorted(pinned | set(additions))
    except Exception:
        log.exception("Altcoin discovery failed; using configured symbols only")
        return sorted(pinned)


async def binance_monitor(session):
    if not BINANCE_SYMBOLS:
        log.warning("No BINANCE_SYMBOLS configured; signal engine is idle.")
        return

    symbols = await discover_trading_universe(session)
    histories = {
        symbol: {tf: deque(maxlen=MAX_CANDLES) for tf in INTERVALS}
        for symbol in symbols
    }

    # Warm up indicators with closed historical candles before evaluating live setups.
    for symbol in symbols:
        for interval in INTERVALS:
            try:
                rows = await load_binance_history(session, symbol, interval)
                histories[symbol][interval].extend(rows)
                log.info("Loaded %s closed %s candles for %s",
                         len(rows), interval, symbol)
            except Exception:
                log.exception("Could not load Binance history for %s %s",
                              symbol, interval)

    streams = "/".join(
        f"{symbol.lower()}@kline_{interval}"
        for symbol in symbols for interval in INTERVALS
    )
    url = f"wss://fstream.binance.com/market/stream?streams={streams}"

    while True:
        try:
            async with websockets.connect(
                url, ping_interval=20, ping_timeout=60,
                close_timeout=10, open_timeout=20, max_size=2**20
            ) as ws:
                log.info("Connected to Binance USD-M Futures market streams: %s", symbols)
                async for raw in ws:
                    packet = json.loads(raw)
                    item = packet.get("data", {})
                    kline = item.get("k")
                    if not kline or not kline.get("x"):
                        continue  # Only evaluate fully closed candles.

                    symbol = item.get("s", "").upper()
                    interval = kline.get("i")
                    if symbol not in histories or interval not in INTERVALS:
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
                    store_closed_candle(histories[symbol][interval], candle)

                    # A new setup is checked only when a 5m candle has closed.
                    if interval != "5m":
                        continue

                    signal = evaluate(symbol, histories[symbol])
                    if not signal:
                        continue

                    now = time.time()
                    previous = last_signal_sent.get(symbol)
                    if previous:
                        cooldown = (SIGNAL_SAME_SIDE_COOLDOWN
                                    if previous["side"] == signal["side"]
                                    else SIGNAL_FLIP_COOLDOWN)
                        if now - previous["sent_at"] < cooldown:
                            continue

                    if symbol not in SIGNAL_DELIVERY_SYMBOLS:
                        log.info("SHADOW ONLY %s %s; excluded by historical validation",
                                 signal["side"], symbol)
                        last_signal_sent[symbol] = {
                            "side": signal["side"], "sent_at": now,
                            "candle_id": signal["candle_id"],
                        }
                        continue

                    if not ENABLE_SIGNAL_DELIVERY:
                        log.info("SHADOW SIGNAL %s %s confidence=%s; Telegram delivery disabled",
                                 signal["side"], symbol, signal["confidence"])
                        last_signal_sent[symbol] = {
                            "side": signal["side"], "sent_at": now,
                            "candle_id": signal["candle_id"],
                        }
                        continue

                    sent = await telegram_send(session, format_signal(signal))
                    if sent:
                        last_signal_sent[symbol] = {
                            "side": signal["side"], "sent_at": now,
                            "candle_id": signal["candle_id"],
                        }
                        log.info("Delivered %s signal for %s, confidence=%s",
                                 signal["side"], symbol, signal["confidence"])
                    else:
                        log.warning("Signal %s %s was not delivered; it remains eligible for retry",
                                    signal["side"], symbol)

        except Exception as exc:
            log.exception("Binance signal feed disconnected (%s); retrying in 15 sec",
                          type(exc).__name__)
            await asyncio.sleep(15)


async def telegram_send_http(text):
    timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        await telegram_send(session, text)

async def main():
    if not BOT_TOKEN or not CHAT_ID:
        log.warning("Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in Railway variables.")
    timeout = aiohttp.ClientTimeout(total=30)
    connector = aiohttp.TCPConnector(limit=30)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        symbols = await discover_trading_universe(session) if BINANCE_SYMBOLS else []
        tasks = [binance_monitor(session)]
        if ENABLE_MARKET_INTEL:
            tasks.append(run_market_intelligence(session, symbols))
        log.info(
            "Starting strict Telegram delivery: LONG/SHORT signals and large BUY/SELL executions only. "
            "Price-only, funding, OI, SEC and RSS alerts are disabled."
        )
        log.info("Signal delivery=%s for %s; public trade tape=%s",
                 ENABLE_SIGNAL_DELIVERY, sorted(SIGNAL_DELIVERY_SYMBOLS), ENABLE_MARKET_INTEL)
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
