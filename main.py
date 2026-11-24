import asyncio
import hashlib
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

seen = set()
last_prices = {}
last_sec_check = {}
ticker_to_cik = {}

def utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

def fingerprint(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8", errors="ignore")).hexdigest()

def is_allowed_telegram_alert(message):
    lines = [line.strip().upper() for line in str(message).splitlines() if line.strip()]
    if not lines:
        return False
    if any("CRYPTO MOVE" in line or "BINANCE SPOT TICKER STREAM" in line for line in lines):
        return False
    header = lines[0]
    if header in {"BOGDAN INTELLIGENCE | LONG", "BOGDAN INTELLIGENCE | SHORT"}:
        return True
    if header.startswith("LARGE EXECUTION | HYPERLIQUID |"):
        return any(line.startswith(("TAKER: BUY", "TAKER: SELL")) for line in lines[1:])
    if header.startswith("ORDER FLOW INTELLIGENCE |"):
        return True
    return False

async def telegram_send(session, text):
    # Legacy entrypoint safety net. Block all price-move, SEC and RSS alerts.
    if not is_allowed_telegram_alert(text):
        log.info("Telegram alert suppressed by strict BUY/SELL/LONG/SHORT filter")
        return False
    if not BOT_TOKEN or not CHAT_ID:
        log.warning("Telegram not configured. Alert was not sent:\n%s", text)
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": CHAT_ID, "text": text, "disable_web_page_preview": True}
    try:
        async with session.post(url, json=payload, timeout=15) as resp:
            body = await resp.json()
            if resp.status != 200 or not body.get("ok"):
                log.error("Telegram API error: %s", body)
    except Exception:
        log.exception("Telegram delivery failed")

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

async def binance_monitor():
    if not BINANCE_SYMBOLS:
        return
    streams = "/".join(f"{s}@ticker" for s in BINANCE_SYMBOLS)
    url = f"wss://stream.binance.com:9443/stream?streams={streams}"
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=60,
                                          close_timeout=10, max_size=2**20) as ws:
                log.info("Connected to Binance streams: %s", BINANCE_SYMBOLS)
                async for raw in ws:
                    import json
                    packet = json.loads(raw)
                    item = packet.get("data", {})
                    symbol = item.get("s", "")
                    price = float(item.get("c", 0) or 0)
                    change = float(item.get("P", 0) or 0)
                    if not symbol or price <= 0:
                        continue
                    previous = last_prices.get(symbol)
                    last_prices[symbol] = price
                    # Price-only ticker events are intentionally never sent to Telegram.

        except Exception:
            log.exception("Binance websocket disconnected; reconnecting in 5 sec")
            await asyncio.sleep(5)

async def telegram_send_http(text):
    timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        await telegram_send(session, text)

async def main():
    if not BOT_TOKEN or not CHAT_ID:
        log.warning("Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in .env to enable delivery.")
    timeout = aiohttp.ClientTimeout(total=30)
    connector = aiohttp.TCPConnector(limit=20)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        await asyncio.gather(sec_monitor(session), rss_monitor(session), binance_monitor())

if __name__ == "__main__":
    asyncio.run(main())
