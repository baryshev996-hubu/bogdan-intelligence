"""Conservative upcoming spot-listing monitor using official exchange data only.

This module does not claim to detect private insider information or predict returns.
It alerts only when an exchange publishes an explicit future listing time.
"""
import asyncio
import html
import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

log = logging.getLogger("bogdan-intel.listings")

ENABLED = os.getenv("ENABLE_UPCOMING_LISTINGS", "true").lower() == "true"
POLL_SECONDS = max(120, int(os.getenv("LISTING_POLL_SECONDS", "300")))
LOOKAHEAD_HOURS = max(1, min(168, int(os.getenv("LISTING_LOOKAHEAD_HOURS", "168"))))
MIN_EVENT_SCORE = max(0, min(100, int(os.getenv("LISTING_MIN_EVENT_SCORE", "65"))))
STATE_PATH = Path(os.getenv("LISTING_STATE_PATH", "/data/upcoming_listings_seen.json"))
KYIV = ZoneInfo("Europe/Kyiv")

EXCLUDED = (
    "tokenized stock", "tokenised stock", "pre-ipo", "tradfi", "stock token",
    "leveraged token", " etf", "delist", "margin trading", "earn", "convert",
    "launchpool", "launchpool subscription", "equity", "equities", "stock",
)

LISTING_WORDS = ("will list", "new listing", "spot trading", "spot listing", "listing of", "perpetual", "futures", "swap")


def _clean(value):
    value = html.unescape(str(value or ""))
    value = re.sub(r"<[^>]+>", " ", value)
    return re.sub(r"\s+", " ", value).strip()


def _market_type(title, description=""):
    text = (str(title) + " " + str(description)).lower()
    if any(word in text for word in EXCLUDED):
        return None
    if any(word in text for word in ("perpetual", "futures", "future contract", "perp", "swap")):
        return "FUTURES"
    if any(word in text for word in ("will list", "new listing", "spot trading", "spot listing", "listing of")):
        return "SPOT"
    return None


def _parse_explicit_utc(text):
    """Parse only timestamps that explicitly state UTC/GMT."""
    text = _clean(text)
    patterns = [
        (r"(20\d{2}-\d{2}-\d{2})[ T]+(\d{1,2}:\d{2})(?::\d{2})?\s*\(?\s*(?:UTC|GMT)\s*\)?", False),
        (r"(\d{1,2}\s+[A-Za-z]{3,9}\s+20\d{2})[, ]+(\d{1,2}:\d{2})(?::\d{2})?\s*\(?\s*(?:UTC|GMT)\s*\)?", False),
        (r"([A-Za-z]{3,9}\s+\d{1,2},?\s+20\d{2})[, ]+(\d{1,2}:\d{2})(?::\d{2})?\s*\(?\s*(?:UTC|GMT)\s*\)?", False),
        (r"(\d{1,2}:\d{2})(?::\d{2})?\s*\(?\s*(?:UTC|GMT)\s*\)?\s*(?:on\s+)?(20\d{2}-\d{2}-\d{2})", True),
        (r"(\d{1,2}:\d{2})(?::\d{2})?\s*\(?\s*(?:UTC|GMT)\s*\)?\s*(?:on\s+)?([A-Za-z]{3,9}\s+\d{1,2},?\s+20\d{2})", True),
    ]
    for pattern, reverse in patterns:
        match = re.search(pattern, text, re.I)
        if not match:
            continue
        date_part, time_part = (match.group(2), match.group(1)) if reverse else (match.group(1), match.group(2))
        for fmt in ("%Y-%m-%d %H:%M", "%d %b %Y %H:%M", "%d %B %Y %H:%M",
                    "%b %d, %Y %H:%M", "%B %d, %Y %H:%M", "%b %d %Y %H:%M",
                    "%B %d %Y %H:%M"):
            try:
                return datetime.strptime(date_part + " " + time_part, fmt).replace(tzinfo=timezone.utc)
            except ValueError:
                pass
    return None


def _extract_ticker(title):
    match = re.search(r"\(([A-Z0-9]{2,15})\)", str(title))
    if match:
        return match.group(1)
    match = re.search(r"\b([A-Z0-9]{2,12})/USDT\b", str(title), re.I)
    return match.group(1).upper() if match else None


def _load_seen():
    try:
        return set(json.loads(STATE_PATH.read_text("utf-8")))
    except Exception:
        return set()


def _save_seen(seen):
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(sorted(seen)[-5000:]), encoding="utf-8")
    tmp.replace(STATE_PATH)


async def _json_get(session, url, params=None):
    async with session.get(url, params=params, headers={"User-Agent": "BOGDAN-INTELLIGENCE/1.0"}, timeout=18) as response:
        response.raise_for_status()
        return await response.json(content_type=None)


async def _binance_listings(session):
    url = "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query"
    data = await _json_get(session, url, {"type": 1, "pageNo": 1, "pageSize": 30, "catalogId": 48})
    result = data.get("data") or {}
    catalogs = result.get("catalogs") or []
    articles = []
    for catalog in catalogs:
        articles.extend(catalog.get("articles") or [])
    # Some response variants expose articles directly.
    articles.extend(result.get("articles") or [])
    out = []
    for item in articles:
        title = _clean(item.get("title"))
        code = str(item.get("code") or item.get("id") or "")
        if not code:
            continue
        detail_url = "https://www.binance.com/en/support/announcement/detail/" + code
        detail_text = ""
        try:
            detail = await _json_get(
                session,
                "https://www.binance.com/bapi/composite/v1/public/cms/article/detail/query",
                {"articleCode": code},
            )
            article = (detail.get("data") or {}).get("article") or {}
            detail_text = _clean(article.get("body") or article.get("content") or "")
        except Exception:
            log.debug("Binance detail API unavailable for %s", code)
        if not detail_text:
            try:
                async with session.get(detail_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=12) as response:
                    if response.status == 200:
                        detail_text = _clean(await response.text())
            except Exception:
                log.debug("Binance detail page fetch failed for %s", code)
        market_type = _market_type(title, detail_text)
        when = _parse_explicit_utc(detail_text) or _parse_explicit_utc(title)
        if when and market_type:
            out.append({
                "id": "binance:" + code, "exchange": "Binance", "title": title,
                "market_type": market_type, "ticker": _extract_ticker(title + " " + detail_text), "when": when,
                "url": detail_url, "source": "Binance official announcement",
            })
    return out


async def _bybit_listings(session):
    data = await _json_get(
        session, "https://api.bybit.com/v5/announcements/index",
        {"locale": "en-US", "type": "new_crypto", "page": 1, "limit": 50},
    )
    result = data.get("result") or {}
    out = []
    for item in result.get("list") or []:
        title = _clean(item.get("title"))
        description = _clean(item.get("description"))
        market_type = _market_type(title, description)
        if not market_type:
            continue
        when = _parse_explicit_utc(title + " " + description)
        if not when:
            continue
        out.append({
            "id": "bybit:" + str(item.get("url") or item.get("publishTime") or title),
            "exchange": "Bybit", "title": title, "market_type": market_type, "ticker": _extract_ticker(title + " " + description),
            "when": when, "url": item.get("url") or "https://announcements.bybit.com/",
            "source": "Bybit official announcement",
        })
    return out


async def _okx_listings(session):
    out = []
    now_ms = int(time.time() * 1000)
    for inst_type, market_type in (("SPOT", "SPOT"), ("SWAP", "FUTURES"), ("FUTURES", "FUTURES")):
        data = await _json_get(
            session, "https://www.okx.com/api/v5/public/instruments", {"instType": inst_type}
        )
        for item in data.get("data") or []:
            try:
                list_ms = int(item.get("listTime") or 0)
            except (TypeError, ValueError):
                continue
            if list_ms <= now_ms:
                continue
            inst = item.get("instId", "")
            if market_type == "SPOT" and not inst.endswith("-USDT"):
                continue
            if market_type == "FUTURES" and not (inst.endswith("-USDT-SWAP") or inst.endswith("-USDT")):
                continue
            base = item.get("baseCcy") or inst.split("-")[0]
            when = datetime.fromtimestamp(list_ms / 1000, tz=timezone.utc)
            out.append({
                "id": "okx:" + inst_type + ":" + inst + ":" + str(list_ms),
                "exchange": "OKX", "title": f"OKX scheduled {market_type} listing {inst}",
                "market_type": market_type, "ticker": base, "when": when,
                "url": "https://www.okx.com/help/section/announcements-new-listings",
                "source": f"OKX public instruments API ({inst_type} listTime)",
            })
    return out


def _event_score(event, exchange_count):
    # This is an event-quality score, not a predicted return or probability of profit.
    score = 55
    if event["ticker"]:
        score += 5
    if exchange_count >= 2:
        score += 20
    hours = (event["when"] - datetime.now(timezone.utc)).total_seconds() / 3600
    if 0 <= hours <= 24:
        score += 15
    elif 24 < hours <= 72:
        score += 10
    elif 72 < hours <= 168:
        score += 5
    return min(95, score)


def _format_alert(event, score, exchanges):
    utc = event["when"].astimezone(timezone.utc)
    kyiv = event["when"].astimezone(KYIV)
    other = ", ".join(sorted(exchanges - {event["exchange"]})) or "пока не подтверждён"
    return (
        "🚨 BOGDAN INTELLIGENCE | UPCOMING LISTING\n"
        f"Токен: {event['ticker'] or 'тикер уточняется'}\n"
        f"Биржа: {event['exchange']} | SPOT\n"
        f"Дата и время: {kyiv:%d.%m.%Y %H:%M} Киев\n"
        f"Время UTC: {utc:%d.%m.%Y %H:%M}\n"
        f"Другие подтверждения: {other}\n"
        f"Оценка события: {score}/100 (не прогноз доходности)\n"
        "Статус: время подтверждено официальным источником\n"
        f"Источник: {event['source']}\n"
        f"Ссылка: {event['url']}\n\n"
        "Тактика: не покупать вслепую на открытии. Проверить спред, глубину стакана, "
        "объём и реакцию цены. Вход только после подтверждения импульса и с заранее заданным стопом."
    )


async def run_upcoming_listings_monitor(session):
    if not ENABLED:
        log.info("UPCOMING LISTINGS disabled by ENABLE_UPCOMING_LISTINGS=false")
        return
    seen = _load_seen()
    log.info("UPCOMING LISTINGS enabled | sources=Binance,Bybit,OKX | markets=SPOT,FUTURES | lookahead=%sh | min_event_score=%s",
             LOOKAHEAD_HOURS, MIN_EVENT_SCORE)
    while True:
        gathered = []
        for name, fetcher in (
            ("Binance", _binance_listings),
            ("Bybit", _bybit_listings),
            ("OKX", _okx_listings),
        ):
            try:
                gathered.extend(await fetcher(session))
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Listing source unavailable: %s", name)

        now = datetime.now(timezone.utc)
        horizon = now.timestamp() + LOOKAHEAD_HOURS * 3600
        by_ticker = {}
        for event in gathered:
            if not event["ticker"]:
                continue
            if not now.timestamp() < event["when"].timestamp() <= horizon:
                continue
            by_ticker.setdefault((event["ticker"], event.get("market_type", "SPOT")), set()).add(event["exchange"])

        candidates = []
        for event in gathered:
            if not event["ticker"] or not now.timestamp() < event["when"].timestamp() <= horizon:
                continue
            exchanges = by_ticker.get((event["ticker"], event.get("market_type", "SPOT")), {event["exchange"]})
            score = _event_score(event, len(exchanges))
            if score >= MIN_EVENT_SCORE and event["id"] not in seen:
                candidates.append((event, score, exchanges))

        candidates.sort(key=lambda row: row[0]["when"])
        for event, score, exchanges in candidates:
            from app.mail import telegram_send
            delivered = await telegram_send(session, _format_alert(event, score, exchanges))
            if delivered:
                seen.add(event["id"])
                _save_seen(seen)
                log.info("UPCOMING LISTING SENT | %s | %s | %s | event_score=%s",
                         event["exchange"], event["ticker"], event["when"].isoformat(), score)
        await asyncio.sleep(POLL_SECONDS)
