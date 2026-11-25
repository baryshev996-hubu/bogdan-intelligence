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
from html.parser import HTMLParser
from pathlib import Path
from zoneinfo import ZoneInfo

from app.listing_research import research_token, format_research, is_crypto_instrument, excluded_ticker

log = logging.getLogger("bogdan-intel.listings")

ENABLED = os.getenv("ENABLE_UPCOMING_LISTINGS", "true").lower() == "true"
POLL_SECONDS = max(120, int(os.getenv("LISTING_POLL_SECONDS", "300")))
LOOKAHEAD_HOURS = max(1, min(168, int(os.getenv("LISTING_LOOKAHEAD_HOURS", "168"))))
MIN_EVENT_SCORE = max(0, min(100, int(os.getenv("LISTING_MIN_EVENT_SCORE", "65"))))
STATE_PATH = Path(os.getenv("LISTING_STATE_PATH", "/data/upcoming_listings_seen.json"))
KYIV = ZoneInfo("Europe/Kyiv")

EXCLUDED = (
    "tokenized stock", "tokenised stock", "pre-ipo", "tradfi", "stock token",
    "leveraged token", " etf", "delist", "removal of", "will remove", "margin trading", "earn", "convert",
    "launchpool", "launchpool subscription", "equity", "equities", "stock",
    "commodity", "commodities", "forex", "precious metal", "pre-market equity",
    "bstocks", "tokenized securities", "stock options", "trading competition", "trading bots",
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
    if match:
        return match.group(1).upper()
    match = re.search(r"\b([A-Z0-9]{2,12})USDT\b", str(title), re.I)
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
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; BOGDAN-INTELLIGENCE/1.0)",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.binance.com/en/support/announcement/list/48",
    }
    async with session.get(url, params=params, headers=headers, timeout=18) as response:
        response.raise_for_status()
        return await response.json(content_type=None)


async def _binance_listing_page(session):
    """Fallback to Binance's official announcement page when its CMS API rejects requests."""
    page_url = "https://www.binance.com/en/support/announcement/list/48"
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/131.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.binance.com/en/support/announcement",
    }
    async with session.get(page_url, headers=headers, timeout=20) as response:
        response.raise_for_status()
        page = await response.text(errors="ignore")

    found = {}
    href_pattern = re.compile(
        r"""href=["'](?P<href>[^"']*/support/announcement/(?:detail/)?(?P<code>[a-f0-9]{32})[^"']*)["']""",
        re.I,
    )
    for match in href_pattern.finditer(page):
        code = match.group("code")
        left = page.rfind("<a", 0, match.start())
        right = page.find("</a>", match.end())
        if left < 0 or right < 0:
            continue
        title = _clean(page[left:right + 4])
        if title:
            found[code] = {
                "code": code,
                "title": title,
                "url": "https://www.binance.com/en/support/announcement/" + code,
            }

    # Next.js payload fallback: article code/title may be embedded without an anchor.
    if not found:
        code_matches = re.findall(r'"code"\s*:\s*"([a-f0-9]{32})"', page, re.I)
        title_matches = re.findall(r'"title"\s*:\s*"((?:\\.|[^"\\])*)"', page)
        for code, title in zip(code_matches, title_matches):
            clean_title = _clean(title.encode("utf-8").decode("unicode_escape"))
            if clean_title:
                found[code] = {
                    "code": code,
                    "title": clean_title,
                    "url": "https://www.binance.com/en/support/announcement/detail/" + code,
                }
    return list(found.values())


async def _binance_cms_listings(session):
    url = "https://www.binance.com/bapi/apex/v1/public/apex/cms/article/list/query"
    articles = []
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/131.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en",
            "Referer": "https://www.binance.com/en/messages/v2/group/announcement",
            "lang": "en",
        }
        async with session.get(
            url, params={"type": 1, "pageNo": 1, "pageSize": 30, "catalogId": 48},
            headers=headers, timeout=18
        ) as response:
            response.raise_for_status()
            data = await response.json(content_type=None)
        result = data.get("data") or {}
        catalogs = result.get("catalogs") or []
        for catalog in catalogs:
            articles.extend(catalog.get("articles") or [])
        articles.extend(result.get("articles") or [])
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        log.warning("Binance CMS API unavailable; trying official announcement HTML: %s",
                    str(exc)[:180])
        try:
            data = await _json_get(session,
                "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query",
                {"type": 1, "pageNo": 1, "pageSize": 30, "catalogId": 48})
            result = data.get("data") or {}
            articles = list(result.get("articles") or [])
            for catalog in result.get("catalogs") or []:
                articles.extend(catalog.get("articles") or [])
        except asyncio.CancelledError:
            raise
        except Exception:
            log.warning("Binance composite CMS fallback unavailable")
    if not articles:
        articles = await _binance_listing_page(session)
    if not articles:
        raise RuntimeError("No announcement articles parsed; listing coverage unverified")

    out = []
    for item in articles:
        title = _clean(item.get("title"))
        code = str(item.get("code") or item.get("id") or "")
        if not code:
            continue
        detail_url = item.get("url") or (
            "https://www.binance.com/en/support/announcement/" + code
        )
        detail_text = ""
        try:
            detail = await _json_get(
                session,
                "https://www.binance.com/bapi/composite/v1/public/cms/article/detail/query",
                {"articleCode": code},
            )
            article = (detail.get("data") or {}).get("article") or {}
            detail_text = _clean(article.get("body") or article.get("content") or "")
        except asyncio.CancelledError:
            raise
        except Exception:
            log.debug("Binance detail API unavailable for %s; trying article page", code)
        if not detail_text:
            try:
                async with session.get(
                    detail_url,
                    headers={"User-Agent": "Mozilla/5.0 (compatible; BOGDAN-INTELLIGENCE/1.0)"},
                    timeout=15,
                ) as response:
                    if response.status == 200:
                        detail_text = _clean(await response.text())
            except asyncio.CancelledError:
                raise
            except Exception:
                log.debug("Binance detail page fetch failed for %s", code)

        market_type = _market_type(title, detail_text)
        when = _parse_explicit_utc(detail_text) or _parse_explicit_utc(title)
        if when and market_type:
            out.append({
                "id": "binance:" + code,
                "exchange": "Binance",
                "title": title,
                "market_type": market_type,
                "ticker": _extract_ticker(title + " " + detail_text),
                "when": when,
                "url": detail_url,
                "source": "Binance official announcement",
                "official_text": detail_text[:12000],
            })
    return out

class _TelegramAnnouncementParser(HTMLParser):
    """Extract only message bodies, not navigation, footers or unrelated links."""
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.depth = 0
        self.current = None
        self.messages = []

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if self.current is None:
            if tag == "div" and "tgme_widget_message_text" in attrs.get("class", "").split():
                self.current = {"parts": [], "links": []}
                self.depth = 1
            return
        if tag == "div":
            self.depth += 1
        if tag == "br":
            self.current["parts"].append("\n")
        if tag == "a":
            href = attrs.get("href", "")
            if re.fullmatch(r"https://www\.binance\.com/[a-zA-Z-]+/support/announcement/(?:detail/)?[a-fA-F0-9]{32}/?(?:\?[^#]*)?", href):
                self.current["links"].append(href)
        if tag in ("p", "b", "strong"):
            self.current["parts"].append("\n")

    def handle_data(self, data):
        if self.current is not None:
            self.current["parts"].append(data)

    def handle_endtag(self, tag):
        if self.current is None:
            return
        if tag in ("p", "b", "strong"):
            self.current["parts"].append("\n")
        if tag == "div":
            self.depth -= 1
            if self.depth == 0:
                self.messages.append(self.current)
                self.current = None


def _telegram_listing_events(page):
    parser = _TelegramAnnouncementParser()
    parser.feed(page)
    if not parser.messages:
        raise RuntimeError("Official Telegram announcement page contains no message bodies")
    out = []
    for message in parser.messages:
        raw = "".join(message["parts"])
        lines = [_clean(line) for line in raw.splitlines() if _clean(line)]
        if not lines or not message["links"]:
            continue
        title = lines[0]
        # Require a new trading launch, not an update, a campaign or a delisting.
        if not re.search(r"\b(?:will list|will launch|new listing|will open trading)\b", title, re.I):
            continue
        if any(word in title.lower() for word in ("multiple", "quarterly", "delivery contract", "pre-market", "pre market")):
            continue
        if any(word in raw.lower() for word in ("bstocks", "tradfi", "tokenized securities", "tokenised securities", "stock options")):
            continue
        market_type = _market_type(title)
        ticker = _extract_ticker(title)
        if not market_type or not ticker or excluded_ticker(ticker):
            continue
        # Reject multiple explicit symbols in a single notice, where times can differ.
        pairs = set(re.findall(r"\b([A-Z0-9]{2,15})\s*/?\s*(?:USDT|USDC)\b", title))
        if len(pairs) > 1:
            continue
        when = None
        for line in lines[1:]:
            # A deposit/withdrawal opening is not a trading launch.
            if re.search(r"\b(?:deposits?|withdrawals?)\b", line, re.I):
                continue
            if re.search(r"\b(?:trad(?:e|ing)|launch|list(?:ing)?)\b", line, re.I):
                when = _parse_explicit_utc(line)
                if when:
                    break
        if not when:
            continue
        link = message["links"][0].split("?")[0].rstrip("/")
        article_id = link.rsplit("/", 1)[-1].lower()
        out.append(dict(id="binance:" + article_id, exchange="Binance", title=title,
                        market_type=market_type, ticker=ticker, when=when, url=link,
                        source="Binance official Telegram announcement",
                        official_text=raw[:12000]))
    return out, len(parser.messages)


async def _binance_telegram_listings(session):
    url = "https://t.me/s/binance_announcements"
    async with session.get(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "text/html"}, timeout=15) as response:
        response.raise_for_status()
        page = await response.text()
    events, posts = _telegram_listing_events(page)
    log.info("BINANCE OFFICIAL TELEGRAM OK | posts=%d | events=%d", posts, len(events))
    return events


async def _binance_listings(session):
    # Independent sources: one failure must not discard successful results.
    batches = await asyncio.gather(_binance_telegram_listings(session),
                                   _binance_cms_listings(session), return_exceptions=True)
    combined = {}
    success = 0
    for name, batch in zip(("Telegram", "CMS/HTML"), batches):
        if isinstance(batch, asyncio.CancelledError):
            raise batch
        if isinstance(batch, Exception):
            log.warning("BINANCE ANNOUNCEMENT SOURCE DEGRADED | %s | %s", name, type(batch).__name__)
            continue
        success += 1
        for event in batch:
            if event["id"] not in combined:
                combined[event["id"]] = event
    if not success:
        raise RuntimeError("All Binance announcement sources failed")
    log.info("BINANCE ANNOUNCEMENT COVERAGE | sources=%d/2 | events=%d", success, len(combined))
    return list(combined.values())


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
            "official_text": description[:12000],
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
            if not is_crypto_instrument(item) or excluded_ticker(base):
                continue
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
    e = html.escape
    separator = "━━━━━━━━━━━━━━━━━━"
    blocks = max(0, min(10, int(score / 10 + .5)))
    lines = [
        "🚨 BOGDAN INTELLIGENCE | UPCOMING LISTING",
        "",
        f"<b>{e(str(event['ticker']))} | NEW LISTING</b>",
        f"🏛 <b>{e(event['exchange'])} · {e(event.get('market_type', 'SPOT'))}</b>",
        separator,
        "🗓 <b>Старт торгов</b>",
        f"<b>{kyiv:%d.%m.%Y · %H:%M} Киев</b>",
        f"UTC: {utc:%d.%m.%Y · %H:%M}",
        separator,
        f"📊 <b>Оценка события: {score}/100</b>",
        "🟩" * blocks + "⬜" * (10 - blocks),
        "Оценка подтверждения и близости события, не вероятность прибыли.",
        f"Другие биржи: {e(other)}",
        separator,
    ]
    lines.extend(format_research(event.get("research") or {}))
    lines.extend([
        separator,
        "⚠️ <b>Перед входом</b>",
        "Проверить спред, глубину стакана, объём и реакцию цены. Не покупать вслепую на открытии.",
        "Листинг не гарантирует рост. Стоп и размер позиции определить заранее.",
        separator,
        f"Источник: {e(event['source'])}",
        e(event['url']),
        "<i>Официальное расписание | Аналитика, не торговый сигнал</i>",
    ])
    return "\n".join(lines)


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
                events = await fetcher(session)
                gathered.extend(events)
                log.info("LISTING SOURCE OK | %s | events=%d", name, len(events))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("LISTING SOURCE DEGRADED | %s | %s: %s",
                            name, type(exc).__name__, str(exc)[:240])

        now = datetime.now(timezone.utc)
        horizon = now.timestamp() + LOOKAHEAD_HOURS * 3600
        by_ticker = {}
        for event in gathered:
            if not event["ticker"] or excluded_ticker(event["ticker"]):
                continue
            if not now.timestamp() < event["when"].timestamp() <= horizon:
                continue
            by_ticker.setdefault((event["ticker"], event.get("market_type", "SPOT")), set()).add(event["exchange"])

        candidates = []
        for event in gathered:
            if not event["ticker"] or excluded_ticker(event["ticker"]) or not now.timestamp() < event["when"].timestamp() <= horizon:
                continue
            exchanges = by_ticker.get((event["ticker"], event.get("market_type", "SPOT")), {event["exchange"]})
            score = _event_score(event, len(exchanges))
            if score >= MIN_EVENT_SCORE and event["id"] not in seen:
                candidates.append((event, score, exchanges))

        candidates.sort(key=lambda row: row[0]["when"])
        for event, score, exchanges in candidates:
            from app.mail import telegram_send
            event["research"] = await research_token(session, event)
            delivered = await telegram_send(session, _format_alert(event, score, exchanges))
            if delivered:
                seen.add(event["id"])
                _save_seen(seen)
                log.info("UPCOMING LISTING SENT | %s | %s | %s | event_score=%s",
                         event["exchange"], event["ticker"], event["when"].isoformat(), score)
            else:
                log.warning("UPCOMING LISTING NOT DELIVERED | %s | %s",
                            event["exchange"], event["ticker"])
        await asyncio.sleep(POLL_SECONDS)
