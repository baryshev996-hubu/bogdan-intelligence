"""Optional public token research; no score is invented for missing data."""
import asyncio
import html
import logging
import math
import os
import re
import time
from urllib.parse import quote

log = logging.getLogger("bogdan-intel.listings.research")
STOCK_TICKERS = frozenset("TENCENT MEITUAN KUAISHOU JD AAPL TSLA NVDA AMZN MSFT GOOGL GOOG META SPX NDX SPCX SPACEX XAU XAG".split())
CACHE = {}


def excluded_ticker(ticker):
    return str(ticker or "").upper() in STOCK_TICKERS


def is_crypto_instrument(item):
    return str(item.get("instCategory") or "") == "1"


def _number(value):
    if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value) and value >= 0:
        return value
    return None


def _normalized(value):
    return " ".join(re.findall(r"[a-z0-9]+", str(value or "").lower()))


def choose_coin(event, rows):
    """Require a unique match by name and ticker, not ticker alone."""
    ticker = str(event.get("ticker") or "").upper()
    title = " " + _normalized(event.get("title")) + " "
    matches = [row for row in rows if str(row.get("symbol") or "").upper() == ticker and row.get("id")
               and _normalized(row.get("name")) and _normalized(row.get("name")) != ticker.lower()
               and (" " + _normalized(row["name"]) + " ") in title]
    return matches[0] if len(matches) == 1 else None


async def _get(session, url, params=None, headers=None):
    async with session.get(url, params=params, headers=headers, timeout=8) as response:
        response.raise_for_status()
        return await response.json(content_type=None)


def _market_summary(row):
    cap, fdv, volume = (_number(row.get(k)) for k in ("market_cap", "fully_diluted_valuation", "total_volume"))
    circulating, supply, maximum = (_number(row.get(k)) for k in ("circulating_supply", "total_supply", "max_supply"))
    denominator = maximum if maximum and maximum >= (circulating or 0) else supply
    float_pct = 100 * circulating / denominator if circulating is not None and denominator and circulating <= denominator else None
    turnover = volume / cap if volume is not None and cap else None
    dilution = fdv / cap if fdv is not None and cap else None
    flags = []
    if float_pct is not None and float_pct < 20:
        flags.append("Низкая доля токенов в обращении (<20%).")
    if dilution is not None and dilution >= 5:
        flags.append("FDV в 5+ раз выше капитализации: риск размывания.")
    if volume is not None and volume < 1000000:
        flags.append("Суточный объём менее $1 млн; глубина стакана неизвестна.")
    known = sum(value is not None for value in (cap, fdv, volume, float_pct))
    return dict(cap=cap, fdv=fdv, volume=volume, float_pct=float_pct,
                turnover=turnover, dilution=dilution, flags=flags, coverage=known)


async def _investor_research(session, name):
    key = os.getenv("DEFILLAMA_API_KEY", "").strip()
    if not key:
        return []
    data = await _get(session, "https://pro-api.llama.fi/" + key + "/api/raises")
    investors = set()
    for row in data.get("raises", []):
        if _normalized(row.get("name")) != _normalized(name):
            continue
        for field in ("leadInvestors", "otherInvestors"):
            values = row.get(field) or []
            if isinstance(values, list):
                investors.update(str(x)[:100] for x in values if isinstance(x, str) and x)
    return sorted(investors)[:8]


def official_funding_excerpt(event):
    """Keep an explicit source excerpt, not an inferred investor relationship."""
    source = str(event.get("official_text") or "")
    if not source:
        return None
    for sentence in re.split(r"(?<=[.!?])\s+|\n+", source):
        sentence = " ".join(sentence.split())
        if 15 <= len(sentence) <= 400 and re.search(
                r"\b(?:backed by|investors include|investors are|funding round led by|investment led by)\b",
                sentence, re.I):
            # Reject generic promotional language and ambiguous disclaimers.
            if not re.search(r"\b(?:not|may|could|rumor|rumour|unconfirmed)\b", sentence, re.I):
                return sentence
    return None


async def research_token(session, event):
    cache_key = (event.get("ticker"), event.get("title"))
    previous = CACHE.get(cache_key)
    if previous and time.monotonic() - previous[0] < 3600:
        return previous[1]
    result = {"status": "н/д: нет подтверждённых фундаментальных данных"}
    excerpt = official_funding_excerpt(event)
    if excerpt:
        result["official_funding_excerpt"] = excerpt
        result["official_funding_url"] = event.get("url", "")
    headers = {"Accept": "application/json"}
    key = os.getenv("COINGECKO_DEMO_API_KEY", "").strip()
    if key:
        headers["x-cg-demo-api-key"] = key
    root = "https://api.coingecko.com/api/v3"
    try:
        data = await _get(session, root + "/search", {"query": event["ticker"]}, headers)
        coin = choose_coin(event, data.get("coins", []))
        if coin is None:
            result["status"] = "н/д: проект не удалось однозначно сопоставить по названию и тикеру"
        else:
            result.update(name=str(coin.get("name")), coin_id=coin["id"])
            data = await _get(session, root + "/coins/markets",
                              {"vs_currency": "usd", "ids": coin["id"]}, headers)
            row = next((r for r in data if r.get("id") == coin["id"]), None)
            if row:
                result.update(_market_summary(row), status="CoinGecko: сопоставление по названию и тикеру")
            try:
                result["investors"] = await _investor_research(session, result["name"])
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("LISTING INVESTOR DATA UNAVAILABLE | %s", type(exc).__name__)
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        log.warning("LISTING FUNDAMENTALS UNAVAILABLE | %s | %s", event.get("ticker"), type(exc).__name__)
    if len(CACHE) >= 500:
        CACHE.clear()
    CACHE[cache_key] = (time.monotonic(), result)
    return result


def format_research(result):
    def usd(value):
        return "н/д" if value is None else "$" + format(value, ",.0f")

    def pct(value):
        return "н/д" if value is None else f"{value:.1f}%"

    def ratio(value):
        return "н/д" if value is None else f"{value:.2f}x"

    e = html.escape
    lines = [
        "🔎 <b>Анализ проекта</b>",
        e(result.get("status", "н/д")),
        f"Капитализация: {usd(result.get('cap'))}",
        f"FDV: {usd(result.get('fdv'))}",
        f"Объём торгов 24ч: {usd(result.get('volume'))}",
        f"Доля токенов в обращении: {pct(result.get('float_pct'))}",
        f"FDV / капитализация: {ratio(result.get('dilution'))}",
        f"Объём / капитализация: {ratio(result.get('turnover'))}",
        f"Полнота рыночных данных: {result.get('coverage', 0)}/4",
        "Глубина стакана на новой бирже: пока неизвестна.",
    ]
    investors = result.get("investors") or []
    lines.append("Инвесторы: " + (e(", ".join(investors)) + " (DefiLlama, совпадение по названию; требуется сверка)" if investors else "н/д; не подтверждены"))
    if result.get("official_funding_excerpt"):
        lines.append("Финансирование (цитата из объявления, проверить контекст): " + e(result["official_funding_excerpt"]))
        lines.append("Источник: " + e(result.get("official_funding_url", "")))
    elif not investors:
        lines.append("База раундов: https://defillama.com/raises (автоматический доступ требует API-плана).")
    if result.get("coin_id"):
        lines.append("CoinGecko: https://www.coingecko.com/en/coins/" + quote(str(result["coin_id"]), safe=""))
    if investors:
        lines.append("DefiLlama: https://defillama.com/raises")
    if result.get("flags"):
        lines.extend("⚠️ " + e(flag) for flag in result["flags"])
    else:
        lines.append("⚠️ Отсутствие отмеченных рисков не означает их отсутствие.")
    return lines
