"""Public-market early intelligence. Read-only; never places orders or modifies signals."""
import asyncio
import html
import json
import logging
import math
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from zoneinfo import ZoneInfo

from app.listing_research import excluded_ticker, is_crypto_instrument

log = logging.getLogger("bogdan-intel.early")
ENABLED = os.getenv("ENABLE_EARLY_INTELLIGENCE", "true").lower() == "true"
STATE = Path(os.getenv("EARLY_INTELLIGENCE_STATE", "/data/early_intelligence_seen.json"))
HEADER = "🕵️ BOGDAN INTELLIGENCE | EARLY INTELLIGENCE"
SEP = "━━━━━━━━━━━━━━━━━━"
QUEUE = asyncio.Queue(maxsize=16)
_last_symbol = {}
_last_global = 0.0
KYIV = ZoneInfo("Europe/Kyiv")


def _ms(value):
    try:
        result = int(value)
        return result if 1577836800000 <= result <= 4102444800000 else None
    except (ValueError, TypeError, OverflowError):
        return None


def _event(exchange, symbol, ticker, status, launch, url):
    if not symbol or not ticker or excluded_ticker(ticker):
        return None
    return dict(exchange=exchange, symbol=symbol, ticker=ticker, status=status,
                launch=_ms(launch), url=url, id=exchange + ":" + symbol + ":" + status)


def binance_events(data):
    if not isinstance(data.get("symbols"), list) or not data["symbols"]:
        raise ValueError("invalid Binance exchangeInfo")
    out = []
    for row in data["symbols"]:
        if (row.get("status") != "PENDING_TRADING" or row.get("quoteAsset") != "USDT"
                or row.get("contractType") != "PERPETUAL" or row.get("underlyingType") != "COIN"):
            continue
        event = _event("Binance", row.get("symbol"), row.get("baseAsset"), row["status"],
                       row.get("onboardDate"), "https://fapi.binance.com/fapi/v1/exchangeInfo")
        if event:
            out.append(event)
    return out


def bybit_events(rows):
    out = []
    for row in rows:
        if (row.get("status") not in ("PreLaunch", "PendingOpen")
                or row.get("contractType") != "LinearPerpetual" or row.get("quoteCoin") != "USDT"
                or row.get("symbolType") != ""):
            continue
        event = _event("Bybit", row.get("symbol"), row.get("baseCoin"), row["status"],
                       row.get("launchTime"), "https://bybit-exchange.github.io/docs/v5/market/instrument")
        if event:
            # LaunchTime may describe the pre-market phase, not the ordinary listing.
            event["phase"] = (row.get("preListingInfo") or {}).get("curAuctionPhase")
            out.append(event)
    return out


def okx_events(data):
    if str(data.get("code")) != "0" or not isinstance(data.get("data"), list):
        raise ValueError("invalid OKX instruments")
    out = []
    for row in data["data"]:
        symbol = row.get("instId") or ""
        if (row.get("state") != "preopen" or not symbol.endswith("-USDT-SWAP")
                or not is_crypto_instrument(row)):
            continue
        event = _event("OKX", symbol, symbol.split("-")[0], "preopen", row.get("listTime"),
                       "https://www.okx.com/api/v5/public/instruments?instType=SWAP")
        if event:
            out.append(event)
    return out


async def _get(session, url, params=None):
    async with session.get(url, params=params, timeout=12) as response:
        response.raise_for_status()
        data = await response.json(content_type=None)
    if not isinstance(data, dict):
        raise ValueError("invalid API response")
    return data


async def _binance(session):
    return binance_events(await _get(session, "https://fapi.binance.com/fapi/v1/exchangeInfo"))


async def _bybit(session, status):
    rows, cursors, cursor = [], set(), ""
    for _ in range(5):
        params = dict(category="linear", status=status, limit=1000)
        if cursor:
            params["cursor"] = cursor
        data = await _get(session, "https://api.bybit.com/v5/market/instruments-info", params)
        result = data.get("result") or {}
        if data.get("retCode") != 0 or not isinstance(result.get("list"), list):
            raise ValueError("invalid Bybit instruments")
        rows.extend(result["list"])
        cursor = result.get("nextPageCursor") or ""
        if not cursor:
            return bybit_events(rows)
        if cursor in cursors:
            raise ValueError("repeated cursor")
        cursors.add(cursor)
    raise ValueError("Bybit pagination incomplete")


async def _okx(session):
    return okx_events(await _get(session, "https://www.okx.com/api/v5/public/instruments", {"instType": "SWAP"}))


def _load_seen():
    try:
        return set(json.loads(STATE.read_text("utf-8")))
    except FileNotFoundError:
        return set()
    except Exception:
        log.exception("EARLY INTELLIGENCE state unreadable")
        return set()


def _save_seen(seen):
    STATE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE.with_suffix(".tmp")
    tmp.write_text(json.dumps(sorted(seen)), encoding="utf-8")
    tmp.replace(STATE)


def format_prelaunch(event, now=None):
    e = html.escape
    now = now or datetime.now(timezone.utc)
    launch = event.get("launch")
    time_label = "н/д"
    if launch:
        dt = datetime.fromtimestamp(launch / 1000, tz=timezone.utc)
        time_label = dt.astimezone(KYIV).strftime("%d.%m.%Y · %H:%M Киев") + (" (уже наступило)" if dt <= now else "")
    return "\n".join([
        HEADER, "", f"<b>{e(event['symbol'])} | PRE-MARKET WATCH</b>",
        "🟡 Новый статус инструмента в публичном API", SEP,
        f"Биржа: <b>{e(event['exchange'])}</b>",
        f"Статус API: <code>{e(event['status'])}</code>",
        f"Время из API: {time_label}",
        f"Фаза: {e(str(event.get('phase') or 'н/д'))}", SEP,
        "🧠 <b>Что это означает</b>",
        "Инструмент замечен в публичном API. Это не подтверждение обычного листинга или его времени.",
        "На Bybit время может относиться к премаркету. Возможны переносы и отмены.", SEP,
        f"Источник: {e(event['url'])}",
        "<i>Режим наблюдения. Не торговый сигнал.</i>",
    ])


def anomaly(symbol, candles, btc_candles):
    """Closed 5m price/volume/flow divergence, never a forecast."""
    bars = list(candles)
    if len(bars) < 21 or symbol == "BTCUSDT":
        return None
    c = bars[-1]
    o, close, vol = c["open"], c["close"], c["volume"]
    buy = c.get("taker_buy_volume")
    if min(o, close, vol) <= 0 or buy is None or not 0 <= buy <= vol:
        return None
    avg = median(x["volume"] for x in bars[-21:-1])
    if avg <= 0:
        return None
    move = 100 * (close / o - 1)
    ratio = vol / avg
    share = buy / vol
    btc = next((x for x in reversed(btc_candles) if x["close_time"] == c["close_time"]), None)
    if not btc or btc["open"] <= 0:
        return None
    btc_move = 100 * (btc["close"] / btc["open"] - 1)
    relative = move - btc_move
    direction = 1 if move > 0 else -1
    if (ratio < 3 or abs(move) < 1.5 or vol * close < 500000
            or direction * relative < 1.2 or (share < .70 if direction == 1 else share > .30)):
        return None
    if not all(math.isfinite(x) for x in (move, ratio, share, relative)):
        return None
    return dict(symbol=symbol, move=move, ratio=ratio, share=share * 100,
                relative=relative, btc_move=btc_move, candle=c["close_time"], usd=vol * close)


def format_anomaly(row):
    e = html.escape
    icon = "🟢" if row["move"] > 0 else "🔴"
    return "\n".join([
        HEADER, "", f"<b>{e(row['symbol'])} | MARKET ANOMALY</b>",
        f"{icon} Аномалия закрытой свечи 5m", SEP,
        f"Цена: {row['move']:+.2f}%",
        f"Объём: <b>{row['ratio']:.2f}x медианы 20 свечей</b>",
        f"Приблизительный оборот: {row['usd']:,.0f} USDT",
        f"Taker BUY: {row['share']:.1f}% | SELL: {100-row['share']:.1f}%",
        f"BTC: {row['btc_move']:+.2f}% | Относительно BTC: {row['relative']:+.2f} п.п.", SEP,
        "Объём, цена и поток совпали по направлению. Причина неизвестна; это не доказательство действий китов, инсайда или будущего листинга.",
        "<i>Binance Futures | Наблюдение, не LONG/SHORT сигнал.</i>",
    ])


def observe_candles(symbol, candles, btc_candles):
    global _last_global
    now = time.monotonic()
    if QUEUE.full() or (_last_global and now - _last_global < 600) or now - _last_symbol.get(symbol, -1e9) < 1800:
        return
    row = anomaly(symbol, candles, btc_candles)
    if row:
        QUEUE.put_nowait(row)
        _last_symbol[symbol] = now
        _last_global = now


async def _market_sender(session, sender):
    while True:
        row = await QUEUE.get()
        try:
            if int(time.time() * 1000) - row["candle"] <= 300000:
                ok = await sender(session, format_anomaly(row))
                log.info("EARLY ANOMALY | %s | delivered=%s", row["symbol"], ok)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("EARLY ANOMALY delivery failed")
        finally:
            QUEUE.task_done()


async def _instrument_monitor(session, sender):
    seen = _load_seen()
    while True:
        started = time.monotonic()
        sources = ("Binance", "Bybit PreLaunch", "Bybit PendingOpen", "OKX")
        results = await asyncio.gather(_binance(session), _bybit(session, "PreLaunch"),
                                       _bybit(session, "PendingOpen"), _okx(session),
                                       return_exceptions=True)
        for name, result in zip(sources, results):
            if isinstance(result, asyncio.CancelledError):
                raise result
            if isinstance(result, Exception):
                log.warning("EARLY SOURCE DEGRADED | %s | %s", name, type(result).__name__)
                continue
            log.info("EARLY SOURCE OK | %s | candidates=%d", name, len(result))
            for event in result:
                if event["id"] in seen:
                    continue
                try:
                    if await sender(session, format_prelaunch(event)):
                        seen.add(event["id"])
                        _save_seen(seen)
                        log.info("EARLY PREMARKET SENT | %s | %s", event["exchange"], event["symbol"])
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.exception("EARLY PREMARKET delivery failed")
        await asyncio.sleep(max(5, 60 - (time.monotonic() - started)))


async def run_early_intelligence(session, sender):
    log.info("EARLY INTELLIGENCE enabled | public instrument polling=60s | strict closed 5m anomalies")
    await asyncio.gather(_instrument_monitor(session, sender), _market_sender(session, sender))
