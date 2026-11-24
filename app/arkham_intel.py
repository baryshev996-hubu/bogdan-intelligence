"""Arkham on-chain whale-flow monitor.

Polls Arkham's official Intel API for large transfers involving configured
entities/wallets. Read-only: it never places exchange orders.
"""
import asyncio
import json
import logging
import os
import time
from pathlib import Path

log = logging.getLogger("bogdan-intel.arkham")

API_KEY = os.getenv("ARKHAM_API_KEY", "").strip()
BASE_URL = "https://api.arkm.com"
WATCHLIST = [
    item.strip() for item in os.getenv(
        "ARKHAM_WATCHLIST", "binance,coinbase,okx,bybit"
    ).split(",") if item.strip()
]
MIN_USD = max(100_000.0, float(os.getenv("ARKHAM_MIN_TRANSFER_USD", "1000000")))
POLL_SECONDS = max(90, int(os.getenv("ARKHAM_POLL_SECONDS", "180")))
LOOKBACK = os.getenv("ARKHAM_LOOKBACK", "15m").strip() or "15m"
RESULT_LIMIT = max(1, min(25, int(os.getenv("ARKHAM_RESULT_LIMIT", "15"))))
STATE_PATH = os.getenv("ARKHAM_STATE_PATH", "/data/arkham_seen.json")
MAX_SEEN = 5000


def _load_seen():
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        return set(data.get("seen", []))
    except FileNotFoundError:
        return set()
    except Exception:
        log.exception("Arkham state could not be read; starting with empty dedupe cache")
        return set()


def _save_seen(seen):
    path = Path(STATE_PATH)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump({"seen": list(seen)[-MAX_SEEN:]}, f, separators=(",", ":"))
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def _first(obj, *keys, default=None):
    if not isinstance(obj, dict):
        return default
    for key in keys:
        value = obj.get(key)
        if value not in (None, ""):
            return value
    return default


def _party(value):
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        entity = value.get("arkhamEntity") or value.get("entity") or {}
        if isinstance(entity, dict):
            label = _first(entity, "name", "label", "id")
            if label:
                return str(label)
        return str(_first(value, "name", "label", "address", "id", default="unknown"))
    return "unknown"


def _transfer_usd(row):
    raw = _first(row, "historicalUSD", "usdValue", "valueUSD", "usd", "amountUsd")
    try:
        if raw is not None:
            return float(raw)
    except (TypeError, ValueError):
        pass
    token = row.get("token") if isinstance(row.get("token"), dict) else {}
    raw = _first(token, "price", "usdPrice")
    amount = _first(row, "value", "amount", "unitValue")
    try:
        if raw is not None and amount is not None:
            return float(raw) * float(amount)
    except (TypeError, ValueError):
        pass
    return 0.0


def _transfer_id(row):
    tx = _first(row, "transactionHash", "txHash", "hash", "id")
    chain = _first(row, "chain", "chainName", default="")
    log_index = _first(row, "logIndex", "transactionIndex", default="")
    return "|".join((str(chain), str(tx or ""), str(log_index)))


def _format_transfer(row):
    source = _party(row.get("from"))
    destination = _party(row.get("to"))
    token = row.get("token") if isinstance(row.get("token"), dict) else {}
    symbol = str(_first(token, "symbol", "name", default=_first(row, "symbol", default="UNKNOWN")))
    chain = str(_first(row, "chain", "chainName", default="unknown"))
    usd = _transfer_usd(row)
    tx_hash = _first(row, "transactionHash", "txHash", "hash")
    timestamp = _first(row, "blockTimestamp", "timestamp", "time")
    url = ("https://intel.arkm.com/explorer/tx/" + str(tx_hash)) if tx_hash else ""
    lines = [
        "🐳 ARKHAM WHALE INTELLIGENCE | " + symbol.upper(),
        "Объём: $" + format(usd, ",.0f"),
        "Сеть: " + chain,
        "Откуда: " + source[:90],
        "Куда: " + destination[:90],
    ]
    if timestamp:
        lines.append("Время транзакции: " + str(timestamp))
    if url:
        lines.append("Транзакция: " + url)
    lines.append("Интерпретация: крупный on-chain перевод. Сам по себе не доказывает покупку или продажу.")
    return "\n".join(lines)


async def run_arkham_monitor(session):
    if not API_KEY:
        log.warning("ARKHAM DISABLED | set ARKHAM_API_KEY to enable on-chain monitoring")
        return
    if not WATCHLIST:
        log.warning("ARKHAM DISABLED | ARKHAM_WATCHLIST is empty")
        return

    seen = _load_seen()
    log.info(
        "ARKHAM START | watchlist=%s | min_usd=$%.0f | poll=%ss | lookback=%s",
        WATCHLIST, MIN_USD, POLL_SECONDS, LOOKBACK
    )
    headers = {"API-Key": API_KEY, "Accept": "application/json"}

    while True:
        try:
            params = {
                "base": ",".join(WATCHLIST),
                "flow": "all",
                "timeLast": LOOKBACK,
                "usdGte": str(int(MIN_USD)),
                "sortKey": "time",
                "sortDir": "desc",
                "limit": str(RESULT_LIMIT),
            }
            async with session.get(
                BASE_URL + "/transfers",
                params=params,
                headers=headers,
                timeout=25,
            ) as response:
                if response.status == 401 or response.status == 403:
                    body = (await response.text())[:500]
                    log.error("ARKHAM AUTH/ACCESS ERROR | HTTP %s | %s", response.status, body)
                    await asyncio.sleep(900)
                    continue
                if response.status == 429:
                    log.warning("ARKHAM RATE LIMITED | backing off 5 minutes")
                    await asyncio.sleep(300)
                    continue
                response.raise_for_status()
                payload = await response.json(content_type=None)

            if isinstance(payload, list):
                rows = payload
            elif isinstance(payload, dict):
                rows = payload.get("transfers") or payload.get("data") or payload.get("results") or []
            else:
                rows = []

            if not isinstance(rows, list):
                log.warning("ARKHAM unexpected response shape: %s", type(rows).__name__)
                rows = []

            fresh = []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                tx_id = _transfer_id(row)
                usd = _transfer_usd(row)
                if not tx_id.strip("|") or tx_id in seen or usd < MIN_USD:
                    continue
                fresh.append((tx_id, row, usd))

            # API returns newest first. Deliver in chronological order.
            for tx_id, row, usd in reversed(fresh):
                from app.mail import telegram_send
                delivered = await telegram_send(session, _format_transfer(row))
                if delivered:
                    seen.add(tx_id)
                    _save_seen(seen)
                    log.info("ARKHAM ALERT SENT | tx=%s | usd=%.0f", tx_id[:100], usd)
                else:
                    log.warning("ARKHAM ALERT NOT SENT | tx=%s", tx_id[:100])

            if len(seen) > MAX_SEEN:
                seen = set(list(seen)[-MAX_SEEN:])
                _save_seen(seen)

        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Arkham polling failed; retrying after 60s")
            await asyncio.sleep(60)
            continue

        await asyncio.sleep(POLL_SECONDS)
