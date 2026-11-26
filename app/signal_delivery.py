"""Durable per-recipient delivery of current 5m signals."""
import asyncio
import logging
import os
import sqlite3
import time
from contextlib import closing

from app.signal_engine import format_signal

log = logging.getLogger("bogdan-intel.delivery")
PATH = os.getenv("SIGNAL_QUEUE_PATH", "/data/signal_delivery.sqlite")


def connect():
    db = sqlite3.connect(PATH)
    db.execute("""CREATE TABLE IF NOT EXISTS deliveries (
        symbol TEXT, candle INTEGER, recipient TEXT, message TEXT,
        next_try INTEGER DEFAULT 0, done INTEGER DEFAULT 0,
        PRIMARY KEY(symbol, candle, recipient))""")
    return db


def queue_signal(signal, recipients):
    candle = int(signal["candle_id"])
    message = format_signal(signal)
    with closing(connect()) as db, db:
        db.execute("DELETE FROM deliveries WHERE candle < ?", (candle - 86400000,))
        db.executemany("INSERT OR IGNORE INTO deliveries(symbol,candle,recipient,message) VALUES(?,?,?,?)",
                       [(signal["symbol"], candle, recipient, message) for recipient in recipients])


async def deliver_signals(session, sender):
    with closing(connect()) as db:
        while True:
            now = int(time.time())
            with db:
                # Expire after 90s from the 5m close and never deliver pre-v2 queued alerts.
                expired = db.execute(
                    "UPDATE deliveries SET done=2 WHERE done=0 AND "
                    "(candle+390000<=? OR instr(message, ?) = 0)",
                    (now * 1000, "Оценка качества условий, не вероятность прибыли")
                ).rowcount
            if expired:
                log.warning("SIGNAL DELIVERY EXPIRED | recipients=%d", expired)
            row = db.execute("""SELECT rowid,symbol,recipient,message FROM deliveries
                                WHERE done=0 AND next_try<=? ORDER BY next_try,rowid LIMIT 1""",
                             (now,)).fetchone()
            if not row:
                await asyncio.sleep(0.5)
                continue
            key, symbol, recipient, message = row
            ok = await sender(session, message, recipient_only=recipient)
            with db:
                db.execute("UPDATE deliveries SET done=?, next_try=? WHERE rowid=?",
                           (int(ok), int(time.time()) + 10, key))
            if ok:
                log.info("SIGNAL DELIVERED | %s | recipient_suffix=%s", symbol, recipient[-4:])
