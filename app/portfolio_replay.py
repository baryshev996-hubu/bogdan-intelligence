"""Replay the leverage challenge on local Binance Vision monthly 5m archives.

Run python -m app.portfolio_replay DIRECTORY YYYY-MM-DD DAYS.
Directory contains symbols.json and SYMBOL_MM.zip files (one UTC calendar month).
Run separately on earlier and held-out months. Use dates after the warm-up.
"""
import csv
import json
import sys
import zipfile
from collections import defaultdict, deque
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timezone
from io import TextIOWrapper
from pathlib import Path
from unittest.mock import patch

from app.leverage import build_leverage_paper_signal
from app.paper_trader import PaperBook
from app.signal_engine import MAX_CANDLES, evaluate

BAR = 300000


def load_symbol(args):
    root, symbol, start, end, month = args
    path = root / f"{symbol}_{month}.zip"
    if not path.exists():
        return symbol, []
    histories = {tf: deque(maxlen=MAX_CANDLES) for tf in ("5m", "15m", "1h")}
    partial = {}
    records = []
    previous = None
    with zipfile.ZipFile(path) as archive, archive.open(archive.namelist()[0]) as data:
        for row in csv.reader(TextIOWrapper(data)):
            if not row[0].isdigit():
                continue
            ts = int(row[0])
            if ts >= end:
                break
            if previous is not None and ts != previous + BAR:
                partial.clear()
                for history in histories.values():
                    history.clear()
            previous = ts
            o,h,l,c,v = map(float,row[1:6])
            bar = dict(open_time=ts, close_time=ts+BAR-1, open=o, high=h, low=l, close=c, volume=v)
            histories["5m"].append(bar)
            for tf, width in (("15m",3*BAR),("1h",12*BAR)):
                if ts % width == 0:
                    partial[tf] = dict(bar)
                elif tf in partial:
                    agg = partial[tf]
                    agg.update(high=max(h,agg["high"]), low=min(l,agg["low"]), close=c,
                               volume=agg["volume"]+v, close_time=ts+BAR-1)
                if (ts+BAR) % width == 0 and tf in partial:
                    histories[tf].append(partial.pop(tf))
            if ts < start:
                continue
            signal = None
            # Exact necessary conditions in evaluate, to skip expensive indicator work.
            if h > l and abs(c-o) >= .45*(h-l) and len(histories["5m"]) >= 21:
                signal = evaluate(symbol,histories,as_of_ms=ts+BAR-1)
            records.append((bar,signal))
    return symbol, records


class ReplayBook(PaperBook):
    def _load(self):
        return self._fresh()

    def _save_data(self, data=None):
        pass


def replay(root, date, days):
    symbols = json.loads((root/"symbols.json").read_text())
    start = int(date.timestamp()*1000)
    end = start + days*86400000
    if datetime.fromtimestamp((end-1)/1000,timezone.utc).month != date.month:
        raise ValueError("select a single archive month")
    timeline = defaultdict(list)
    coverage = 0
    with ProcessPoolExecutor(max_workers=6) as pool:
        jobs = [(root,s,start,end,date.strftime("%m")) for s in symbols]
        for symbol, rows in pool.map(load_symbol,jobs):
            coverage += bool(rows)
            for bar, signal in rows:
                timeline[bar["open_time"]].append((symbol,bar,signal))
    now = start//1000
    with patch("app.paper_trader._now",side_effect=lambda:now):
        books = {key:ReplayBook(start_balance=200,risk_fraction=.0025,max_open=5,max_gross_leverage=20,
                                fee_rate=.0005,impulse_exit_r=100,impulse_body_ratio=.99,impulse_close_position=.01)
                 for key in ("none","90/15","360/60")}
        last = defaultdict(dict)
        counts = defaultdict(int)
        for ts, rows in sorted(timeline.items()):
            now = (ts+BAR)//1000
            for name, book in books.items():
                # Simulate the same strategy throughout this observation period.
                book.state["ends_at"] = end//1000 + 1
                for sym,bar,_ in rows:
                    if sym in book.state["open_trades"]:
                        book.on_candle(sym,bar)
                for sym,_,signal in rows:
                    if not signal:
                        continue
                    prev = last[name].get(sym)
                    if prev and name!="none":
                        same,flip = map(int,name.split("/"))
                        if now-prev[0] < 60*(same if prev[1]==signal["side"] else flip):
                            continue
                    counts[name]+=1
                    last[name][sym]=(now,signal["side"])
                    setup=build_leverage_paper_signal(signal, book.state["balance"])
                    if setup:
                        book.open_trade(setup)
        for name,book in books.items():
            trades=book.state["closed_trades"]
            print(date.date(), days, coverage, name, counts[name], len(trades),
                  sum(t["net_pnl"]>0 for t in trades), book.status(),flush=True)


if __name__=="__main__":
    replay(Path(sys.argv[1]),datetime.fromisoformat(sys.argv[2]).replace(tzinfo=timezone.utc),int(sys.argv[3]))
