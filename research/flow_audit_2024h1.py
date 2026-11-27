"""Fixed rules, Jan-Jun 2024. One position per symbol per variant."""
import concurrent.futures
import hashlib
from pathlib import Path
import subprocess
import numpy as np
import pandas as pd

SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT"]
MONTHS = pd.period_range("2024-01", "2024-06", freq="M").astype(str).tolist()
CACHE = Path("/tmp/bogdan_flow_archives")
CACHE.mkdir(exist_ok=True)
ROOT = "https://data.binance.vision/data/futures/um/monthly"

def fetch(args):
    sym, month, kind = args
    folder, tag = (f"klines/{sym}/5m", "5m") if kind == "bar" else (f"fundingRate/{sym}", "fundingRate")
    name = f"{sym}-{tag}-{month}.zip"
    path = CACHE / name
    url = f"{ROOT}/{folder}/{name}"
    if not path.exists():
        subprocess.run(["curl", "-fsSL", "--retry", "2", "--max-time", "45", url, "-o", str(path)], check=True)
    checksum = subprocess.check_output(["curl", "-fsSL", "--retry", "2", "--max-time", "45", url+".CHECKSUM"]).split()[0].decode()
    assert hashlib.sha256(path.read_bytes()).hexdigest() == checksum
    return args, pd.read_csv(path)

def trades(bars, funding, sym):
    t, o, h, l, c, v, buy = (bars[col].to_numpy() for col in ["open_time","open","high","low","close","volume","taker_buy_volume"])
    assert np.all(np.diff(t) == 300000)
    hi = bars.high.shift().rolling(12).max().to_numpy()
    lo = bars.low.shift().rolling(12).min().to_numpy()
    prev = bars.close.shift()
    atr = pd.concat([bars.high-bars.low,(bars.high-prev).abs(),(bars.low-prev).abs()],axis=1).max(axis=1).rolling(14).mean().to_numpy()
    down = (l < lo) & (c > lo)
    up = (h > hi) & (c < hi)
    share = np.divide(buy,v,out=np.full_like(buy,0.5),where=v>0)
    dates = pd.to_datetime(t,unit="ms",utc=True)
    ft = funding.calc_time.to_numpy()
    fr = funding.last_funding_rate.to_numpy()
    rows = []
    for variant in ["all","against_flow","with_flow"]:
        next_free = 0
        for i in np.flatnonzero(down ^ up):
            entry = i+1
            if entry < next_free or entry+11 >= len(t) or dates[i].month != dates[entry+11].month or not np.isfinite(atr[i]):
                continue
            side = 1 if down[i] else -1
            directed = side*(share[i]-.5)
            if variant=="against_flow" and directed > -.1 or variant=="with_flow" and directed < .1:
                continue
            price = o[entry]
            risk = atr[i]
            if risk <= 0:
                continue
            for j in range(entry,entry+12):
                adverse = price-l[j] if side==1 else h[j]-price
                favorable = h[j]-price if side==1 else price-l[j]
                gross = side*(c[j]-price)
                if adverse >= risk:
                    gross = min(-risk,side*(o[j]-price))
                    break
                if favorable >= 1.5*risk:
                    gross = 1.5*risk
                    break
            # Exclude funding at the entry open. Funding while held valued at bar open.
            a,b = np.searchsorted(ft,[t[entry],t[j]],side="right")
            funding_cost = side*sum(rate*o[np.searchsorted(t,ts,side="right")-1] for ts,rate in zip(ft[a:b],fr[a:b]))
            turnover = price + price + side*gross
            net = (gross-funding_cost-.0007*turnover)/risk
            stress = (gross-funding_cost-.0012*turnover)/risk
            rows.append([sym,variant,dates[entry],net,stress])
            next_free = j+1
    return rows

if __name__ == "__main__":
    tasks = [(s,m,k) for s in SYMBOLS for m in MONTHS for k in ["bar","fund"]]
    with concurrent.futures.ThreadPoolExecutor(12) as pool:
        data = dict(pool.map(fetch,tasks))
    rows = []
    for s in SYMBOLS:
        bars, fund = [pd.concat([data[s,m,k] for m in MONTHS],ignore_index=True) for k in ["bar","fund"]]
        print("coverage",s,len(bars),len(fund),flush=True)
        rows.extend(trades(bars,fund,s))
    df = pd.DataFrame(rows,columns=["sym","variant","time","net","stress"])
    for (month,variant), g in df.groupby([df.time.dt.strftime("%Y-%m"), "variant"]):
        print(month,variant,len(g),round(100*(g.net>0).mean(),1),round(g.net.sum(),1),round(g.stress.sum(),1))
