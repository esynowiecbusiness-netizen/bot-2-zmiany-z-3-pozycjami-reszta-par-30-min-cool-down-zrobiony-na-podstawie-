from dataclasses import dataclass
from itertools import combinations
from typing import List, Tuple

from utils import log_prices, log_returns, corr, ols_beta, zscore, realized_vol

@dataclass
class PairScore:
    a: str
    b: str
    score: float
    corr_lr: float
    spread_vol: float
    beta: float

def score_pairs(feed, candidates: List[str], timeframe: str, lookback: int, beta_window: int,
                min_corr: float, max_vol_ratio: float) -> List[PairScore]:
    closes = {}
    logs = {}
    lrs = {}

    for s in candidates:
        c = feed.fetch_close(s, timeframe, lookback)
        lp = log_prices(c)
        closes[s] = c
        logs[s] = lp
        lrs[s] = log_returns(lp)

    vols = [realized_vol(lrs[s]) for s in candidates if len(lrs[s]) > 50]
    base_vol = sorted(vols)[len(vols)//2] if vols else 0.0
    if base_vol == 0.0:
        base_vol = 1e-9

    out: List[PairScore] = []
    for a, b in combinations(candidates, 2):
        la, lb = logs[a], logs[b]
        ra, rb = lrs[a], lrs[b]
        if len(la) < 100 or len(lb) < 100 or len(ra) < 80 or len(rb) < 80:
            continue

        c_lr = corr(ra.tail(400), rb.tail(400))
        if c_lr < min_corr:
            continue

        vola = realized_vol(ra.tail(400))
        volb = realized_vol(rb.tail(400))
        if max(vola, volb) / base_vol > max_vol_ratio:
            continue

        beta = ols_beta(la.tail(beta_window), lb.tail(beta_window))
        spread = (la - beta * lb).dropna()
        if len(spread) < 100:
            continue

        spread_vol = float(spread.tail(400).std(ddof=1))
        z = abs(zscore(spread.tail(400)))
        penalty_z = min(z, 3.0) / 3.0

        score = (c_lr ** 1.5) / (spread_vol + 1e-9) * (1.0 - 0.35 * penalty_z)
        out.append(PairScore(a=a, b=b, score=score, corr_lr=c_lr, spread_vol=spread_vol, beta=beta))

    out.sort(key=lambda x: x.score, reverse=True)
    return out

def pick_top_pairs(scored: List[PairScore], k: int) -> List[Tuple[str,str]]:
    picked = []
    used = set()
    for p in scored:
        if p.a in used or p.b in used:
            continue
        picked.append((p.a, p.b))
        used.add(p.a); used.add(p.b)
        if len(picked) >= k:
            break
    return picked
