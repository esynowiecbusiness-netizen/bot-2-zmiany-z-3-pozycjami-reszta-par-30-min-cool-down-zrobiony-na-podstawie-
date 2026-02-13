import numpy as np
import pandas as pd

def log_prices(close: pd.Series) -> pd.Series:
    s = close.astype(float).replace(0, np.nan).dropna()
    return np.log(s)

def log_returns(logp: pd.Series) -> pd.Series:
    return logp.diff().dropna()

def corr(a: pd.Series, b: pd.Series) -> float:
    if len(a) < 30 or len(b) < 30:
        return 0.0
    return float(a.corr(b))

def ols_beta(y: pd.Series, x: pd.Series) -> float:
    if len(y) < 50 or len(x) < 50:
        return 1.0
    xv = x.values
    yv = y.values
    varx = np.var(xv)
    if varx == 0:
        return 1.0
    cov = np.cov(yv, xv, ddof=1)[0, 1]
    return float(cov / varx)

def zscore(series: pd.Series) -> float:
    if len(series) < 50:
        return 0.0
    mu = series.mean()
    sd = series.std(ddof=1)
    if sd == 0 or np.isnan(sd):
        return 0.0
    return float((series.iloc[-1] - mu) / sd)

def realized_vol(lr: pd.Series) -> float:
    if len(lr) < 50:
        return 0.0
    return float(lr.std(ddof=1))

def now_utc_day_key(ts: float) -> str:
    return pd.to_datetime(ts, unit="s", utc=True).strftime("%Y-%m-%d")
