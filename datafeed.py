import pandas as pd
import MetaTrader5 as mt5

_TIMEFRAME_MAP = {
    "1m": mt5.TIMEFRAME_M1,
    "5m": mt5.TIMEFRAME_M5,
    "15m": mt5.TIMEFRAME_M15,
    "1h": mt5.TIMEFRAME_H1,
    "4h": mt5.TIMEFRAME_H4,
    "1d": mt5.TIMEFRAME_D1,
}

class DataFeed:
    def __init__(self, mt5c):
        self.mt5c = mt5c

    def _resolve_symbol(self, s: str) -> str:
        if mt5.symbol_info(s) is not None:
            return s
        s2 = s.replace("/", "")
        if mt5.symbol_info(s2) is not None:
            return s2
        return s

    def fetch_close(self, symbol: str, timeframe: str, limit: int) -> pd.Series:
        tf = _TIMEFRAME_MAP.get(timeframe)
        if tf is None:
            raise ValueError(f"Unsupported timeframe: {timeframe}")

        sym = self._resolve_symbol(symbol)
        self.mt5c.ensure_symbol(sym)

        rates = self.mt5c.get_ohlcv(sym, tf, n=limit)
        if rates is None or len(rates) == 0:
            return pd.Series(dtype="float64")

        df = pd.DataFrame(rates)
        df["time"] = pd.to_datetime(df["time"], unit="s", utc=True)
        df.set_index("time", inplace=True)
        return df["close"].astype(float)

    def fetch_last(self, symbol: str) -> float:
        sym = self._resolve_symbol(symbol)
        self.mt5c.ensure_symbol(sym)
        tick = self.mt5c.get_tick(sym)
        return float((tick.bid + tick.ask) / 2.0)
