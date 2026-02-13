from dataclasses import dataclass
from typing import List

@dataclass
class Config:
    candidates: List[str] | None = None

    timeframe: str = "1m"
    lookback: int = 720
    beta_window: int = 720

    z_entry: float = 2.3
    z_exit: float = 0.5
    max_hold_minutes: int = 240

    top_pairs: int = 5
    max_open_positions: int = 2

    min_corr: float = 0.55
    max_vol_ratio: float = 2.0

    poll_seconds: int = 20

def default_config() -> Config:
    c = Config()
    c.candidates = ["BTC/USD", "ETH/USD", "BNB/USD", "SOL/USD", "XRP/USD", "ADA/USD", "XDG/USD", "LINK/USD", "TRX/USD", "DOT/USD", "MATIC/USD", "LTC/USD"
]
    return c

